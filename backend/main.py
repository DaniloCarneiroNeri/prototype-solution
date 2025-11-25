import os
import uvicorn
import re
import json
import math
from fastapi import FastAPI, UploadFile, File, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import aiohttp
from io import BytesIO
from typing import Optional

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

HERE_API_KEY = os.getenv("HERE_API_KEY")


# ============================================================
# NORMALIZAÇÃO DO ENDEREÇO
# ============================================================
def normalize_address(raw):
    """
    Normaliza endereços ao padrão:
    'RUA/AVENIDA, QUADRA-LOTE'
    Com tratamento seguro (sem exceções).
    """

    try:
        if pd.isna(raw) or str(raw).strip() == "":
            return ""

        text = str(raw).strip()
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('/', ' ')
        text_upper = text.upper()

        # -------------------------------------------------------------------------
        # 1. Regra especial: Rua Fxx -> Rua F-xx
        # -------------------------------------------------------------------------
        if re.search(r"\bRUA\s+F(\d+)\b", text_upper):
            text = re.sub(r"\b(RUA\s+F)(\d+)\b", r"\1-\2", text, flags=re.IGNORECASE)
            text_upper = text.upper()

        # -------------------------------------------------------------------------
        # 2. Regex tolerantes p/ QUADRA e LOTE
        # Inclui versões com ":"  → Quadra:07 | Lote:07
        # -------------------------------------------------------------------------

        # QUADRA — inclui: q, qd, qdra, quadra, quad, quad:, q:, qd:, quadra:
        q_full = re.search(
            r"\bQ(?:U?A?D?R?A?)?[:\.\s]*([0-9]+[A-Z]?)\b",
            text_upper
        )

        # Q28L1
        q_comp = re.search(r"\bQ([0-9]+)[^\d]?L([0-9]+)\b", text_upper)

        # LOTE — inclui: l, lt, lote, lote:, l:
        l_full = re.search(
            r"\bL(?:O?T?E?)?[:\.\s]*([0-9]+[A-Z]?)\b",
            text_upper
        )

        quadra = None
        lote = None

        if q_full:
            quadra = q_full.group(1).lstrip("0") or "0"

        if q_comp:
            quadra = quadra or (q_comp.group(1).lstrip("0") or "0")
            lote = lote or (q_comp.group(2).lstrip("0") or "0")

        if l_full:
            lote = lote or (l_full.group(1).lstrip("0") or "0")

        # -------------------------------------------------------------------------
        # 3. Fallback "15-20"
        # -------------------------------------------------------------------------
        if not (quadra and lote):
            fb = re.search(r"\b([0-9]+)\s*-\s*([0-9]+)\b", text)
            if fb:
                quadra = quadra or fb.group(1).lstrip("0")
                lote = lote or fb.group(2).lstrip("0")

        # -------------------------------------------------------------------------
        # 4. Definição da rua (com proteção anti None)
        # -------------------------------------------------------------------------
        cut_index = len(text)

        separators = [",", " - ", " Nº", " NUMERO", " CASA", " APT", " APTO"]

        for sep in separators:
            idx = text_upper.find(sep)
            if idx != -1 and idx < cut_index:
                cut_index = idx

        regex_positions = [
            q_full.start() if q_full else None,
            l_full.start() if l_full else None,
            q_comp.start() if q_comp else None,
        ]

        for pos in regex_positions:
            if pos is not None and pos < cut_index:
                cut_index = pos

        street = text[:cut_index].strip().rstrip(" ,-./")

        # -------------------------------------------------------------------------
        # 5. Sanitização
        # -------------------------------------------------------------------------
        invalid = {"0", "00", "SN", "S/N", "NULL"}
        if quadra and quadra.upper() in invalid:
            quadra = None
        if lote and lote.upper() in invalid:
            lote = None

        # -------------------------------------------------------------------------
        # 6. Resultado final SEM EXCEÇÕES
        # -------------------------------------------------------------------------
        if quadra and lote:
            return f"{street}, {quadra}-{lote}"

        return street

    except Exception as e:
        return f"[ERRO-NORMALIZE] {str(e)}"


# ============================================================
# GEOCODING HERE
# ============================================================
async def geocode_with_here(address: str):
    if not HERE_API_KEY:
        print("HERE_API_KEY NOT FOUND → returning dummy coords")
        return -16.70, -49.20, None

    url = f"https://geocode.search.hereapi.com/v1/geocode?q={address}&apiKey={HERE_API_KEY}"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    print("HERE error:", await response.text())
                    return None, None, None

                data = await response.json()

                if "items" in data and len(data["items"]) > 0:
                    item = data["items"][0]
                    pos = item.get("position", {})

                    lat = pos.get("lat")
                    lng = pos.get("lng")

                    postal = None
                    address_info = item.get("address", {})
                    if "postalCode" in address_info:
                        postal = address_info["postalCode"]

                    return lat, lng, postal

        except Exception as e:
            print("HERE Exception:", e)

    return None, None, None


# ============================================================
# ENDPOINT /upload
# ============================================================
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        raw = await file.read()
        df = pd.read_excel(BytesIO(raw))
    except Exception as e:
        raise HTTPException(400, f"Erro ao ler Excel: {e}")

    if "Destination Address" not in df.columns or "Zipcode/Postal code" not in df.columns:
        raise HTTPException(422, "Colunas obrigatórias ausentes")

    df["Normalized_Address"] = df.apply(
    lambda r: normalize_address(r["Destination Address"]),
    axis=1)

    final_lat = []
    final_lng = []
    partial_flags = [] 

    for idx, row in df.iterrows():
        normalized = row["Normalized_Address"]
        cep_original = str(row["Zipcode/Postal code"]).strip()
        bairro_raw = row.get("Bairro", "")
        bairro = "" if pd.isna(bairro_raw) else str(bairro_raw).strip()
        bairro_upper = bairro.upper()

        # -------------------------------
        # 1️⃣ Primeira tentativa
        # -------------------------------
        lat, lng, cep_here = await geocode_with_here(normalized)

        match_ok = cep_here and cep_here.replace("-", "") == cep_original.replace("-", "")

        if match_ok:
            final_lat.append(lat)
            final_lng.append(lng)
            partial_flags.append(False)
            continue

        # -------------------------------
        # 2️⃣ Segunda tentativa (com Bairro)
        # -------------------------------
        second_query = f"{normalized}, {bairro}"
        lat2, lng2, cep_here2 = await geocode_with_here(second_query)

        match_ok2 = cep_here2 and cep_here2.replace("-", "") == cep_original.replace("-", "")

        if match_ok2:
            final_lat.append(lat2)
            final_lng.append(lng2)
            partial_flags.append(False)
            continue
        # -----------------------------------------------
        # 3️⃣ Terceira tentativa (substituir bairro → Novo Horizonte)
        #    Somente nos bairros autorizados
        # -----------------------------------------------
        BAIRROS_RETRY = {
            "SETOR FAIÇALVILLE",
            "FAIÇALVILLE II",
            "JARDIM VILA BOA"
        }    

        if bairro_upper in BAIRROS_RETRY:
            third_query = f"{normalized}, Novo Horizonte"
            lat3, lng3, cep_here3 = await geocode_with_here(third_query)

            match_ok3 = cep_here3 and cep_here3.replace("-", "") == cep_original.replace("-", "")

            if match_ok3:
                final_lat.append(lat3)
                final_lng.append(lng3)
                partial_flags.append(False)
                continue

        # ----------------------------------------------------------------
        # 4️⃣ QUARTA TENTATIVA: tentar lote+1 e lote-1 (endereço parcial)
        # ----------------------------------------------------------------
        # Aplique apenas se normalized contém quadra-lote
        match_quad_lote = re.search(r",\s*([0-9]+)-([0-9]+)$", normalized)

        if match_quad_lote:
            quadra_num = int(match_quad_lote.group(1))
            lote_num = int(match_quad_lote.group(2))

            # tenta primeiro lote +1
            tentativa1 = f"{normalized.rsplit(',', 1)[0]}, {quadra_num}-{lote_num + 1}"
            lat4, lng4, cep_here4 = await geocode_with_here(tentativa1)

            match_ok4 = cep_here4 and cep_here4.replace("-", "") == cep_original.replace("-", "")

            if match_ok4:
                final_lat.append(lat4)
                final_lng.append(lng4)
                partial_flags.append(True)   # <--- MARCA COMO PARCIAL
                continue

            # tenta primeiro lote +1 de novo
            tentativa1 = f"{normalized.rsplit(',', 1)[0]}, {quadra_num}-{lote_num + 1}"
            lat4, lng4, cep_here4 = await geocode_with_here(tentativa1)

            match_ok4 = cep_here4 and cep_here4.replace("-", "") == cep_original.replace("-", "")

            if match_ok4:
                final_lat.append(lat4)
                final_lng.append(lng4)
                partial_flags.append(True)   # <--- MARCA COMO PARCIAL
                continue
        
            # tenta lote -1
            tentativa2 = f"{normalized.rsplit(',', 1)[0]}, {quadra_num}-{max(lote_num - 1, 0)}"
            lat5, lng5, cep_here5 = await geocode_with_here(tentativa2)

            match_ok5 = cep_here5 and cep_here5.replace("-", "") == cep_original.replace("-", "")

            if match_ok5:
                final_lat.append(lat5)
                final_lng.append(lng5)
                partial_flags.append(True)  # <--- MARCA COMO PARCIAL
                continue

            # tenta lote -1 denovo
            tentativa2 = f"{normalized.rsplit(',', 1)[0]}, {quadra_num}-{max(lote_num - 1, 0)}"
            lat5, lng5, cep_here5 = await geocode_with_here(tentativa2)

            match_ok5 = cep_here5 and cep_here5.replace("-", "") == cep_original.replace("-", "")

            if match_ok5:
                final_lat.append(lat5)
                final_lng.append(lng5)
                partial_flags.append(True)  # <--- MARCA COMO PARCIAL
                continue

        # -------------------------------
        # 3️⃣ Falhou → "Não encontrado"
        # -------------------------------
        final_lat.append("Não encontrado")
        final_lng.append("Não encontrado")
        partial_flags.append(False)

    df["Geo_Latitude"] = final_lat
    df["Geo_Longitude"] = final_lng
    df["Partial_Match"] = partial_flags

    # Sanitização de saída
    records = []
    for r in df.to_dict(orient="records"):
        clean = {}
        for k, v in r.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                clean[k] = None
            else:
                clean[k] = v
        clean["Partial_Match"] = r.get("Partial_Match", False)
        records.append(clean)

    return {
        "filename": file.filename,
        "rows": len(records),
        "columns_count": len(df.columns),
        "data": records
    }

@app.get("/")
def root():
    return {"status": "ok", "message": "Backend online"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)