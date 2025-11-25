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

BAIRROS_RETRY = {
    "SETOR FAIÇALVILLE",
    "FAIÇALVILLE II",
    "JARDIM VILA BOA"
}

# ============================================================
# NORMALIZAÇÃO DO ENDEREÇO
# ============================================================
def normalize_address(raw, bairro=""):
    """
    Normaliza endereço. Se quadra/lote não encontrados e bairro ∈ BAIRROS_RETRY:
    troca para 'Novo Horizonte' e tenta novamente.
    """

    def _normalize(text):
        # -------------------------
        # Bloco interno reutilizável
        # -------------------------
        text = str(text).strip()
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('/', ' ')
        text_upper = text.upper()

        # Rua Fxx → Rua F-xx
        text = re.sub(
            r"\b(RUA\s+F)(\d+)\b",
            r"\1-\2",
            text,
            flags=re.IGNORECASE
        )
        text_upper = text.upper()

        quadra = lote = None

        # Regex tolerantes
        q_full = re.search(r"\bQ(?:U?A?D?R?A?)?\.?\s*([0-9]+[A-Z]?)\b", text_upper)
        q_comp = re.search(r"\bQ([0-9]+)[^\d]?L([0-9]+)\b", text_upper)
        l_full = re.search(r"\bL(?:O?T?E?)?\.?\s*([0-9]+[A-Z]?)\b", text_upper)

        if q_full:
            quadra = q_full.group(1)
        if q_comp:
            quadra = quadra or q_comp.group(1)
            lote = lote or q_comp.group(2)
        if l_full:
            lote = lote or l_full.group(1)

        # fallback 15-20
        if not (quadra and lote):
            fb = re.search(r"\b([0-9]+)\s*-\s*([0-9]+)\b", text)
            if fb:
                quadra = quadra or fb.group(1)
                lote = lote or fb.group(2)

        # cortar rua
        cut_index = len(text)
        separators = [",", " - ", " Nº", " NUMERO", " CASA", " APT", " APTO"]
        for sep in separators:
            idx = text_upper.find(sep)
            if idx != -1 and idx < cut_index:
                cut_index = idx

        # posições seguras
        for pos in [q_full.start() if q_full else None,
                    l_full.start() if l_full else None,
                    q_comp.start() if q_comp else None]:
            if pos is not None and pos < cut_index:
                cut_index = pos

        street = text[:cut_index].strip().rstrip(" ,-./")

        # validar "s/n"
        invalid = {"0", "00", "SN", "S/N", "NULL"}
        if quadra and quadra.upper() in invalid:
            quadra = None
        if lote and lote.upper() in invalid:
            lote = None

        if quadra and lote:
            return f"{street}, {quadra}-{lote}"

        # sem quadra/lote
        return street

    # ---------------------------------------------------------
    # PRIMEIRA TENTATIVA
    # ---------------------------------------------------------
    result = _normalize(raw)

    # Detectar se rua sem quadra/lote → "não encontrado"
    nao_encontrou = "-" not in result and "QUADRA" not in result.upper()

    # ---------------------------------------------------------
    # REGRAS DE RETENTATIVA POR BAIRRO
    # ---------------------------------------------------------
    if nao_encontrou:
        bairro_upper = str(bairro).strip().upper()

        if bairro_upper in BAIRROS_RETRY:
            # modificar bairro → Novo Horizonte
            novo_endereco = f"{raw}, Novo Horizonte"

            # TENTAR NOVAMENTE
            result_retry = _normalize(novo_endereco)

            return result_retry

    return result


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
    lambda r: normalize_address(r["Destination Address"], r.get("Bairro", "")),
    axis=1)

    final_lat = []
    final_lng = []

    for idx, row in df.iterrows():
        normalized = row["Normalized_Address"]
        cep_original = str(row["Zipcode/Postal code"]).strip()
        bairro = row.get("Bairro", "")

        # -------------------------------
        # 1️⃣ Primeira tentativa
        # -------------------------------
        lat, lng, cep_here = await geocode_with_here(normalized)

        match_ok = cep_here and cep_here.replace("-", "") == cep_original.replace("-", "")

        if match_ok:
            final_lat.append(lat)
            final_lng.append(lng)
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
            continue

        # -------------------------------
        # 3️⃣ Falhou → "Não encontrado"
        # -------------------------------
        final_lat.append("Não encontrado")
        final_lng.append("Não encontrado")

    df["Geo_Latitude"] = final_lat
    df["Geo_Longitude"] = final_lng

    # Sanitização de saída
    records = []
    for r in df.to_dict(orient="records"):
        clean = {}
        for k, v in r.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                clean[k] = None
            else:
                clean[k] = v
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