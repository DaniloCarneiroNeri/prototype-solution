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
def normalize_address(raw, bairro):
    """
    Normaliza endereços ao padrão:
    'RUA/AVENIDA, QUADRA-LOTE'
    Com tratamento seguro (sem exceções).
    """

    try:
        if pd.isna(raw) or str(raw).strip() == "":
            return ""

        text = str(raw).strip()
        bairro = "" if pd.isna(bairro) else str(bairro).strip()

        # -------------------------------
        # 1. Regra especial CONDOMÍNIO
        # Se Destination Address OU Bairro contiver
        # cond, condominio, condomínio, etc.
        # -------------------------------
        raw_combined = f"{text} {bairro}".upper()

        ignore_cond = [
            "CONDOMINIO DAS ESMERALDAS",
            "CONDOMÍNIO DAS ESMERALDAS"
        ]

        if any(bad in raw_combined for bad in ignore_cond):
            is_condominio = False

        elif any(word in raw_combined for word in [
            "COND",
            "CONDOMINIO",
            "CONDOMÍNIO",
            "JARDINS LISBOA"
        ]):
            is_condominio = True

        else:
            is_condominio = False

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
        # 1.1 Regras adicionais de normalização de ruas
        # Exemplos:
        # Rua AC3  -> Rua AC-003
        # Rua W 11 -> Rua W-011
        # R W 4    -> RUA W-004
        # R RI 3   -> RUA RI-003
        # -------------------------------------------------------------------------
        
        # Converte "R " para "RUA " para uniformizar
        text = re.sub(r"\bR\s+", "RUA ", text, flags=re.IGNORECASE)
        text_upper = text.upper()

        # Captura "RUA AC3", "RUA AC 3", "RUA RI 15", etc.
        rua_codigo = re.search(
            r"\bRUA\s+([A-Z]{1,3})\s*[- ]?\s*(\d{1,3})\b",
            text_upper
        )

        if rua_codigo:
            codigo = rua_codigo.group(1).upper()             # AC / W / RI
            numero = rua_codigo.group(2).zfill(3)            # zero-pad → 3 dígitos

            novo_padrao = f"RUA {codigo}-{numero}"

            # substitui somente a parte capturada
            text = re.sub(
                r"\bRUA\s+[A-Z]{1,3}\s*[- ]?\s*\d{1,3}\b",
                novo_padrao,
                text,
                flags=re.IGNORECASE
            )

            text_upper = text.upper()

        # -------------------------------------------------------------------------
        # 1.2 Regras adicionais: BL e RC
        # -------------------------------------------------------------------------

        text_upper = text.upper()

        # ============================================================
        # TRATAMENTO PARA "BL"
        # Exemplos:
        # Rua BL11  -> Rua BL-011
        # R BL9     -> Rua BL-009
        # Rua BL7   -> Rua BL-007
        # ============================================================

        rua_bl = re.search(
            r"\bRUA\s+BL\s*[- ]?\s*(\d{1,3})\b",
            text_upper
        )

        # Também captura variação "R BL9"
        rua_bl_alt = re.search(
            r"\bR\s+BL\s*[- ]?\s*(\d{1,3})\b",
            text_upper
        )

        if rua_bl or rua_bl_alt:
            numero = (rua_bl.group(1) if rua_bl else rua_bl_alt.group(1)).zfill(3)

            novo_padrao = f"RUA BL-{numero}"

            text = re.sub(
                r"\b(?:RUA|R)\s+BL\s*[- ]?\s*\d{1,3}\b",
                novo_padrao,
                text,
                flags=re.IGNORECASE
            )

            text_upper = text.upper()

        # ============================================================
        # TRATAMENTO PARA "RC"
        # Formato final: RUA RC-<numero>
        # Exemplos:
        # Rua RC51  -> Rua RC-51
        # RUA RC27 -> Rua RC-27
        # RUA RC 5 -> Rua RC-5
        # ============================================================

        rua_rc = re.search(
            r"\b(?:RUA|R)\s+RC\s*[- ]?\s*(\d{1,3})\b",
            text_upper
        )

        if rua_rc:
            numero = rua_rc.group(1).lstrip("0") or "0"   # mantém o número original
            novo_padrao = f"RUA RC-{numero}"

            text = re.sub(
                r"\b(?:RUA|R)\s+RC\s*[- ]?\s*\d{1,3}\b",
                novo_padrao,
                text,
                flags=re.IGNORECASE
            )

            text_upper = text.upper()

        # -------------------------------------------------------------------------
        # 2. NOVAS REGRAS para QUADRA e LOTE (super tolerantes)
        #
        # QUADRA aceita:
        #   Q, QD, QDR, QDRA, QUADRA...
        #   Q B7      → quadra = 7
        #   QD B 03   → quadra = 3
        #   Quadra B11 → quadra = 11
        #   Qd B2 → quadra = 2
        #
        # LOTE aceita:
        #   L, LT, LTE, LOTE...
        #   LT 14 → lote = 14
        #   Lote 9 → lote = 9
        #   LT B02 → lote = 2 (letra ignorada)
        # -------------------------------------------------------------------------

        quadra = None
        lote   = None

        # QUADRA
        q_match = re.search(
            r"\bQ(?:U?A?D?R?A?)?[:\.\s]*([A-Z]?\s*\d{1,3}\s*[A-Z]?)\b",
            text_upper
        )

        # LOTE
        l_match = re.search(
            r"\bL(?:O?T?E?)?[:\.\s]*([A-Z]?\s*\d{1,3}\s*[A-Z]?)\b",
            text_upper
        )

        # -------------------------
        # EXTRAÇÃO DA QUADRA
        # -------------------------
        if q_match:
            raw = q_match.group(1) or ""
            # remove tudo que não é número (letras antes/depois são descartadas)
            digits = re.sub(r"[^0-9]", "", raw)
            if digits:
                quadra = digits.lstrip("0") or "0"

        # -------------------------
        # EXTRAÇÃO DO LOTE
        # -------------------------
        if l_match:
            raw = l_match.group(1) or ""
            digits = re.sub(r"[^0-9]", "", raw)
            if digits:
                lote = digits.lstrip("0") or "0"

        # -------------------------------------------------------------------------
        # 3. Fallback para padrão "15-20"
        # -------------------------------------------------------------------------
        if not (quadra and lote):
            fb = re.search(r"\b([0-9]+)\s*-\s*([0-9]+)\b", text_upper)
            if fb:
                quadra = quadra or (fb.group(1).lstrip("0") or "0")
                lote   = lote   or (fb.group(2).lstrip("0") or "0")

        # -------------------------------------------------------------------------
        # 4. Definição da rua (com proteção anti None)
        # -------------------------------------------------------------------------
        cut_index = len(text)

        # Marcadores que indicam o fim do nome da rua
        separators = [",", " - ", " Nº", " NUMERO", " CASA", " APT", " APTO"]

        for sep in separators:
            idx = text_upper.find(sep)
            if idx != -1 and idx < cut_index:
                cut_index = idx

        # Posições onde começam as informações de quadra/lote
        regex_positions = [
            q_match.start() if q_match else None,
            l_match.start() if l_match else None
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

        # ============================================================
        # 7. REGRA FINAL — Se é condomínio, força o nome da rua
        # ============================================================
        if is_condominio:
            if quadra and lote:
                return f"Condominio, {quadra}-{lote}"
            elif quadra:
                return f"Condominio, {quadra}"
            else:
                return "Condominio"

        # ============================================================
        # 8. Retorno normal (rua padrão)
        # ============================================================
        if quadra and lote:
            return f"{street}, {quadra}-{lote}"

        return ""

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
        df = df.reset_index(drop=True)
    except Exception as e:
        raise HTTPException(400, f"Erro ao ler Excel: {e}")

    if "Destination Address" not in df.columns or "Zipcode/Postal code" not in df.columns:
        raise HTTPException(422, "Colunas obrigatórias ausentes")

    df["Normalized_Address"] = df.apply(
    lambda r: normalize_address(r["Destination Address"], r["Bairro"]),
    axis=1)

    final_lat = []
    final_lng = []
    partial_flags = [] 
    cond_flags = []

    for idx, row in df.iterrows():
        normalized = row.get("Normalized_Address", "")
        cep_original = str(row["Zipcode/Postal code"]).strip()
        bairro_raw = row.get("Bairro", "")
        bairro = "" if pd.isna(bairro_raw) else str(bairro_raw).strip()
        bairro_upper = bairro.upper()

        # -------------------------------
        # Validação condominio
        # -------------------------------
        cond_keywords = ["Condominio"]

        if any(k in normalized for k in cond_keywords):
            cond_flags.append(True)
            final_lat.append("")
            final_lng.append("")
            partial_flags.append(False)
            continue

        cond_flags.append(False)

        # -------------------------------
        # Primeira tentativa
        # -------------------------------
        match_quad_lote = re.search(r",\s*([0-9]+)-([0-9]+)$", normalized)
        match_quad_quadra = re.search(r"\bQUADRA\b|\bQ([0-9]+)", normalized, re.IGNORECASE)

        if not match_quad_lote and not match_quad_quadra:
            final_lat.append("Não encontrado")
            final_lng.append("Não encontrado")
            partial_flags.append(False)
            continue
        
        lat, lng, cep_here = await geocode_with_here(normalized)

        match_ok = cep_here and cep_here.replace("-", "") == cep_original.replace("-", "")

        if match_ok:
            final_lat.append(lat)
            final_lng.append(lng)
            partial_flags.append(False)
            continue

        # -------------------------------
        # tentativa (com Bairro)
        # -------------------------------
        second_query = f"{normalized}, {bairro}"
        lat2, lng2, cep_here2 = await geocode_with_here(second_query)

        match_ok2 = cep_here2 and cep_here2.replace("-", "") == cep_original.replace("-", "")

        if match_ok2:
            final_lat.append(lat2)
            final_lng.append(lng2)
            partial_flags.append(False)
            continue

        # ---------------------------------------------------------
        # 2.5 TENTATIVA — Rua iniciando com número (ex.: "Rua 9 de Julho")
        # ---------------------------------------------------------

        # Captura rua antes da vírgula
        rua_raw = normalized.split(",", 1)[0].strip()

        # Regex para capturar número no início
        m_num = re.match(r"^(.*?\b)(\d+)(.*)$", rua_raw)
        if m_num:
            prefixo = m_num.group(1) or ""   # "Rua "
            numero  = m_num.group(2) or ""   # "9"
            sufixo  = m_num.group(3) or ""   # " de Julho"

            # Função simples para números por extenso
            numeros_extenso = {
                "1": "um", "2": "dois", "3": "três", "4": "quatro", "5": "cinco",
                "6": "seis", "7": "sete", "8": "oito", "9": "nove",
                "10": "dez", "11": "onze", "12": "doze", "13": "treze",
                "14": "quatorze", "15": "quinze", "16": "dezesseis",
                "17": "dezessete", "18": "dezoito", "19": "dezenove",
                "20": "vinte", "21": "vinte e um", "22": "vinte e dois",
                "23": "vinte e três", "24": "vinte e quatro", "25": "vinte e cinco",
                "30": "trinta", "40": "quarenta", "50": "cinquenta",
                "60": "sessenta", "70": "setenta", "80": "oitenta", "90": "noventa"
            }

            def numero_por_extenso(n):
                if n in numeros_extenso:
                    return numeros_extenso[n]

                # 21–99 (composto)
                if len(n) == 2:
                    dezenas = n[0] + "0"       # "9" → "90"
                    unidade = n[1]             # "7"
                    if dezenas in numeros_extenso and unidade in numeros_extenso:
                        return f"{numeros_extenso[dezenas]} e {numeros_extenso[unidade]}"
                return n  # fallback

            numero_extenso = numero_por_extenso(numero).capitalize()

            # Montamos rua por extenso
            rua_convertida = f"{prefixo}{numero_extenso}{sufixo}".strip()

            # Montamos nova query preservando quadra e lote
            tentativa_extenso = rua_convertida
            if "," in normalized:
                tentativa_extenso += ", " + normalized.split(",",1)[1].strip()

            # Faz a tentativa
            lat3, lng3, cep_here3 = await geocode_with_here(tentativa_extenso)

            match_ok3 = cep_here3 and cep_here3.replace("-", "") == cep_original.replace("-", "")

            if match_ok3:
                final_lat.append(lat3)
                final_lng.append(lng3)
                partial_flags.append(False)
                continue

        # -------------------------------
        # Trativa com cidade (com Bairro) (sem cep)
        # -------------------------------
        cidade = "Goiania, Goiânia - GO"
        second_query = f"{normalized}, {bairro}, {cidade}"
        lat2, lng2, cep_here2 = await geocode_with_here(second_query)

        if lat2:
            final_lat.append(lat2)
            final_lng.append(lng2)
            partial_flags.append(True)
            continue
        
        # -----------------------------------------------
        # tentativa (substituir bairro → Novo Horizonte)
        # Somente nos bairros autorizados
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

        # ----------------------------------------------
        # Tentativa parcial — lote ± 1 / ± 2
        # ----------------------------------------------
        match_quad_lote = re.search(r",\s*([0-9]+)-([0-9]+)$", normalized)

        if match_quad_lote:
            quadra_num = int(match_quad_lote.group(1))
            lote_num = int(match_quad_lote.group(2))

            base = normalized.rsplit(",", 1)[0]

            offsets = [1, 2, -1, -2]

            found_partial = False
            for off in offsets:
                novo_lote = lote_num + off
                if novo_lote < 0:
                    continue

                tentativa = f"{base}, {quadra_num}-{novo_lote}"
                latp, lngp, cep_part = await geocode_with_here(tentativa)

                match_okp = cep_part and cep_part.replace("-", "") == cep_original.replace("-", "")

                if match_okp:
                    final_lat.append(latp)
                    final_lng.append(lngp)
                    partial_flags.append(True)   # <<< PARCIAL
                    found_partial = True
                    break

            if found_partial:
                continue

        # -------------------------------
        # Falhou → "Não encontrado"
        # -------------------------------
        final_lat.append("Não encontrado")
        final_lng.append("Não encontrado")
        partial_flags.append(False)

    df["Geo_Latitude"] = final_lat
    df["Geo_Longitude"] = final_lng
    df["Partial_Match"] = pd.Series(partial_flags, index=df.index, dtype=bool)
    df["Cond_Match"] = pd.Series(cond_flags, index=df.index, dtype=bool)

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
        clean["Cond_Match"] = r.get("Cond_Match", False)
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