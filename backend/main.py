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
    Mantém a lógica original; melhora robustez dos regex para capturar
    RC / QUADRA / LOTE em formatos grudados, com pontuação e variações.
    """
    try:
        if pd.isna(raw) or str(raw).strip() == "":
            return ""

        text = str(raw).strip()
        bairro = "" if pd.isna(bairro) else str(bairro).strip()

        # -------------------------------
        # 1. Regra especial CONDOMÍNIO (mantida)
        # -------------------------------
        raw_combined = f"{text} {bairro}".upper()

        ignore_cond = [
            "CONDOMINIO DAS ESMERALDAS",
            "CONDOMÍNIO DAS ESMERALDAS",
            "CASA",
            "SALA"
        ]

        if any(bad in raw_combined for bad in ignore_cond):
            is_condominio = False
        elif any(word in raw_combined for word in [
            "COND",
            "COND.",
            "CONDOMINIO",
            "CONDOMÍNIO",
            "JARDINS LISBOA",
            "BLOCO",
            "APT",
            "APTO",
            "AP",
            "APT.",
            "APTO.",
            "AP.",
            "PRÉDIO",
            "PREDIO",
            "RESIDENCIAL MIAMI",
            "EDIFÍCIO",
            "EDIFICIO"
        ]):
            is_condominio = True
        else:
            is_condominio = False

        # limpeza básica
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('/', ' ')
        text_upper = text.upper()

        # -------------------------------------------------------------------------
        # 1. Regra especial: Rua Fxx -> Rua F-xx  (mantida)
        # -------------------------------------------------------------------------
        if re.search(r"\bRUA\s+F(\d+)\b", text_upper):
            text = re.sub(r"\b(RUA\s+F)(\d+)\b", r"\1-\2", text, flags=re.IGNORECASE)
            text_upper = text.upper()

        # -------------------------------------------------------------------------
        # 1.1 Regras adicionais de normalização de ruas
        # -------------------------------------------------------------------------

        # Converte "R " para "RUA " para uniformizar (mantido)
        text = re.sub(r"(^|\s)R\s+(?=[A-Z])", r"\1RUA ", text, flags=re.IGNORECASE)
        text_upper = text.upper()

        # Captura "RUA AC3", "RUA AC 3", "RUA RI 15", etc. (mantido, mas robusto)
        rua_codigo = re.search(
            r"\bRUA\s+([A-Z]{1,3})\s*[- ]?\s*(\d{1,3})\b",
            text_upper
        )

        if rua_codigo:
            codigo = rua_codigo.group(1).upper()
            numero = rua_codigo.group(2).zfill(3)  # zero-pad → 3 dígitos
            novo_padrao = f"RUA {codigo}-{numero}"

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

        # BL (mantido)
        rua_bl = re.search(r"\bRUA\s+BL\s*[- ]?\s*(\d{1,3})\b", text_upper)
        rua_bl_alt = re.search(r"\bR\s+BL\s*[- ]?\s*(\d{1,3})\b", text_upper)

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

        # RC — tornar captura robusta: captura mesmo se seguido por letra/pontuação
        rc_search = re.search(r"(\b(?:RUA|R)\s+RC\s*[- ]?\s*)(\d{1,3})", text_upper, flags=0)
        if rc_search:
            rc_num = rc_search.group(2).lstrip("0") or "0"
            def _rc_sub(m):
                prefix = m.group(1)
                return f"{prefix}{rc_num}"

            text = re.sub(r"(\b(?:RUA|R)\s+RC\s*[- ]?\s*)(\d{1,3})", lambda m: _rc_sub(m), text, flags=re.IGNORECASE)

            text = re.sub(
                r"(\b(?:RUA|R)\s+RC\s*[- ]?\s*)(\d{1,3})",
                lambda m: f"RUA RC-{rc_num}",
                text,
                flags=re.IGNORECASE
            )
            text_upper = text.upper()

        # ------------------------------- (mantido)
        # Regra nova: converter "Rua <numero>" para extenso (mantido)
        # -------------------------------
        m_rua_num = re.match(r"^RUA\s+(\d+)\b", text_upper)
        if m_rua_num:
            num_rua = int(m_rua_num.group(1))
            def extenso(n):
                unidades = ["","um","dois","três","quatro","cinco","seis","sete","oito","nove"]
                especiais = {"10":"dez","11":"onze","12":"doze","13":"treze","14":"quatorze","15":"quinze",
                            "16":"dezesseis","17":"dezessete","18":"dezoito","19":"dezenove"}
                dezenas = ["","","vinte","trinta","quarenta","cinquenta","sessenta","setenta","oitenta","noventa"]
                n = str(n)
                v = int(n)
                if v < 10:
                    return unidades[v]
                if n in especiais:
                    return especiais[n]
                if v % 10 == 0:
                    return dezenas[v//10]
                d = v//10
                u = v%10
                return f"{dezenas[d]} e {unidades[u]}"

            texto_extenso = extenso(num_rua).capitalize()
            text = re.sub(r"^RUA\s+\d+", f"Rua {texto_extenso}", text, flags=re.IGNORECASE)
            text_upper = text.upper()

        # -------------------------------------
        # 2. NOVAS REGRAS para QUADRA e LOTE (robustas)
        # -------------------------------------
        quadra = None
        lote   = None

        # QUADRA: aceitar Q, QD, Qd., QDR, QUADRA, Q23, Qd7, Quadra 8lote 8 (pontos e sem espaço)
        q_match = re.search(
            r"""
            \b
            Q(?:U?A?D?R?A?)? 
            \.?
            \s*[:,.\- ]?\s*
            ([A-Z]?\d{1,3}[A-Z]?) 
            (?=\s*(?:L|LT|LOTE|L\.|\d|$))
            """,
            text_upper,
            flags=re.VERBOSE
        )
        
        # LOTE:
        l_match = re.search(
            r"""
            \b
            L(?:T|TE|OTE)? 
            \.?
            \s*[:,.\- ]?\s*
            ([A-Z]?\d{1,3}[A-Z]?)
            """,
            text_upper,
            flags=re.VERBOSE
        )

        # -------------------------
        # EXTRAÇÃO DA QUADRA
        # -------------------------
        if q_match:
            raw = q_match.group(1) or ""
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
        # 3. Fallback para padrão "15-20" (mantido)
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

        # Posições onde começam as informações de quadra/lote (se detectados)
        regex_positions = [
            (q_match.start() if q_match else None),
            (l_match.start() if l_match else None)
        ]

        for pos in regex_positions:
            if pos is not None and pos < cut_index:
                cut_index = pos

        street = text[:cut_index].strip().rstrip(" ,-./")

        # -------------------------------------------------------------------------
        # 5. Sanitização (mantida)
        # -------------------------------------------------------------------------
        invalid = {"0", "00", "SN", "S/N", "NULL"}

        if quadra and str(quadra).upper() in invalid:
            quadra = None

        if lote and str(lote).upper() in invalid:
            lote = None

        # ============================================================
        # 7. REGRA FINAL — Se é condomínio, força o nome da rua
        # ============================================================
        if is_condominio:
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