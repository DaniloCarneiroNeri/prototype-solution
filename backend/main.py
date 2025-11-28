import os
import uvicorn
import re
import json
import math
import asyncio
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import aiohttp
from io import BytesIO
from typing import Optional, Dict, Any

# ============================================================
# CONFIGURAÇÕES
# ============================================================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

HERE_API_KEY = os.getenv("HERE_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") 

# ============================================================
# INTEGRAÇÃO IA (GOOGLE GEMINI - GRATUITO/ROBUSTO)
# ============================================================
async def parse_address_with_ai(raw_text: str) -> Dict[str, str]:
    """
    Usa Google Gemini para estruturar endereços.
    Requer GOOGLE_API_KEY.
    """
    if not GOOGLE_API_KEY:
        return None

    try:
        import google.generativeai as genai
        
        genai.configure(api_key=GOOGLE_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash') # Modelo rápido e econômico

        prompt = (
            f"Analise o endereço abaixo (contexto Goiânia) e extraia em JSON: "
            f"{{'rua': string, 'quadra': string, 'lote': string, 'bairro': string}}. "
            f"Se não tiver quadra/lote, tente achar o número. Padronize 'RUA T 63' para 'Rua T-63'. "
            f"Texto: '{raw_text}'"
        )

        # O Gemini não tem modo JSON nativo estrito como OpenAI, então pedimos texto e limpamos
        response = await asyncio.to_thread(model.generate_content, prompt)
        text_resp = response.text
        
        # Limpeza básica para garantir JSON
        json_str = text_resp.replace("```json", "").replace("```", "").strip()
        data = json.loads(json_str)
        return data

    except Exception as e:
        print(f"Erro IA (Gemini): {e}")
        return None

# ============================================================
# UTILITÁRIOS
# ============================================================
def extract_street_base(addr: str) -> str:
    """Remove Quadra, Lote e pontuação para comparar nomes de rua."""
    if not addr: return ""
    up = str(addr).upper()
    cut = len(up)
    for token in [" Q", " QUADRA", " QD", " LT", " LOTE", ",", " - "]:
        pos = up.find(token)
        if pos != -1:
            cut = min(cut, pos)
    return addr[:cut].strip()

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
            "EDIFICIO",
            "ATIBAIA"
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


        # ============================================================
        # REGRA ESPECIAL — RUA MDV-x  (SEM ZERO-PAD)
        # ============================================================
        rua_mdv = re.search(
            r"\bRUA\s+MDV\s*[- ]?\s*(\d{1,3})\b",
            text_upper
        )

        if rua_mdv:
            numero = rua_mdv.group(1).lstrip("0") or "0"
            novo_padrao = f"RUA MDV-{numero}"

            text = re.sub(
                r"\bRUA\s+MDV\s*[- ]?\s*\d{1,3}\b",
                novo_padrao,
                text,
                flags=re.IGNORECASE
            )
            text_upper = text.upper()


        # ============================================================
        # Captura “RUA AC3”, “RUA AC 3”, “RUA RI 15”, etc.
        # *** EXCLUINDO MDV ***
        # ============================================================
        rua_codigo = re.search(
            r"\bRUA\s+(?!MDV)([A-Z]{1,3})\s*[- ]?\s*(\d{1,3})\b",
            text_upper
        )

        if rua_codigo:
            codigo = rua_codigo.group(1).upper()
            numero = rua_codigo.group(2).zfill(3)  # zero-pad → 3 dígitos
            novo_padrao = f"RUA {codigo}-{numero}"

            text = re.sub(
                r"\bRUA\s+(?!MDV)[A-Z]{1,3}\s*[- ]?\s*\d{1,3}\b",
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

        # RC — tornar captura robusta e aplicar zero-pad de 3 dígitos
        rc = re.search(r"\b(?:RUA|R)\s+RC\s*[- ]?\s*(\d{1,3})\b", text_upper)
        if rc:
            numero = rc.group(1).lstrip("0") or "0"
            numero = numero.zfill(3)  # aplicar zero-pad para RC também
            novo_padrao = f"RUA RC-{numero}"

            text = re.sub(
                r"\b(?:RUA|R)\s+RC\s*[- ]?\s*\d{1,3}\b",
                novo_padrao,
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
            Q
            (?:        
                U?A?D?R?A?       
                |UA             
                |S               
                |UANDRA          
            )?
            \.?
            \s*[:,.\- ]?\s*
            ([A-Z]?\d{1,3}[A-Z]?)
            """,
            text_upper,
            flags=re.VERBOSE
        )

        # LOTE: aceitar L, LT, LOTE, Lt01, lote8, L01, com/sem ponto
        l_match = re.search(
            r"""
            \b
            L
            (?:T|TE|OTE)?   # LT, LTE, LOTE
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
# GEOCODING HERE (COM DETECÇÃO DE PRECISÃO)
# ============================================================
async def geocode_with_here(query: str, city_context: str = "Goiânia", state_context: str = "GO"):
    """
    Retorna lat, lng, postal, street, status E AGORA TAMBÉM O resultType.
    resultType diz se ele achou o número exato ('houseNumber') ou só a rua ('street').
    """
    if not HERE_API_KEY:
        return None, None, None, None, "API_KEY_MISSING", None

    final_query = query
    # Força contexto geográfico se não presente
    if "GOIANIA" not in query.upper().replace("â", "A"):
        final_query = f"{query}, {city_context}, {state_context}, Brasil"

    encoded_query = final_query.replace(" ", "%20")
    # Pedimos resultType na resposta
    url = f"https://geocode.search.hereapi.com/v1/geocode?q={encoded_query}&apiKey={HERE_API_KEY}&limit=1"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    return None, None, None, None, f"HTTP_{response.status}", None

                data = await response.json()
                items = data.get("items", [])

                if not items:
                    return None, None, None, None, "NOT_FOUND", None

                item = items[0]
                pos = item.get("position", {})
                addr = item.get("address", {})
                
                # Importante: Qual foi o nível de precisão?
                # 'houseNumber' = Achou o número/lote exato
                # 'street' = Não achou o número, devolveu o centro da rua
                result_type = item.get("resultType", "unknown")

                lat = pos.get("lat")
                lng = pos.get("lng")
                postal = addr.get("postalCode")
                street_found = addr.get("street")
                
                return lat, lng, postal, street_found, "OK", result_type

        except Exception as e:
            return None, None, None, None, str(e), None

# ============================================================
# LÓGICA PRINCIPAL (REFATORADA PARA VIZINHOS)
# ============================================================
async def find_best_location(normalized_addr: str, original_cep: str, bairro: str, original_raw: str):
    
    # --- 1. Tentar parse via IA (Gemini) se disponível ---
    ai_parsed = None
    if GOOGLE_API_KEY:
        ai_parsed = await parse_address_with_ai(original_raw)
        if ai_parsed and ai_parsed.get('rua'):
            q = ai_parsed.get('quadra', '')
            l = ai_parsed.get('lote', '')
            ql_str = f", Quadra {q}, Lote {l}" if q and l else ""
            if ql_str:
                normalized_addr = f"{ai_parsed['rua']}{ql_str}"

    # Limpeza CEP
    cep_clean = str(original_cep).replace("-", "").replace(".", "").strip()
    
    # Lista de estratégias "Exatas"
    strategies = []
    
    # A: Normalizada
    strategies.append({"query": normalized_addr, "type": "EXACT_NORMALIZED"})
    
    # B: Com Bairro
    if bairro:
        strategies.append({"query": f"{normalized_addr}, {bairro}", "type": "WITH_BAIRRO"})

    # --- EXECUÇÃO TENTATIVA EXATA ---
    street_fallback = None # Guarda o resultado da rua caso não ache o lote vizinho

    for strat in strategies:
        lat, lng, found_cep, found_street, status, r_type = await geocode_with_here(strat["query"])
        
        if status != "OK": continue

        # Validação de Nome da Rua
        street_base_in = extract_street_base(normalized_addr).upper()
        street_base_out = extract_street_base(found_street).upper()
        name_match = (street_base_in in street_base_out) or (street_base_out in street_base_in)
        
        if not name_match:
            continue # Rua errada, ignora

        # Validação CRÍTICA: Se for resultType == 'street', significa que a HERE 
        # ignorou o Quadra/Lote e devolveu o centro da rua.
        # Nós NÃO queremos aceitar isso imediatamente, pois pode estar longe.
        # Guardamos como fallback, mas vamos tentar os vizinhos.
        
        if r_type == "houseNumber" or r_type == "pointAddress":
            return lat, lng, False, strat["type"]
        else:
            if street_fallback is None:
                street_fallback = (lat, lng, True, f"{strat['type']}_APPROX")

    # --- ESTRATÉGIA: VIZINHOS (Lotes +/- 1 e 2) ---
    # Só chegamos aqui se a busca exata falhou ou retornou apenas "street" (centro da rua)
    
    # Regex para extrair Rua, Quadra e Lote
    # Procura padrões como "Rua X, Qd 10 Lt 20" ou "Rua X, Quadra 10 Lote 20"
    match_ql = re.search(r"(.*?)[\s,]+(?:QD|QUADRA|Q)\.?\s*(\d+)[\s,]+(?:LT|LOTE|L)\.?\s*(\d+)", normalized_addr, re.IGNORECASE)
    
    if match_ql:
        base_rua = match_ql.group(1).strip()
        q_num = match_ql.group(2)
        l_num_str = match_ql.group(3)
        
        try:
            l_num = int(l_num_str)
            
            # Tenta vizinhos: -1, +1, -2, +2
            offsets = [-1, 1, -2, 2]
            
            for offset in offsets:
                new_lote = l_num + offset
                if new_lote <= 0: continue
                
                # Monta query explícita: "Rua X, Quadra Y, Lote Z"
                # Forçamos o formato extenso para ajudar a API
                neighbor_query = f"{base_rua}, Quadra {q_num}, Lote {new_lote}, Goiânia, GO"
                
                lat, lng, found_cep, found_street, status, r_type = await geocode_with_here(neighbor_query)
                
                if status == "OK" and (r_type == "houseNumber" or r_type == "pointAddress"):
                    # Se achou um vizinho com precisão "houseNumber", é muito melhor que o centro da rua
                    return lat, lng, True, f"NEIGHBOR_LOTE_{offset}"
                    
        except ValueError:
            pass # Lote não era numérico

    # --- FALLBACK ---
    # Se não achou exato (houseNumber) e não achou vizinhos (houseNumber),
    # devolvemos o centro da rua (street) que achamos na primeira etapa, se houver.
    if street_fallback:
        return street_fallback[0], street_fallback[1], street_fallback[2], street_fallback[3]

    return "Não encontrado", "Não encontrado", False, "FAILED"


# ============================================================
# ENDPOINT
# ============================================================
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        raw = await file.read()
        df = pd.read_excel(BytesIO(raw))
    except Exception as e:
        raise HTTPException(400, f"Erro ao ler Excel: {e}")

    req_cols = ["Destination Address", "Zipcode/Postal code"]
    if not all(col in df.columns for col in req_cols):
        raise HTTPException(422, "Colunas obrigatórias ausentes")

    if "Bairro" not in df.columns: df["Bairro"] = ""

    results_lat = []
    results_lng = []
    results_partial = []
    results_method = []

    for idx, row in df.iterrows():
        raw_addr = row["Destination Address"]
        cep = row["Zipcode/Postal code"]
        bairro = row["Bairro"]
        
        normalized = normalize_address(raw_addr, bairro)
        
        # Skip Condominios se necessario (mantido sua logica)
        if "CONDOMINIO" in str(normalized).upper() and "RUA" not in str(normalized).upper():
             results_lat.append("")
             results_lng.append("")
             results_partial.append(False)
             results_method.append("COND_SKIP")
             continue

        lat, lng, is_partial, method = await find_best_location(normalized, cep, bairro, raw_addr)
        
        results_lat.append(lat)
        results_lng.append(lng)
        results_partial.append(is_partial)
        results_method.append(method)

    df["Geo_Latitude"] = results_lat
    df["Geo_Longitude"] = results_lng
    df["Partial_Match"] = results_partial
    df["Match_Method"] = results_method

    records = json.loads(df.to_json(orient="records"))

    return {
        "filename": file.filename,
        "rows": len(records),
        "data": records
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)