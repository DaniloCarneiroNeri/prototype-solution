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
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# ============================================================
# INTEGRAÇÃO IA (GEMINI - CORREÇÃO DE MODELOS)
# ============================================================
async def parse_address_with_ai(raw_text: str) -> Dict[str, str]:
    """
    Tenta estruturar o endereço.
    Atualizado para tentar modelos estáveis se os experimentais falharem.
    """
    if not GOOGLE_API_KEY:
        return None

    try:
        import google.generativeai as genai
        genai.configure(api_key=GOOGLE_API_KEY)
        
        # Lista atualizada de modelos (do mais novo para o mais antigo/estável)
        models_to_try = [
            'gemini-1.5-flash', 
            'gemini-1.5-flash-latest', 
            'gemini-1.5-pro',
            'gemini-1.0-pro', 
            'gemini-pro'
        ]
        
        prompt = (
            f"Extraia o endereço do texto para JSON (chaves: rua, quadra, lote, bairro). "
            f"Ignore números como '0' ou '00' se houver quadra/lote. "
            f"Exemplo: 'Rua RC 18 Q23 Lt01' -> rua='Rua RC 18', quadra='23', lote='1'. "
            f"Texto: '{raw_text}'"
        )

        for model_name in models_to_try:
            try:
                model = genai.GenerativeModel(model_name)
                # Executa em thread separada
                response = await asyncio.to_thread(model.generate_content, prompt)
                
                text_resp = response.text
                # Limpeza robusta do JSON
                json_str = text_resp.replace("```json", "").replace("```", "").strip()
                if not json_str.startswith("{"):
                    # Tenta achar o primeiro {
                    idx = json_str.find("{")
                    if idx != -1: json_str = json_str[idx:]
                    
                data = json.loads(json_str)
                return data
            except Exception as e:
                # Silencia erro e tenta o próximo modelo
                continue
                
        return None

    except Exception as e:
        print(f"Erro Crítico IA: {e}")
        return None

# ============================================================
# UTILITÁRIOS E REGEX (MANTENDO A SUA LÓGICA BASE)
# ============================================================
def extract_street_base(addr: str) -> str:
    """Remove Quadra, Lote e pontuação para comparar nomes de rua."""
    if not addr: return ""
    up = str(addr).upper()
    # Corta antes de indicativos de quadra/lote ou vírgulas
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
# GEOCODING HERE (OTIMIZADO)
# ============================================================
async def geocode_with_here(query: str, city_context: str = "Goiânia", state_context: str = "GO"):
    """
    Consulta a API da HERE.
    Adiciona contexto forçado de cidade/estado na query string para evitar ambiguidade.
    """
    if not HERE_API_KEY:
        return None, None, None, None, "API_KEY_MISSING"

    # Monta uma query qualificada para melhorar a precisão
    # Se a query já não tiver "Goiânia", adicionamos.
    final_query = query
    if "GOIANIA" not in query.upper().replace("â", "A"):
        final_query = f"{query}, {city_context}, {state_context}, Brasil"

    encoded_query = final_query.replace(" ", "%20")
    url = f"https://geocode.search.hereapi.com/v1/geocode?q={encoded_query}&apiKey={HERE_API_KEY}&limit=1"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    return None, None, None, None, f"HTTP_{response.status}"

                data = await response.json()
                items = data.get("items", [])

                if not items:
                    return None, None, None, None, "NOT_FOUND"

                item = items[0]
                pos = item.get("position", {})
                addr = item.get("address", {})

                lat = pos.get("lat")
                lng = pos.get("lng")
                postal = addr.get("postalCode")
                street_found = addr.get("street")
                
                # Verifica score de relevância se disponível (opcional)
                scoring = item.get("scoring", {})
                
                return lat, lng, postal, street_found, "OK"

        except Exception as e:
            return None, None, None, None, str(e)

# ============================================================
# HELPER: Extração de Quadra/Lote
# ============================================================
def extract_quadra_lote_values(text):
    """
    Tenta extrair apenas os NÚMEROS de (quadra, lote) para comparação.
    Remove zeros à esquerda para comparar '05' com '5'.
    """
    if not text: return None, None
    
    # Normaliza para facilitar o regex
    text_clean = text.upper().replace(".", " ").replace(",", " ")
    
    # Padrões comuns: "QD 05", "QUADRA 5", "Q-05", "Q05"
    # Grupo 1: Quadra, Grupo 2: Lote
    
    # Tenta achar Quadra
    q_val = None
    # Regex: Procura Q, QD, QUADRA seguido opcionalmente de letras e depois digitos
    match_q = re.search(r"\b(?:Q|QD|QUADRA|QDA)[^0-9]*(\d+)\b", text_clean)
    if match_q:
        q_val = str(int(match_q.group(1))) # Remove zeros à esquerda (05 -> 5)

    # Tenta achar Lote
    l_val = None
    match_l = re.search(r"\b(?:L|LT|LOTE)[^0-9]*(\d+)\b", text_clean)
    if match_l:
        l_val = str(int(match_l.group(1)))

    return q_val, l_val

# ============================================================
# ROTINA PRINCIPAL DE BUSCA
# ============================================================
async def find_best_location(normalized_addr: str, original_cep: str, bairro: str, original_raw: str):
    
    # 1. Tentar parse via IA (Mantido o log para debug, mas o código agora sobrevive sem ela)
    ai_parsed = None
    if GOOGLE_API_KEY:
        try:
            # print(f"--- DEBUG IA: Tentando parse... ---") 
            ai_parsed = await parse_address_with_ai(original_raw)
        except Exception as e:
            print(f"--- ERRO CRÍTICO IA: {e} ---")

    # Extrai Quadra e Lote da ENTRADA (Input) para validação rigorosa
    # Tenta pegar da string crua, que costuma ter "Qd 05"
    input_q, input_l = extract_quadra_lote_values(original_raw)
    if not input_q:
        input_q, input_l = extract_quadra_lote_values(normalized_addr)

    strategies = []
    
    cep_clean = str(original_cep).replace("-", "").replace(".", "").strip()
    
    # --- ESTRATÉGIAS DE BUSCA ---
    
    # A: Busca Exata Normalizada
    strategies.append({"query": normalized_addr, "type": "EXACT_NORMALIZED"})

    # B: Busca com Bairro
    if bairro:
        strategies.append({"query": f"{normalized_addr}, {bairro}", "type": "WITH_BAIRRO"})

    # C: Variações de Zero (Rua 1 vs Rua 01)
    m = re.search(r"(RUA\s+[A-Z]+)-(\d+)", normalized_addr.upper())
    if m:
        prefix = m.group(1)
        num = m.group(2)
        variants = [str(int(num)), num.zfill(2), num.zfill(3)]
        for v in variants:
            if v == num: continue
            new_addr = normalized_addr.replace(f"{prefix}-{num}", f"{prefix}-{v}")
            strategies.append({"query": new_addr, "type": "ZERO_VARIANT"})

    # --- EXECUÇÃO ---
    for strat in strategies:
        lat, lng, found_cep, found_street, status = await geocode_with_here(strat["query"])
        
        if status != "OK": continue

        # === NOVA VALIDAÇÃO RIGOROSA DE QUADRA ===
        # Se temos uma quadra no input, a API PRECISA retornar a mesma quadra (se retornar alguma).
        found_q, _ = extract_quadra_lote_values(found_street)
        
        if input_q and found_q:
            if input_q != found_q:
                print(f"--- REJEITADO ({strat['type']}): Input Quadra {input_q} != Found Quadra {found_q} ---")
                continue # Pula este resultado, está errado!
        # =========================================

        # Validação de CEP e Nome da Rua
        cep_match = False
        if found_cep and cep_clean:
            if found_cep.replace("-", "") == cep_clean:
                cep_match = True
        
        street_base_in = extract_street_base(normalized_addr).upper()
        street_base_out = extract_street_base(found_street).upper()
        name_match = street_base_in in street_base_out or street_base_out in street_base_in

        # Se passou na validação de Quadra acima, verificamos CEP ou Nome
        if cep_match or (name_match and len(street_base_in) > 3):
            return lat, lng, False, strat["type"]

    # --- ESTRATÉGIA D: VIZINHOS (Lotes +/- 1 e 2) ---
    # Só executamos se achamos Quadra e Lote na entrada (Input) ou via IA
    
    search_q = input_q
    search_l = input_l

    # Se regex falhou, tenta backup da IA
    if (not search_q or not search_l) and ai_parsed:
        search_q = ai_parsed.get('quadra')
        search_l = ai_parsed.get('lote')

    if search_q and search_l:
        try:
            l_num = int(search_l)
            base_rua = extract_street_base(normalized_addr)
            if ai_parsed and ai_parsed.get('rua'): 
                base_rua = ai_parsed.get('rua')

            offsets = [1, -1, 2, -2]
            for offset in offsets:
                new_lote = l_num + offset
                if new_lote <= 0: continue
                
                # Monta query explícita: "Rua X, Quadra Y, Lote Z"
                neighbor_query = f"{base_rua}, Quadra {search_q}, Lote {new_lote}"
                if bairro: neighbor_query += f", {bairro}"

                print(f"--- Tentando vizinho: {neighbor_query} ---")
                lat, lng, found_cep, found_street, status = await geocode_with_here(neighbor_query)
                
                if status == "OK":
                    # Validação Rigorosa de Quadra também no Vizinho
                    found_q_neighbor, _ = extract_quadra_lote_values(found_street)
                    if found_q_neighbor and search_q != found_q_neighbor:
                        print(f"--- Vizinho REJEITADO: Quadra retornada {found_q_neighbor} != Buscada {search_q} ---")
                        continue

                    return lat, lng, True, f"PARTIAL_LOTE_{offset}"
        except ValueError:
            pass

    # --- ESTRATÉGIA E: APENAS A RUA ---
    # Se chegou aqui, não achamos o lote/quadra exato.
    # Mas só retornamos o centroide da rua se não houver restrição de quadra forte não atendida?
    # O usuário pediu: "Se não for a quadra, coloque como errado". 
    # Então, se a API retornar uma rua genérica (sem quadra), tudo bem (warning).
    # Mas se retornar com quadra errada, rejeitamos.
    
    street_only = extract_street_base(normalized_addr)
    if street_only and len(street_only) > 3:
        query_street = f"{street_only}, {bairro or ''}"
        lat, lng, _, found_street, status = await geocode_with_here(query_street)
        
        if status == "OK":
             # Validação Final de Quadra
             found_q_street, _ = extract_quadra_lote_values(found_street)
             if input_q and found_q_street and input_q != found_q_street:
                 return "Não encontrado", "Não encontrado", False, "FAILED_WRONG_QUADRA"

             if extract_street_base(found_street).upper().startswith(street_only.upper()):
                 return lat, lng, True, "STREET_CENTROID"

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

    # Colunas obrigatórias
    req_cols = ["Destination Address", "Zipcode/Postal code"]
    if not all(col in df.columns for col in req_cols):
        raise HTTPException(422, "Colunas obrigatórias ausentes")

    # Garante coluna de Bairro
    if "Bairro" not in df.columns:
        df["Bairro"] = ""

    results_lat = []
    results_lng = []
    results_partial = []
    results_method = []

    # Processamento Assíncrono (para ser rápido)
    # Se o arquivo for muito grande, ideal é usar tasks em background ou batch
    for idx, row in df.iterrows():
        raw_addr = row["Destination Address"]
        cep = row["Zipcode/Postal code"]
        bairro = row["Bairro"]
        
        # 1. Normalização (Sua função original)
        normalized = normalize_address(raw_addr, bairro)
        
        # 2. Verifica se é condomínio (skip lógico)
        if "CONDOMINIO" in normalized.upper() and "RUA" not in normalized.upper():
            results_lat.append("")
            results_lng.append("")
            results_partial.append(False)
            results_method.append("COND_SKIP")
            continue

        # 3. Busca Inteligente
        lat, lng, is_partial, method = await find_best_location(normalized, cep, bairro, raw_addr)
        
        results_lat.append(lat)
        results_lng.append(lng)
        results_partial.append(is_partial)
        results_method.append(method)

    df["Geo_Latitude"] = results_lat
    df["Geo_Longitude"] = results_lng
    df["Partial_Match"] = results_partial
    df["Match_Method"] = results_method

    # Sanitização JSON
    records = json.loads(df.to_json(orient="records"))

    return {
        "filename": file.filename,
        "rows": len(records),
        "data": records
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)