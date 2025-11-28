import os
import uvicorn
import re
import json
import math
import asyncio
import difflib
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
# INTEGRAÇÃO IA (GEMINI COM FALLBACK)
# ============================================================
async def parse_address_with_ai(raw_text: str) -> Dict[str, str]:
    """
    Tenta estruturar o endereço usando múltiplos modelos do Gemini.
    """
    if not GOOGLE_API_KEY:
        return None

    try:
        import google.generativeai as genai
        genai.configure(api_key=GOOGLE_API_KEY)
        
        # Lista de modelos para tentar (do mais rápido/novo para o mais estável)
        models_to_try = ['gemini-1.5-flash', 'gemini-1.5-pro', 'gemini-pro']
        
        prompt = (
            f"Extraia o endereço do texto para JSON (chaves: rua, quadra, lote, bairro). "
            f"Contexto: Goiânia, Goiás. "
            f"Regra: Padronize 'RUA T 63' para 'Rua T-63'. Se tiver 'Qd13', separe para 'Quadra 13'. "
            f"Texto: '{raw_text}'"
        )

        for model_name in models_to_try:
            try:
                model = genai.GenerativeModel(model_name)
                # Executa em thread separada para não bloquear
                response = await asyncio.to_thread(model.generate_content, prompt)
                
                text_resp = response.text
                # Limpa markdown do JSON
                json_str = text_resp.replace("```json", "").replace("```", "").strip()
                data = json.loads(json_str)
                return data
            except Exception as e:
                # Se der erro (ex: 404 model not found), tenta o próximo
                print(f"Erro no modelo {model_name}: {e}")
                continue
                
        return None

    except Exception as e:
        print(f"Erro Geral IA: {e}")
        return None

# ============================================================
# UTILITÁRIOS DE TEXTO
# ============================================================
def clean_street_name(name: str) -> str:
    """Remove rótulos como RUA, AVENIDA e pontuação para comparação."""
    if not name: return ""
    s = name.upper()
    # Remove prefixos comuns
    for prefix in ["RUA ", "AVENIDA ", "AV ", "ALAMEDA ", "RODOVIA ", "TRAVESSA "]:
        if s.startswith(prefix):
            s = s[len(prefix):]
    
    # Remove sufixos de quadra/lote para pegar só o 'nome' raiz da rua
    # Ex: "MDV 13 QUADRA 10" -> "MDV 13"
    cut_markers = [" Q", " QD", " QUADRA", " L", " LT", " LOTE", ",", "-"]
    
    # Acha o primeiro marcador e corta
    first_cut = len(s)
    for m in cut_markers:
        idx = s.find(m)
        if idx != -1 and idx < first_cut:
            first_cut = idx
            
    return s[:first_cut].strip()

def check_street_similarity(input_street: str, found_street: str) -> bool:
    """
    Verifica se a rua encontrada é a mesma da solicitada.
    Retorna True se for similar o suficiente.
    """
    s1 = clean_street_name(input_street)
    s2 = clean_street_name(found_street)
    
    # Se uma delas ficou vazia, não dá pra validar, assume erro ou ignora
    if len(s1) < 2 or len(s2) < 2:
        return False
        
    # Verifica contensão direta (ex: "T-63" contido em "AVENIDA T-63")
    if s1 in s2 or s2 in s1:
        return True
        
    # Verifica similaridade (Levenshtein)
    ratio = difflib.SequenceMatcher(None, s1, s2).ratio()
    return ratio > 0.85  # Exige 85% de similaridade

def normalize_address(raw, bairro):
    """
    Normaliza string para facilitar busca.
    Separa 'Qd13' em 'Quadra 13'.
    """
    try:
        if pd.isna(raw) or str(raw).strip() == "":
            return ""
        
        text = str(raw).strip()
        
        # Upper
        text = text.upper()
        
        # Separar Qd/Lt grudados (Ex: Qd13 -> Qd 13, Lt27 -> Lt 27)
        text = re.sub(r'(QD|QUADRA|Q)\.?\s*(\d+)', r'Quadra \2', text)
        text = re.sub(r'(LT|LOTE|L)\.?\s*(\d+)', r'Lote \2', text)
        
        # Remove caracteres estranhos
        text = re.sub(r'[^A-Z0-9\s,.-]', '', text)

        if bairro and not pd.isna(bairro):
            return f"{text}, {str(bairro).upper()}"
        return text
    except:
        return str(raw)

# ============================================================
# GEOCODING HERE
# ============================================================
async def geocode_with_here(query: str, city_context: str = "Goiânia", state_context: str = "GO"):
    if not HERE_API_KEY:
        return None, None, None, None, "API_KEY_MISSING", None

    # Garante contexto
    final_query = query
    if "GOIANIA" not in query.upper().replace("Â", "A"):
        final_query = f"{query}, {city_context}, {state_context}, Brasil"

    encoded_query = final_query.replace(" ", "%20")
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
                result_type = item.get("resultType", "unknown")
                scoring = item.get("scoring", {}) # Relevância

                lat = pos.get("lat")
                lng = pos.get("lng")
                postal = addr.get("postalCode")
                street_found = addr.get("street")
                
                return lat, lng, postal, street_found, "OK", result_type

        except Exception as e:
            return None, None, None, None, str(e), None

# ============================================================
# LÓGICA DE BUSCA INTELIGENTE
# ============================================================
async def find_best_location(normalized_addr: str, original_cep: str, bairro: str, original_raw: str):
    """
    Busca Cascata: Exata -> IA -> Vizinhos (apenas Lote)
    """
    
    # Extração de Quadra e Lote do texto normalizado via Regex
    # Tenta achar padrões como "Quadra 25 Lote 12" ou "Q 25 L 12"
    q_match = re.search(r'(?:QUADRA|QD|Q)\s*(\d+)', normalized_addr)
    l_match = re.search(r'(?:LOTE|LT|L)\s*(\d+)', normalized_addr)
    
    q_num = q_match.group(1) if q_match else None
    l_num = l_match.group(1) if l_match else None
    
    # Extração do nome da rua base para validação
    input_street_base = clean_street_name(normalized_addr)

    # --- 1. TENTATIVA EXATA (NORMALIZADA) ---
    # Busca exatamente como veio
    lat, lng, cep_f, street_f, status, r_type = await geocode_with_here(normalized_addr)
    
    if status == "OK":
        # Validação 1: Nome da rua
        if check_street_similarity(input_street_base, street_f):
            # Validação 2: Tipo de resultado
            # Se achou 'houseNumber' (número exato) ou 'pointAddress', confia.
            if r_type in ['houseNumber', 'pointAddress']:
                return lat, lng, False, "EXACT_NORMALIZED"
            
            # Se devolveu 'street' (centro da rua), SÓ aceitamos se NÃO tivermos Quadra/Lote para buscar.
            # Se temos Quadra/Lote, 'street' é um resultado ruim (impreciso).
            if r_type == 'street' and not (q_num and l_num):
                 return lat, lng, True, "STREET_CENTROID_NO_QL"

    # --- 2. TENTATIVA VIA IA (Se falhou ou foi impreciso) ---
    # Só aciona IA se tiver chave
    ai_q, ai_l, ai_rua = None, None, None
    if GOOGLE_API_KEY:
        ai_data = await parse_address_with_ai(original_raw)
        if ai_data:
            ai_rua = ai_data.get('rua')
            ai_q = ai_data.get('quadra')
            ai_l = ai_data.get('lote')
            
            # Se a IA achou algo novo, tenta buscar esse endereço limpo
            if ai_rua and ai_q and ai_l:
                # Atualiza números se o regex falhou antes
                if not q_num: q_num = ai_q
                if not l_num: l_num = ai_l
                
                query_ai = f"{ai_rua}, Quadra {ai_q}, Lote {ai_l}, {bairro or ''}"
                lat_ai, lng_ai, _, street_ai, status_ai, r_type_ai = await geocode_with_here(query_ai)
                
                if status_ai == "OK" and check_street_similarity(ai_rua, street_ai):
                    if r_type_ai in ['houseNumber', 'pointAddress']:
                        return lat_ai, lng_ai, False, "AI_EXACT"

    # --- 3. ESTRATÉGIA DE VIZINHOS (APENAS LOTE) ---
    # Requisito: "Não busque quadras adjacentes, apenas lotes."
    if q_num and l_num and input_street_base:
        try:
            l_val = int(l_num)
            # Tenta vizinhos: Lote original, depois -1, +1, -2, +2
            # Adicionei o 0 (original) aqui de novo com formatação explícita, 
            # pois as vezes a busca normalizada falha por "sujeira", mas esta limpa passa.
            offsets = [0, -1, 1, -2, 2] 
            
            for offset in offsets:
                target_lote = l_val + offset
                if target_lote <= 0: continue
                
                # Monta query limpa e explícita
                # Ex: "Rua MDV 13, Quadra 23, Lote 14, Goiânia"
                query_neighbor = f"{input_street_base}, Quadra {q_num}, Lote {target_lote}, {bairro or ''}"
                
                lat_n, lng_n, _, street_n, status_n, r_type_n = await geocode_with_here(query_neighbor)
                
                if status_n == "OK":
                    # Valida nome da rua
                    if not check_street_similarity(input_street_base, street_n):
                        continue
                        
                    # Só aceita se for exato (achou o lote vizinho)
                    if r_type_n in ['houseNumber', 'pointAddress']:
                        match_type = "EXACT_RETRY" if offset == 0 else f"PARTIAL_LOTE_{offset}"
                        return lat_n, lng_n, (offset != 0), match_type

        except ValueError:
            pass # Lote não é numérico

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