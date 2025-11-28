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
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") # Opcional: Para parser inteligente

# ============================================================
# INTEGRAÇÃO IA (OPENAI) - Parser Inteligente
# ============================================================
async def parse_address_with_ai(raw_text: str) -> Dict[str, str]:
    """
    Usa IA para estruturar endereços difíceis que o Regex pode perder.
    Requer OPENAI_API_KEY. Retorna dict com componentes.
    """
    if not OPENAI_API_KEY:
        return None

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=OPENAI_API_KEY)

        prompt = (
            f"Extraia o endereço do texto abaixo para formato JSON com chaves: "
            f"rua, quadra, lote, bairro, numero (se houver). "
            f"O contexto é Goiânia, Brasil. Padronize 'RUA T 63' para 'Rua T-63'. "
            f"Texto: '{raw_text}'"
        )

        response = await client.chat.completions.create(
            model="gpt-4o-mini", # Modelo rápido e barato
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0
        )
        
        content = response.choices[0].message.content
        return json.loads(content)
    except Exception as e:
        print(f"Erro IA: {e}")
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

def normalize_address(raw, bairro):
    # --- (MANTIVE SUA LÓGICA DE REGEX AQUI, APENAS OMITI PARA ECONOMIZAR ESPAÇO VISUAL) ---
    # --- COLE AQUI O SEU CÓDIGO DA FUNÇÃO normalize_address ORIGINAL ---
    # --- Vou assumir que ela retorna "RUA X, Q-L" ou similar ---
    
    # ... Insira o corpo da sua função normalize_address original ...
    # Para fins de execução neste exemplo, usarei uma versão simplificada wrapper:
    # No seu código final, MANTENHA A SUA VERSÃO COMPLETA.
    try:
        if pd.isna(raw) or str(raw).strip() == "":
            return ""
        
        text = str(raw).strip().upper()
        # ... (Sua logica complexa aqui) ...
        # Retorno simplificado para o exemplo (mas use o seu):
        return f"{text} {bairro or ''}".strip()
    except:
        return str(raw)

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
# ROTINA PRINCIPAL DE BUSCA (A Lógica Robusta)
# ============================================================
async def find_best_location(normalized_addr: str, original_cep: str, bairro: str, original_raw: str):
    """
    Tenta encontrar o endereço usando múltiplas estratégias em cascata.
    """
    
    # 1. Tentar parse via IA se disponível (muito útil para endereços bagunçados)
    # Se você configurar a OPENAI_KEY, descomente a lógica abaixo
    ai_parsed = None
    if OPENAI_API_KEY:
         ai_parsed = await parse_address_with_ai(original_raw)
         if ai_parsed and ai_parsed.get('rua'):
             # Tenta montar um endereço limpo com a IA
             q = ai_parsed.get('quadra', '')
             l = ai_parsed.get('lote', '')
             ql_str = f", Quadra {q}, Lote {l}" if q and l else ""
             normalized_addr = f"{ai_parsed['rua']}{ql_str}"

    strategies = []
    
    # Limpeza para comparação
    cep_clean = str(original_cep).replace("-", "").replace(".", "").strip()
    
    # --- ESTRATÉGIA A: Busca Exata Normalizada ---
    strategies.append({
        "query": normalized_addr,
        "type": "EXACT_NORMALIZED"
    })

    # --- ESTRATÉGIA B: Busca com Bairro Explícito (Goiânia) ---
    if bairro:
        strategies.append({
            "query": f"{normalized_addr}, {bairro}",
            "type": "WITH_BAIRRO"
        })

    # --- ESTRATÉGIA C: Tentar Variações de Zero (Rua AC-1 vs AC-01) ---
    # Extrai parte da rua e número se existir padrão "RUA XX-YY"
    m = re.search(r"(RUA\s+[A-Z]+)-(\d+)", normalized_addr.upper())
    if m:
        prefix = m.group(1)
        num = m.group(2)
        variants = [str(int(num)), num.zfill(2), num.zfill(3)]
        for v in variants:
            if v == num: continue
            # Reconstrói a string trocando o número
            new_addr = normalized_addr.replace(f"{prefix}-{num}", f"{prefix}-{v}")
            strategies.append({"query": new_addr, "type": "ZERO_VARIANT"})

    # --- EXECUÇÃO DAS ESTRATÉGIAS ---
    for strat in strategies:
        lat, lng, found_cep, found_street, status = await geocode_with_here(strat["query"])
        
        if status != "OK": continue

        # Validação:
        # 1. CEP bate?
        cep_match = False
        if found_cep and cep_clean:
            if found_cep.replace("-", "") == cep_clean:
                cep_match = True
        
        # 2. Rua bate (pelo menos o começo)?
        street_base_in = extract_street_base(normalized_addr).upper()
        street_base_out = extract_street_base(found_street).upper()
        name_match = street_base_in in street_base_out or street_base_out in street_base_in

        # Se CEP bate ou Nome da rua é muito parecido, aceitamos
        if cep_match or (name_match and len(street_base_in) > 3):
            return lat, lng, False, strat["type"]

    # --- ESTRATÉGIA D: VIZINHOS (Lotes +/- 1 e 2) ---
    # Se chegamos aqui, não achamos o exato. Vamos tentar os vizinhos.
    match_ql = re.search(r"(.*)[, ]\s*(\d+)[- ](\d+)", normalized_addr) # Padrão "Rua, Q-L"
    if match_ql:
        base_rua = match_ql.group(1)
        q_num = int(match_ql.group(2))
        l_num = int(match_ql.group(3))
        
        # Tenta vizinhos próximos
        offsets = [1, -1, 2, -2]
        for offset in offsets:
            new_lote = l_num + offset
            if new_lote <= 0: continue
            
            # Recria query: "Rua Tal, Quadra X, Lote Y"
            # Usar formato extenso ajuda a API: "Quadra X Lote Y"
            neighbor_query = f"{base_rua}, Quadra {q_num}, Lote {new_lote}"
            lat, lng, found_cep, found_street, status = await geocode_with_here(neighbor_query)
            
            if status == "OK":
                 # Aqui somos menos rigorosos com CEP, pois estamos buscando vizinho
                 return lat, lng, True, f"PARTIAL_LOTE_{offset}"

    # --- ESTRATÉGIA E: APENAS A RUA (Centroide) ---
    # Última tentativa: Achar onde fica a rua no bairro, sem o numero
    street_only = extract_street_base(normalized_addr)
    if street_only and len(street_only) > 3:
        query_street = f"{street_only}, {bairro or ''}"
        lat, lng, _, found_street, status = await geocode_with_here(query_street)
        if status == "OK":
             # Verifica se o nome retornado tem a ver com o buscado
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