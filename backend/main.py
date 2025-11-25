import os
import uvicorn
import re
import math
import json
import asyncio
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
import aiohttp
from io import BytesIO

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

HERE_API_KEY = os.getenv("HERE_API_KEY")

# --- LÓGICA DE PARSE (A mesma de antes) ---
def parse_address_components(raw):
    if pd.isna(raw) or str(raw).strip() == "":
        return {"full_address": "", "street": "", "quadra": None, "lote": None}

    text = str(raw).strip()
    text = re.sub(r'\s+', ' ', text)
    text = text.replace('/', ' ')
    text_upper = text.upper()

    quadra = None
    lote = None
    q_pattern = r"(?:\bQUADRA|\bQ\.?D?\.?)\s*([0-9]+[A-Z]?)\b"
    m_q = re.search(q_pattern, text_upper)
    if m_q: quadra = m_q.group(1)

    l_pattern = r"(?:\bLOTE|\bL\.?T?\.?)\s*([0-9]+[A-Z]?)\b"
    m_l = re.search(l_pattern, text_upper)
    if m_l: lote = m_l.group(1)

    is_fallback = False
    fallback_match = None
    if not (quadra and lote):
        fb_pattern = r"\b([0-9]+[A-Z]?)\s*-\s*([0-9]+[A-Z]?)\b"
        m_fb = re.search(fb_pattern, text)
        if m_fb:
            quadra = quadra or m_fb.group(1).upper()
            lote = lote or m_fb.group(2).upper()
            is_fallback = True
            fallback_match = m_fb

    cut_index = len(text)
    separators = [",", " - ", " Nº", " NUMERO", " CASA", " APT", " APTO"]
    for sep in separators:
        idx = text_upper.find(sep)
        if idx != -1 and idx < cut_index:
            cut_index = idx

    if m_q and m_q.start() < cut_index: cut_index = m_q.start()
    if m_l and m_l.start() < cut_index: cut_index = m_l.start()
    if is_fallback and fallback_match and fallback_match.start() < cut_index:
        cut_index = fallback_match.start()

    street = text[:cut_index].strip().rstrip(" ,-./")
    invalid_values = ['0', '00', 'SN', 'S/N', 'NULL']
    if quadra and quadra.upper() in invalid_values: quadra = None
    if lote and lote.upper() in invalid_values: lote = None

    full_fmt = street
    if quadra and lote:
        full_fmt = f"{street}, {quadra}-{lote}"

    return {"full_address": full_fmt, "street": street, "quadra": quadra, "lote": lote}

# --- GEOCODING ---
async def geocode_with_here(address: str, session):
    if not HERE_API_KEY:
        # Simulação de delay para você ver o efeito visual no front
        await asyncio.sleep(0.1) 
        return -16.70, -49.20, None

    url = f"https://geocode.search.hereapi.com/v1/geocode?q={address}&apiKey={HERE_API_KEY}"
    try:
        async with session.get(url, timeout=10) as response:
            if response.status != 200: return None, None, None
            data = await response.json()
            if "items" in data and len(data["items"]) > 0:
                item = data["items"][0]
                pos = item.get("position", {})
                addr = item.get("address", {})
                return pos.get("lat"), pos.get("lng"), addr.get("postalCode")
    except Exception as e:
        print("HERE Error:", e)
    return None, None, None

# --- GERADOR DE STREAM ---
async def process_excel_stream(df):
    """
    Gera dados linha a linha no formato NDJSON (Newline Delimited JSON)
    """
    
    # 1. Pré-processamento (rápido)
    processed_data = df["Destination Address"].apply(parse_address_components)
    df_processed = pd.json_normalize(processed_data)
    df["Normalized_Address"] = df_processed["full_address"]
    df["Extracted_Quadra"] = df_processed["quadra"]
    df["Extracted_Lote"] = df_processed["lote"]

    # Inicia colunas vazias para ordenar o JSON final
    df["Geo_Latitude"] = None
    df["Geo_Longitude"] = None

    # 2. Envia METADADOS primeiro (para o front criar o cabeçalho)
    # Convertemos colunas para lista e garantimos ordem
    cols = df.columns.tolist()
    metadata = {
        "type": "metadata",
        "total_rows": len(df),
        "columns": cols
    }
    yield json.dumps(metadata) + "\n"

    # 3. Processa e envia linha a linha
    async with aiohttp.ClientSession() as session:
        for idx, row in df.iterrows():
            normalized = row["Normalized_Address"]
            cep_original = str(row["Zipcode/Postal code"]).strip()
            bairro = row.get("Bairro", "")
            
            lat, lng = "Não encontrado", "Não encontrado"
            
            # Lógica de Geocoding
            h_lat, h_lng, h_cep = await geocode_with_here(normalized, session)
            match1 = h_cep and str(h_cep).replace("-", "") == cep_original.replace("-", "")

            if match1:
                lat, lng = h_lat, h_lng
            else:
                # Tentativa 2
                h_lat2, h_lng2, h_cep2 = await geocode_with_here(f"{normalized}, {bairro}", session)
                match2 = h_cep2 and str(h_cep2).replace("-", "") == cep_original.replace("-", "")
                if match2:
                    lat, lng = h_lat2, h_lng2

            # Atualiza a linha atual
            row["Geo_Latitude"] = lat
            row["Geo_Longitude"] = lng

            # Sanitiza para JSON
            row_dict = row.to_dict()
            clean_row = {}
            for k, v in row_dict.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    clean_row[k] = None
                else:
                    clean_row[k] = v
            
            clean_row["type"] = "data"
            
            # Yield (envia o chunk para o front)
            yield json.dumps(clean_row) + "\n"

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        raw = await file.read()
        df = pd.read_excel(BytesIO(raw))
    except Exception as e:
        raise HTTPException(400, f"Erro ao ler Excel: {e}")

    # Retorna o Stream
    return StreamingResponse(
        process_excel_stream(df),
        media_type="application/x-ndjson"
    )

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)