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
    Formata endereços para o padrão: 'RUA/AVENIDA, QUADRA-LOTE'
    Lógica otimizada para dados inconsistentes (CSV fornecido).
    """
    if pd.isna(raw) or str(raw).strip() == "":
        return ""

    # --- 1. Limpeza Prévia ---
    text = str(raw).strip()
    # Remove quebras de linha e espaços extras
    text = re.sub(r'\s+', ' ', text)
    # Substitui barras por espaço (ex: "Quadra 119/Lote 14" -> "Quadra 119 Lote 14")
    text = text.replace('/', ' ')
    # Remove repetições comuns de prefixos para facilitar a busca (ex: "Rua F22... Rua F22")
    # (A lógica de corte abaixo já resolve isso, mas a limpeza ajuda)
    
    text_upper = text.upper()

    # --- 2. Extração de Quadra e Lote (Busca Global) ---
    quadra = None
    lote = None

    # Regex poderoso para Quadra:
    # Captura: "Q 12", "QD 12", "Q.12", "QUADRA 12", "QD.12"
    # (?:...) agrupa sem capturar, \s* permite espaços opcionais
    q_pattern = r"(?:\bQUADRA|\bQ\.?D?\.?)\s*([0-9]+[A-Z]?)\b"
    
    # Regex poderoso para Lote:
    # Captura: "L 10", "LT 10", "L.10", "LOTE 10", "LT.10"
    l_pattern = r"(?:\bLOTE|\bL\.?T?\.?)\s*([0-9]+[A-Z]?)\b"

    m_q = re.search(q_pattern, text_upper)
    if m_q:
        quadra = m_q.group(1)

    m_l = re.search(l_pattern, text_upper)
    if m_l:
        lote = m_l.group(1)

    # --- 3. Fallback: Padrão "XX-YY" ---
    # Só aciona se não encontrou Q e L explicitamente, para evitar pegar números de telefone ou CEP.
    is_fallback = False
    fallback_match = None
    if not (quadra and lote):
        # Procura por "Numero - Numero" (ex: "Rua das Flores 15-20")
        fb_pattern = r"\b([0-9]+[A-Z]?)\s*-\s*([0-9]+[A-Z]?)\b"
        m_fb = re.search(fb_pattern, text) # Usa text original para não perder case se necessário (mas aqui é numero)
        if m_fb:
            quadra = quadra or m_fb.group(1).upper()
            lote = lote or m_fb.group(2).upper()
            is_fallback = True
            fallback_match = m_fb

    # --- 4. Extração da Rua (O Corte Cirúrgico) ---
    # O nome da rua vai do início até o PRIMEIRO separador encontrado.
    # Separadores: Vírgula, Traço isolado, ou o início da Quadra/Lote.
    
    cut_index = len(text)
    
    # Lista de separadores textuais comuns
    separators = [",", " - ", " Nº", " NUMERO", " CASA", " APT", " APTO"]
    
    for sep in separators:
        idx = text_upper.find(sep)
        if idx != -1 and idx < cut_index:
            cut_index = idx

    # Se achou Quadra ou Lote via Regex, o nome da rua TEM que acabar antes deles
    if m_q and m_q.start() < cut_index:
        cut_index = m_q.start()
    if m_l and m_l.start() < cut_index:
        cut_index = m_l.start()
    if is_fallback and fallback_match and fallback_match.start() < cut_index:
        cut_index = fallback_match.start()

    street = text[:cut_index].strip()
    
    # Limpeza final da rua (remove pontuação sobrando no final)
    street = street.rstrip(" ,-./")

    # --- 5. Formatação Final ---
    
    # Validação de dados inválidos
    invalid_values = ['0', '00', 'SN', 'S/N', 'NULL']
    if quadra and quadra.upper() in invalid_values: quadra = None
    if lote and lote.upper() in invalid_values: lote = None

    if quadra and lote:
        return f"{street}, {quadra}-{lote}"
    
    # Se não achou Q/L (ex: é um prédio ou endereço simples), retorna só a rua limpa
    # Se quiser forçar que apareça algo, poderia colocar um 'S/N'. 
    # Mas o padrão pedido é estrito.
    return street


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

    df["Normalized_Address"] = df["Destination Address"].apply(normalize_address)

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



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
