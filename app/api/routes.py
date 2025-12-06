from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi import APIRouter, Form
import pandas as pd
import asyncio
from io import BytesIO
from pydantic import BaseModel
from app.services.processor import buscar_melhor_localizacao
from app.core.config import settings
from app.services.database import salvar_endereco_editado_db

router = APIRouter()

class EnderecoEditado(BaseModel):
    endereco_normalizado: str
    bairro: str
    cidade: str
    lat: float
    lng: float

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        df = await run_in_threadpool(pd.read_excel, BytesIO(content))
        df = df.reset_index(drop=True)
    except Exception as e:
        raise HTTPException(400, f"Erro ao ler Excel: {e}")

    # máx 10 requisições simultâneas p/ API
    sem = asyncio.Semaphore(settings.MAX_CONCURRENT_REQUESTS)

    async def process_row(index, row):
        async with sem:
            lat, lng, is_partial, is_cond, status, endereco_normalizado = await buscar_melhor_localizacao(row)
            return {
                "idx": index,
                "Geo_Latitude": lat,
                "Geo_Longitude": lng,
                "Partial_Match": is_partial,
                "Cond_Match": is_cond,
                "Status_Log": status,
                "Endereco Normalizado": endereco_normalizado
            }

    tasks = [process_row(idx, row) for idx, row in df.iterrows()]
    
    #Executa tudo, 
    results = await asyncio.gather(*tasks)

    final_data = []
    for r, original in zip(results, df.to_dict(orient="records")):
        merged = {**original, **r}
        final_data.append(merged)

    return {
        "rows": len(final_data),
        "data": final_data
    }

@router.post("/salvar_endereco_editado")
async def salvar_endereco_editado(
    endereco_normalizado: str = Form(...),
    bairro: str = Form(...),
    cidade: str = Form(...),
    lat: float = Form(...),
    lng: float = Form(...)
):
    resultado = salvar_endereco_editado_db(
        endereco_normalizado,
        bairro,
        cidade,
        lat,
        lng
    )
    return resultado