from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.concurrency import run_in_threadpool
import pandas as pd
import asyncio
from io import BytesIO
from app.services.processor import find_best_location
from app.core.config import settings

router = APIRouter()

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        # Roda o Pandas em Thread separada para não travar o servidor
        df = await run_in_threadpool(pd.read_excel, BytesIO(content))
        df = df.reset_index(drop=True)
    except Exception as e:
        raise HTTPException(400, f"Erro ao ler Excel: {e}")

    # Semáforo para controlar concorrência (ex: máx 10 requisições simultâneas p/ API)
    sem = asyncio.Semaphore(settings.MAX_CONCURRENT_REQUESTS)

    async def process_row(index, row):
        async with sem:
            lat, lng, is_partial, is_cond, status = await find_best_location(row)
            return {
                "idx": index,
                "Geo_Latitude": lat,
                "Geo_Longitude": lng,
                "Partial_Match": is_partial,
                "Cond_Match": is_cond,
                "Status_Log": status
            }

    # Cria tarefas para todas as linhas
    tasks = [process_row(idx, row) for idx, row in df.iterrows()]
    
    # Executa tudo em paralelo
    results = await asyncio.gather(*tasks)

    # Reconstrói a resposta
    final_data = []
    for r, original in zip(results, df.to_dict(orient="records")):
        merged = {**original, **r}
        final_data.append(merged)

    return {
        "rows": len(final_data),
        "data": final_data
    }