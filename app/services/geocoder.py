import aiohttp
import urllib.parse
from app.core.config import settings

MEMORY_CACHE = {}

async def geocode_with_here(address: str):
    if not settings.HERE_API_KEY:
        return [], "NO_KEY"

    if address in MEMORY_CACHE:
        return MEMORY_CACHE[address]

    # Força uso de + para espaço e , literal
    encoded_query = urllib.parse.quote_plus(address).replace("%2C", ",")
    url = f"https://geocode.search.hereapi.com/v1/geocode?q={encoded_query}&apiKey={settings.HERE_API_KEY}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    return [], "API_ERROR"

                data = await response.json()
                # RETORNA A LISTA INTEIRA DE CANDIDATOS
                if "items" in data and len(data["items"]) > 0:
                    items = data["items"]
                    MEMORY_CACHE[address] = (items, "OK")
                    return items, "OK"

    except Exception as e:
        print(f"Erro HERE: {e}")
        return [], "EXCEPTION"

    return [], "NOT_FOUND"