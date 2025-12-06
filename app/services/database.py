from supabase import create_client, Client
from app.core.config import settings

if not settings.SUPABASE_URL or not settings.SUPABASE_KEY:
    print("AVISO: SUPABASE_URL ou SUPABASE_KEY não configurados no config.py")
    supabase = None
else:
    supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

def salvar_endereco_encontrado(dados: dict):
    if not supabase:
        return

    try:
        endereco_norm = dados.get("endereco_normalizado")

        if not endereco_norm:
            print("Dados sem endereco_normalizado")
            return None
        
        existente = (
            supabase.table("enderecos_processados")
            .select("*")
            .eq("endereco_normalizado", endereco_norm)
            .execute()
        )

        if existente.data and len(existente.data) > 0:
            return {
                "mensagem": "Endereço já existe",
                "registro": existente.data[0]
            }
        response = (
            supabase.table("enderecos_processados")
            .insert(dados)
            .execute()
        )
        return response

    except Exception as e:
        print(f"Erro ao salvar no Supabase: {e}")
        return None
    
def buscar_coordenadas(endereco_normalizado: str):

    if not supabase:
        return {"erro": "Supabase não configurado"}

    if not endereco_normalizado:
        return {"erro": "endereco_normalizado é obrigatório"}

    try:
        resultado = (
            supabase.table("enderecos_processados")
            .select("lat, lng, endereco_normalizado")
            .eq("endereco_normalizado", endereco_normalizado)
            .execute()
        )

        if resultado.data and len(resultado.data) > 0:
            registro = resultado.data[0]
            return {
                "latitude": registro.get("lat"),
                "longitude": registro.get("lng"),
                "endereco_normalizado": registro.get("endereco_normalizado"),
                "mensagem": "Endereço encontrado"
            }

        return None

    except Exception as e:
        return {"erro": f"Erro ao consultar Supabase: {e}"}
    

def salvar_endereco_editado_db(endereco_normalizado: str, bairro: str, cidade: str, lat: float, lng: float):
    try:
        inserido = (
            supabase.table("enderecos_processados")
            .insert({
                "endereco_normalizado": endereco_normalizado,
                "bairro": bairro,
                "cidade": cidade,
                "lat": lat,
                "lng": lng
            })
            .execute()
        )

        return {
            "mensagem": "Endereço inserido manualmente",
            "registro": inserido.data[0]
        }

    except Exception as e:
        print("Erro ao salvar endereço no banco:", e)
        return {"erro": str(e)}

    
