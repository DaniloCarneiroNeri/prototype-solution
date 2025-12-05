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
    
