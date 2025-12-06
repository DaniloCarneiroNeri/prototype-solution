import re
from rapidfuzz import fuzz
import pandas as pd

def similaridade_texto(a, b):
    if not a or not b: return 0
    return fuzz.token_set_ratio(str(a).upper(), str(b).upper()) / 100.0

def extrair_numeros(texto):
    return re.findall(r'\d+', texto)

def separar_letras_numeros(texto):
    """Garante espaço entre letras e números (ex: 'd77' -> 'd 77')"""
    texto = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', texto)
    texto = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', texto)
    return texto

def extrair_valores_quadra_lote(texto):
    if not texto: return None, None
    
    # 1. Limpeza agressiva
    texto_limpo = texto.upper().replace(",", " ")
    texto_limpo = separar_letras_numeros(texto_limpo)
    
    val_quadra = None
    val_lote = None

    # 2. Regex Robusto
    padrao_quadra = r"\b(?:Q|QD|D|QUADRA|QDA|QU|QR|QUAD|QDR)\s*[-.]?\s*0*(\d+)\b"
    padrao_lote = r"\b(?:L|LT|LOTE|LO|LTO|LOT)\s*[-.]?\s*0*(\d+)\b"

    # Busca Quadra
    match_q = re.search(padrao_quadra, texto_limpo)
    if match_q: 
        val_quadra = str(int(match_q.group(1)))

    # Busca Lote
    match_l = re.search(padrao_lote, texto_limpo)
    if match_l: 
        val_lote = str(int(match_l.group(1)))
    
    # 3. Fallback (Último recurso)
    if not val_quadra or not val_lote:
        # Regex mais restritivo para evitar pegar ano ou CEP
        matches = re.finditer(r"\b(\d{1,4})\s*[-/]\s*(\d{1,4})\b", texto_limpo)
        for m in matches:
            v1 = m.group(1)
            v2 = m.group(2)
            
            if int(v1) > 2000 or int(v2) > 2000: continue
            
            if val_quadra and not val_lote:
                val_lote = str(int(v2))
            elif not val_quadra:
                val_quadra = str(int(v1))
                val_lote = str(int(v2))
            break 

    return val_quadra, val_lote

def extrair_base_rua(endereco):
    if not endereco: return ""
    texto = endereco.upper().strip()
    
    # Lista de paradas para cortar o nome da rua
    separadores = [
        ",", " - ", " NUMERO", " Nº",
        " Q", " QD", " QUADRA", " Q.", " QU ", " QR ",
        " L", " LT", " LOTE", " L.",
        " ESQUINA", " ESQ" 
    ]
    
    melhor_indice = len(texto)
    for sep in separadores:
        idx = texto.find(sep)
        if idx != -1 and idx < melhor_indice and idx > 2:
            melhor_indice = idx
            
    return texto[:melhor_indice].strip()

def normalizar_endereco(raw, bairro):
    try:
        if pd.isna(raw) or str(raw).strip() == "": return ""
        texto = str(raw).strip()
        bairro_str = "" if pd.isna(bairro) else str(bairro).strip()

        texto_completo = f"{texto} {bairro_str}".upper()
        
        # Filtro de Condomínios
        palavras_condominio = ["COND", "COND.", "CONDOMINIO", "CONDOMÍNIO", "JARDINS", "EDIFÍCIO", "BLOCO", "APARTAMENTO", "APTO", "APT", "BL.", "BL"]
        if any(k in texto_completo for k in palavras_condominio):
             if "RESIDENCIAL CANADA" not in texto_completo and "VEREDA DOS BURITIS" not in texto_completo:
                 return "Condominio"

        texto_upper = texto.upper()
        
        # 1. Garante espaços entre letras e números (ex: RI17 -> RI 17)
        texto_upper = separar_letras_numeros(texto_upper)

        # 2. Padronização de prefixos
        texto_upper = re.sub(r"(^|\s)R[.]?\s+(?=[A-Z])", r"\1RUA ", texto_upper, flags=re.IGNORECASE)
        texto_upper = re.sub(r"(^|\s)AV[.]?\s+(?=[A-Z])", r"\1AVENIDA ", texto_upper, flags=re.IGNORECASE)

        # 3. Formatação de Código de Rua (ex: RC-010)
        def formatar_codigo(match):
            prefixo = match.group(1).upper()
            palavras_reservadas = [
                'RUA', 'AV', 'AVENIDA', 'ALAMEDA', 'VIA', 'RODOVIA',
                'QD', 'LT', 'Q', 'L', 'QU', 'QR', 'AP', 'BL', 'CASA',
                'QUADRA', 'QDA', 'LOTE', 'LTO' 
            ]
            
            if prefixo in palavras_reservadas:
                return match.group(0)
            
            numero = match.group(2).zfill(3)
            return f"{prefixo}-{numero}"

        texto_upper = re.sub(r"\b([A-Z]{1,4})\s*[-]?\s*(\d+)\b", formatar_codigo, texto_upper)
        
        quadra, lote = extrair_valores_quadra_lote(texto_upper)
        base_rua = extrair_base_rua(texto_upper)
        
        base_rua = re.sub(r'\s*[-]?\s*(QD|LT|QU|QR|QUADRA|LOTE)\s*[-]?\s*\d+', '', base_rua).strip()
        base_rua = base_rua.replace(" ,", ",")

        if quadra and lote:
            return f"{base_rua}, {quadra}-{lote}" 
        elif quadra:
            return f"{base_rua}, {quadra}-{lote}"
        
        return base_rua

    except Exception as e:
        print(f"Erro normalizacao: {e}")
        return str(raw)