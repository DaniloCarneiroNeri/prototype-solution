import re
from rapidfuzz import fuzz
import pandas as pd

def similaridade_texto(a, b):
    if not a or not b: return 0
    return fuzz.token_set_ratio(str(a).upper(), str(b).upper()) / 100.0

def extrair_numeros(texto):
    return re.findall(r'\d+', texto)

def separar_letras_numeros(texto):

    texto = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', texto)
    texto = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', texto)
    return texto

def extrair_valores_quadra_lote(texto):
    if not texto: return None, None
    
    # Pré-processamento agressivo para separar "lixo"
    texto_limpo = texto.upper().replace(".", " ").replace(",", " ")
    texto_limpo = separar_letras_numeros(texto_limpo)
    
    val_quadra = None
    val_lote = None

    # Regex expandido para QU, QR, QDA, etc.
    padrao_quadra = r"\b(?:Q|QD|D|QUADRA|QDA|QU|QR|QUAD)\s*0*(\d+)\b"
    padrao_lote = r"\b(?:L|LT|LOTE|LO|LTO)\s*0*(\d+)\b"

    match_q = re.search(padrao_quadra, texto_limpo)
    if match_q: 
        val_quadra = str(int(match_q.group(1)))

    match_l = re.search(padrao_lote, texto_limpo)
    if match_l: 
        val_lote = str(int(match_l.group(1)))
    
    # Fallback: tenta padrão numérico solto "10-20" se não achou explícito
    if not val_quadra or not val_lote:
        matches = re.finditer(r"\b(\d{1,4})\s*[-/]\s*(\d{1,4})", texto_limpo)
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
    separadores = [",", " - ", " Q", " QD", " Q.", " LT", " L.", " QUADRA", " LOTE", " QU ", " QR ", " NUMERO", " Nº"]
    
    melhor_indice = len(texto)
    for sep in separadores:
        idx = texto.find(sep)
        if idx != -1 and idx < melhor_indice:
            melhor_indice = idx
    return texto[:melhor_indice].strip()

def normalizar_endereco(raw, bairro):
    try:
        if pd.isna(raw) or str(raw).strip() == "": return ""
        texto = str(raw).strip()
        bairro_str = "" if pd.isna(bairro) else str(bairro).strip()

        texto_completo = f"{texto} {bairro_str}".upper()
        palavras_condominio = ["COND", "COND.", "CONDOMINIO", "CONDOMÍNIO", "JARDINS LISBOA", "RESIDENCIAL MIAMI", "EDIFÍCIO", "BLOCO", "APARTAMENTO", "APTO.","APTO", "APT.", "APT","BL.","AP","BL","AP."]
        
        if any(k in texto_completo for k in palavras_condominio):
             if "RESIDENCIAL CANADA" not in texto_completo and "VEREDA DOS BURITIS" not in texto_completo:
                 return "Condominio"

        texto_upper = texto.upper()
        
        # Garante espaços (ex: RI17 -> RI 17)
        texto_upper = separar_letras_numeros(texto_upper)

        texto_upper = re.sub(r"(^|\s)R[.]?\s+(?=[A-Z])", r"\1RUA ", texto_upper, flags=re.IGNORECASE)
        texto_upper = re.sub(r"(^|\s)AV[.]?\s+(?=[A-Z])", r"\1AVENIDA ", texto_upper, flags=re.IGNORECASE)

        # Formatação de Código (RC-010)
        def formatar_codigo(match):
            prefixo = match.group(1).upper()
            if prefixo in ['RUA', 'AV', 'QD', 'LT', 'Q', 'L', 'QU', 'QR', 'AP', 'BL', 'CASA']:
                return match.group(0)
            
            numero = match.group(2).zfill(3)
            return f"{prefixo}-{numero}"

        texto_upper = re.sub(r"\b([A-Z]{1,4})\s*[-]?\s*(\d+)\b", formatar_codigo, texto_upper)
        
        quadra, lote = extrair_valores_quadra_lote(texto_upper)
        base_rua = extrair_base_rua(texto_upper)
        
        # Remove lixo duplicado na rua
        base_rua = re.sub(r'\b(QD|LT|QU|QR)\s*\d+', '', base_rua).strip()
        base_rua = base_rua.replace(" ,", ",")

        if quadra and lote:
            return f"{base_rua}, {quadra}-{lote}"
        elif quadra:
            return f"{base_rua}, Q-{quadra}"
        
        return base_rua

    except Exception as e:
        print(f"Erro normalizacao: {e}")
        return str(raw)