import re
import pandas as pd

#Extrai Lote e Quadra via Regex
def extract_quadra_lote_values(text):
    if not text: return None, None
    text_clean = text.upper().replace(".", " ").replace(",", " ")
    
    q_val = None
    l_val = None

    match_q = re.search(r"\b(?:Q|QD|QUADRA|QDA)\s*0*(\d+)", text_clean)
    if match_q: 
        q_val = str(int(match_q.group(1)))

    match_l = re.search(r"\b(?:L|LT|LOTE)\s*0*(\d+)", text_clean)
    if match_l: 
        l_val = str(int(match_l.group(1)))
    
    if not q_val or not l_val:
        matches = re.finditer(r"\b(\d{1,4})\s*-\s*(\d{1,4})", text_clean)
        
        for m in matches:
            v1 = m.group(1)
            v2 = m.group(2)
            
            if int(v1) > 4000 or int(v2) > 4000:
                continue

            if q_val and not l_val:
                l_val = str(int(v2))
            elif not q_val:
                q_val = str(int(v1))
                l_val = str(int(v2))
            break 

    return q_val, l_val

def extract_street_base(address):
    if not address: return ""
    text = address.upper().strip()
    separators = [",", " - ", " Q", " QD", " Q.", " LT", " L.", " QUADRA", " LOTE", " NUMERO", " Nº"]
    best_index = len(text)
    for sep in separators:
        idx = text.find(sep)
        if idx != -1 and idx < best_index:
            best_index = idx
    return text[:best_index].strip()

def normalize_address(raw, bairro):
    try:
        if pd.isna(raw) or str(raw).strip() == "": return ""
        text = str(raw).strip()
        bairro = "" if pd.isna(bairro) else str(bairro).strip()

        # Condomínios
        raw_combined = f"{text} {bairro}".upper()
        cond_keywords = ["COND", "COND.", "CONDOMINIO", "CONDOMÍNIO", "JARDINS LISBOA", "RESIDENCIAL MIAMI", "EDIFÍCIO", "BLOCO", "APARTAMENTO", "APTO.","APTO", "APT.", "APT","BL.","AP","BL","AP."]
        is_condominio = False
        if any(k in raw_combined for k in cond_keywords):
             if "RESIDENCIAL CANADA" not in raw_combined and "VEREDA DOS BURITIS" not in raw_combined:
                 is_condominio = True

        # Limpeza
        text = re.sub(r'\s+', ' ', text).replace('/', ' ')
        text_upper = text.upper()

        # Prefixos
        text = re.sub(r"(^|\s)R[.]?\s+(?=[A-Z])", r"\1RUA ", text_upper, flags=re.IGNORECASE)
        text = re.sub(r"(^|\s)AV[.]?\s+(?=[A-Z])", r"\1AVENIDA ", text_upper, flags=re.IGNORECASE)

        # === CORREÇÃO CRÍTICA: FORÇAR 3 DÍGITOS (RC 11 -> RC-011) ===
        # Regex procura: Palavra (MDV/RC/VB) + Espaço opcional + Dígitos
        # Ex: "Rua RC 11" -> Captura "RC" e "11"
        match_code = re.search(r"\b(MDV|RC|VB|APM|CP)\s*[-]?\s*(\d+)\b", text, flags=re.IGNORECASE)
        if match_code:
            prefixo = match_code.group(1)
            numero = match_code.group(2).zfill(3) # AQUI ESTÁ A MÁGICA: 11 vira 011
            # Substitui na string original
            text = re.sub(r"\b(MDV|RC|VB|APM|CP)\s*[-]?\s*(\d+)\b", f"{prefixo}-{numero}", text, flags=re.IGNORECASE)

        # Regra Zeros Genérica para outras ruas (Rua F-1 -> Rua F-01)
        # (Opcional, mantido para compatibilidade)
        rua_codigo = re.search(r"\bRUA\s+([A-Z]{1,3})\s*[- ]?\s*(\d{1,3})\b", text.upper())
        if rua_codigo:
            code = rua_codigo.group(1)
            num = rua_codigo.group(2).zfill(3) 
            # Só substitui se não for um dos códigos já tratados acima para evitar duplicidade
            if code not in ["MDV", "RC", "VB", "APM", "CP"]:
                text = re.sub(r"\bRUA\s+[A-Z]{1,3}\s*[- ]?\s*\d{1,3}\b", f"RUA {code}-{num}", text, flags=re.IGNORECASE)

        # Extração
        q, l = extract_quadra_lote_values(text)
        street_base = extract_street_base(text)

        if is_condominio:
            return "Condominio"

        # === SAÍDA OTIMIZADA PARA A API (Q-L) ===
        if q and l:
            return f"{street_base}, {q}-{l}"
        elif q:
            return f"{street_base}, Q-{q}"
        
        return street_base

    except Exception as e:
        return str(raw)