import asyncio
import re
from difflib import SequenceMatcher
from app.services.normalizer import normalize_address, extract_quadra_lote_values, extract_street_base
from app.services.geocoder import geocode_with_here

def text_similarity(a, b):
    if not a or not b: return 0
    return SequenceMatcher(None, str(a).upper(), str(b).upper()).ratio()

def extract_numbers_from_string(text):
    """Extrai números inteiros de uma string"""
    return re.findall(r'\d+', text)

def clean_street_name(name):
    """Remove hifens, zeros a esquerda e tipos de logradouro para comparacao"""
    # Remove RUA, AV, etc
    name = re.sub(r'\b(RUA|AV|AVENIDA|ALAMEDA|TRAVESSA)\b', '', name.upper())
    # Remove hifens
    name = name.replace("-", " ")
    # Remove zeros a esquerda de numeros isolados
    name = re.sub(r'\b0+(\d+)\b', r'\1', name)
    return name.strip()

# ============================================================
# SELETOR DE CANDIDATOS (COM TRAVAS DE SEGURANÇA)
# ============================================================
def pick_best_candidate(candidates, target_data):
    best_candidate = None
    best_score = -1
    best_log = "FAILED"

    tgt_rua_raw = target_data['base_rua'].upper()
    #tgt_rua_clean = clean_street_name(tgt_rua_raw)
    tgt_nums = extract_numbers_from_string(tgt_rua_raw)
    
    tgt_bairro = target_data['bairro'].upper()
    tgt_cidade = target_data['cidade_alvo'].upper()
    tgt_q = target_data['quadra']

    for item in candidates:
        address = item.get("address", {})
        found_city = address.get("city", "").upper()
        found_street = address.get("street", "").upper()
        found_district = address.get("district", "").upper()
        found_label = address.get("label", "").upper()
        score = item.get("scoring",{})
        fieldScore = score.get("fieldScore",{})
        city = fieldScore.get("city","")
        houseNumber = fieldScore.get("houseNumber","")
        street = fieldScore.get("streets","")
        position = item.get("position",{})
        lat = position.get("lat","")
        lng = position.get("lng","")

        if city == 1.0 and houseNumber == 1.0 and street[0] >= 0.83:
            log = "EXACT_MATCH"
            best_candidate = (lat, lng, False, False, log)
            return best_candidate
        
        # --- 1. FILTRO DE CIDADE ---
        if tgt_cidade:
            if tgt_cidade not in found_city and found_city not in tgt_cidade:
                if text_similarity(tgt_cidade, found_city) < 0.8:
                    continue 

        if "QUADRA" in found_street:
            found_street = found_street.split("QUADRA")[0]

        found_street = found_street.strip()
        # --- 2. FILTRO DE NOME DE RUA ---
        name_match = True if (text_similarity(tgt_rua_raw, found_street) >= 0.8 or tgt_rua_raw in found_street) else False

        if not name_match:
                continue 

        # --- 3. FILTRO DE NÚMERO DA RUA (TRAVA VB 31) ---
        if tgt_nums:
            fnd_nums = extract_numbers_from_string(found_street)
            # Só aplicamos a trava se a rua encontrada também tiver números explícitos
            if fnd_nums:
                tgt_ints = {int(n) for n in tgt_nums}
                fnd_ints = {int(n) for n in fnd_nums}
                
                # Se não houver intersecção, é rua errada.
                if not tgt_ints.intersection(fnd_ints):
                    continue

        # --- 4. VERIFICAÇÃO DE QUADRA (TRAVA MDV 13) ---
        found_q, _ = extract_quadra_lote_values(found_label)
        if not found_q:
            found_q, _ = extract_quadra_lote_values(found_street)
        if not found_q:
            found_q, _ = extract_quadra_lote_values(address.get("houseNumber", ""))

        is_quadra_match = (tgt_q and found_q and tgt_q == found_q)
        is_quadra_mismatch = (tgt_q and found_q and tgt_q != found_q)
        
        # SE A QUADRA FOR DIFERENTE, REJEITA IMEDIATAMENTE
        if is_quadra_mismatch:
            continue

        # --- 5. FILTRO DE BAIRRO ---
        bairro_score = 0
        bairro_ok = True
        if tgt_bairro:
            if tgt_bairro in found_label or tgt_bairro in found_district:
                bairro_score = 1.0
            else:
                bairro_score = text_similarity(tgt_bairro, found_district)
            
            if bairro_score < 0.4:
                # Se for rua curta (RC 1), bairro errado é fatal
                if len(tgt_rua_raw) < 5: continue 
                bairro_ok = False

        # --- PONTUAÇÃO ---
        score = 0
        log = ""

        if tgt_q and found_q and tgt_q == found_q:
            score += 100
            log = "EXACT_MATCH"
        else:
            # Não achou quadra no texto (mas a rua e número bateram)
            score += 50
            log = "STREET_FOUND_NO_QUADRA"

        if not bairro_ok:
            score -= 30
            log = "BAIRRO_MISMATCH"

        if score > best_score:
            best_score = score
            best_pos = item.get("position", {})
            
            is_partial = True
            if score >= 100 and bairro_ok:
                is_partial = False
            
            best_candidate = (best_pos.get("lat"), best_pos.get("lng"), is_partial, False, log)

    return best_candidate


async def find_best_location(row):
    raw_addr = str(row.get("Destination Address", ""))
    bairro_input = str(row.get("Bairro", "")).strip()
    cidade_input = str(row.get("City", "Goiânia")).strip()
    
    normalized = normalize_address(raw_addr, bairro_input)
    
    if normalized == "Condominio":
        return "", "", False, True, "CONDOMINIO_DETECTED"

    target_q, target_l = extract_quadra_lote_values(raw_addr)
    if not target_q:
        target_q, target_l = extract_quadra_lote_values(normalized)

    base_rua = extract_street_base(normalized)
    
    # Objeto de dados para o validador
    target_data = {
        'base_rua': base_rua,
        'bairro': bairro_input,
        'cidade_alvo': cidade_input,
        'quadra': target_q,
        'lote': target_l
    }

    strategies = []
    
    # --- ESTRATÉGIAS DE BUSCA ---
    
    # Detecção de Código (MDV, RC, VB)
    match_code = re.search(r"\b(RUA|AV|ALAMEDA|AVENIDA)\s+([A-Z]{1,3})[-\s]*0*(\d+)\b", base_rua.upper())
    
    queries = []
    
    if match_code and target_q and target_l:
        prefix = match_code.group(1)
        code = match_code.group(2)
        num_raw = int(match_code.group(3))
        
        # 1. Formato "Chave de Ouro" (RC-001, 5-5) - Prioridade
        q1 = f"{prefix} {code}-{str(num_raw).zfill(3)}, {target_q}-{target_l}"
        queries.append(q1)
        
        # 2. Formato Simples (RC-1, 5-5)
        q2 = f"{prefix} {code}-{num_raw}, {target_q}-{target_l}"
        if q2 != q1: queries.append(q2)
        
    elif target_q and target_l:
        # Ruas normais (Avenida Toronto, 50-17)
        queries.append(normalized)
    else:
        queries.append(normalized)

    # Monta lista de estratégias com e sem contexto
    for q in queries:
        strategies.append({"q": f"{q}, {cidade_input} - Goiás", "type": "STRICT"}) # Tenta sem bairro (mas validador checa!)
        strategies.append({"q": f"{q}, {bairro_input}, {cidade_input}", "type": "CONTEXT"})

    # Fallback: Apenas Rua e Bairro (sem quadra/lote na query)
    clean_street = base_rua.replace("-", " ")
    strategies.append({"q": f"{clean_street}, {bairro_input}, {cidade_input}", "type": "STREET_ONLY"})

    # --- EXECUÇÃO ---
    
    final_result = ("Não encontrado", "Não encontrado", False, False, "FAILED")
    
    # Variável para guardar o melhor resultado encontrado nos loops
    # (Para não parar no primeiro erro, mas sim pegar o melhor de todos)
    global_best_score = -1 
    
    for strat in strategies:
        items, status = await geocode_with_here(strat["q"])
        if status != "OK" or not items: continue

        # Chama o validador
        result = pick_best_candidate(items, target_data)
        
        if result:
            lat, lng, partial, cond, log = result
            
            # Sistema de Pontuação para decidir se paramos ou continuamos tentando
            score = 0
            if "EXACT_MATCH" in log: score = 100
            elif "QUADRA_MISMATCH" in log: score = 50
            else: score = 30
            
            # Se achou um EXATO VERDE, para tudo e retorna.
            if score == 100:
                return result
            
            # Se for melhor que o que temos, guarda.
            if score > global_best_score:
                global_best_score = score
                return result

    # --- TENTATIVA VIZINHOS ---
    if target_q and target_l and global_best_score < 100:
        try:
            l_num = int(target_l)
            offsets = [-1, 1, -2, 2, -3, 3, -4, 4, -5, 5]
            
            for offset in offsets:
                new_lote = l_num + offset
                if new_lote <= 0: continue
                
                # Vizinho sempre usa contexto completo
                query = f"{base_rua}, {target_q}-{new_lote}, {bairro_input}, {cidade_input}"
                items, status = await geocode_with_here(query)
                
                if status == "OK" and items:
                    res_neigh = pick_best_candidate(items, target_data)
                    if res_neigh:
                        lat, lng, _, _, log = res_neigh
                        # Se o vizinho tem bairro certo e quadra certa (EXACT), usamos ele
                        if "EXACT_MATCH" in log:
                            return lat, lng, True, False, f"NEIGHBOR_LOTE_{offset}"
        except:
            pass

    return final_result