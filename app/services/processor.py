import asyncio
import re
import pandas as pd
from app.services.geocoder import geocode_with_here
from app.services.normalizer import (
    normalizar_endereco, 
    extrair_valores_quadra_lote, 
    extrair_base_rua,
    extrair_numeros,
    similaridade_texto
)

def log_candidato(idx, status, msg, detalhes=""):
    print(f"   -> Cand {idx}: [{status}] {msg} | {detalhes}")

def selecionar_melhor_candidato(lista_candidatos, dados_alvo):
    melhor_candidato = None
    melhor_pontuacao = -1
    
    print("\n--- INICIO ANALISE DE CANDIDATOS ---")

    # Extração dados alvo
    rua_alvo = dados_alvo['base_rua'].upper()
    numeros_alvo = extrair_numeros(rua_alvo)
    set_numeros_alvo = {int(n) for n in numeros_alvo} if numeros_alvo else set()
    
    bairro_alvo = dados_alvo['bairro'].upper()
    cidade_alvo = dados_alvo['cidade_alvo'].upper()
    quadra_alvo = dados_alvo['quadra']
    lote_alvo = dados_alvo['lote']
    
    print(f"ALVO: Rua: {rua_alvo} | Q: {quadra_alvo} L: {lote_alvo} | Bairro: {bairro_alvo}")

    if pd.isna(quadra_alvo) or pd.isna(lote_alvo):
        print("!!! FALHA CRÍTICA: Sem Quadra/Lote no Alvo")
        return ("Não encontrado", "Não encontrado", False, False, "FAILED_NO_QD_LT_TARGET")

    for idx, candidato in enumerate(lista_candidatos):
        endereco = candidato.get("address", {})
        rua_encontrada = endereco.get("street", "").upper()
        bairro_encontrado = endereco.get("district", "").upper()
        numero_encontrado = endereco.get("houseNumber", "").upper()
        rotulo = endereco.get("label", "")
        
        pontuacoes = candidato.get("scoring", {}).get("fieldScore", {})
        posicao = candidato.get("position", {})

        # 1. Exact Match HERE
        score_cidade = pontuacoes.get("city", 0)
        score_numero = pontuacoes.get("houseNumber", 0)
        score_rua_lista = pontuacoes.get("streets", [0])
        score_rua = score_rua_lista[0] if score_rua_lista else 0

        if (score_cidade == 1.0 and score_numero == 1.0 and score_rua >= 0.83):
            log_candidato(idx, "SUCESSO", "API Exact Match (100%)", f"Rua: {rua_encontrada}")
            return (posicao.get("lat"), posicao.get("lng"), False, False, "EXACT_MATCH_API")

        # 2. Validação Cidade
        cidade_encontrada = endereco.get("city", "").upper()
        if cidade_alvo and cidade_alvo not in cidade_encontrada:
             if similaridade_texto(cidade_alvo, cidade_encontrada) < 0.8:
                log_candidato(idx, "IGNORADO", "Cidade Divergente", cidade_encontrada)
                continue 

        # Limpeza rua
        rua_limpa = rua_encontrada.split("QUADRA")[0].strip()

        # 3. Validação Números Rua
        if set_numeros_alvo:
            nums_enc = extrair_numeros(rua_limpa)
            if nums_enc:
                set_enc = {int(n) for n in nums_enc}
                if not set_numeros_alvo.intersection(set_enc):
                    log_candidato(idx, "IGNORADO", "Número da Rua Incompatível", f"Alvo:{set_numeros_alvo} vs Enc:{set_enc}")
                    continue

        # 4. Validação Quadra
        q_enc, _ = extrair_valores_quadra_lote(rotulo)
        if not q_enc: q_enc, _ = extrair_valores_quadra_lote(rua_encontrada)
        if not q_enc: q_enc, _ = extrair_valores_quadra_lote(numero_encontrado)

        if quadra_alvo and q_enc and quadra_alvo != q_enc:
            log_candidato(idx, "REJEITADO", "Quadra Diferente", f"Alvo:{quadra_alvo} vs Enc:{q_enc}")
            continue

        # 5. Validação Bairro
        bairro_valido = True
        if bairro_alvo:
            if bairro_alvo not in rotulo.upper() and bairro_alvo not in bairro_encontrado:
                sim = similaridade_texto(bairro_alvo, bairro_encontrado)
                if sim < 0.45:
                    if len(rua_alvo) < 6: 
                        log_candidato(idx, "REJEITADO", "Bairro Errado (Rua Curta)", f"{bairro_alvo} vs {bairro_encontrado}")
                        continue 
                    bairro_valido = False

        # --- Pontuação ---
        pontuacao = 0
        log_txt = ""

        if quadra_alvo and q_enc and quadra_alvo == q_enc:
            pontuacao += 100
            log_txt = "MATCH_QUADRA_OK"
        else:
            pontuacao += 50
            log_txt = "MATCH_RUA_ONLY"

        if not bairro_valido:
            pontuacao -= 30
            log_txt += "_BAIRRO_MISMATCH"

        log_candidato(idx, "CANDIDATO", f"Score: {pontuacao}", f"Log: {log_txt} | Rua: {rua_encontrada}")

        if pontuacao > melhor_pontuacao:
            melhor_pontuacao = pontuacao
            e_parcial = not (pontuacao >= 100 and bairro_valido)
            melhor_candidato = (posicao.get("lat"), posicao.get("lng"), e_parcial, False, log_txt)

    return melhor_candidato


async def buscar_melhor_localizacao(linha_planilha):
    print("\n" + "="*60)
    endereco_bruto = str(linha_planilha.get("Destination Address", ""))
    print(f"[INPUT RAW]: {endereco_bruto}")
    
    bairro_input = str(linha_planilha.get("Bairro", "")).strip()
    cidade_input = str(linha_planilha.get("City", "Goiânia")).strip()
    
    # 1. Normalização
    endereco_normalizado = normalizar_endereco(endereco_bruto, bairro_input)
    print(f"[NORMALIZED]: {endereco_normalizado}")
    
    if endereco_normalizado == "Condominio":
        return "", "", False, True, "CONDOMINIO_DETECTED"

    target_q, target_l = extrair_valores_quadra_lote(endereco_bruto)
    # Tenta extrair do normalizado se falhar no bruto
    if not target_q:
        target_q, target_l = extrair_valores_quadra_lote(endereco_normalizado)

    print(f"[EXTRACTED]: Q: {target_q} | L: {target_l}")

    base_rua = extrair_base_rua(endereco_normalizado)
    
    dados_alvo = {
        'base_rua': base_rua,
        'bairro': bairro_input,
        'cidade_alvo': cidade_input,
        'quadra': target_q,
        'lote': target_l
    }

    estrategias = []
    
    # Estratégia 1: Normalizado (Melhor caso: RC-017, 10-20)
    estrategias.append({"q": f"{endereco_normalizado}, {cidade_input}", "type": "NORMALIZED"})
    
    # Estratégia 2: Se tiver Q/L, tenta formato explícito
    if target_q and target_l:
         estrategias.append({"q": f"{base_rua}, QD {target_q} LT {target_l}, {cidade_input}", "type": "EXPLICIT_QL"})

    # Estratégia 3: Rua e Bairro apenas
    rua_limpa = base_rua.replace("-", " ") 
    estrategias.append({"q": f"{rua_limpa}, {bairro_input}, {cidade_input}", "type": "STREET_ONLY"})

    melhor_resultado_global = ("Não encontrado", "Não encontrado", False, False, "FAILED")
    maior_score_global = -1 
    
    for strat in estrategias:
        print(f"[SEARCH QUERY]: {strat['q']} ({strat['type']})")
        
        itens_retornados, status = await geocode_with_here(strat["q"])
        if status != "OK" or not itens_retornados: 
            print("   -> 0 resultados encontrados.")
            continue

        print(f"   -> {len(itens_retornados)} candidatos encontrados.")
        resultado = selecionar_melhor_candidato(itens_retornados, dados_alvo)
        
        if resultado:
            lat, lng, parcial, cond, log = resultado
            
            score_atual = 100 if "MATCH_QUADRA_OK" in log or "EXACT" in log else 50
            if "BAIRRO_MISMATCH" in log: score_atual -= 20
            
            print(f"   -> [RESULTADO ESTRATÉGIA]: {log} (Score: {score_atual})")
            
            if score_atual >= 100:
                print("[DECISÃO]: Match Perfeito encontrado. Encerrando busca.")
                return resultado
            
            if score_atual > maior_score_global:
                maior_score_global = score_atual
                melhor_resultado_global = resultado

    # Vizinhos...
    if target_q and target_l and maior_score_global < 100:
        print("[NEIGHBOR]: Tentando busca por vizinhos...")
        # (Código dos vizinhos mantido igual, apenas adicione prints se necessário)
        pass

    return melhor_resultado_global