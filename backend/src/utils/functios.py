import pandas as pd
import regex
import re

# Função para extrair os OCDs dos certificados
def ocds(df: pd.DataFrame) -> list:

    # Verifica se o DataFrame está vazio antes de tentar processar os certificados
    if df.empty:
        return []
        
    list_ocds = []
    
    # Itera sobre cada certificado na coluna do DataFrame
    for certificado in df:
        
        # Extrai as duas primeiras letras do certificado para verificar se é um certificado do tipo '7C'
        iniciais_certificado = certificado[0:2]
        
        # Se as iniciais do certificado forem '7C', adiciona '7C' à lista de OCDs (se ainda não estiver presente)
        if iniciais_certificado == '7C':
            if iniciais_certificado not in list_ocds:
                list_ocds.append(iniciais_certificado)
        
        # Remove o primeiro número encontrado e TUDO o que vem depois dele
        certificado_cortado = regex.split(r'\d', certificado)[0]

        # Remove caracteres indesejados, mantendo apenas letras, hífens, underscores e espaços.
        # Também remove espaços, hífens e underscores extras no início e no final.
        ocd = regex.sub(r'[^a-zA-Z\p{L}-_/ ]+', '', certificado_cortado).strip('/- ')

        # Adiciona o ocd na lista de ocds
        if ocd not in list_ocds:
            if not 'TESTE' in ocd.upper() and len(ocd) > 1 and ocd != '':
                list_ocds.append(ocd)

    return list_ocds
