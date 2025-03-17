import pandas as pd
import re
from unidecode import unidecode

def certificados_ocd(ocd_enviado,ano, mes):
    
    try:
        df = pd.read_parquet(f'arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return

    list_certificado = []
    list_ocds = []
    data = []

    #adiciona itens em list_ocds e list_certificado
    for certificado in df['Certificado de Conformidade Técnica']:
        
        certificado = unidecode(certificado).upper()
        
        #Deixa apenas letras e traço
        ocd = re.sub(r'[^a-zA-Z-]+', '', certificado).strip()
        
        # Remove traços no início da string
        ocd = re.sub(r'^-+', '', ocd)

        # Remove traços no final da string
        ocd = re.sub(r'-+$', '', ocd)

        #Adiciona o ocd na lista de ocds
        if ocd not in list_ocds:
            list_ocds.append(ocd)
        
        
        #Adiciona todos os certificados em uma lista
        if certificado not in list_certificado: 
            list_certificado.append(certificado) 
    
    qtd_certificados_ocd = 0
    # Adiciona itens em data
    for ocd in list_ocds:
        for certificado in list_certificado:
            if ocd in certificado:
                qtd_certificados_ocd += 1
        
        #Adiciona o ocd e a quantidade de certificados que ele tem
        data.append(
            {
                'ocd':ocd,
                'value':qtd_certificados_ocd
            }
        )
        qtd_certificados_ocd = 0  
        
    #Seleciona o item que contem as informações do ocd enviado
    item = next((dicionario for dicionario in data if dicionario['ocd'] == ocd_enviado.upper()), None)
    
    if item:
        print(item)
        
    else:
        print('OCD não identificado.')
        

certificados_ocd('MODERNA', 2025, 'fevereiro')

