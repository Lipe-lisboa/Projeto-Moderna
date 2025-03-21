import pandas as pd
import calendar
import locale
from datetime import datetime
import os


datas_dict = {}

def datas_menssais (ano:int):
    
    # Para que os meses fiquem em português
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    
    for mes in range(1,13):
        
        #pega o ultimo dia de cada mês 
        _, ultimo_dia  = calendar.monthrange(ano,mes)
        
        if mes < 10:
            data_inicio = f'01/0{mes}/{ano}'
            data_limite = f'{ultimo_dia}/0{mes}/{ano}'
            
        else:
            data_inicio = f'01/{mes}/{ano}'
            data_limite = f'{ultimo_dia}/{mes}/{ano}'
            
        nome_mes = calendar.month_name[mes]
        
        #adiciona os dados no dicionario
        datas_dict[nome_mes] = {
            'data_inicio': data_inicio,
            'data_limite': data_limite
        }

def convert_parquet(df:pd.DataFrame, file:str):
    df.to_parquet(file, index=False)

def dividir_csv_por_mes(ano:int):
    datas_menssais(ano)

    file = 'Produtos_Homologados_Anatel.csv'

    try:
        df = pd.read_csv(file, sep=";", encoding="utf-8")  # Lendo diretamente para um DataFrame
        
        
        # Transformei a coluna "Data da Homologação" para o tipo: datetime
        df['Data do Certificado de Conformidade Técnica'] = pd.to_datetime(df['Data do Certificado de Conformidade Técnica'], format='%d/%m/%Y')

        
        for mes in datas_dict:
            
            data_string = datas_dict[mes]['data_inicio']
            partes_data = data_string.split('/')
            ano = partes_data[2]  # O ano é o terceiro elemento (índice 2)
            pasta_base_csv = 'arquivos_csv'
            pasta_base_parquet = 'arquivos_parquet'

            # Peguei as datas do dicionario e converti para o formato 'yyyy-mm-dd'
            data_inicio = datetime.strptime(datas_dict[mes]['data_inicio'], '%d/%m/%Y').strftime('%Y-%m-%d')
            data_limite = datetime.strptime(datas_dict[mes]['data_limite'], '%d/%m/%Y').strftime('%Y-%m-%d')
              
            # A filtragem só aceita datas do tipo: yyyy-mm-dd (por isso tive que converter as datas do dicionario)   
            df_filtrado = df[(df['Data do Certificado de Conformidade Técnica'] >= data_inicio) & (df['Data do Certificado de Conformidade Técnica'] <= data_limite)]
            
            
            df['Data do Certificado de Conformidade Técnica'] = df['Data do Certificado de Conformidade Técnica'].astype(str)

            
            # Se "df_filtrado" não estiver vazio:
            if  not df_filtrado.empty:
                
                #Criando a pasta do ano, dentro da pasta: pasta_base_parquet
                pasta_ano = os.path.join(pasta_base_parquet, ano)
                if not os.path.exists(pasta_ano):
                    os.makedirs(pasta_ano)
                
                # convertendo csv em parquet
                convert_parquet(df_filtrado, f'{pasta_ano}/certificados_de_{mes}.parquet')
            
    except FileNotFoundError:
        print(f"Erro: Arquivo '{file}' não encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")