import pandas as pd
import calendar
import locale
from datetime import datetime


datas_dict = {}

def datas_menssais (ano):
    datas_dict = {ano: []}
    

    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    
    for mes in range(1,13):
        _, ultimo_dia  = calendar.monthrange(ano,mes)
        
        if mes < 10:
            data_inicio = f'01/0{mes}/{ano}'
            data_limite = f'{ultimo_dia}/0{mes}/{ano}'
            
        else:
            data_inicio = f'01/{mes}/{ano}'
            data_limite = f'{ultimo_dia}/{mes}/{ano}'
            
        nome_mes = calendar.month_name[mes]
        
        datas_dict[nome_mes] = {
            'data_inicio': data_inicio,
            'data_limite': data_limite
        }
datas_menssais(2025)



def dividir_csv_por_mes():
    file = 'arquivos_csv/Produtos_Homologados_Anatel.csv'

    try:
        df = pd.read_csv(file, sep=";", encoding="utf-8")  # Lendo diretamente para um DataFrame

        # Transformei a coluna "Data da Homologação" para o tipo: datetime
        df['Data da Homologação'] = pd.to_datetime(df['Data da Homologação'], format='%d/%m/%Y')
        
        for mes in datas_dict:
            # Peguei as datas do dicionario e converti para o formato 'yyyy-mm-dd'
            data_inicio = datetime.strptime(datas_dict[mes]['data_inicio'], '%d/%m/%Y').strftime('%Y-%m-%d')
            data_limite = datetime.strptime(datas_dict[mes]['data_limite'], '%d/%m/%Y').strftime('%Y-%m-%d')
              
            #A filtragem só aceita datas do tipo: yyyy-mm-dd (por isso tive que converter as datas do dicionario)   
            df_filtrado = df[(df['Data da Homologação'] >= data_inicio) & (df['Data da Homologação'] <= data_limite)]
            
            # Se "df_filtrado" não estiver vazio:
            if  not df_filtrado.empty:
                # pega o df filtrado e transforma em um arquivo csv 
                df_filtrado.to_csv(f'arquivos_csv/Homologacoes_de_{mes}.csv', encoding='utf-8', index=False, sep=';')
            
                
    except FileNotFoundError:
        print(f"Erro: Arquivo '{file}' não encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

dividir_csv_por_mes()
        