import pandas as pd
import os
from pyspark.sql import SparkSession as SS, functions as F
from pyspark.sql.functions import col


def convert_parquet(df:pd.DataFrame, file:str):
    df.to_parquet(file, index=False)

def dividir_csv_por_mes(ano:int):
    lista_mes = [
        {'janeiro': '01'},
        {'fevereiro': '02'},
        {'março':'03'},
        {'abril': '04'},
        {'maio': '05'},
        {'junho': '06'},
        {'julho': '07'},
        {'agosto': '08'},
        {'setembro': '09'},
        {'outubro': '10'},
        {'novembro': '11'},
        {'dezembro': '12'},
    ]
    
    try:
        spark = SS.builder.appName("LerCSV").getOrCreate()
        file = 'Produtos_Homologados_Anatel.csv'
        df = spark.read.csv(
            file,
            header=True,
            sep=";",
            inferSchema=True,
            encoding='utf-8',
        )        
        
        for dicionario in lista_mes:
            for mes, data in dicionario.items():
    
                pasta_base_parquet = 'arquivos_parquet'
                
                # Filtra as linhas onde "Data do Certificado de Conformidade Técnica" contém "03/2025"
                df_filtrado = df.filter(col("Data do Certificado de Conformidade Técnica").contains(f'{data}/{str(ano)}'))
                
                # Coleta os dados como uma lista de linhas
                dados = df_filtrado.collect()
                    
                # Se "df_filtrado" não estiver vazio:
                if len(dados) != 0:

                    # Cria um Pandas DataFrame a partir dos dados
                    pandas_df = pd.DataFrame(dados, columns=df_filtrado.columns)
                    
                    #Criando a pasta do ano, dentro da pasta: pasta_base_parquet
                    pasta_ano = os.path.join(pasta_base_parquet, str(ano))
                    if not os.path.exists(pasta_ano):
                        os.makedirs(pasta_ano)
                    
                    # convertendo csv em parquet
                    convert_parquet(pandas_df, f'{pasta_ano}/certificados_de_{mes}.parquet')
        
        spark.stop()
        
    except FileNotFoundError:
        print(f"Erro: Arquivo '{file}' não encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")