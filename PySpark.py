from pyspark.sql import SparkSession as SS, functions as F
from pyspark.sql.functions import col
import pandas as pd

def certificados_moderna():
    spark = SS.builder.appName( "Projeto" ).getOrCreate() 
    df = spark.read.parquet("arquivos_parquet/2025/certificados_de_março.parquet")

    coluna = "Certificado de Conformidade Técnica"

    df_filtrado = df.filter(F.col(coluna).contains("MODERNA")).select(coluna)

    quantidade_certificados = df_filtrado.distinct().count()
    
    return quantidade_certificados

#print(certificados_moderna())


spark = SS.builder.appName("LerCSV").getOrCreate()

df = spark.read.csv(
    "Produtos_Homologados_Anatel.csv",
    header=True,
    sep=";",
    inferSchema=True,
    encoding='utf-8',
)

lista_d = [
    {'março':'03/2025'}
]

for dicionario in lista_d:
    for mes, data in dicionario.items():
    
        # Filtra as linhas onde "Data do Certificado de Conformidade Técnica" contém "03/2025"
        df_filtrado = df.filter(col("Data do Certificado de Conformidade Técnica").contains(data))
        
        if  df_filtrado != None:
            # Coleta os dados como uma lista de linhas
            dados = df_filtrado.collect()

            # Cria um Pandas DataFrame a partir dos dados
            pandas_df = pd.DataFrame(dados, columns=df_filtrado.columns)
            file = f'arquivos_parquet/2025/certificados_de_{mes}.parquet'
            pandas_df.to_parquet(file, index=None)
        

spark.stop()
