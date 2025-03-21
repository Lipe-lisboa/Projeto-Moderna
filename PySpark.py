from pyspark.sql import SparkSession as SS, functions as F
from teste import ocds


spark = SS.builder.appName( "Projeto" ).getOrCreate() 
df = spark.read.parquet("arquivos_parquet/2025/certificados_de_março.parquet")

coluna = "Certificado de Conformidade Técnica"

df_filtrado = df.filter(F.col(coluna).contains("MODERNA")).select(coluna)

quantidade_certificados = df_filtrado.distinct().count()
#print('certificados:',quantidade_certificados)


def certificados_ocds(ano:int, mes:str):
    try:
        df = spark.read.parquet(f'arquivos_parquet/{str(ano)}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        return "arquivo não encontrado"

    coluna = "Certificado de Conformidade Técnica"

    lista_ocd = ocds(ano, mes) 

    saida = []

    for ocd in lista_ocd:
        df_filtrado = df.filter(F.col(coluna).contains(ocd)).select(coluna)
        quantidade_certificados = df_filtrado.distinct().count()
        
        saida.append({
            'ocd':ocd,
            'quantidade_de_certificado':quantidade_certificados
        })
    
    return saida


certificados = certificados_ocds(2024, 'janeiro')
print(certificados)
for c in certificados:
    print(c)