from pyspark.sql import SparkSession as SS, functions as F


spark = SS.builder.appName( "Projeto" ).getOrCreate() 
df = spark.read.parquet("arquivos_parquet/2025/certificados_de_março.parquet")

coluna = "Certificado de Conformidade Técnica"

df_filtrado = df.filter(F.col(coluna).contains("MODERNA")).select(coluna)

quantidade_certificados = df_filtrado.distinct().count()
print('certificados:',quantidade_certificados)
print(spark.version)