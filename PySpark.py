from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName( "ReadingParquet" ).getOrCreate() 

df = spark.read.parquet( "arquivos_parquet/2025/Homologacoes_de_março.parquet" ) 
df.show() 