from fastapi import FastAPI
from pyspark.sql import SparkSession as SS, functions as F


# uvicorn api:app --reload
app = FastAPI()

@app.get("/")
def hello_world_root():
    return {"Hello": "World"}

menu = [
    {   'id': 1,
        'name': 'coffee',
        'price': 2.5
     },
    {
        'id': 2,
        'name': 'cake',
        'price': 10
    },
    {
        'id': 3,
        'name': 'tea',
        'price': 3.2
    },
    {
        'id': 4,
        'name': 'croissant',
        'price': 5.79
    }
]

@app.get("/items/{item_id}")
def get_item(item_id: int):

    search = list(filter(lambda x: x["id"] == item_id, menu))

    if search == []:
        return {'Error': 'Item does not exist'}

    return {'Item': search[0]}


@app.get("/certificados/{ocd_enviado}")
def certificados_ocd(ocd_enviado: str,ano:int, mes:str):
    spark = SS.builder.appName( "Projeto" ).getOrCreate()
    try:
        df = spark.read.parquet(f'../arquivos_parquet/{str(ano)}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        return "arquivo não encontrado"

    coluna = "Certificado de Conformidade Técnica"

    df_filtrado = df.filter(F.col(coluna).contains(ocd_enviado.upper())).select(coluna)
    
    if df_filtrado:
        quantidade_certificados = df_filtrado.distinct().count()
        
        saida = {
            'ocd':ocd_enviado.upper(),
            'quantidade_de_certificado':quantidade_certificados
        }
    
    else:
        saida = 'OCD não encontrado'
    
    
    return saida
