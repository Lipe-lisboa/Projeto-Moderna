from fastapi import FastAPI
from pyspark.sql import SparkSession as SS, functions as F
import pandas as pd
import regex


def ocds(ano, mes):
    try:
        df = pd.read_parquet(f'../arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return
    
    list_ocds = []

    #adiciona itens em list_ocds e list_certificado
    for certificado in df['Certificado de Conformidade Técnica']:
        
        #Deixa apenas letras e traço
        ocd = regex.sub(r'[^a-zA-Z\p{L}-_/ ]+', '', certificado).strip('/- ')
        

        #Adiciona o ocd na lista de ocds
        if ocd not in list_ocds:
            if ocd != '':
                list_ocds.append(ocd)

    return list_ocds

# uvicorn api:app --reload
app = FastAPI()

@app.get("/certificados/{ocd_enviado}")
def certificados_ocd(ocd_enviado: str,ano:int, mes:str):
    spark = SS.builder.appName( "Projeto" ).getOrCreate()
    
    try:
        df = spark.read.parquet(f'../arquivos_parquet/{str(ano)}/certificados_de_{mes.lower()}.parquet')
        
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


@app.get("/certificados/{mes}/{ano}")
def certificados_ocds(ano:int, mes:str):
    spark = SS.builder.appName( "Projeto" ).getOrCreate()
    
    try:
        df = spark.read.parquet(f'../arquivos_parquet/{str(ano)}/certificados_de_{mes.lower()}.parquet')
    except FileNotFoundError:
        return "Arquivo não encontrado"


    coluna = "Certificado de Conformidade Técnica"

    lista_ocd = ocds(ano, mes) 

    saida = []

    if lista_ocd:
        for ocd in lista_ocd:
            df_filtrado = df.filter(F.col(coluna).contains(ocd)).select(coluna)
            quantidade_certificados = df_filtrado.distinct().count()
            
            saida.append({
                'ocd':ocd,
                'quantidade_de_certificado':quantidade_certificados
            })
        
        return saida