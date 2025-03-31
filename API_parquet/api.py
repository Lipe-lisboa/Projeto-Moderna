from fastapi import FastAPI, Query
from pyspark.sql import SparkSession as SS, functions as F
import pandas as pd
import regex
from pyspark.errors.exceptions.captured import AnalysisException
from typing import Optional
from enum import Enum

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
            if not 'TESTE' in ocd.upper() and len(ocd) > 1 and ocd != '':
                list_ocds.append(ocd)

    return list_ocds

def tipo_produto(ano,mes):
    try:
        df = pd.read_parquet(f'../arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return
    
    list_tp_produto = []
    #adiciona itens em list_ocds e list_certificado
    for produto in df['Tipo do Produto']:
        
        if produto not in list_tp_produto:
            list_tp_produto.append(produto)

    return list_tp_produto

app = FastAPI()

@app.get("/certificados/{ocd_enviado}")
def certificados_ocd(ocd_enviado: str,ano:int, mes:str):
    spark = SS.builder.appName( "Projeto" ).getOrCreate()
    
    try:
        df = spark.read.parquet(f'../arquivos_parquet/{str(ano)}/certificados_de_{mes.lower()}.parquet')
        
    except FileNotFoundError:
        return "arquivo não encontrado"
    except AnalysisException as e:
        return f"Erro ao tentar ler o arquivo Parquet: {e} "

    coluna = "Certificado de Conformidade Técnica"

    df_filtrado = df.filter(F.col(coluna).contains(ocd_enviado)).select(coluna)
    
    if df_filtrado:
        quantidade_certificados = df_filtrado.distinct().count()
        
        saida = {
            'ocd':ocd_enviado.upper(),
            'quantidade_de_certificado':quantidade_certificados
        }
    
    else:
        saida = 'OCD não encontrado'
    
    
    return saida


@app.get("/certificados")
def certificados_ocds(ano:int, mes:str,  produto: Optional[str] = None):
    spark = SS.builder.appName( "Projeto" ).getOrCreate()
    
    try:
        df = spark.read.parquet(f'../arquivos_parquet/{str(ano)}/certificados_de_{mes.lower()}.parquet')
    except FileNotFoundError:
        return "Arquivo não encontrado"
    except AnalysisException as e:
        return f"Erro ao tentar ler o arquivo Parquet: {e} "

    coluna_certificados = "Certificado de Conformidade Técnica"

    lista_ocd = ocds(ano, mes) 
    print(len(lista_ocd))

    saida = []

    dict_name = {
        'UL-BR': 'UL',
        'OCP': 'OCPTELLI',
        'ABCP-OCD':'ABCP',
        'OCD':'BRICS',
        'MT':'MASTER',
        'ELD':'ELDORADO',
        'QC':'QCCERT',
        'TÜV': 'TUV',
        'BRC': 'BRACERT',
        'BRA': 'BR APPROVAL',
    }



    if lista_ocd:
        for ocd in lista_ocd:

            # .select(coluna_certificados)

            if produto is not None:
                df_filtrado = df.filter(F.col(coluna_certificados).contains(ocd))
                df_filtrado = df_filtrado.filter(F.upper(F.col("Tipo do Produto")).contains(produto.upper())).select(coluna_certificados)
                quantidade_certificados = df_filtrado.distinct().count()
            else:
                df_filtrado = df.filter(F.col(coluna_certificados).contains(ocd)).select(coluna_certificados)
                quantidade_certificados = df_filtrado.distinct().count()

            if quantidade_certificados != 0:
                if ocd in dict_name:
                    ocd = dict_name[ocd]
                saida.append({
                    'ocd':ocd.upper(),
                    'quantidade_de_certificado':quantidade_certificados
                })
            
            
            if 'OCD-ABCP' in lista_ocd and 'OCD' in lista_ocd:
                cert_abcp = None
                for dicionario in saida:
                    if dicionario['ocd'] == 'ABCP':
                        cert_abcp = dicionario['quantidade_de_certificado']
                for dicionario in saida:
                    if dicionario['ocd'] == 'BRICS':
                        dicionario['quantidade_de_certificado'] = dicionario['quantidade_de_certificado'] - cert_abcp
        return saida