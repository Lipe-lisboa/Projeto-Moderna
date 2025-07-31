from fastapi import FastAPI, Query
from pyspark.sql import SparkSession as SS, functions as F
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql.functions import lpad, col
from pyspark.sql.types import StringType
from typing import Optional
from modules.functios import ocds
from index import *

app = FastAPI()

@app.get("/criar parquets")
def criar_parquets(
    ano:int, 
    ):
    try:
        extraindo_arquivos_parquets(ano)
        return {"message": f"Arquivos Parquet para o ano {ano} foram criados com sucesso."}
    except Exception as e:
        return {"error": str(e)}

   



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



@app.get("/certificados")
def certificados_ocds(
    ano:int, 
    mes:str, 
    tipo_certificado: Optional[str] = Query(None, description="Opções de tipo de ceritficado"),
    ):
    spark = SS.builder.appName( "Projeto" ).getOrCreate()
    
    try:
        df = spark.read.parquet(f'../arquivos_parquet/{str(ano)}/certificados_de_{mes}.parquet')
    except FileNotFoundError:
        return "Arquivo não encontrado"
    except AnalysisException as e:
        return f"Erro ao tentar ler o arquivo Parquet: {e} "

    coluna_certificados = "Certificado de Conformidade Técnica"
    coluna_nm_homologacao = "Número de Homologação"

    
    lista_ocd = ocds(ano, mes) 

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
        'TELECOM': 'INTERTEK',
        'IBR': 'IBR TECH',
        'TEL': 'ACTA',
        '7C': 'SEVEN COMPLIANCE'
    }
    
    if lista_ocd:

        for ocd in lista_ocd:
                
            if tipo_certificado is not None:
                abreviacao_ano = str(ano)[2:4]
            
                # Converter a coluna para StringType e aplicar o padding
                df_padronizado = df.withColumn(
                    coluna_nm_homologacao,  # Nome da coluna a ser editada
                    lpad(col(coluna_nm_homologacao).cast(StringType()), 12, "0")
                )

                if tipo_certificado.lower() == "inicial":
                    df_filtrado_inicial = df_padronizado.filter(F.substring(F.col(coluna_nm_homologacao), 6, 2) == abreviacao_ano)
                    quantidade_certificados = df_filtrado_inicial.select(coluna_nm_homologacao).distinct().count()
                    
                    df_filtrado_ocd = df_filtrado_inicial.filter(F.col(coluna_certificados).contains(ocd))
                    quantidade_certificados = df_filtrado_ocd.select(coluna_certificados).distinct().count()  
                    
                elif tipo_certificado.lower() == "manutenção":
                    df_filtrado_manutencao = df_padronizado.filter(F.substring(F.col(coluna_nm_homologacao), 6, 2) != abreviacao_ano)
                    quantidade_certificados = df_filtrado_manutencao.select(coluna_nm_homologacao).distinct().count()
                    
                    df_filtrado_ocd = df_filtrado_manutencao.filter(F.col(coluna_certificados).contains(ocd))
                    quantidade_certificados = df_filtrado_ocd.select(coluna_certificados).distinct().count()
                      
            else:
                df_filtrado = df.filter(F.col(coluna_certificados).contains(ocd)).select(coluna_certificados)
                quantidade_certificados = df_filtrado.distinct().count()
                

            if quantidade_certificados:
                if ocd in dict_name:
                    ocd = dict_name[ocd]
                saida.append({
                    'ocd':ocd.upper(),
                    'quantidade_de_certificado':quantidade_certificados
                })
            
            
        if 'ABCP-OCD' in lista_ocd and 'OCD' in lista_ocd:
            cert_abcp = 0
            for dicionario in saida:
                if dicionario['ocd'] == 'ABCP':
                    cert_abcp = dicionario['quantidade_de_certificado']
            
            for dicionario in saida:
                if dicionario['ocd'] == 'BRICS':
                    if dicionario['quantidade_de_certificado'] > cert_abcp:
                        dicionario['quantidade_de_certificado'] = dicionario['quantidade_de_certificado'] - cert_abcp
        
        # Ordena a lista pelo valor (chave 'valor') em ordem decrescente
        saida_ordenada = sorted(saida, key=lambda item: item['quantidade_de_certificado'], reverse=True)        
        return saida_ordenada
    
    
