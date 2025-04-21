from fastapi import FastAPI, Query
from pyspark.sql import SparkSession as SS, functions as F
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql.functions import lpad, col
from pyspark.sql.types import StringType

import pandas as pd
import regex
import re
from typing import Optional

def ocds(ano, mes):
    try:
        df = pd.read_parquet(f'../arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return
        
    list_ocds = []
    
    for certificado in df['Certificado de Conformidade Técnica']:
        
        
        iniciais_certificado = certificado[0:2]
        
        if iniciais_certificado == '7C':
            if iniciais_certificado not in list_ocds:
                list_ocds.append(iniciais_certificado)
        
        #Deixa apenas letras e traço
        ocd = regex.sub(r'[^a-zA-Z\p{L}-_/ ]+', '', certificado).strip('/- ')
        

        #Adiciona o ocd na lista de ocds
        if ocd not in list_ocds:
            if not 'TESTE' in ocd.upper() and len(ocd) > 1 and ocd != '':
                list_ocds.append(ocd)

    return list_ocds

def certificados(ano, mes):
    try:
        df = pd.read_parquet(f'../arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return
        
    
    #lista de certificados com apenas numeros 
    lista_certificado_digitos = []
    
    #todos certificados 
    todos_certificados = []
    
    #qtd total de certificados 
    qtd_total = 0
    
    for certificado in df['Certificado de Conformidade Técnica']:
        
        if certificado not in todos_certificados:
            todos_certificados.append(certificado)
            qtd_total += 1
        
        contem_letra = bool(re.search(r'[a-zA-Z]', certificado))
        if not contem_letra:
            if certificado not in lista_certificado_digitos:
                lista_certificado_digitos.append(certificado)
        
    return lista_certificado_digitos, qtd_total

def function_tipo_certificado(ano, mes):
    try:
        df = pd.read_parquet(f'../arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return
    
    abreviacao_ano = str(ano)[2:4]
    
    homolocacoes_iniciais = []
    qtd_caracteres = []
    qtd_homolocacoes_iniciais = 0
    for numero_homologacao in df['Número de Homologação']:
        numero_homologacao = str(numero_homologacao)
        qtd_caracter = len(numero_homologacao)
        
        if qtd_caracter not in qtd_caracteres:
            qtd_caracteres.append(qtd_caracter)
            
        
        ano_certicado = None
        if qtd_caracter < 12:
            qtd_zero = 12 - qtd_caracter
            numero_homologacao = (qtd_zero * '0')+numero_homologacao 
            
    
        ano_certicado = numero_homologacao[5:7]
        
        if int(ano_certicado) > int(abreviacao_ano):
            print('ano do certificado:',ano_certicado)
            

            
        if abreviacao_ano == ano_certicado:
            if numero_homologacao not in homolocacoes_iniciais:
                homolocacoes_iniciais.append(numero_homologacao)
                qtd_homolocacoes_iniciais += 1
    
    print('quantidade de caracteres:',qtd_caracteres)
    return qtd_homolocacoes_iniciais
            

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
def certificados_ocds(
    ano:int, 
    mes:str, 
    produtos: Optional[str] = Query(None, description="Opções de produto (separadas por vírgula)"),
    tipo_certificado: Optional[str] = Query(None, description="Opções de tipo de ceritficado"),
    ):
    spark = SS.builder.appName( "Projeto" ).getOrCreate()
    
    try:
        df = spark.read.parquet(f'../arquivos_parquet/{str(ano)}/certificados_de_{mes.lower()}.parquet')
    except FileNotFoundError:
        return "Arquivo não encontrado"
    except AnalysisException as e:
        return f"Erro ao tentar ler o arquivo Parquet: {e} "

    coluna_certificados = "Certificado de Conformidade Técnica"
    coluna_nm_homologacao = "Número de Homologação"

    
    lista_ocd = ocds(ano, mes) 
#    lista_certificado_digitos, qtd_total  = certificados(ano, mes)
    
#    certificacao_inicial = function_tipo_certificado(ano, mes)
#    print('qtd certificacoes iniciais:',certificacao_inicial)

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
        if tipo_certificado is not None:
            abreviacao_ano = str(ano)[2:4]
            
            # Converter a coluna para StringType e aplicar o padding
            df_padronizado = df.withColumn(
                coluna_nm_homologacao,  # Nome da coluna a ser editada
                lpad(col(coluna_nm_homologacao).cast(StringType()), 12, "0")
            )

        for ocd in lista_ocd:
            if produtos is not None:
                list_p = produtos.split(',')
                quantidade_certificados_list = []
                for p in list_p:
                    df_filtrado = df.filter(F.col(coluna_certificados).contains(ocd))
                    df_filtrado = df_filtrado.filter(F.upper(F.col("Tipo do Produto")).contains(p.upper().strip())).select(coluna_certificados)
                    quantidade_certificados_list.append(df_filtrado.distinct().count())
             
                quantidade_certificados = sum(quantidade_certificados_list)
                
            elif tipo_certificado is not None:

                df_filtrado_inicial = df_padronizado.filter(F.substring(F.col(coluna_nm_homologacao), 6, 2) == abreviacao_ano)
                quantidade_certificados = df_filtrado_inicial.select(coluna_nm_homologacao).distinct().count()
                
                df_filtrado_ocd = df_filtrado_inicial.filter(F.col(coluna_certificados).contains(ocd))
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
    