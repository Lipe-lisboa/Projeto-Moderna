from fastapi import FastAPI, Query
from pyspark.sql import SparkSession as SS, functions as F
import pandas as pd
import regex
import re
from pyspark.errors.exceptions.captured import AnalysisException
from typing import Optional

def ocds(ano, mes):
    try:
        df = pd.read_parquet(f'../arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return
        

    list_ocds = []
    
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
                
        
        #Deixa apenas letras e traço
        ocd = regex.sub(r'[^a-zA-Z\p{L}-_/ ]+', '', certificado).strip('/- ')
        

        #Adiciona o ocd na lista de ocds
        if ocd not in list_ocds:
            if not 'TESTE' in ocd.upper() and len(ocd) > 1 and ocd != '':
                list_ocds.append(ocd)

    return list_ocds, lista_certificado_digitos, qtd_total


def tipo_certificado(ano, mes):
    try:
        df = pd.read_parquet(f'../arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return
    
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
            
    
        ano_certicado = f'{numero_homologacao[5]}{numero_homologacao[6]}'
        
        if int(ano_certicado) > 24:
            print(ano_certicado)
            

            
        if '24' == ano_certicado:
            if numero_homologacao not in homolocacoes_iniciais:
                homolocacoes_iniciais.append(numero_homologacao)
                qtd_homolocacoes_iniciais += 1
    
    print(qtd_caracteres)
    return qtd_homolocacoes_iniciais
            
        
    
    
    
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
def certificados_ocds(ano:int, mes:str, produtos: Optional[str] = Query(None, description="Opções de produto (separadas por vírgula)")):
    spark = SS.builder.appName( "Projeto" ).getOrCreate()
    
    try:
        df = spark.read.parquet(f'../arquivos_parquet/{str(ano)}/certificados_de_{mes.lower()}.parquet')
    except FileNotFoundError:
        return "Arquivo não encontrado"
    except AnalysisException as e:
        return f"Erro ao tentar ler o arquivo Parquet: {e} "

    coluna_certificados = "Certificado de Conformidade Técnica"

    lista_ocd, lista_certificado_digitos, qtd_total = ocds(ano, mes) 
    
    inicial = tipo_certificado(ano, mes)
    print(inicial)

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
        'TEL': 'ACTA'
    }
    
    if lista_ocd:
        for ocd in lista_ocd:
            if produtos is not None:
                list_p = produtos.split(',')
                quantidade_certificados_list = []
                for p in list_p:
                    df_filtrado = df.filter(F.col(coluna_certificados).contains(ocd))
                    df_filtrado = df_filtrado.filter(F.upper(F.col("Tipo do Produto")).contains(p.upper().strip())).select(coluna_certificados)
                    quantidade_certificados_list.append(df_filtrado.distinct().count())
             
                quantidade_certificados = sum(quantidade_certificados_list)
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
    