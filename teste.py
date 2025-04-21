import pandas as pd
import re
from unidecode import unidecode
import regex

from pyspark.sql import SparkSession as SS, functions as F
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql.functions import lpad, col
from pyspark.sql.types import StringType

def certificados_ocd(ocd_enviado,ano, mes):
    
    try:
        df = pd.read_parquet(f'arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return

    list_certificado = []
    data = []
    list_ocds = []

    #adiciona itens em list_ocds e list_certificado
    for certificado in df['Certificado de Conformidade Técnica']:
        
        certificado = unidecode(certificado).upper()
        
        #Deixa apenas letras e traço
        ocd = re.sub(r'[^a-zA-Z-]+', '', certificado).strip()
        
        # Remove traços no início da string
        ocd = re.sub(r'^-+', '', ocd)

        # Remove traços no final da string
        ocd = re.sub(r'-+$', '', ocd)

        #Adiciona o ocd na lista de ocds
        if ocd not in list_ocds:
            list_ocds.append(ocd)
        
        if certificado not in list_certificado: 
            list_certificado.append(certificado) 
    
    qtd_certificados_ocd = 0
    # Adiciona itens em data
    for ocd in list_ocds:
        for certificado in list_certificado:
            if ocd in certificado:
                qtd_certificados_ocd += 1
        
        #Adiciona o ocd e a quantidade de certificados que ele tem
        data.append(
            {
                'ocd':ocd,
                'value':qtd_certificados_ocd
            }
        )
        qtd_certificados_ocd = 0  
        
    #Seleciona o item que contem as informações do ocd enviado
    item = next((dicionario for dicionario in data if dicionario['ocd'] == ocd_enviado.upper()), None)
    
    if item:
        print(item)
        print()
        
    else:
        print('OCD não identificado.')
        

#certificados_ocd('MODERNA', 2024, 'fevereiro')


def ocds(ano, mes):
    try:
        df = pd.read_parquet(f'arquivos_parquet/{str(ano)}/certificados_de_{str(mes)}.parquet')
        
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

#ocds(2024, 'janeiro')


def tipo_produto(ano,mes):
    try:
        df = pd.read_parquet(f'arquivos_parquet/{str(ano)}/certificados_de_{str(mes)}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return
    
    list_tp_produto = []
    #adiciona itens em list_ocds e list_certificado
    for produto in df['Tipo do Produto']:
        
        if produto not in list_tp_produto:
            list_tp_produto.append(produto)

    return list_tp_produto

#print(tipo_produto(2024,'fevereiro'))


def homologacao(ano,mes):
    spark = SS.builder.appName( "Projeto" ).getOrCreate()
    file = f'arquivos_parquet/{str(ano)}/certificados_de_{mes.lower()}.parquet'
    
    try:
        df = spark.read.parquet(file)
    except FileNotFoundError:
        return "Arquivo não encontrado"
    except AnalysisException as e:
        return f"Erro ao tentar ler o arquivo Parquet: {e} "
    
    coluna_certificados = "Certificado de Conformidade Técnica"
    coluna_nm_homologacao = "Número de Homologação"

    # Converter a coluna para StringType e aplicar o padding
    df_padronizado = df.withColumn(
        coluna_nm_homologacao,  # Nome da coluna a ser editada
        lpad(col(coluna_nm_homologacao).cast(StringType()), 12, "0")
    )

    print(f'qtd de linhas: {df_padronizado.count()}')
    #df_padronizado.select(coluna_nm_homologacao).show(df_padronizado.count(), False) # Para visualizar o DataFrame resultante
              
                  
    abreviacao_ano = str(ano)[2:4]

    df_filtrado_inicial = df_padronizado.filter(F.substring(F.col(coluna_nm_homologacao), 6, 2) == abreviacao_ano)
    
    quantidade_certificados = df_filtrado_inicial.select(coluna_nm_homologacao).distinct().count()
    print(f'qtd de certificações iniciais: {quantidade_certificados}')
    
    df_filtrado_ocd = df_filtrado_inicial.filter(F.col(coluna_certificados).contains('EL'))
    quantidade_certificados = df_filtrado_ocd.select(coluna_certificados).distinct().count()
    print(f'qtd certificados moderna inicial: {quantidade_certificados}')
    
    return df_padronizado

homologacao(2024,'outubro')


