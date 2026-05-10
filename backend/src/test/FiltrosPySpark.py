from pyspark.sql import SparkSession as SS, functions as F
from pyspark.sql.functions import col
import pandas as pd
import os
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql.functions import lpad, col
from pyspark.sql.types import StringType

# 1. Obter o diretório do script atual
script_dir = os.path.dirname(os.path.abspath(__file__))

# 2. Construir o caminho para o diretório raiz do projeto
project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))


def filtro_certificados_moderna():
    spark = SS.builder.appName( "Projeto" ).getOrCreate() 
    df = spark.read.parquet(os.path.join(project_root, "arquivos_parquet/2025/certificados_de_março.parquet"))

    coluna = "Certificado de Conformidade Técnica"

    df_filtrado = df.filter(F.col(coluna).contains("MODERNA")).select(coluna)

    quantidade_certificados = df_filtrado.distinct().count()
    
    return quantidade_certificados

print(filtro_certificados_moderna())


def filtro_certificas_periodo(datas):
    spark = SS.builder.appName("LerCSV").getOrCreate()
    
    csv_file_path = os.path.join(project_root, "docs/Produtos_Homologados_Anatel.csv")

    df = spark.read.csv(
        csv_file_path,
        header=True,
        sep=";",
        inferSchema=True,
        encoding='utf-8',
    )

    for data in datas:
        for mes, data in data.items():
        
            # Filtra as linhas onde "Data do Certificado de Conformidade Técnica" contém "03/2025"
            df_filtrado = df.filter(col("Data do Certificado de Conformidade Técnica").contains(data))
            
            if  df_filtrado != None:
                # Coleta os dados como uma lista de linhas
                dados = df_filtrado.collect()

                # Cria um Pandas DataFrame a partir dos dados
                pandas_df = pd.DataFrame(dados, columns=df_filtrado.columns)

                file = os.path.join(project_root, f"arquivos_parquet/2025/certificados_de_{mes}.parquet")

                pandas_df.to_parquet(file, index=None)
            

    spark.stop()
    return
    
#dicts_datas = [{'março':'03/2025'}]

#filtro_certificas_periodo(dicts_datas)

def homologacao(ano:int,mes:str,tipo_de_servico: str, ocd:str):
    spark = SS.builder.appName( "Projeto" ).getOrCreate()
    file = os.path.join(project_root, f'arquivos_parquet/{str(ano)}/certificados_de_{mes.lower()}.parquet')
    
    
    #verificando arquivo
    try:
        df = spark.read.parquet(file)
    except FileNotFoundError:
        return "Arquivo não encontrado"
    except AnalysisException as e:
        return f"Erro ao tentar ler o arquivo Parquet: {e} "
    
    
    #nome das colunas que desejo verificar
    coluna_certificados = "Certificado de Conformidade Técnica"
    coluna_nm_homologacao = "Número de Homologação"

    # Converter a coluna para StringType e aplicar o padding
    df_padronizado = df.withColumn(
        coluna_nm_homologacao,  # Nome da coluna a ser editada
        
        #transforma a coluna para string e depois preenche 0 as que não tem 12 caracteres
        lpad(col(coluna_nm_homologacao).cast(StringType()), 12, "0")
    )


    print(f'qtd de linhas: {df_padronizado.count()}')
    #df_padronizado.select(coluna_nm_homologacao).show(df_padronizado.count(), False) # Para visualizar o DataFrame resultante
              
               
    #pega os dois ultimos numeros do ano enviado   
    abreviacao_ano = str(ano)[2:4]

    
    if tipo_de_servico.lower() == "inicial":
        #filtra o df para que selecione apenas as homologações que contem os digititos do ano enviado (abreviacao_ano)
        df_filtrado_inicial = df_padronizado.filter(F.substring(F.col(coluna_nm_homologacao), 6, 2) == abreviacao_ano)
        
        #conta a quantidade de certificados
        quantidade_certificados_iniciais = df_filtrado_inicial.select(coluna_nm_homologacao).distinct().count()
        print(f'qtd de certificações iniciais: {quantidade_certificados_iniciais}')
        
        #filtra para que selecione apenas os certificados da moderna
        df_filtrado_ocd = df_filtrado_inicial.filter(F.col(coluna_certificados).contains(ocd.upper()))
        quantidade_certificados_iniciais = df_filtrado_ocd.select(coluna_certificados).distinct().count()
        print(f'qtd de certificações iniciais {ocd}: {quantidade_certificados_iniciais}')
        
    elif tipo_de_servico.lower() == "manutenção":
        #filtra o df para que selecione apenas as homologações que contem os digititos do ano enviado (abreviacao_ano)
        df_filtrado_manutencao = df_padronizado.filter(F.substring(F.col(coluna_nm_homologacao), 6, 2) != abreviacao_ano)
        
        #conta a quantidade de certificados
        quantidade_certificados_manutencao = df_filtrado_manutencao.select(coluna_nm_homologacao).distinct().count()
        print(f'qtd de manutenções: {quantidade_certificados_manutencao}')
        
        #filtra para que selecione apenas os certificados da moderna
        df_filtrado_ocd = df_filtrado_manutencao.filter(F.col(coluna_certificados).contains(ocd.upper()))
        quantidade_certificados_manutencao = df_filtrado_ocd.select(coluna_certificados).distinct().count()
        print(f'qtd de manutencões {ocd}: {quantidade_certificados_manutencao}')
        
    else:
        print("Tipo de serviço não encontrado")
            
            
    return df_padronizado

#homologacao(2024,'outubro',"manutenção","moderna")

