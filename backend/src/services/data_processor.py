import os
from pyspark.sql import SparkSession as SS
from pyspark.sql.functions import col
import pandas as pd
import pyspark.sql.functions as F
from pyspark.errors.exceptions.captured import AnalysisException
from utils.functios import ocds
from typing import Optional

class DataProcessor:
    def __init__(self, root_dir: str, docs_dir: str, parquet_base_dir: str):        
        
        self.root_dir = root_dir
        self.docs_dir = docs_dir
        self.parquet_base_dir = parquet_base_dir

        self.lista_mes = {
            'janeiro': '01',
            'fevereiro': '02',
            'março':'03',
            'abril': '04',
            'maio': '05',
            'junho': '06',
            'julho': '07',
            'agosto': '08',
            'setembro': '09',
            'outubro': '10',
            'novembro': '11',
            'dezembro': '12',
        }

        self.dict_name = {
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

    def process_and_convert_to_parquet(self, ano:int):
        # Caminho do arquivo CSV extraído
        csv_file_path = self.docs_dir / "Produtos_Homologados_Anatel.csv"

        # Verifica se o arquivo CSV existe
        if not os.path.exists(csv_file_path):
            print(f"Erro: O arquivo '{csv_file_path}' não foi encontrado.")
            return
        
        try:
            # Inicializa a Spark Session
            spark = SS.builder.appName("AnatelProcessor").getOrCreate()

            # Lê o CSV usando Spark
            df = spark.read.csv(
                str(csv_file_path), # Convertendo Path para string
                header=True, # Indica que o CSV tem cabeçalho
                sep=";", # Separador de campos
                inferSchema=True, # Infere o esquema dos dados
                encoding='utf-8', # Codificação dos dados
            ).select(*[
                         "Data do Certificado de Conformidade Técnica",
                         "Certificado de Conformidade Técnica"
                ]) # Seleciona apenas as colunas necessárias 

            # Cria a pasta de saída para o ano, se não existir
            pasta_ano = self.root_dir / "arquivos_parquet" / str(ano) # Define o caminho da pasta do ano
            pasta_ano.mkdir(parents=True, exist_ok=True) # Cria a pasta se não existir

            # Filtra e salva os dados por mês
            for mes_name, mes_cod in self.lista_mes.items():
                filtro = f'{mes_cod}/{str(ano)}' # Formato esperado no campo de data
                # Filtragem no Spark
                df_filtrado = df.filter(col("Data do Certificado de Conformidade Técnica").contains(filtro))
                dados = df_filtrado.collect()

                # Se houver dados, converte e salva
                if len(dados) != 0:
                    # Converte diretamente para Pandas (Spark já tem o método .toPandas())
                    pandas_df = pd.DataFrame(dados, columns=df_filtrado.columns)

                    # Define o caminho do arquivo de saída
                    arquivo_saida = pasta_ano / f"certificados_de_{mes_name}.parquet"
                    pandas_df.to_parquet(arquivo_saida, index=False, coerce_timestamps='us')
                    print(f"Gerado: {arquivo_saida.name}")

            spark.stop()
            return f"Processamento do ano {ano} concluído com sucesso!"

        except Exception as e:
            if 'spark' in locals(): 
                spark.stop()
            return f"Erro no processamento Spark/Parquet: {e}"

    def contar_certificados(self, ano:int, mes:str, ocd_enviado: Optional[str] = None):

        spark = SS.builder.appName( "Projeto" ).getOrCreate()

        spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

        path = self.parquet_base_dir / str(ano) / f"certificados_de_{mes}.parquet"
        
        try:
            df = spark.read.parquet(str(path))

        except FileNotFoundError:
            return "arquivo não encontrado"
        except AnalysisException as e:
            return f"Erro ao tentar ler o arquivo Parquet: {e} "

        coluna = "Certificado de Conformidade Técnica"

        if ocd_enviado is not None:
            lista_ocd = [ocd_enviado]
        else:
            lista_ocd = ocds(ano, mes)

        saida = {}

        if lista_ocd:

            for ocd in lista_ocd:

                df_filtrado = df.filter(F.col(coluna).contains(ocd)).select(coluna)
                quantidade_certificados = df_filtrado.distinct().count()

                # Verifica se há certificados para o OCD atual
                if quantidade_certificados:
                    if ocd in self.dict_name:# Verifica se o OCD está no dicionário
                        ocd = self.dict_name[ocd] # Mapeia o nome do OCD conforme o dicionário
                    saida[ocd.upper()] = quantidade_certificados # Armazena a contagem no dicionário

        # Lógica específica ABCP vs BRICS
        # Se ambos ABCP e BRICS estiverem presentes, subtrai a contagem de ABCP de BRICS
        if 'ABCP' in saida and 'BRICS' in saida:
            if saida['BRICS'] > saida['ABCP']:
                saida['BRICS'] -= saida['ABCP'] # Remove a contagem de ABCP de BRICS

        # transforma o dicionário em uma lista de dicionários
        resultado = [
                {"ocd": k, "quantidade_de_certificado": v} 
                for k, v in saida.items()
            ]
        
        # Ordena a lista pelo valor (chave 'valor') em ordem decrescente
        dados = sorted(resultado, key=lambda item: item['quantidade_de_certificado'], reverse=True)        
        
        spark.stop()
        return dados