import os
import pandas as pd
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

            # Lê o CSV usando Pandas para garantir a leitura correta dos dados, especialmente com encoding e separadores
            df = pd.read_csv(
                    csv_file_path, 
                    sep=";", 
                    encoding='utf-8',
                    usecols=["Data do Certificado de Conformidade Técnica", "Certificado de Conformidade Técnica"]
                )
            
            # Converte a coluna para datetime (evita erros de texto)
            col_data = "Data do Certificado de Conformidade Técnica"
            df[col_data] = pd.to_datetime(df[col_data], dayfirst=True, errors='coerce')

            # Cria a pasta de saída para o ano, se não existir
            pasta_ano = self.root_dir / "arquivos_parquet" / str(ano) # Define o caminho da pasta do ano
            pasta_ano.mkdir(parents=True, exist_ok=True) # Cria a pasta se não existir

            # Filtra e salva os dados por mês
            for mes_name, mes_cod in self.lista_mes.items():
                mes_int = int(mes_cod)
                
                # Filtragem do DataFrame para o mês e ano específicos
                df_mes = df[(df[col_data].dt.month == mes_int) & (df[col_data].dt.year == ano)]

                # Se houver dados, converte e salva
                if not df_mes.empty:
                    # Define o caminho do arquivo de saída
                    arquivo_saida = pasta_ano / f"certificados_de_{mes_name}.parquet"

                    # Converte o DataFrame do Pandas para Parquet usando o método to_parquet
                    df_mes.to_parquet(arquivo_saida, index=False)

            return f"Processamento do ano {ano} concluído com sucesso!"

        except Exception as e:
            return f"Erro no processamento Pandas/Parquet: {e}"

    def contar_certificados(self, ano:int, mes:str, ocd_enviado: Optional[str] = None):

        path = self.parquet_base_dir / str(ano) / f"certificados_de_{mes}.parquet"
        
        try:
            df = pd.read_parquet(path) # Lê o arquivo Parquet usando Pandas

        except FileNotFoundError:
            return "arquivo não encontrado"
        except AnalysisException as e:
            return f"Erro ao tentar ler o arquivo Parquet: {e} "

        if ocd_enviado is not None:
            lista_ocd = [ocd_enviado]
        else:
            lista_ocd = ocds(ano, mes)

        saida = {}

        coluna = "Certificado de Conformidade Técnica"

        if lista_ocd:

            for ocd in lista_ocd:

                df_filtrado = df[df[coluna].str.contains(ocd, na=False)] # Filtra o DataFrame para o OCD atual, ignorando valores nulos
                quantidade_certificados = df_filtrado[coluna].nunique() # Conta o número de certificados únicos para o OCD atual


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
        
        return dados