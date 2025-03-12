from baixar_arquivo_zip import baixar_arquivo
from extrair_csv import extrair
from dividir_converter_csv_mes import datas_menssais, convert_parquet, dividir_csv_por_mes

# Juntando todas as etapas

url_do_arquivo = 'https://www.anatel.gov.br/dadosabertos/paineis_de_dados/certificacao_de_produtos/produtos_certificados.zip' # url do arquivo zip
nome_do_arquivo_local = 'produtos_certificados.zip' # nome para o arquivo

baixar_arquivo(url_do_arquivo, nome_do_arquivo_local)

zip_path = "produtos_certificados.zip"  # Caminho do arquivo ZIP
extrair(zip_path)

dividir_csv_por_mes(2025)