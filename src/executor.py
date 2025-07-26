from baixar_arquivo_zip import baixar_arquivo
from extrair_csv import extrair
from dividir_converter_csv_mes import dividir_csv_por_mes
import sys


# Juntando todas as etapas



url_do_arquivo = 'https://www.anatel.gov.br/dadosabertos/paineis_de_dados/certificacao_de_produtos/produtos_certificados.zip' # url do arquivo zip
caminho_do_arquivo = '../docs/produtos_certificados.zip' # nome para o arquivo

baixar_arquivo(url_do_arquivo, caminho_do_arquivo)

zip_path = "../docs/produtos_certificados.zip"  # Caminho do arquivo ZIP
extrair(zip_path)


#Argumentos da linha de comando: python .\index.py 2025 2024 2023 ...
print("Argumentos:", sys.argv[1:])

if len(sys.argv) > 1:
    for arg in sys.argv[1:]:
        try:
            ano = int(arg)
            dividir_csv_por_mes(int(arg))
        except TypeError:
            print('Erro: argumento deve ser do tipo Int')
        except Exception as e:
            print(f"Ocorreu um erro: {e}")