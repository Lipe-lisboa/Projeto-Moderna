import zipfile
import requests
import os

# Classe responsável por gerenciar o download e extração dos arquivos de dados
class FileManager:
    def __init__(self, root_dir: str, docs_dir: str):

        # URL do arquivo ZIP contendo os dados de produtos certificados
        self.url = 'https://www.anatel.gov.br/dadosabertos/paineis_de_dados/certificacao_de_produtos/produtos_certificados.zip'
        
        self.root_dir = root_dir
        self.docs_dir = docs_dir
        self.name_file = "produtos_certificados.zip"
        self.zip_path = self.docs_dir / self.name_file # Caminho completo para o arquivo ZIP a ser baixado

    # Método para baixar o arquivo ZIP dos dados de produtos certificados
    def download_data(self):
        response = requests.get(self.url)
        with open(self.zip_path, 'wb') as f:
            for bloco in response.iter_content(chunk_size=8192):
                f.write(bloco)

        print(f'"{self.zip_path}" baixado com sucesso!')

        return self.zip_path

    # Método para extrair o arquivo CSV do ZIP baixado
    def extract_data(self):

        # Verifica se o arquivo ZIP foi baixado corretamente antes de tentar extrair
        if not os.path.exists(self.zip_path):
            print(f"Erro: O arquivo {self.zip_path} não foi encontrado.")
            return False
        
        try:

            # Tenta abrir o arquivo ZIP para extrair o conteúdo
            with zipfile.ZipFile(self.zip_path, "r") as zip_ref:
                # Busca todos os arquivos que terminam com .csv
                csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]

                # Verifica se encontrou arquivos CSV
                if not csv_files:
                    print(f"Aviso: Nenhum arquivo CSV encontrado dentro de {self.zip_path}.")
                    return False

                # Extrai apenas o primeiro CSV encontrado
                arquivo_alvo = csv_files[0]
                zip_ref.extract(arquivo_alvo, path=self.docs_dir)

                print(f"Sucesso: '{arquivo_alvo}' extraído para {self.docs_dir}")
                return True

        # Trata erros comuns relacionados à manipulação de arquivos ZIP
        except zipfile.BadZipFile:
            print(f"Erro: O arquivo '{self.name_file}' está corrompido ou não é um ZIP válido.")
        except Exception as e:
            print(f"Ocorreu um erro inesperado na extração: {e}")
        
        return False