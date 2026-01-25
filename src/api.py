from fastapi import FastAPI
from services.data_processor import DataProcessor
from services.file_manager import FileManager
from pathlib import Path

# 1. Pega o caminho de onde este arquivo .py está
CURRENT_DIR = Path(__file__).resolve().parent

# 2. Sobe dois níveis (sai de 'services', sai de 'src') para chegar na raiz
ROOT_DIR = CURRENT_DIR.parent

# 3. Define os caminhos das pastas de destino
DOCS_DIR = ROOT_DIR / "docs"
PARQUET_DIR = ROOT_DIR / "arquivos_parquet"

processor = DataProcessor(ROOT_DIR, DOCS_DIR, PARQUET_DIR)
file_manager = FileManager(ROOT_DIR, DOCS_DIR)

app = FastAPI()

@app.get("/criar-parquets")
def criar_parquets(
    ano:int, 
    ):
    try:

        file_manager.download_data()
        file_manager.extract_data()
        result = processor.process_and_convert_to_parquet(ano)

        return {
            "status": "Success",
            "message": f"Arquivos Parquet para o ano {ano} foram criados com sucesso.",
            "result": result
            }
    except Exception as e:
        return {
            "status": "Fail",
            "mensagem": str(e),
            "result": result
            }

   
@app.get("/certificados/{ocd_enviado}")
def certificados_ocd(ocd_enviado: str,ano:int, mes:str):
    
    dados = processor.contar_certificados(ano, mes, ocd_enviado)

    if dados is None:
        return {
            "status": "Fail",
            "mensagem": "Ocorreu um erro ao processar os dados.",
            "result": None
            }
    
    return {
        "status": "Success",
        "mensagem": f"Contagem de certificados para o ano {ano} e mês {mes} obtida com sucesso.",
        "result": dados
    }


@app.get("/certificados")
def certificados_ocds(
    ano:int, 
    mes:str, 
    ):

    dados = processor.contar_certificados(ano, mes)

    if dados is None:
        return {
            "status": "Fail",
            "mensagem": "Ocorreu um erro ao processar os dados.",
            "result": None
            }
    
    return {
        "status": "Success",
        "mensagem": f"Contagem de certificados para o ano {ano} e mês {mes} obtida com sucesso.",
        "result": dados
    }