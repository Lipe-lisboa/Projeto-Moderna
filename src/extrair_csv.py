import zipfile

zip_path = "../docs/produtos_certificados.zip"  # Caminho do arquivo ZIP


def extrair(zip_path:str):
    # Abrindo o ZIP
    with zipfile.ZipFile(zip_path, "r") as zip_file:
        # Listar os arquivos dentro do ZIP
        file_names = zip_file.namelist()
        
        csv_file = None
        
        for f in file_names:
            if f.endswith(".csv"):
                csv_file = f

        if csv_file:
            zip_file.extract(csv_file, path="../docs") #extraindo arquivo csv
            print(f'"{csv_file}" extraido com sucesso!')
        else:
            print("Nenhum arquivo CSV encontrado no ZIP.")

#extrair(zip_path)
        