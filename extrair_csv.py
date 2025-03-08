import zipfile

zip_path = "produtos_certificados.zip"  # Caminho do arquivo ZIP

# Abrindo o ZIP
with zipfile.ZipFile(zip_path, "r") as zip_file:
    # Listar os arquivos dentro do ZIP
    file_names = zip_file.namelist()
    print("Arquivos no ZIP:", file_names)
    
    csv_file = None
    
    for f in file_names:
        if f.endswith(".csv"):
            csv_file = f

    if csv_file:
        zip_file.extract(csv_file) #extraindo arquivo csv
    else:
        print("Nenhum arquivo CSV encontrado no ZIP.")
        