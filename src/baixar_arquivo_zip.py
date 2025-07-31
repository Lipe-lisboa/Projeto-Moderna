import requests

def baixar_arquivo(url:str, nome_arquivo:str):
    resposta = requests.get(url, stream=True)
    resposta.raise_for_status()  # Verifica se houve erros na requisição


    #Essa parte do código pega o arquivo baixado da internet em "pedaços", 
    # e escreve cada pedaço no seu computador, montando o arquivo final
    with open(nome_arquivo, 'wb') as arquivo:
        for bloco in resposta.iter_content(chunk_size=8192):
            arquivo.write(bloco)

    print(f'"{nome_arquivo}" baixado com sucesso!')


url_do_arquivo = 'https://www.anatel.gov.br/dadosabertos/paineis_de_dados/certificacao_de_produtos/produtos_certificados.zip' # url do arquivo zip
caminho_do_arquivo = 'docs/produtos_certificados.zip' # nome para o arquivo

#baixar_arquivo(url_do_arquivo, caminho_do_arquivo)
