import pandas as pd
import regex
import re



def ocds(ano, mes):
    try:
        df = pd.read_parquet(f'../arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return
        
    list_ocds = []
    
    for certificado in df['Certificado de Conformidade Técnica']:
        
        
        iniciais_certificado = certificado[0:2]
        
        if iniciais_certificado == '7C':
            if iniciais_certificado not in list_ocds:
                list_ocds.append(iniciais_certificado)
        
        #Deixa apenas letras e traço
        ocd = regex.sub(r'[^a-zA-Z\p{L}-_/ ]+', '', certificado).strip('/- ')
        

        #Adiciona o ocd na lista de ocds
        if ocd not in list_ocds:
            if not 'TESTE' in ocd.upper() and len(ocd) > 1 and ocd != '':
                list_ocds.append(ocd)

    return list_ocds

def certificados(ano, mes):
    try:
        df = pd.read_parquet(f'../arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return
        
    
    #lista de certificados com apenas numeros 
    lista_certificado_digitos = []
    
    #todos certificados 
    todos_certificados = []
    
    #qtd total de certificados 
    qtd_total = 0
    
    for certificado in df['Certificado de Conformidade Técnica']:
        
        if certificado not in todos_certificados:
            todos_certificados.append(certificado)
            qtd_total += 1
        
        contem_letra = bool(re.search(r'[a-zA-Z]', certificado))
        if not contem_letra:
            if certificado not in lista_certificado_digitos:
                lista_certificado_digitos.append(certificado)
        
    return lista_certificado_digitos, qtd_total

def function_tipo_certificado(ano, mes):
    try:
        df = pd.read_parquet(f'../arquivos_parquet/{ano}/certificados_de_{mes}.parquet')
        
    except FileNotFoundError:
        print("arquivo não encontrado")
        return
    
    abreviacao_ano = str(ano)[2:4]
    
    homolocacoes_iniciais = []
    qtd_caracteres = []
    qtd_homolocacoes_iniciais = 0
    for numero_homologacao in df['Número de Homologação']:
        numero_homologacao = str(numero_homologacao)
        qtd_caracter = len(numero_homologacao)
        
        if qtd_caracter not in qtd_caracteres:
            qtd_caracteres.append(qtd_caracter)
            
        
        ano_certicado = None
        if qtd_caracter < 12:
            qtd_zero = 12 - qtd_caracter
            numero_homologacao = (qtd_zero * '0')+numero_homologacao 
            
    
        ano_certicado = numero_homologacao[5:7]
        
        if int(ano_certicado) > int(abreviacao_ano):
            print('ano do certificado:',ano_certicado)
            

            
        if abreviacao_ano == ano_certicado:
            if numero_homologacao not in homolocacoes_iniciais:
                homolocacoes_iniciais.append(numero_homologacao)
                qtd_homolocacoes_iniciais += 1
    
    print('quantidade de caracteres:',qtd_caracteres)
    return qtd_homolocacoes_iniciais
         