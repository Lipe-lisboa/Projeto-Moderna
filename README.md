<h1 align="center">
  Market Share
</h1>

<p align="center">
 <img src="https://img.shields.io/static/v1?label=Instagram&message=@filipe_lisboa07&color=8257E5&labelColor=000000" alt="@filipe_lisboa07" />
 <img src="https://img.shields.io/static/v1?label=Tipo&message=Automacao&color=8257E5&labelColor=000000" alt="Automação" />
</p>

API criada para o Market Share do OCD (Organismo de Certificação Designado) Moderna Tecnologia.

## Tecnologias

- [FastAPI](https://fastapi.tiangolo.com/#requirements)
- [PysPark](https://spark.apache.org/docs/latest/api/python/index.html)


## Como Executar

- Clonar repositório git
- Renomeie a pasta arquivos_parquet_exemplo (retire o "exemplo")
```
arquivos_parquet_exemplo -> arquivos_parquet
```
- Para executar a aplicação entre no diretorio em que a API se encontra (src):

```
cd backend
cd src    
uvicorn api:app --reload
```

A API poderá ser acessada em [127.0.0.1:8000](http://127.0.0.1:8000).
O Swagger poderá ser visualizado em [127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

## API Endpoints

Para fazer as requisições HTTP abaixo, foi utilizada a ferramenta [Insomnia](https://insomnia.rest/):


- Criar arquivos parquets
 ```
http://127.0.0.1:8000/criar-parquets?ano=2025

{
	"status": "Success",
	"mensagem": "Arquivos Parquet para o ano 2025 foram criados com sucesso.",
	"result": "Processamento do ano 2025 concluído com sucesso!"
}

  ```


- Certificados OCDS
```
http://127.0.0.1:8000/certificados?ano=2025&mes=JANEIRO


{
	"status": "Success",
	"mensagem": "Contagem de certificados para o ano 2025 e mês JANEIRO obtida com sucesso.",
	"result": [
		{
			"ocd": "MODERNA",
			"quantidade_de_certificado": XXX
		},
		{
			"ocd": "NCC",
			"quantidade_de_certificado": XX
		},
		{
			"ocd": "ICC",
			"quantidade_de_certificado": XX
		}
	]
}
```

- Certificados OCD Moderna
```
http://127.0.0.1:8000/certificados/MODERNA?ano=2025&mes=JANEIRO


{
	"status": "Success",
	"mensagem": "Contagem de certificados para o ano 2025 e mês JANEIRO obtida com sucesso.",
	"result": [
		{
			"ocd": "MODERNA",
			"quantidade_de_certificado": XX
		}
	]
}
```
