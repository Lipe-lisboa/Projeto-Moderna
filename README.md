<h1 align="center">
  Market Share
</h1>

<p align="center">
 <img src="https://img.shields.io/static/v1?label=Instagram&message=@filipe_lisboa07&color=8257E5&labelColor=000000" alt="@filipe_lisboa07" />
 <img src="https://img.shields.io/static/v1?label=Tipo&message=Desafio&color=8257E5&labelColor=000000" alt="Desafio" />
</p>

API criada para o Market Share do OCD (Organismo de Certificação Designado) Moderna Tecnologia.

## Tecnologias

- [FastAPI](https://fastapi.tiangolo.com/#requirements)
- [PysPark](https://spark.apache.org/docs/latest/api/python/index.html)


## Como Executar

- Clonar repositório git

- Para executar a aplicação entre no diretorio em que a API se encontra (src):

```
cd src    
uvicorn api:app --reload
```

A API poderá ser acessada em [127.0.0.1:8000](http://127.0.0.1:8000).
O Swagger poderá ser visualizado em [127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

## API Endpoints

Para fazer as requisições HTTP abaixo, foi utilizada a ferramenta [Insomnia](https://insomnia.rest/):


- Criar arquivos parquets
 ```
http://127.0.0.1:8000/criar parquets?ano=2025

{
	"message": "Arquivos Parquet para o ano 2025 foram criados com sucesso."
}

  ```


- Certificados OCDS
```
http://127.0.0.1:8000/certificados?ano=2025&mes=JANEIRO


[
	{
		"ocd": "MODERNA",
		"quantidade_de_certificado": xxx
	},
	{
		"ocd": "NCC",
		"quantidade_de_certificado": xxx
	},
	{
		"ocd": "ICC",
		"quantidade_de_certificado": xx
	},
	{
		"ocd": "CPQD",
		"quantidade_de_certificado": xx
	},
	{
		"ocd": "BRACERT",
		"quantidade_de_certificado": xx
	},
	{
		"ocd": "MASTER",
		"quantidade_de_certificado": xx
	}
]
```

- Certificados OCDS com filtro de "tipo_certificado"
```
http://127.0.0.1:8000/certificados?ano=2025&mes=JANEIRO&tipo_certificado=inicial


[
	{
		"ocd": "MODERNA",
		"quantidade_de_certificado": xxx
	},
	{
		"ocd": "NCC",
		"quantidade_de_certificado": xxx
	},
	{
		"ocd": "ICC",
		"quantidade_de_certificado": xx
	},
	{
		"ocd": "CPQD",
		"quantidade_de_certificado": xx
	},
	{
		"ocd": "BRACERT",
		"quantidade_de_certificado": xx
	},
	{
		"ocd": "MASTER",
		"quantidade_de_certificado": xx
	}
]
```


- Certificados OCD Moderna
```
http://127.0.0.1:8000/certificados/MODERNA?ano=2025&mes=JANEIRO


{
	"ocd": "MODERNA",
	"quantidade_de_certificado": xx
}
```
