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

  ```


- Certificados OCDS
```
http://127.0.0.1:8000/certificados?ano=2025&mes=JANEIRO


[
	{
		"ocd": "NCC",
		"quantidade_de_certificado": 104
	},
	{
		"ocd": "MODERNA",
		"quantidade_de_certificado": 82
	},
	{
		"ocd": "ICC",
		"quantidade_de_certificado": 56
	},
	{
		"ocd": "CPQD",
		"quantidade_de_certificado": 51
	},
	{
		"ocd": "BRACERT",
		"quantidade_de_certificado": 31
	},
	{
		"ocd": "MASTER",
		"quantidade_de_certificado": 25
	},
	{
		"ocd": "DEKRA",
		"quantidade_de_certificado": 23
	},
	{
		"ocd": "UL",
		"quantidade_de_certificado": 22
	},
	{
		"ocd": "OCPTELLI",
		"quantidade_de_certificado": 18
	},
	{
		"ocd": "PCN",
		"quantidade_de_certificado": 17
	},
	{
		"ocd": "VERSYS",
		"quantidade_de_certificado": 13
	},
	{
		"ocd": "ELDORADO",
		"quantidade_de_certificado": 13
	},
	{
		"ocd": "ABCP",
		"quantidade_de_certificado": 11
	},
	{
		"ocd": "TUV",
		"quantidade_de_certificado": 10
	},
	{
		"ocd": "CCPE",
		"quantidade_de_certificado": 9
	},
	{
		"ocd": "QCCERT",
		"quantidade_de_certificado": 8
	},
	{
		"ocd": "BR APPROVAL",
		"quantidade_de_certificado": 3
	},
	{
		"ocd": "CTCP",
		"quantidade_de_certificado": 3
	},
	{
		"ocd": "BRICS",
		"quantidade_de_certificado": 3
	},
	{
		"ocd": "ACTA",
		"quantidade_de_certificado": 2
	},
	{
		"ocd": "LMP",
		"quantidade_de_certificado": 1
	}
]
```

- Certificados OCDS com filtro de "tipo_certificado"
```
http://127.0.0.1:8000/certificados?ano=2025&mes=JANEIRO&tipo_certificado=inicial


[
	{
		"ocd": "NCC",
		"quantidade_de_certificado": 67
	},
	{
		"ocd": "MODERNA",
		"quantidade_de_certificado": 47
	},
	{
		"ocd": "ICC",
		"quantidade_de_certificado": 34
	},
	{
		"ocd": "CPQD",
		"quantidade_de_certificado": 27
	},
	{
		"ocd": "PCN",
		"quantidade_de_certificado": 14
	},
	{
		"ocd": "BRACERT",
		"quantidade_de_certificado": 10
	},
	{
		"ocd": "VERSYS",
		"quantidade_de_certificado": 8
	},
	{
		"ocd": "UL",
		"quantidade_de_certificado": 8
	},
	{
		"ocd": "MASTER",
		"quantidade_de_certificado": 7
	},
	{
		"ocd": "TUV",
		"quantidade_de_certificado": 7
	},
	{
		"ocd": "CCPE",
		"quantidade_de_certificado": 6
	},
	{
		"ocd": "QCCERT",
		"quantidade_de_certificado": 5
	},
	{
		"ocd": "DEKRA",
		"quantidade_de_certificado": 4
	},
	{
		"ocd": "OCPTELLI",
		"quantidade_de_certificado": 3
	},
	{
		"ocd": "ELDORADO",
		"quantidade_de_certificado": 2
	},
	{
		"ocd": "BR APPROVAL",
		"quantidade_de_certificado": 2
	},
	{
		"ocd": "CTCP",
		"quantidade_de_certificado": 2
	},
	{
		"ocd": "ABCP",
		"quantidade_de_certificado": 1
	},
	{
		"ocd": "LMP",
		"quantidade_de_certificado": 1
	},
	{
		"ocd": "BRICS",
		"quantidade_de_certificado": 1
	}
]
```


- Certificados OCD Moderna
```
http://127.0.0.1:8000/certificados/MODERNA?ano=2025&mes=JANEIRO


{
	"ocd": "MODERNA",
	"quantidade_de_certificado": 82
}
```
