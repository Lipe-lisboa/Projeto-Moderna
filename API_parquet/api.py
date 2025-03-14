from fastapi import FastAPI
from pathlib import Path

# uvicorn api:app --reload
app = FastAPI()

@app.get("/")
def hello_world_root():
    return {"Hello": "World"}


#OBTER item
#Com uma base de dados, desenvolvemos as rotas para buscar a informação (GET), conforme código a seguir:
menu = [
    {   'id': 1,
        'name': 'coffee',
        'price': 2.5
     },
    {
        'id': 2,
        'name': 'cake',
        'price': 10
    },
    {
        'id': 3,
        'name': 'tea',
        'price': 3.2
    },
    {
        'id': 4,
        'name': 'croissant',
        'price': 5.79
    }
]

@app.get('/get-item/{item_id}')
def get_item(item_id: int = Path(description="Preencha com o ID do item que deseja visualizar")):

    search = list(filter(lambda x: x["id"] == item_id, menu))

    if search == []:
        return {'Error': 'Item does not exist'}

    return {'Item': search[0]}

@app.get('/get-by-name')
def get_item(name: str = None):

    search = list(filter(lambda x: x["name"] == name, menu))

    if search == []:
        return {'item': 'Does not exist'}

    return {'Item': search[0]}


@app.get('/list-menu')
def list_menu():
    return {'Menu': menu}

@app.post('/create-item/{item_id}')
def create_item(item_id, item):

    search = list(filter(lambda x: x["id"] == item_id, menu))

    if search != []:
        return {'Error': 'Item exists'}

    item = item.dict()
    item['id'] = item_id

    menu.append(item)
    return item