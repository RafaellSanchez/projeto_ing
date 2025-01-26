import requests
import pandas as pd
from datetime import datetime

data_hora_inclusao = datetime.now().strftime('%Y%m%d_%H%M%S')

url = 'https://api.worldbank.org/v2/region?format=json'

response = requests.get(url)

if response.status_code == 200:
    dados = response.json()
    
    lista = []

    metadata = dados[0] 
    regions = dados[1]

    print("Informações de Paginação:")
    print(f"Página: {metadata['page']}")
    print(f"Páginas: {metadata['pages']}")
    print(f"Por Página: {metadata['per_page']}")
    print(f"Total: {metadata['total']}")

    print("\nLista de Regiões:")
    for region in regions:
        print(f"ID: {region['id']}, Código: {region['code']}, ISO2: {region['iso2code']}, Nome: {region['name']}")
        
        codigo_id = region['id']
        codigo = region['code']
        isocode = region['iso2code']
        nome = region['name']
        
        lista.append({
            'id': codigo_id,
            'cod': codigo,
            'isocode': isocode,
            'nome': nome,
            'datahora': data_hora_inclusao
        })
    
    df = pd.DataFrame(lista)
    caminho = '/workspaces/projeto_ing/apis/app/src/bank/'
    arquivo = f'bank_world_{data_hora_inclusao}.txt'
    
    df.to_csv(f'{caminho}{arquivo}',  sep=';', index=False)
    print(f'arquivo salvo: {arquivo}')
    print(f'caminho salvo: {caminho}')

else:
    print(f"Erro ao acessar a API: Código de status {response.status_code}")
