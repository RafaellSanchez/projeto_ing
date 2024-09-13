import requests
import pandas as pd
from datetime import datetime

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
data = datetime.now().strftime('%Y-%m-%d')

url = 'https://parallelum.com.br/fipe/api/v1/carros/marcas'
response = requests.get(url)

dados = response.json()

data_list = []

if dados:
    if 'marcas' in dados:
        modelos = dados['marcas']
        for modelo in modelos:
            codigo = modelo['codigo']
            nome = modelo['nome']
            # print(f"codigo: {codigo}")
            # print(f"nome: {nome}")
            
            data_list.append({
                'codigo': codigo,
                'modelo': nome
            })
    else:  
        for modelo in dados:
            codigo = modelo['codigo']
            nome = modelo['nome']
            # print(f"codigo: {codigo}")
            # print(f"nome: {nome}")
            
            data_list.append({
                'codigo': codigo,
                'modelo': nome 
            })

df = pd.DataFrame(data_list)
print(df)

file = f'resultado_marcas_{timestamp}.txt'
path = '/workspaces/projeto_ing/apis/app/src/car/'

df.to_csv(f'{path}{file}', sep=';', index=False)
print(f'Arquivo salvo: {file}')
print(f'Caminho: {path}')