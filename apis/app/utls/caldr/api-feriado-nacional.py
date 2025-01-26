import requests
import os
import pandas as pd
from datetime import datetime

data_hora_inclusao = datetime.now().strftime('%Y%m%d_%H%M%S')

url = 'https://brasilapi.com.br/api/feriados/v1/2024'

response = requests.get(url)
dados = response.json()

lista = []

for resultado in dados:
    data = resultado['date']
    feriado = resultado['name']
    tipo = resultado['type']
    
    lista.append({
        'feriado': feriado,
        'tipo': tipo,
        'data': data,
        'data_hora_inclusao': data_hora_inclusao
        
    })
    
df = pd.DataFrame(lista)

caminho = '/workspaces/projeto_ing/apis/app/src/caldr/'
arquivo = f'feriado_nacional_{data_hora_inclusao}.txt'

df.to_csv(f'{caminho}{arquivo}', sep=';', index=False)
print(f'arquivo salvo: {arquivo}')
print(f'caminho salvo: {caminho}')