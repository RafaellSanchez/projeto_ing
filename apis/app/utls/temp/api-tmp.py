import requests
import datetime
from datetime import datetime
import pandas as pd
import re


api_key = '7d345b55e9fbcf6989728ab565b60c59'
cidade = {
    'São Paulo', 
    'Rio de Janeiro', 
    'Mairiporã', 
    'Atibaia',
    'Franco da Rocha',
    'Curitiba',
    'Belo Horizonte',
    'Osasco',
    'Guarulhos',
    'Barueri',
    'Alphaville',
    'Caieiras',
    'Bragança Paulista',
    'Praia Grande',
    'Santos'
    }
# lat = ' -23.19.07'
# lon = '46.35.12'
# excl = ''

lista = []

for cidades in cidade:
    link = f"https://api.openweathermap.org/data/2.5/weather?q={cidades}&appid={api_key}&lang=pt_br"
    requisicao = requests.get(link)
    requisicao_js = requisicao.json()
    # requisicao_js['clouds']['probabilidade_de_chuva'] = requisicao_js['clouds'].pop('all')
   

    descricao = requisicao_js['weather'][0]['description']
    temperatura = requisicao_js['main']['temp'] - 273.15
    data = requisicao_js['dt']
    prob = requisicao_js['clouds']
    nome = requisicao_js['name']
    pais = requisicao_js['sys']['country']
    temp_max = requisicao_js['main']['temp_max'] - 273.15
    temp_min = requisicao_js['main']['temp_min'] - 273.15
    umidade = requisicao_js['main']['humidity']
    timestamp = data
    data_legivel = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    dt_atual = datetime.now().strftime('%Y-%m-%d')
    latitude = requisicao_js['coord']['lat']
    longitude = requisicao_js['coord']['lon']
    descricao = requisicao_js['weather']
    desc = requisicao_js['weather'][0]['description']
    probabilidade = requisicao_js['clouds']['all']


    for resultado in requisicao_js.items():
        print(resultado)
        
    lista.append({
        'cidade': nome,
        'pais': pais,
        'temperatura': temperatura,
        'temp_max': temp_max,
        'temp_min': temp_min,
        'umidade': umidade,
        'prob_chuva': probabilidade,
        'data': dt_atual,
        'latitude': latitude,
        'longitude': longitude,
        'tempo': desc,
        })


df = pd.DataFrame(lista)
print(df)

dt = datetime.now().strftime('%Y%m%d_%H%M%S')
save = '/workspaces/projeto_ing/apis/app/src/temp/'
arq = f'arquivo_temp_{dt}.txt'

df.to_csv(f'{save}{arq}', sep=';', index=False)
print(f'df salvo no caminho: {save}')
