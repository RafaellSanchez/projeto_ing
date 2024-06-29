import requests
import pandas as pd
from datetime import datetime
import time


timestamp = datetime.now().strftime('%Y-%m-%d')
time.sleep(3)
print('carregando api')
# url_api = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados/29/municipios'
url_api = 'https://servicodados.ibge.gov.br/api/v1/localidades/distritos?orderBy=nome'
response = requests.get(url_api)
dados = response.json()

dados_list = []
time.sleep(3)
print('carregando dados da api...')
if dados:
    for resultado in dados:
        id = resultado['id']
        nome = resultado['nome']
        id_mun = resultado['municipio']['id']
        nm_mun = resultado['municipio']['nome']
        id_mcr = resultado['municipio']['microrregiao']['mesorregiao']['id']
        nm_mcr = resultado['municipio']['microrregiao']['mesorregiao']['nome']
        id_uf = resultado['municipio']['microrregiao']['mesorregiao']['UF']['id']
        nm_uf = resultado['municipio']['microrregiao']['mesorregiao']['UF']['nome']
        sigla_uf = resultado['municipio']['microrregiao']['mesorregiao']['UF']['sigla']
        id_reg = resultado['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['id']
        nm_reg = resultado['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['nome']
        sigla_rg = resultado['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['sigla']
        
        
        dados_list.append({
            'id': id,
            'nome': nome,
            'id_mun': id_mun,
            'nm_mun': nm_mun,
            'id_mcr': id_mcr,
            'nm_mcr': nm_mcr,
            'id_uf': id_uf,
            'nm_uf': nm_uf,
            'sigla_uf': sigla_uf,
            'id_reg': id_reg,
            'nm_reg': nm_reg,
            'sigla_rg': sigla_rg,
            'dtigtao': timestamp
            
        })


df = pd.DataFrame(dados_list)
print(df)

file = f'dados_geo.txt'
path = '/workspaces/projeto_ing/API/app/src/geo/'

df.to_csv(f'{path}{file}', sep=';', index=False)
print(f'Arquivo salvo: {file}')
print(f'Caminho: {path}')