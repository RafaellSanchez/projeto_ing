sys.path.append("/../../utils/")
sys.path.append("/workspaces/projeto_ing/utils/")

import requests
import pandas as pd
import time
from datetime import datetime
import os
import shutil
import importlib
import sys
from helpers import mensagem

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
arquivos = '/workspaces/projeto_ing/apis/app/src/cep/'

time.sleep(3)
mensagem('Iniciando aplicação!')
#Lista de logradouros
listas = ['Estrada','Estrada Armando','Mairipora','Rua','Estrada Rio Acima','Rio Abaixo','Avenida','Rodovia']

# url = 'https://viacep.com.br/ws/RS/Porto Alegre/Domingos+Jose/json/'
#realizar a iteração da listas de logradouros com a url da api
print('Realizando iteração!')
time.sleep(3)
for lista in listas:
    url = f'https://viacep.com.br/ws/SP/Mairipora/{lista}/json/'
    response = requests.get(url)
    
    print('Verificando status da requisição!')
    time.sleep(3)
    if response.status_code == 200:
        print ('requisição: OK!')
    else:
        print ('Erro!')
    
    dados = response.json()
    print('transformando no formato json')
    
    dados_lista = []
    print('iteração na api')
    #para cada nome na lista, será realizado uma iteração na api
    for resultado in dados:
        cep = resultado['cep']
        logradouro = resultado['logradouro']
        bairro = resultado['bairro']
        localidade = resultado['localidade']
        uf = resultado['uf']
        ibge = resultado['ibge']
        gia = resultado['gia']
        ddd = resultado['ddd']
        siafi = resultado['siafi']
        
        
        print('realizando append em uma lista')
        dados_lista.append({
            
            'cep': cep,
            'logradouro': logradouro,
            'bairro': bairro,
            'localidade': localidade,
            'uf': uf,
            'ibge': ibge,
            'gia': gia,
            'ddd': ddd,
            'siafi': siafi
            
            })
        
    print('transformando em um dataframe')
    #salvar cada iteração em uma dataframe e depois salva-lo em um arquivo txt delimitado 
    df = pd.DataFrame(dados_lista)
    
    mensagem(f'Preparando para salvar: {lista}')
    caminho = '/workspaces/projeto_ing/apis/app/src/cep/'
    arquivo = f'dados_cep_mairipora_{lista}_{timestamp}.txt'
    
    df.to_csv(f'{caminho}{arquivo}', sep=';', index=False)
    print(f'Arquivo salvo: {arquivo}')
    print(f'Caminho: {caminho}')


mensagem('api finalizada!')

mensagem('iniciando concatenacao')
df_lista = []

for f in os.listdir(arquivos):
    if f.endswith('.txt'):
        caminho_arquivo = os.path.join(arquivos, f)
        print(f'Lendo arquivo: {f}')

        try:
            df = pd.read_csv(caminho_arquivo, sep=';')
            df_lista.append(df)
        except pd.errors.EmptyDataError:
            print(f"Aviso: O arquivo '{f}' está vazio e será ignorado.")
        
if df_lista:
    df_concat = pd.concat(df_lista, ignore_index=True)
    caminho = '/workspaces/projeto_ing/apis/app/src/cep/'
    arquivo = f'dados_cep_concat_{timestamp}.txt'
    
    df_concat.to_csv(f'{caminho}{arquivo}', sep=';', index=False)
    print(f'Arquivo salvo: {arquivo}')
    print(f'Caminho: {caminho}')
else:
    print("Nenhum arquivo válido foi encontrado para concatenação.")

mensagem('movendo arquivos para bckup')

files = '/workspaces/projeto_ing/apis/app/src/cep/'
dest = '/workspaces/projeto_ing/apis/app/src/backp/'
key = 'mairipora'
list_file = os.listdir(files)
# print(list_file)

try:
    
    for root, dirs, file in os.walk(files):
        print(f'procurando em: {root}')
        for nome_arquivo in file:
            print(f'verificando arquivo: {nome_arquivo}')
            if key in nome_arquivo:
                caminho_completo = os.path.join(root, nome_arquivo)
                print(f'arquivo contendo a chave encontrado: {caminho_completo}')
                
                shutil.move(caminho_completo, dest)
                print('arquivos contendo a chave movidos para backup')
            else:
                print('arquivos não encontrados')

except FileExistsError as e:
    print(e)