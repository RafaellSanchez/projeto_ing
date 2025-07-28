import os
import json
import sqlite3
import pandas as pd
import shutil
from datetime import datetime

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
data_igtao = datetime.now().strftime('%Y-%m-%d')

caminho_backup = '/workspaces/projeto_ing/INGESTION/app/src/backp/'
ctrl_ing = "/workspaces/projeto_ing/INGESTION/app/src/archiving_ctrl/arquivo_controle.txt"
landing = '/workspaces/projeto_ing/INGESTION/app/src/landing/'
layouts = '/workspaces/projeto_ing/INGESTION/app/src/layouts/'

try:
    read_landing = os.listdir(landing)
    print("Conteúdos do diretório 'landing':", read_landing)
    read_layouts = os.listdir(layouts)
    print("Conteúdos do diretório 'layouts':", read_layouts)
    print('=====================')

    for item in read_layouts:
        accept = os.path.join(layouts, item)
        if os.path.isdir(accept):
            print(f'{item} é uma pasta')
            subconteudos = os.listdir(accept)
            print(f'Conteúdos de {item}: {subconteudos}')

            caminho_estrutura_json = os.path.join(accept, 'estrutura.json')
            if os.path.exists(caminho_estrutura_json):
                print(f'Encontrado estrutura.json em {item}')
                
                with open(caminho_estrutura_json, 'r') as f:
                    config_estrutura = json.load(f)
                print("Configuração estrutura:", config_estrutura)

                with open(ctrl_ing, 'r+') as controle_ingestao:
                    conteudo_load = controle_ingestao.readlines()
                    print(f'Conteúdo do arquivo de controle de ingestão: {conteudo_load}')
                
                for processo in config_estrutura['estrutura']:
                    key = processo['chave']['palavra_chave']
                    banco = processo['chave']['banco']
                    tabela = processo['chave']['tabela']
                    nm_arquivo = processo['chave']['nome_arquivo']

                    for root, dirs, lista in os.walk(landing):
                        print(f'Procurando em {root}...')
                        for nome_arquivo in lista:
                            print(f'Verificando arquivo: {nome_arquivo}')
                            if key in nome_arquivo:
                                caminho_completo = os.path.join(root, nome_arquivo)
                                print(f'Arquivo contendo a chave encontrado: {caminho_completo}')
                                sav = os.path.basename(caminho_completo)
                                print(f'Conteúdo a gravar: {sav}')
                                
                                parte_util = sav.split('_')[1]
                                print(f'Separando nome do arquivo: {parte_util}')
                                tab_part = 'tabela_'

                                if sav + '\n' not in conteudo_load:
                                    with open(ctrl_ing, 'a') as controle_ingestao:
                                        controle_ingestao.write(f"{sav}\n")
                                    print(f'Dado salvo: {sav}')
                                    print('=================================')
                                    
                                    concat = tab_part + parte_util
                                    caminho_tabela_json = os.path.join(accept, f'tabela_{parte_util}.json')
                                    if os.path.exists(caminho_tabela_json):
                                        with open(caminho_tabela_json, 'r') as f:
                                            config_colunas = json.load(f)
                                        print('Lendo estrutura de coluna json')
                                        print(f'Conteúdo da config: {config_colunas}')
                                        print(f'Conteúdo do item: {item}')
                                        print('==========================')

                                        if caminho_completo.endswith('.txt'):
                                            df = pd.read_csv(caminho_completo, delimiter=';', skiprows=[0], names=[col['nome'] for col in config_colunas['colunas'] if 'nome' in col])
                                        elif caminho_completo.endswith('.csv'):
                                            df = pd.read_csv(caminho_completo, skiprows=[0], names=[col['nome'] for col in config_colunas['colunas'] if 'nome' in col])
                                        elif caminho_completo.endswith('.bin'):
                                            # Lógica para arquivos binários
                                            df = pd.DataFrame()  # Placeholder para lógica de arquivos binários
                                        else:
                                            raise ValueError("Formato de arquivo não suportado")

                                        for col in config_colunas['colunas']:
                                            if 'nome' in col and 'tipo' in col:
                                                if col['tipo'] == 'int':
                                                    df[col['nome']] = df[col['nome']].astype(int)
                                                elif col['tipo'] == 'float':
                                                    df[col['nome']] = df[col['nome']].astype(float)
                                                elif col['tipo'] == 'data':
                                                    df[col['nome']] = pd.to_datetime(df[col['nome']], format=col.get('formato', None))

                                        banco_caminho = f'/workspaces/projeto_ing/INGESTION/database/{banco}'
                                        conn = sqlite3.connect(banco_caminho)
                                        print(f'Banco de dados: {banco_caminho}')
                                        print(f'Banco de dados criado se não existir: {banco}')
                                        print(f'Conectado!')
                                        print(f"====================================")

                                        cursor = conn.cursor()
                                        nm_tb = f'tabela_{parte_util}'
                                        sql_create_table = f"CREATE TABLE IF NOT EXISTS {nm_tb} ("
                                        for col in config_colunas['colunas']:
                                            if 'nome' in col and 'tipo' in col:
                                                sql_create_table += f"{col['nome']} {col['tipo']}, "
                                        sql_create_table = sql_create_table[:-2] + ")"
                                        cursor.execute(sql_create_table)
                                        print(f'Tabela criada: {nm_tb}')

                                        for index, row in df.iterrows():
                                            mapped_values = [row.get(col['nome'], None) for col in config_colunas['colunas'] if 'nome' in col]
                                            print("Valores mapeados:", mapped_values)
                                            sql = f"INSERT INTO {nm_tb} ({', '.join([col['nome'] for col in config_colunas['colunas'] if 'nome' in col])}) VALUES ({', '.join(['?' for _ in mapped_values])})"
                                            print("SQL:", sql)
                                            cursor.execute(sql, mapped_values)
                                            

                                        conn.commit()
                                        conn.close()
                                        print("Ingestão de dados concluída com sucesso!")
                                        
                                        shutil.move(caminho_completo, caminho_backup)
                                        print(f'Arquivo enviado para backup: {caminho_completo}')
                                        print(f'Destino: {caminho_backup}')
                                        
except Exception as e:
    print(f"Erro: {e}")
