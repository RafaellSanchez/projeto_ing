import os
import json
import sqlite3
import pandas as pd
import shutil
from datetime import datetime
import time

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
data_igtao = datetime.now().strftime('%Y-%m-%d')
datahoje = str(data_igtao)
print(datahoje)

caminho_backup = "/workspaces/projeto_ing/ingestion/backup/backp/"
ctrl_ing = "/workspaces/projeto_ing/ingestion/backup/archiving_ctrl/arquivo_controle.txt"
landing = "/workspaces/projeto_ing/ingestion/app/src/landing/"
layouts = "/workspaces/projeto_ing/ingestion/app/src/layouts/"
files = '/workspaces/projeto_ing/apis/app/src/'
controle = '/workspaces/projeto_ing/ingestion/backup/archiving_ctrl/arquivo_controle.txt'


time.sleep(3)
print('===============================================')
print('iniciando movimentação de arquivos')
list = os.listdir(files)
print(list)
print(controle)
print('MOVENDO ARQUIVOS')


for item in list:
    accept = os.path.join(files, item)
    print(f'item encontrado no diretório: {item}')
    subconteudo = os.listdir(accept)
    print(f'conteudo: {subconteudo}')
    print('--------------------------------------')

    for root, dirs, lista in os.walk(files):
        print(f'proncurando em: {root}')
        print(f'----------------------')
        print(f'dirs: {dirs}')
        for nome_arquivo in lista:
            print(f'verificando nome do arquivo: {nome_arquivo}')
            if nome_arquivo in nome_arquivo:
                caminho_completo = os.path.join(root, nome_arquivo)
                print(f'caminho completo: {caminho_completo}')
                
                sep_name = os.path.basename(nome_arquivo)
                print(f'nome separado do arquivo: {sep_name}')
                print(f'------------------------------------')
                
                sep_data = sep_name.split('_')[3:4]
                print(f'separando data: {sep_data}')
                if str(sep_data) >= str(datahoje):
                    with open(controle, 'r+') as ctrl:
                        conteudo_ctrl = ctrl.readlines()
                        print(f'conteudo do arquivo de controle: {conteudo_ctrl}')
                        
                        if sep_name + '\n' not in conteudo_ctrl:            
                            with open(controle, 'a') as control_move:
                                control_move.write(f'{nome_arquivo}\n')
                                print('salvando nome do arquivo no controle')

                            print(f'arquivo que sera movido: {nome_arquivo}')
                            shutil.copy(caminho_completo, landing)
                            print(f'Arquivo enviado: {caminho_completo}')
                            print(f'Destino: {landing}')
                        else:
                            print('----------------------------------------------')
                            print(f'Arquivo já existe no controle: {nome_arquivo}')
                               
                else:
                    print('Erro no formato do arquivo')
print('codigo finalizado!')
print('===============================================')

time.sleep(3)
print('Iniciando ingestão de arquivos')
read_landing = os.listdir(landing)
print("Conteúdos do diretório 'landing':", read_landing)
read_layouts = os.listdir(layouts)
print("Conteúdos do diretório 'layouts':", read_layouts)
print('===============================================')

try:
    for item in read_layouts:
        accept = os.path.join(layouts, item)
        if os.path.isdir(accept):
            print('========================')
            print(f'item encontrado no diretório: {item}')
            subconteudos = os.listdir(accept)
            print('========================')
            print(f'conteudo: {subconteudos}')
            
            caminho_estrutura_json = os.path.join(accept, 'estrutura.json')
            if os.path.exists(caminho_estrutura_json):
                print(f'Encontrado estrutura.json em {item}')
                
                with open(ctrl_ing, 'r+') as controle_ingestao:
                    conteudo_load = controle_ingestao.readlines()
                    print(f'Conteúdo do arquivo de controle de ingestão: {conteudo_load}')
                    
                with open(caminho_estrutura_json, 'r') as f:
                    config_estrutura = json.load(f)
                    print("Configuração estrutura:", config_estrutura)
                    print("=======================")
                    print("Iniciando ingestão:")
                    print("=======================")
                    for estr in config_estrutura:
                        nm_arq = estr['nome_arquivo']
                        key = estr['chave']['palavra_chave']
                        banco = estr['chave']['banco']
                        tabela = estr['chave']['tabela']
                        print('=======================')
                        print(estr)
                        print(nm_arq)
                        print(key)
                        print(banco)
                        print(tabela)
                        print('=======================')
                                
                        for root, dirs, lista in os.walk(landing):
                            print(f'Procurando em {root}...')
                            for nome_arquivo in lista:
                                print(f'Verificando arquivo: {nome_arquivo}')
                                if key in nome_arquivo:
                                    caminho_completo = os.path.join(root, nome_arquivo)
                                    print(f'Arquivo contendo a chave encontrado: {caminho_completo}')
                                    
                                    sav = os.path.basename(caminho_completo)
                                    print(f'Conteúdo a gravar: {sav}')
                                    
                                    if sav + '\n' not in conteudo_load:
                                        with open(ctrl_ing, 'a') as controle_ingestao:
                                            controle_ingestao.write(f"{sav}\n")
                                            print(f'nomenclatura do arquivo salvo: {sav}')
                                            print('=================================')
                                    
                                    parte_util = sav.split('_')[1]
                                    if parte_util == key:
                                        print('Chave encontrada')
                                        print(f'Separando nome do arquivo: {parte_util}')
                                        caminho_tabela_json = os.path.join(accept, f'{tabela}.json')
                                        print(f'local para criar tabela: {caminho_tabela_json}')
                                        if os.path.exists(caminho_tabela_json):
                                            with open(caminho_tabela_json, 'r') as f:     
                                                config_colunas = json.load(f)
                                            print(config_colunas)
                                            print(f'resultado configuracao colunas: {config_colunas}')
                                            print('===============================')
                                            for j in config_colunas['colunas']:
                                                print(f'iterando config: {j}')
                                            
                                            if caminho_completo.endswith('.txt'):
                                                df = pd.read_csv(caminho_completo, delimiter=';', skiprows=[0], names=[col['nome'] for col in config_colunas['colunas'] if 'nome' in col])
                                                print(f'dados a carregar:{df}')
                                                print('estrutura de colunas txt:')
                                                print(df.columns)
                                                print('=====================')
                                            elif caminho_completo.endswith('.csv'):
                                                df = pd.read_csv(caminho_completo, skiprows=[0], names=[col['nome'] for col in config_colunas['colunas'] if 'nome' in col])
                                                print(f'dados a carregar:{df}')
                                                print('estrutura de colunas csv:')
                                                print(df.columns)
                                                print('=====================')
                                            elif caminho_completo.endswith('.bin'): 
                                                pass
                                            
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
                                                        
                                            banco_caminho = f'/workspaces/projeto_ing/database/sqlite/bronze/{banco}'
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
                                            print(f'estrutura da tabela criada: {sql_create_table}')
                                            
                                            for index, row in df.iterrows():
                                                mapped_values = [row.get(col['nome'], None) for col in config_colunas['colunas'] if 'nome' in col]
                                                print("Valores mapeados:", mapped_values)
                                                sql = f"INSERT INTO {nm_tb} ({', '.join([col['nome'] for col in config_colunas['colunas'] if 'nome' in col])}) VALUES ({', '.join(['?' for _ in mapped_values])})"
                                                print("SQL:", sql)
                                                cursor.execute(sql, mapped_values)
                                        
                                            conn.commit()
                                            conn.close()
                                            print("Ingestão de dados concluída com sucesso!")
                                            print("====================================")
                                            
                                        shutil.move(caminho_completo, caminho_backup)
                                        print(f'Arquivo enviado para backup: {caminho_completo}')
                                        print(f'Destino: {caminho_backup}')
                                        print('==========================')
                                    else:
                                        print('Chave não encontrada no arquivo')
                                    break
except Exception as e:
    print(f"Erro: {e}")

