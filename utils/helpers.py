import pymongo
import pandas as pd
import json
from pymongo import MongoClient
import sqlite3
from cryptography.fernet import Fernet
import pandas as pd

def conectar_mongo (uri, db, collection ):
    from pymongo import MongoClient
    uri = 'mongodb+srv://rafaelsanchez:1bHoI3DTdqfa6tNU@cluster001dev.vgq0k.mongodb.net/'
    # uri = 'mongodb+srv://rafaelsanchesz:OYm5fyyVcgAObf35@cluster-githubsanchez.vgq0k.mongodb.net/'
    client = MongoClient(uri)
    db = client['meu_banco']
    collection = db['minha_colecao']
    
    return uri, client, collection


def ler_arquivo(caminho, delimitador=","):
    import pandas as pd
    
    try:
        df = pd.read_csv(caminho, delimiter=delimitador)  
        return df
    except FileNotFoundError:
        print(f"Arquivo não encontrado: {caminho}")
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo: {e}")
        

def conectar_sqlite(banco, caminho_banco=None):
    import sqlite3
    # Se o caminho do banco não for fornecido, usa o caminho padrão
    if caminho_banco is None:
        caminho_banco = f"/workspaces/projeto_ing/database/sqlite/bronze/{banco}"
    
    conn = sqlite3.connect(caminho_banco)
    cursor = conn.cursor()
    
    return conn, cursor, caminho_banco


def consult_sqlite(banco, caminho_banco=None, tabela=None, query=None):
    import sqlite3
    
    if caminho_banco is None:
        caminho_banco = f"/workspaces/projeto_ing/database/sqlite/bronze/{banco}"
    
    conn = sqlite3.connect(caminho_banco)
    cursor = conn.cursor()
    
    if query is None:
        if tabela is None:
            raise ValueError("A tabela deve ser fornecida se a query não for especificada.")
        query = f'SELECT * FROM {tabela}'
    
    cursor.execute(query)
    consulta = cursor.fetchall()
    
    for resultado in consulta:
        print(resultado)
        
    conn.close()
    
    return consulta




def cripto_dados(caminho, chave=None, cipher_suite=None, arquivocript=None, delimitador=';'):
    from cryptography.fernet import Fernet
    import pandas as pd

    try:
        df = pd.read_csv(caminho, sep=delimitador)
        if chave is None:
            chave = Fernet.generate_key()
        if cipher_suite is None:
            cipher_suite = Fernet(chave)
        
        json_string = df.to_json()
        encrypted_data = cipher_suite.encrypt(json_string.encode())
        
        if arquivocript:
            with open(arquivocript, "wb") as file:
                file.write(encrypted_data)
                print('Arquivo criptografado salvo!')
                print(f'Caminho: {arquivocript}')
        
        return df, chave, cipher_suite, encrypted_data, arquivocript
    except FileNotFoundError:
        print(f"Arquivo não encontrado: {caminho}")
    except Exception as e:
        print(f"Ocorreu um erro ao processar o arquivo: {e}")

def mensagem(msg):
    print('-' *25)
    print(msg)
    print('-' *25)
    
    
    
    
import os
import shutil
from datetime import datetime

def envio_arquivos(files_dir, landing_dir, controle_path):
    """
    Função para enviar arquivos para um diretório de destino (landing),
    verificando um arquivo de controle para evitar duplicatas.
    
    Args:
        files_dir (str): Diretório onde os arquivos estão localizados.
        landing_dir (str): Diretório de destino para envio dos arquivos.
        controle_path (str): Caminho para o arquivo de controle.
    """
    # Gerar timestamp e data atual
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    datahoje = datetime.now().strftime('%Y%m%d')
    print(f"Data de hoje: {datahoje}")

    # Listar subdiretórios no diretório de origem
    subdirs = os.listdir(files_dir)
    print(f"Subdiretórios encontrados: {subdirs}")

    for item in subdirs:
        item_path = os.path.join(files_dir, item)
        print(f"Verificando item no diretório: {item}")
        
        if os.path.isdir(item_path):
            subconteudo = os.listdir(item_path)
            print(f"Conteúdo do subdiretório {item}: {subconteudo}")
        print('--------------------------------------')

    # Processar arquivos
    for root, _, files in os.walk(files_dir):
        print(f"Procurando em: {root}")
        for nome_arquivo in files:
            print(f"Verificando arquivo: {nome_arquivo}")
            
            caminho_completo = os.path.join(root, nome_arquivo)
            sep_name = os.path.basename(nome_arquivo)
            print(f"Nome do arquivo: {sep_name}")

            # Extrair data do nome do arquivo (assume formato esperado)
            try:
                sep_data = sep_name.split('_')[3:4]
                print(f'separando data: {sep_data}')
            except IndexError:
                print(f"Erro ao separar data no nome do arquivo: {sep_name}")
                continue
 
            if str(sep_data) >= str(datahoje):
                with open(controle_path, 'r+') as ctrl:
                    conteudo_ctrl = ctrl.readlines()
                    print(f'conteudo do arquivo de controle: {conteudo_ctrl}')
                    
                    if sep_name + '\n' not in conteudo_ctrl:            
                        with open(controle_path, 'a') as control_move:
                            control_move.write(f'{nome_arquivo}\n')
                            print('salvando nome do arquivo no controle')

                        print(f'arquivo que sera movido: {nome_arquivo}')
                        shutil.copy(caminho_completo, landing_dir)
                        print(f'Arquivo enviado: {caminho_completo}')
                        print(f'Destino: {landing_dir}')
                    else:
                        print('----------------------------------------------')
                        print(f'Arquivo já existe no controle: {nome_arquivo}')
            else:
                print(f"Erro no formato ou data do arquivo: {sep_name}")
    print("Processo finalizado!")



if __name__ == "__main__":
    # Diretórios e arquivo de controle
    files_dir = '/workspaces/projeto_ing/tests/app/src/teste/'
    landing_dir = '/workspaces/projeto_ing/tests/app/src/teste_landing/'
    controle_path = '/workspaces/projeto_ing/ingestion/backup/archiving_ctrl/arquivo_controle.txt'
    
    # Chamada da função
    envio_arquivos(files_dir, landing_dir, controle_path)
