import pymongo
import pandas as pd
import json
from pymongo import MongoClient
import sqlite3
from cryptography.fernet import Fernet
import pandas as pd

def conectar_mongo (uri, db, collection ):
    from pymongo import MongoClient
    
    uri = 'mongodb+srv://rafaelsanchesz:OYm5fyyVcgAObf35@cluster-githubsanchez.vgq0k.mongodb.net/'
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
        caminho_banco = f"/workspaces/projeto_ing/database/{banco}"
    
    conn = sqlite3.connect(caminho_banco)
    cursor = conn.cursor()
    
    return conn, cursor, caminho_banco


def consult_sqlite(banco, caminho_banco=None, tabela=None, query=None):
    import sqlite3
    
    if caminho_banco is None:
        caminho_banco = f"/workspaces/projeto_ing/database/{banco}"
    
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