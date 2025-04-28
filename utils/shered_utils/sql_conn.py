import pymongo
import pandas as pd
import json
from pymongo import MongoClient
import sqlite3
from cryptography.fernet import Fernet
from datetime import datetime
import importlib
import sql_conn
importlib.reload(sql_conn)
import os

def mensagem(msg):
    print('-' *25)
    print(msg)
    print('-' *25)

def log_ingestao(log_igtao_tb):
    """
    Funcao para gravar o log de ingestão do dado bruto.
    """
    
    banco_log = '/workspaces/projeto_ing/database/sqlite/bronze/bnc_log_igtao.db'
    conn_log = sqlite3.connect(banco_log)
    tabela_log = 'tb_log_igtao'

    if not log_igtao_tb:
        raise ValueError("O nome do arquivo 'log_igtao_tb' não pode ser vazio.")

    try:
        sep_tabela = log_igtao_tb.split('_')[1]
        nomelog_tabela = 'tabela_' + sep_tabela
        nomelog_arquivo = log_igtao_tb
        tipolog_arquivo = log_igtao_tb.split('.')[-1]
        datalog_igtao = datetime.now().strftime('%Y-%m-%d')
        datalog_hora = datetime.now().strftime('%Y%m%d%H%M%S')
        nomelog_banco = 'banco_' + sep_tabela

        print('Ingerindo dados de log:')
        print(f'Tabela: {nomelog_tabela}')
        print(f'Banco: {nomelog_banco}')
        print(f'Arquivo: {nomelog_arquivo}')

        log_igtao = [{
            'nome_banco': nomelog_banco,
            'nome_tabela': nomelog_tabela,
            'nome_arquivo': nomelog_arquivo,
            'tipo_arquivo': tipolog_arquivo,
            'data_inclusao': datalog_hora,
            'dt_igtao': datalog_igtao
        }]

        df_log = pd.DataFrame(log_igtao)

        query_insert = f"""
        INSERT INTO {tabela_log} (
            nome_banco, nome_tabela, nome_arquivo, tipo_arquivo, data_inclusao, dt_igtao
        ) VALUES (?, ?, ?, ?, ?, ?);
        """

        conn_log.executemany(query_insert, df_log.itertuples(index=False, name=None))
        conn_log.commit()

        print('Log de ingestão gravado na tabela.')

    except Exception as e:
        print(f"Erro ao inserir log: {e}")
    finally:
        conn_log.close()
        
 
def conectar_mongodb():
    """
    Conecta ao MongoDB e retorna a coleção.
    
    Retorna:
    - client (MongoClient): Cliente MongoDB.
    - collection (Collection): Coleção MongoDB para inserção.
    """

    uri = os.getenv("MONGO_URI", "mongodb+srv://rafaelsanchesz:OYm5fyyVcgAObf35@cluster001dev.vgq0k.mongodb.net/")
    client = MongoClient(uri)
    db = client['dev_mongo001_rf']
    collection = db['collection_test_mng_dev']

    try:
        client.admin.command('ping')
        print("Conexão bem-sucedida com o MongoDB!")
        return client, collection
    except Exception as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        return None, None



def conectar_bronze(db_sqlite, tabela=None, query=None):
    """
    Conecta ao banco de dados SQLite e retorna os resultados de uma consulta.
    
    Parâmetros:
    - db_sqlite (str): Nome do banco SQLite.
    - tabela (str): Nome da tabela a ser consultada (obrigatório se query não for especificada).
    - query (str, opcional): Consulta SQL personalizada.

    Retorna:
    - conn: Conexão com o SQLite.
    - cursor: Cursor do banco.
    - caminho_bronze: Caminho do banco SQLite.
    - consulta: Resultado da consulta (lista de tuplas).
    - colunas: Nome das colunas retornadas.
    - query: A consulta SQL executada.
    """

    caminho_bronze = f"/workspaces/projeto_ing/database/sqlite/bronze/{db_sqlite}"
    conn = sqlite3.connect(caminho_bronze)
    cursor = conn.cursor()
    print(f'Conectado ao SQLite Bronze: {db_sqlite}')

    show_tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
    cursor.execute(show_tables_query)
    tables = cursor.fetchall()

    print("Tabelas no banco de dados:")
    for table in tables:
        print(table[0])
    
    if query is None:
        if tabela is None:
            raise ValueError("A tabela deve ser fornecida se a query não for especificada.")
        query = f"SELECT * FROM {tabela}"
    
    cursor.execute(query)
    consulta = cursor.fetchall()
    colunas = [desc[0] for desc in cursor.description]

    return conn, cursor, caminho_bronze, consulta, colunas, query


def enviar_para_mongodb(consulta, colunas, mongo_collection):
    """
    Converte os dados do SQLite em um DataFrame e insere no MongoDB.

    Parâmetros:
    - consulta (list of tuples): Dados retornados do SQLite.
    - colunas (list): Lista com os nomes das colunas.
    - mongo_collection (Collection): Coleção do MongoDB onde os dados serão inseridos.
    """
    
    if not consulta:
        print("Nenhum dado para inserir no MongoDB.")
        return
    
    df = pd.DataFrame(consulta, columns=colunas)
    df = df.where(pd.notna(df), None)
    
    data_dict = df.to_dict("records")
    mongo_collection.insert_many(data_dict)
    print(data_dict)
    print("Dados inseridos com sucesso no MongoDB!")


###### EXEMPLO DE EXECUÇÃO #############################

# conn, cursor, caminho_bronze, consulta, colunas, query = conectar_bronze(db_sqlite="bnc_log_igtao.db", tabela="tb_log_igtao")

# client, collection = conectar_mongodb()

# if collection is not None:
#     enviar_para_mongodb(consulta, colunas, collection)

#########################################################


def get_database():
 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb+srv://rafaelsanchesz:OYm5fyyVcgAObf35@cluster001dev.vgq0k.mongodb.net/"
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client['dev_mongo001_rf']
  
# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":   
  
   # Get the database
   dbname = get_database()


def carregamento_landing(data_dict, path=None, file=None, resultado=None ):
    import os
    import sys
    
    if path is None:
        path = '/workspaces/projeto_ing/tests/app/ambiente_test_ing/app/src/landing/'
        print('caminho não informado')
        print('utilizando o padrão:')
        print(f'{path}')
    
    read_landing = os.listdir(path)
    
    for reads in read_landing:
        accept = os.path.join(path, reads)
        print(f'item encontrado: {accept}')
        
        sav = os.path.basename(accept)
        print(f'Conteúdo a gravar: {sav}')
        
        backp = '/workspaces/projeto_ing/tests/app/ambiente_test_ing/backup/arquivos_mongo.txt'
        with open(backp, 'r+') as controle_ingestao: 
            conteudo_load = controle_ingestao.readlines()
            print(f'Conteúdo do arquivo de controle de ingestão: {conteudo_load}')
            
            if sav + '\n' not in conteudo_load:
                with open(backp, 'a') as controle_ingestao:
                    controle_ingestao.write(f"{sav}\n")
                    print(f'nomenclatura do arquivo salvo: {sav}')
                    print('=================================')

                if accept.endswith('.txt'):
                    resultado = pd.read_csv(accept, delimiter=';')
                    resultado = resultado.where(pd.notna(resultado), None)

                    data_dict = resultado.to_dict("records")
                    print(data_dict)
                    print("Dados para inserir com sucesso no MongoDB!")

                else:
                    raise ValueError("Formato de arquivo não suportado")
            else:
                print('arquivo já inserido')

    return data_dict

#exemplo de uso
# dbname = get_database()
# collection_name = dbname["collection_test02_mng_dev"]

# data_dict = carregamento_landing(data_dict)

# collection_name.insert_many(data_dict)