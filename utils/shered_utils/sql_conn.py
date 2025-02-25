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
        

def conectar_mongo (uri, db, collection ):
    """
    Funcao para conectar banco de dados mongodb
    """
    from pymongo import MongoClient
    
    uri = 'mongodb+srv://rafaelsanchesz:OYm5fyyVcgAObf35@cluster-githubsanchez.vgq0k.mongodb.net/'
    client = MongoClient(uri)
    db = client['meu_banco']
    collection = db['minha_colecao']
    
    return uri, client, collection


def conectar_bronze (db_sqlite, caminho_bronze=None, show_tables_query=None, tabela=None, query=None):
    import sqlite3
    caminho_bronze = f"/workspaces/projeto_ing/database/sqlite/bronze/{db_sqlite}"
    conn = sqlite3.connect(caminho_bronze)
    cursor = conn.cursor()
    print(f'conectado ao sqlite bronze: {db_sqlite}')
    
    show_tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
    cursor.execute(show_tables_query)

    tables = cursor.fetchall()
    print("Tabelas no banco de dados:")
    for table in tables:
        print(table[0])
        
    if query is None:
        if tabela is None:
            raise ValueError("A tabela deve ser fornecida se a query não for especificada.")
        query = f"select * from {tabela}"
    
    cursor.execute(query)
    consulta = cursor.fetchall()
    
    for resultado in consulta:
        print(resultado)
    
    return conn, cursor, caminho_bronze, consulta, query


# def consult_sqlite(banco, caminho_banco=None, tabela=None, query=None):
#     import sqlite3
    
#     if caminho_banco is None:
#         caminho_banco = f"/workspaces/projeto_ing/database/sqlite/bronze/{banco}"
    
#     conn = sqlite3.connect(caminho_banco)
#     cursor = conn.cursor()
    
#     if query is None:
#         if tabela is None:
#             raise ValueError("A tabela deve ser fornecida se a query não for especificada.")
#         query = f'SELECT * FROM {tabela}'
    
#     cursor.execute(query)
#     consulta = cursor.fetchall()
    
#     for resultado in consulta:
#         print(resultado)
        
#     conn.close()
    
    # return consulta

# def conectar_sqlite(banco, caminho_banco=None):
#     import sqlite3
#     # Se o caminho do banco não for fornecido, usa o caminho padrão
#     if caminho_banco is None:
#         caminho_banco = f"/workspaces/projeto_ing/database/sqlite/bronze/{banco}"
    
#     conn = sqlite3.connect(caminho_banco)
#     cursor = conn.cursor()
    
#     return conn, cursor, caminho_banco