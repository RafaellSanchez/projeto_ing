import sys
import os
from datetime import datetime
import time
import pandas as pd
import importlib
import sql_conn
importlib.reload(sql_conn)

sys.path.append(os.path.abspath('/workspaces/projeto_ing/utils/shered_utils/'))

from sql_conn import conectar_mongodb
from sql_conn import conectar_bronze
from sql_conn import enviar_para_mongodb
from sql_conn import mensagem
from sql_conn import get_database


''' 
Motor dedicado ao carregamento e envio de dados para o mongo.db

Utilizndo a função conectar_mongo para conexão entre fontes
Utilizndo a função conectar_bronze para realizar a transformação dos dados a serem enviados
Utilizndo a função enviar_para_mongodb para envio do DataFrame para a collection
'''

# path = '/workspaces/projeto_ing/tests/app/ambiente_test_ing/app/src/landing/'
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

def carregamento_landing(data_dict, path=None, file=None, resultado=None ):
    import os
    import sys
    
    if path is None:
        path = '/workspaces/projeto_ing/apis/app/src/'
        print('caminho não informado')
        print('utilizando o padrão:')
        print(f'{path}')
    
    read_landing = os.listdir(path)
    
    for reads in read_landing:
        accept = os.path.join(path, reads)
        print(f'item encontrado: {accept}')
        
        sav = os.path.basename(accept)
        print(f'Conteúdo a gravar: {sav}')
        
        backp = '/workspaces/projeto_ing/ingestion/backup/archiving_ctrl/arquivo_controle_mongo.txt'
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


dbname = get_database()
collection_name = dbname["collection_test02_mng_dev"]

data_dict = carregamento_landing(data_dict)


collection_name.insert_many(data_dict)