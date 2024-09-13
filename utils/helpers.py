import pandas as pd
import pymongo

import pymongo
import pandas as pd
import json
from pymongo import MongoClient

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