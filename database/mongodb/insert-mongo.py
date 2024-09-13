import pandas as pd
import pymongo

arquivo = '/workspaces/projeto_ing/CLOUD/app/data/dados_cep_mairipora_Avenida.txt'

df = pd.read_csv(arquivo, delimiter=';')
json_data = df.to_dict(orient='records')
data = '/workspaces/projeto_ing/CLOUD/app/data/'
with open(f'{data}dados_cep.json', 'w') as f:
    import json
    json.dump(f"{json_data}", f, indent=4)