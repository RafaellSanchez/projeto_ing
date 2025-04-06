import sys
from datetime import datetime
import time

sys.path.append(os.path.abspath('/workspaces/projeto_ing/utils/shered_utils/'))

from sql_conn import conectar_mongodb
from sql_conn import conectar_bronze
from sql_conn import enviar_para_mongodb
from sql_conn import mensagem

''' 
Motor dedicado ao carregamento e envio de dados para o mongo.db

Utilizndo a função conectar_mongo para conexão entre fontes
Utilizndo a função conectar_bronze para realizar a transformação dos dados a serem enviados
Utilizndo a função enviar_para_mongodb para envio do DataFrame para a collection

'''

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')



