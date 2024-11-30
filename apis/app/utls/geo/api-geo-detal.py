from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
import pandas as pd
from datetime import datetime


timestamp = datetime.now().strftime('%Y-%m-%d')
path = '/workspaces/projeto_ing/apis/app/src/geo/'
geolocator = Nominatim(user_agent="my_geolocation_app_v1")

# place = input("Digite o nome do lugar: ")
place = ['Mairiporã', 'São Paulo', 'Atibaia', 'Campinas', 'Guarulhos', 'Osasco']

dados_lista = []
try:
    time.sleep(1)
    for local in place:
        location = geolocator.geocode(local, timeout=10)
    
        if location:
            data = location.raw
            loc_data = data['display_name'].split(", ")
            code = loc_data[-1]

            print("Localização completa:", location.address)
            print("Dados detalhados:", loc_data)
            print("Código (último componente):", loc_data[-1])
            
            dados_lista.append({
                'localizacao_compl': location,
                'dados_detalhados': loc_data,
                'code': code
            })
        else:
            print("Local não encontrado. Tente outro nome.")
    df = pd.DataFrame(dados_lista)
    print(df)
    
    file = f'dados_geo_detal_{timestamp}.txt'

    df.to_csv(f'{path}{file}', sep=';', index=False)
    print(f'Arquivo salvo: {file}')
    print(f'Caminho: {path}')
    
except GeocoderTimedOut:
    print("Erro: A solicitação demorou demais. Tente novamente.")
except GeocoderServiceError as e:
    print(f"Erro ao acessar o serviço de geolocalização: {e}")
except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")
