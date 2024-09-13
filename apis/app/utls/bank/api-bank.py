import pandas as pd
import requests
from datetime import datetime
import time

time.sleep(10)
print('Iniciando extração da api')

timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

url = 'https://opendata.api.bb.com.br/open-banking/channels/v1/banking-agents?page=1&page-size=2'
response = requests.get(url)
dados = response.json()
# display(dados)
print('iniciando get api')

list_data = []
time.sleep(3)
print('iniciando iteracao na api')
#realizando iterações em cada camada da api
for resultado in dados['data']['brand']['companies']:
    nome = resultado['name']
    cnpj = resultado['cnpjNumber']
    contrato = resultado['contractors']  
    
    for result in resultado['contractors']:
        contrato_nome = result['name']
        contrato_cnpj = result['cnpjNumber']
        contrato_bank = result['bankingAgents']   
        
        for cliente in result['bankingAgents']:
            identificacao = cliente['identification']
            corporacao  = cliente['identification']['corporationName']
            grupo = cliente['identification']['groupName']
            isUnderestablishment = cliente['identification']['isUnderestablishment']
            cnpjnumber = cliente['identification']['cnpjNumber']
            localizacao = cliente['locations']
            endereco = cliente['locations']
            
            for local in cliente['locations']:
                postal = local['postalAddress']
                ender = local['postalAddress']['address']
                enderIdentInfo = local['postalAddress']['additionalInfo']
                districtName = local['postalAddress']['districtName']
                townName = local['postalAddress']['townName']
                ibgeCode = local['postalAddress']['ibgeCode']
                postCode = local['postalAddress']['postCode']
                country = local['postalAddress']['country']
                countryCode = local['postalAddress']['countryCode']
                cordLat = local['postalAddress']['geographicCoordinates']['latitude']
                cordLongt = local['postalAddress']['geographicCoordinates']['longitude']
                texception = local['availability']['exception']
                telefone = local['phones']
                
                for dispo in local['availability']['standards']:
                    weekday = dispo['weekday']
                    opentime = dispo['openingTime']
                    closetime = dispo['closingTime']
                    
                    for fone in local['phones']:
                        phonetipo = fone['type']
                        paiscode = fone['countryCallingCode']
                        codearea = fone['areaCode']
                        numerofone = fone['number']
                    
                list_data.append({
                    'nome':nome,
                    'cnpj':cnpj,
                    'contrato_nome': contrato_nome,
                    'contrato_cnpj': contrato_cnpj,
                    'corporacao': corporacao,
                    'grupo': grupo,
                    'isUnderestablishment': isUnderestablishment,
                    'cnpjnumber': cnpjnumber,
                    'ender': ender,
                    'enderIdentInfo': enderIdentInfo,
                    'districtName': districtName,
                    'townName': townName,
                    'ibgeCode': ibgeCode,
                    'postCode': postCode,
                    'country': country,
                    'countryCode': countryCode,
                    'latitude': cordLat,
                    'longitude': cordLongt,
                    'semana': weekday,
                    'open': opentime,
                    'close': closetime,
                    'tipoTelefone': phonetipo,
                    'codigoPais': paiscode,
                    'codigoArea': codearea,
                    'numeroTelefone': numerofone                
                })
print('salvando dados na lista')
time.sleep(3)
print('transformando dados em dataframe')
#transformando minha lista em um dataframe
df = pd.DataFrame(list_data)


print('carregando variaveis')
caminho = '/workspaces/projeto_ing/API/app/src/bank/'
arquivo = f'dados_bank_{timestamp}.txt'


time.sleep(3)
print('salvando...')
df.to_csv(f'{caminho}{arquivo}', sep=';')
print(f'Arquivo salvo no caminho: {caminho}')
print(f'Arquivo: {arquivo}')