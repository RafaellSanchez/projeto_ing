import pandas as pd
from faker import Faker
from datetime import datetime

timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
digtao = datetime.now().strftime('%Y-%m-%d')

fake = Faker('pt_BR')
dados = []


for _ in range(100):
    fake_name = fake.name()
    fake_cpf = fake.cpf()
    fake_email = fake.email()
    fake_cidade = fake.city()
    fake_ender = fake.address()
    fake_bairro = fake.bairro()
    fake_estado = fake.estado()
    fake_est_nome = fake.estado_nome()
    fake_est_sgla = fake.estado_sigla()
    fake_phone = fake.cellphone_number()
    fake_pais = fake.country()
    fake_pais_code = fake.country_code()
    fake_card_exp = fake.credit_card_expire()
    fake_card_full = fake.credit_card_full()
    fake_card_numb = fake.credit_card_number()
    fake_card_prv = fake.credit_card_provider()
    fake_card_secrt = fake.credit_card_security_code()
    fake_dt = fake.date()
    fake_hr = fake.date_time()

    
    dados.append({
        'nome': fake_name,
        'cpf': fake_cpf,
        'email': fake_email,
        'endereco': fake_ender,
        'bairro': fake_bairro,
        'estado': fake_estado,
        'estado_nome': fake_est_nome,
        'sigla': fake_est_sgla,
        'telefone': fake_phone,
        'pais': fake_pais,
        'code': fake_pais_code,
        'cartao_cred': fake_card_full,
        'cartao_num': fake_card_numb,
        'credt_exp': fake_card_exp,
        'cvv': fake_card_secrt,
        'data': fake_dt,
        'data_hora': fake_hr,
        'digtap_ptcap': digtao
        
    })
df = pd.DataFrame(dados)
print(df)

print('carregando variaveis')
caminho = '/workspaces/projeto_ing/apis/app/src/faker/'
arquivo = f'dados_fakes_{timestamp}.txt'


print('salvando...')
df.to_csv(f'{caminho}{arquivo}', sep=';')
print(f'Arquivo salvo no caminho: {caminho}')
print(f'Arquivo: {arquivo}')