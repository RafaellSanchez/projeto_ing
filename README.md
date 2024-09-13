# projeto_ing
Projeto dedicado a extração, transformação e carregamento de dados. 

│
├── README.md                       # Descrição do projeto
├── requirements.txt                # Dependências do Python
│
├── INGESTION/                      # Diretório principal para ingestão de dados
│   └── app/
│       ├── utls/
│       │   └── motor_ing.py        # Script principal de ingestão
│       │
│       └── src/
│           ├── layouts/            # Base da ingestão, com subpastas por tipo de arquivo
│           │   ├── api_name_1/
│           │   │   ├── tabela.json
│           │   │   └── estrutura.json
│           │   ├── api_name_2/
│           │   │   ├── tabela.json
│           │   │   └── estrutura.json
│           │   └── ...             # Outras APIs ou fontes de dados
│           │
│           └── landing/            # Diretório de "landing" para arquivos recebidos
│
├── apis/                           # Scripts para coleta de dados de diferentes APIs
│   ├── api1_collect.py             # Script para coleta de dados da API 1
│   ├── api2_collect.py             # Script para coleta de dados da API 2
│   └── ...                         # Scripts adicionais para outras APIs
│
├── tests/                          # Testes unitários e de integração
│   ├── test_apis.py                # Testes para os scripts de coleta de APIs
│   └── test_ingestion.py           # Testes para os scripts de ingestão
│
└── utils/                          # Funções utilitárias e auxiliares
    └── helpers.py                  # Funções de apoio usadas nos scripts
