# Projeto_ing

Projeto dedicado à extração, transformação e carregamento de dados.

```plaintext
├── README.md

├── apis
│   ├── app
│   │   ├── src
│   │   │   ├── backp
│   │   │   ├── bank
│   │   │   ├── caldr
│   │   │   ├── car
│   │   │   ├── cep
│   │   │   ├── country
│   │   │   ├── geo
│   │   │   └── temp
│   │   ├── tmp
│   │   └── utls
│   └── setting

├── database
│   ├── banco_bank.db
│   ├── duckdb
│   ├── mongodb
│   └── sqlite
│       ├── bronze
│       ├── gold
│       └── silver

├── ingestion
│   ├── app
│   │   ├── src
│   │   │   ├── landing
│   │   │   └── layouts
│   │   └── utls
│   └── backup
│       ├── archiving_ctrl
│       ├── backp
│       └── tmp

├── pipeline
│   ├── app
│   └── setup

├── tests
│   ├── app
│   │   ├── ambiente_test_ing
│   │   ├── src
│   │   └── utls
│   └── setup

└── utils
    ├── __pycache__
    └── setup