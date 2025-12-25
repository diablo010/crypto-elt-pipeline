## Crypto ELT Pipeline

It fetches data through `Coingecko API` and runs the entire process through dockerized containers. The pipeline is scheduled to fetch hourly data using `Airflow`.

Structure:
```bash
crypto-elt/
├── ingestion/
│   └── fetch_crypto.py     # handles api calls for data ingestion, and loads into local data warehouse
├── transformations/
│   ├── marts/hourly_prices.sql     # sql query for storing data into hour_buckets      
│   └── staging/stg_crypto_prices.sql   # tables staged for transformation
├── orchestration/
│   └── dag.py      # automates ingestion and transformation        
├── README.md
└── requirements.txt
```
