## Crypto ELT Pipeline

It fetches data through `CoinGecko API` and runs the entire process through dockerized containers. The pipeline is scheduled to fetches data at an interval of 15 minutes using `Airflow` and gives `average_hourly_prices`.

Structure:
```bash
crypto-elt-pipeline/
├── ingestion/
│   └── fetch_data.py     # handles api calls for data ingestion, and loads into local data warehouse
├── orchestration/
│   └── dag.py      # automates ingestion and transformation 
├── transformations/
│   ├── marts/hourly_prices.sql     # sql query for storing data into hour_buckets      
│   └── staging/stg_crypto_prices.sql   # tables staged for transformation    
├── README.md
└── requirements.txt
```
