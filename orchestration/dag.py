import sys
sys.path.append("/opt/crypto-elt-pipeline")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime, timedelta
from ingestion.fetch_data import ingest_data

default_args = {
    'owner': 'diablo010',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='crypto_data',
    default_args=default_args,
    start_date=datetime(2025, 12, 23),    
    schedule='@hourly',
    catchup=False,
    template_searchpath=[
        "/opt/crypto-elt-pipeline/transformations"
    ]
) as dag:
    
    crypto_data_task = PythonOperator(     # task1: running api calls and storing into tables
        task_id='ingest_data',
        python_callable=ingest_data
)

    run_staging = SQLExecuteQueryOperator(      # TemplateNotFound error: as sql is templated i airflow 3
    task_id="stg_crypto_prices",
    conn_id="postgres_default",    # looks up for connection_object like in connector
    sql="staging/stg_crypto_prices.sql"
)

    run_mart = SQLExecuteQueryOperator(
    task_id="hourly_prices",
    conn_id="postgres_default",
    sql="marts/hourly_prices.sql"
)

    crypto_data_task >> run_staging >> run_mart