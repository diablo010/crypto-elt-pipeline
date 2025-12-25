from dotenv import load_dotenv
import os
from coingecko_sdk import Coingecko
import psycopg2
import datetime

def ingest_data():

    load_dotenv()

    client = Coingecko(
        demo_api_key=os.environ.get("COINGECKO_DEMO_API_KEY"), # for Demo API access
        environment="demo", # "demo" to initialize the client with Demo API access
    )

    coins = ["bitcoin", "ethereum", "solana"]

    prices = client.simple.price.get(
        vs_currencies="usd",
        ids=",".join(coins)     # join accepts strings not dicts
    )

    # print(coin_names)
    # print(type(coin_names))

    ingested_at = datetime.datetime.now()

    records = []

    for coin_id, price_obj in prices.items():
        records.append({
            "coin_id": coin_id,
            "usd_price": price_obj.usd,
            "ingested_at": ingested_at
        })

    # print(records)

    connection = psycopg2.connect(
        database="airflow",
        user="your_user",
        password="your_password",
        host="postgres",
        port=5432
    )

    cursor = connection.cursor()

    # cursor.execute("DROP TABLE IF EXISTS raw_crypto_data;")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_crypto_data (
            id SERIAL PRIMARY KEY,
            coin_id TEXT,
            usd_price FLOAT,
            ingested_at TIMESTAMP
        )""")

    values = [
        (r["coin_id"], r["usd_price"], r["ingested_at"])
        for r in records
    ]

    insert_query = """
    INSERT INTO raw_crypto_data (
        coin_id, 
        usd_price, 
        ingested_at
    )
    VALUES (%s, %s, %s)
    """

    cursor.executemany(insert_query, values)    # many rows but still tuples
    connection.commit()

    # print(type(values))   # class 'list'
    # print(type(values[0]))   # class 'tuple'