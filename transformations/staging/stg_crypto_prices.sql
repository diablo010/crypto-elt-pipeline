DROP TABLE IF EXISTS stg_crypto_prices;

CREATE TABLE stg_crypto_prices AS
SELECT
    id,
    coin_id,
    CAST(usd_price AS NUMERIC) as usd_price,
    CAST(ingested_at AS TIMESTAMP) AS ingested_at
FROM raw_crypto_data;

-- DROP and CREATE logic for hourly runs