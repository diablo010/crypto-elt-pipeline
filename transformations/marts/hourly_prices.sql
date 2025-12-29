DROP TABLE IF EXISTS hourly_crypto_prices;

CREATE TABLE hourly_crypto_prices AS
SELECT
    DATE_TRUNC('hour', ingested_at) AS ingested_hour,   -- gives time bucket: 2025-01-02 02:00:00
    coin_id,
    ROUND(AVG(usd_price), 2) AS avg_price     -- avg as dags reruns take place
FROM stg_crypto_prices
GROUP BY  
    coin_id,
    ingested_hour
ORDER BY
    coin_id asc, 
    ingested_hour asc;

-- AVG is used:
-- bitcoin | 68000 | 2025-01-01 00:05
-- bitcoin | 68120 | 2025-01-01 00:06  ← retry
-- bitcoin | 67980 | 2025-01-01 12:00  ← manual run

-- DROP and CREATE logic for hourly runs