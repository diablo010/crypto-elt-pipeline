CREATE TABLE IF NOT EXISTS hourly_crypto_prices AS
SELECT
    coin_id,
    DATE_TRUNC('hour', ingested_at) AS ingested_hour,   -- gives time bucket: 2025-01-02 02:00:00
    AVG(usd_price) AS avg_price     -- avg as dags reruns take place
FROM stg_crypto_prices
GROUP BY 
    coin_id, 
    ingested_hour;

-- AVG is used:
-- bitcoin | 68000 | 2025-01-01 00:05
-- bitcoin | 68120 | 2025-01-01 00:06  ← retry
-- bitcoin | 67980 | 2025-01-01 12:00  ← manual run
