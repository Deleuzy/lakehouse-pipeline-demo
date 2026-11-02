CREATE OR REPLACE TEMPORARY VIEW valid_production AS
SELECT
  price_area, interval_utc,
  production_lt_100_mw, production_ge_100_mw, offshore_wind_mw, onshore_wind_mw, solar_mw,
  exchange_great_belt_mw, exchange_germany_mw, exchange_netherlands_mw, exchange_great_britain_mw,
  exchange_norway_mw, exchange_sweden_mw, bornholm_se4_mw,
  source, batch_id, ingested_at
FROM checked_production
WHERE size(failed_rules) = 0;
MERGE INTO ${database}.silver_production_5min AS t
USING valid_production AS s
ON t.price_area = s.price_area AND t.interval_utc = s.interval_utc
WHEN MATCHED AND s.ingested_at > t.ingested_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
