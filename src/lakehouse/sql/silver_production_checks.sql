CREATE OR REPLACE TEMPORARY VIEW rule_checked_production AS
SELECT
  *,
  filter(array(
    CASE WHEN interval_utc IS NULL OR price_area IS NULL THEN 'not_null_key' END,
    CASE WHEN price_area NOT IN ('DK1', 'DK2') THEN 'valid_price_area' END,
    CASE WHEN interval_utc > ingested_at THEN 'not_future_interval' END,
    CASE WHEN production_lt_100_mw IS NULL OR production_ge_100_mw IS NULL
           OR offshore_wind_mw IS NULL OR onshore_wind_mw IS NULL OR solar_mw IS NULL THEN 'not_null_production' END,
    CASE WHEN production_lt_100_mw < 0 OR production_ge_100_mw < 0
           OR offshore_wind_mw < 0 OR onshore_wind_mw < 0 OR solar_mw < 0 THEN 'production_non_negative' END,
    CASE WHEN greatest(
           abs(production_lt_100_mw), abs(production_ge_100_mw), abs(offshore_wind_mw), abs(onshore_wind_mw),
           abs(solar_mw), abs(exchange_great_belt_mw), abs(exchange_germany_mw), abs(exchange_netherlands_mw),
           abs(exchange_great_britain_mw), abs(exchange_norway_mw), abs(exchange_sweden_mw), abs(bornholm_se4_mw)
         ) > ${max_abs_mw} THEN 'value_out_of_range' END
  ), x -> x IS NOT NULL) AS base_failures
FROM parsed_production
