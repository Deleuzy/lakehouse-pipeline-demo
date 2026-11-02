CREATE TABLE IF NOT EXISTS ${database}.bronze_energinet_prod_5min (
  record_json STRING,
  source STRING,
  batch_id STRING,
  ingested_at TIMESTAMP
) USING DELTA;
CREATE TABLE IF NOT EXISTS ${database}.bronze_price_areas (
  price_area STRING,
  area_name STRING,
  region STRING,
  source STRING,
  batch_id STRING,
  ingested_at TIMESTAMP
) USING DELTA;
CREATE TABLE IF NOT EXISTS ${database}.silver_production_5min (
  price_area STRING,
  interval_utc TIMESTAMP,
  production_lt_100_mw DOUBLE,
  production_ge_100_mw DOUBLE,
  offshore_wind_mw DOUBLE,
  onshore_wind_mw DOUBLE,
  solar_mw DOUBLE,
  exchange_great_belt_mw DOUBLE,
  exchange_germany_mw DOUBLE,
  exchange_netherlands_mw DOUBLE,
  exchange_great_britain_mw DOUBLE,
  exchange_norway_mw DOUBLE,
  exchange_sweden_mw DOUBLE,
  bornholm_se4_mw DOUBLE,
  source STRING,
  batch_id STRING,
  ingested_at TIMESTAMP
) USING DELTA;
CREATE TABLE IF NOT EXISTS ${database}.quarantine_production_5min (
  quarantine_id STRING,
  batch_id STRING,
  ingested_at TIMESTAMP,
  price_area STRING,
  interval_utc TIMESTAMP,
  failed_rules STRING,
  record_json STRING
) USING DELTA
;
CREATE TABLE IF NOT EXISTS ${database}.gold_daily_area_summary (
  day_utc DATE,
  price_area STRING,
  area_name STRING,
  intervals_observed BIGINT,
  intervals_expected INT,
  completeness_pct DECIMAL(5, 2),
  total_mwh DOUBLE,
  renewable_mwh DOUBLE,
  renewable_share_pct DOUBLE
) USING DELTA
;
CREATE TABLE IF NOT EXISTS ${database}.bronze_events (
  event_id STRING,
  message_id STRING,
  batch_id STRING,
  payload STRING,
  publish_time STRING,
  ingested_at TIMESTAMP
) USING DELTA;
CREATE TABLE IF NOT EXISTS ${database}.dead_letter_events (
  message_id STRING,
  batch_id STRING,
  reason STRING,
  payload STRING,
  received_at TIMESTAMP
) USING DELTA
