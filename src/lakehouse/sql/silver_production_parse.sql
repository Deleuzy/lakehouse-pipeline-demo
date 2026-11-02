CREATE OR REPLACE TEMPORARY VIEW parsed_production AS
WITH raw AS (
  SELECT
    batch_id,
    ingested_at,
    source,
    record_json,
    from_json(
      record_json,
      'Minutes5UTC STRING, PriceArea STRING, ProductionLt100MW DOUBLE, ProductionGe100MW DOUBLE, OffshoreWindPower DOUBLE, OnshoreWindPower DOUBLE, SolarPower DOUBLE, ExchangeGreatBelt DOUBLE, ExchangeGermany DOUBLE, ExchangeNetherlands DOUBLE, ExchangeGreatBritain DOUBLE, ExchangeNorway DOUBLE, ExchangeSweden DOUBLE, BornholmSE4 DOUBLE'
    ) AS r
  FROM bronze_batch
)
SELECT
  batch_id,
  ingested_at,
  source,
  record_json,
  try_to_timestamp(r.Minutes5UTC) AS interval_utc,
  r.PriceArea AS price_area,
  r.ProductionLt100MW AS production_lt_100_mw,
  r.ProductionGe100MW AS production_ge_100_mw,
  r.OffshoreWindPower AS offshore_wind_mw,
  r.OnshoreWindPower AS onshore_wind_mw,
  r.SolarPower AS solar_mw,
  r.ExchangeGreatBelt AS exchange_great_belt_mw,
  r.ExchangeGermany AS exchange_germany_mw,
  r.ExchangeNetherlands AS exchange_netherlands_mw,
  r.ExchangeGreatBritain AS exchange_great_britain_mw,
  r.ExchangeNorway AS exchange_norway_mw,
  r.ExchangeSweden AS exchange_sweden_mw,
  r.BornholmSE4 AS bornholm_se4_mw
FROM raw
