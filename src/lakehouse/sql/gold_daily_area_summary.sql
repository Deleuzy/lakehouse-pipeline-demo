CREATE OR REPLACE TEMPORARY VIEW gold_source AS
WITH per_interval AS (
  SELECT
    price_area,
    CAST(interval_utc AS DATE) AS day_utc,
    production_lt_100_mw + production_ge_100_mw + offshore_wind_mw + onshore_wind_mw + solar_mw AS total_mw,
    offshore_wind_mw + onshore_wind_mw + solar_mw AS renewable_mw
  FROM ${database}.silver_production_5min
  ${day_filter}
),
daily AS (
  SELECT
    price_area,
    day_utc,
    COUNT(*) AS intervals_observed,
    SUM(total_mw) * 5 / 60 AS total_mwh,
    SUM(renewable_mw) * 5 / 60 AS renewable_mwh
  FROM per_interval
  GROUP BY price_area, day_utc
)
SELECT
  d.day_utc,
  d.price_area,
  a.area_name,
  d.intervals_observed,
  ${intervals_per_day} AS intervals_expected,
  round(100.0 * d.intervals_observed / ${intervals_per_day}, 2) AS completeness_pct,
  round(d.total_mwh, 2) AS total_mwh,
  round(d.renewable_mwh, 2) AS renewable_mwh,
  round(100.0 * d.renewable_mwh / NULLIF(d.total_mwh, 0), 2) AS renewable_share_pct
FROM daily d
LEFT JOIN ${database}.silver_price_areas a ON d.price_area = a.price_area;
MERGE INTO ${database}.gold_daily_area_summary AS t
USING gold_source AS s
ON t.day_utc = s.day_utc AND t.price_area = s.price_area
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
