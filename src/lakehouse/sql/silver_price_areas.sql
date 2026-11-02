CREATE OR REPLACE TABLE ${database}.silver_price_areas USING DELTA AS
WITH ranked AS (
  SELECT
    trim(price_area) AS price_area,
    trim(area_name) AS area_name,
    trim(region) AS region,
    ingested_at,
    row_number() OVER (PARTITION BY trim(price_area) ORDER BY ingested_at DESC) AS rn
  FROM ${database}.bronze_price_areas
  WHERE price_area IS NOT NULL
)
SELECT price_area, area_name, region, ingested_at
FROM ranked
WHERE rn = 1
