CREATE OR REPLACE TEMPORARY VIEW checked_production AS
WITH ranked AS (
  SELECT
    c.*,
    row_number() OVER (
      PARTITION BY
        CASE WHEN size(c.base_failures) = 0 THEN c.price_area END,
        CASE WHEN size(c.base_failures) = 0 THEN c.interval_utc END
      ORDER BY c.ingested_at DESC, c.record_json
    ) AS dup_rank
  FROM rule_checked_production c
)
SELECT
  r.*,
  filter(
    concat(r.base_failures, array(
      CASE WHEN size(r.base_failures) = 0 AND r.dup_rank > 1 THEN 'duplicate_superseded' END
    )),
    x -> x IS NOT NULL
  ) AS failed_rules
FROM ranked r
