CREATE OR REPLACE TEMPORARY VIEW quarantined_production AS
SELECT
  sha2(concat_ws('|', batch_id, record_json, dup_rank), 256) AS quarantine_id,
  batch_id,
  ingested_at,
  price_area,
  interval_utc,
  concat_ws(',', failed_rules) AS failed_rules,
  record_json
FROM checked_production
WHERE size(failed_rules) > 0;
MERGE INTO ${database}.quarantine_production_5min AS t
USING quarantined_production AS s
ON t.quarantine_id = s.quarantine_id
WHEN NOT MATCHED THEN INSERT *
