# Data dictionary

All timestamps are UTC. Power values are average MW over a 5-minute interval.
Energy (MWh) = MW × 5 / 60.

## Bronze

### `bronze_energinet_prod_5min`
Raw API records, append-only. One row per record received in a batch.

| Column | Type | Meaning |
|---|---|---|
| record_json | STRING | Record exactly as returned by the API, keys sorted. |
| source | STRING | `sample:<file>` or `live:<url> <start>..<end>`. |
| batch_id | STRING | Id of the pipeline run that loaded the row. |
| ingested_at | TIMESTAMP | Time the row was loaded. |

### `bronze_price_areas`
Raw rows from `data/reference/price_areas.csv`, same metadata columns as above
(`price_area`, `area_name`, `region` as text).

## Silver

### `silver_production_5min`
Typed, validated, one row per (price_area, interval_utc). Only rows that pass every rule.

| Column | Type | Meaning |
|---|---|---|
| price_area | STRING | DK1 (western) or DK2 (eastern). |
| interval_utc | TIMESTAMP | Start of the 5-minute interval. Part of the key. |
| production_lt_100_mw | DOUBLE | Production from units under 100 MW, as reported. |
| production_ge_100_mw | DOUBLE | Production from units of 100 MW and above, as reported. |
| offshore_wind_mw | DOUBLE | Offshore wind production. |
| onshore_wind_mw | DOUBLE | Onshore wind production. |
| solar_mw | DOUBLE | Solar production. |
| exchange_great_belt_mw | DOUBLE | Exchange across the Great Belt. Sign convention as published by Energinet (not checked here). Nullable. |
| exchange_germany_mw | DOUBLE | Exchange with Germany. Nullable. |
| exchange_netherlands_mw | DOUBLE | Exchange with the Netherlands. Null when the feed has no value. |
| exchange_great_britain_mw | DOUBLE | Exchange with Great Britain. Null when the feed has no value. |
| exchange_norway_mw | DOUBLE | Exchange with Norway. Null when the feed has no value. |
| exchange_sweden_mw | DOUBLE | Exchange with Sweden. Nullable. |
| bornholm_se4_mw | DOUBLE | Exchange on the Bornholm–SE4 link. Nullable. |
| source | STRING | Source of the batch the row came from. |
| batch_id | STRING | Batch that last wrote the row. |
| ingested_at | TIMESTAMP | Ingest time of that batch. A newer batch overwrites the row. |

### `silver_price_areas`
One row per price area, latest reference row wins. Trimmed text.

| Column | Type | Meaning |
|---|---|---|
| price_area | STRING | Area code. |
| area_name | STRING | Name, e.g. "Western Denmark". |
| region | STRING | Regions covered. |
| ingested_at | TIMESTAMP | Ingest time of the row used. |

### `quarantine_production_5min`
Rows that failed at least one rule. Nothing is dropped silently.

| Column | Type | Meaning |
|---|---|---|
| quarantine_id | STRING | SHA-256 of batch id, raw record and duplicate rank. Unique per batch; a retried batch does not add rows. |
| batch_id | STRING | Batch the row came from. |
| ingested_at | TIMESTAMP | Ingest time. |
| price_area | STRING | As received (may be null or invalid). |
| interval_utc | TIMESTAMP | As received (may be null). |
| failed_rules | STRING | Comma-separated rule names, see below. |
| record_json | STRING | Raw record, for investigation. |

**Quality rules** (applied in `src/lakehouse/sql/silver_production_checks.sql` and `silver_production_dedupe.sql`):

| Rule | Fails when |
|---|---|
| not_null_key | interval or price area is missing |
| valid_price_area | price area is not DK1 or DK2 |
| not_future_interval | interval is later than the ingest time |
| not_null_production | one of the five production columns is missing |
| production_non_negative | a production column is below 0 |
| value_out_of_range | any value's absolute size is above 20,000 MW |
| duplicate_superseded | same key appears more than once in the batch; the first one (latest ingest, then raw-record order) is kept |

Exchange columns are not required: the feed leaves some of them empty for DK2.

## Gold

### `gold_daily_area_summary`
One row per UTC day and price area. Rebuilt in full from silver on every run.

| Column | Type | Meaning |
|---|---|---|
| day_utc | DATE | UTC day. |
| price_area | STRING | DK1 or DK2. |
| area_name | STRING | From silver_price_areas. |
| intervals_observed | BIGINT | Count of 5-minute rows in silver for the day. |
| intervals_expected | INT | 288 per UTC day. |
| completeness_pct | DECIMAL | intervals_observed / intervals_expected × 100. |
| total_mwh | DOUBLE | Energy from the sum of production columns (see below). |
| renewable_mwh | DOUBLE | Energy from offshore wind + onshore wind + solar. |
| renewable_share_pct | DOUBLE | renewable_mwh / total_mwh × 100. |

**Caveats.** `total_mwh` is the sum of the five production columns as reported. It is
not an official national total. The feed does not separate biomass or other renewables,
so `renewable_share_pct` is a lower bound (wind and solar only).

## Lineage

```
Energinet API ──┐                         ┌─> silver_production_5min ──> gold_daily_area_summary
(JSON records)  ├─> bronze_energinet ─────┤   (MERGE by price_area + interval)
saved sample    ┘   _prod_5min            └─> quarantine_production_5min
                                              (failed rows + rule names)
price_areas.csv ──> bronze_price_areas ──> silver_price_areas ──┘ (join for area_name)
```
