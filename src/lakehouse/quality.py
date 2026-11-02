"""Data quality rules applied in silver. The checks themselves are SQL in sql/silver_production_*.sql.

A row that fails any rule goes to quarantine_production_5min with the rule names
attached. Nothing is dropped silently.
"""

RULE_DESCRIPTIONS = {
    "not_null_key": "Interval timestamp or price area is missing.",
    "valid_price_area": "Price area is not DK1 or DK2.",
    "not_future_interval": "Interval is later than the time the row was ingested.",
    "not_null_production": "One of the five production columns is missing.",
    "production_non_negative": "A production column is below zero.",
    "value_out_of_range": "Any power value is above the absolute ceiling in config.MAX_ABS_MW.",
    "duplicate_superseded": "Same price area and interval appeared more than once in the batch; only the first (latest ingest) is kept.",
}


def describe(rule: str) -> str:
    return RULE_DESCRIPTIONS.get(rule, "Unknown rule.")
