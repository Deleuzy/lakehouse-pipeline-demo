"""Gold layer: business-ready daily tables for analysts.

Gold is a MERGE keyed by (day, price area), so rebuilding a day overwrites that day only.
"""

from datetime import date

from pyspark.sql import SparkSession

from lakehouse import config
from lakehouse.sql_templates import run_sql


def build_gold(spark: SparkSession, database: str, day: date | None = None) -> None:
    """Rebuild gold for one UTC day, or for every day in silver when day is None."""
    day_filter = f"WHERE CAST(interval_utc AS DATE) = DATE '{day.isoformat()}'" if day else ""
    run_sql(
        spark,
        "gold_daily_area_summary.sql",
        database=database,
        day_filter=day_filter,
        intervals_per_day=config.INTERVALS_PER_DAY,
    )


def incomplete_area_days(spark: SparkSession, database: str) -> list[dict]:
    """Area-days with fewer 5-minute intervals than a full UTC day. These are the gaps to chase."""
    rows = spark.sql(
        f"""
        SELECT price_area, day_utc, intervals_observed
        FROM {database}.gold_daily_area_summary
        WHERE intervals_observed < {config.INTERVALS_PER_DAY}
        ORDER BY day_utc, price_area
        """
    ).collect()
    return [
        {"price_area": row["price_area"], "day_utc": str(row["day_utc"]), "intervals_observed": row["intervals_observed"]}
        for row in rows
    ]
