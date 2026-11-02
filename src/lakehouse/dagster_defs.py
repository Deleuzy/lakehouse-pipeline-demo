"""Dagster orchestration: bronze -> silver -> gold as daily partitioned assets.

    dagster dev -m lakehouse.dagster_defs

Each partition is one UTC day, keyed by its date (for example 2026-11-01). Rerunning a
partition gives the same row counts: bronze replaces its own batch, silver and gold MERGE.

Backfill a past range from the CLI:
    dagster job backfill -m lakehouse.dagster_defs -j lakehouse_daily --from 2026-11-01 --to 2026-11-03
"""

from datetime import date
from pathlib import Path

import dagster as dg

from lakehouse import config
from lakehouse.gold import build_gold, incomplete_area_days
from lakehouse.ingest import load_bronze_energinet, load_bronze_price_areas, load_records_file
from lakehouse.silver import build_silver_price_areas, build_silver_production
from lakehouse.spark_session import get_spark
from lakehouse.sql_templates import run_sql

DAILY = dg.DailyPartitionsDefinition(start_date="2026-10-01", timezone="UTC")
LANDING_PREFIX = "energinet_prodex5min_"


class SparkResource(dg.ConfigurableResource):
    database: str = config.DEFAULT_DATABASE
    landing_dir: str = str(config.LANDING_DIR)

    def session(self):
        spark = get_spark()
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        run_sql(spark, "create_tables.sql", database=self.database)
        return spark


def landing_path(landing_dir: str | Path, day: str) -> Path:
    return Path(landing_dir) / f"{LANDING_PREFIX}{day}.json"


@dg.asset(partitions_def=DAILY, group_name="bronze", kinds={"spark"})
def bronze_energinet_prod(context: dg.AssetExecutionContext, lakehouse: SparkResource) -> dg.MaterializeResult:
    day = context.partition_key
    path = landing_path(lakehouse.landing_dir, day)
    if not path.exists():
        raise dg.Failure(description=f"No landing file for {day}: {path}")

    spark = lakehouse.session()
    rows = load_bronze_energinet(
        spark, lakehouse.database, load_records_file(path), f"landing:{path.name}", batch_id=day
    )
    load_bronze_price_areas(spark, lakehouse.database, config.PRICE_AREAS_FILE, batch_id=day)
    return dg.MaterializeResult(metadata={"bronze_rows": rows, "source_file": path.name})


@dg.asset(
    partitions_def=DAILY,
    deps=[bronze_energinet_prod],
    group_name="silver",
    kinds={"spark", "delta"},
    check_specs=[dg.AssetCheckSpec("bronze_reconciles", asset="silver_production")],
)
def silver_production(context: dg.AssetExecutionContext, lakehouse: SparkResource) -> dg.MaterializeResult:
    day = context.partition_key
    spark = lakehouse.session()
    build_silver_price_areas(spark, lakehouse.database)
    counts = build_silver_production(spark, lakehouse.database, batch_id=day)

    reconciles = counts.bronze_rows == counts.valid_rows + counts.quarantined_rows
    return dg.MaterializeResult(
        metadata={
            "bronze_rows": counts.bronze_rows,
            "silver_valid_rows": counts.valid_rows,
            "quarantined_rows": counts.quarantined_rows,
            "rule_counts": counts.rule_counts,
        },
        check_results=[
            dg.AssetCheckResult(
                check_name="bronze_reconciles",
                passed=reconciles,
                metadata={"bronze_rows": counts.bronze_rows, "silver_plus_quarantine": counts.valid_rows + counts.quarantined_rows},
            )
        ],
    )


@dg.asset_check(asset=silver_production, description="Silver has no null keys and no duplicate (area, interval).")
def silver_keys_unique(lakehouse: SparkResource) -> dg.AssetCheckResult:
    spark = lakehouse.session()
    row = spark.sql(
        f"""
        SELECT
          sum(CASE WHEN price_area IS NULL OR interval_utc IS NULL THEN 1 ELSE 0 END) AS null_keys,
          count(*) - count(DISTINCT price_area, interval_utc) AS duplicate_keys
        FROM {lakehouse.database}.silver_production_5min
        """
    ).collect()[0]
    passed = row["null_keys"] == 0 and row["duplicate_keys"] == 0
    return dg.AssetCheckResult(passed=passed, metadata={"null_keys": row["null_keys"], "duplicate_keys": row["duplicate_keys"]})


@dg.asset(partitions_def=DAILY, deps=[silver_production], group_name="gold", kinds={"spark", "delta"}, check_specs=[
    dg.AssetCheckSpec("day_complete", asset="gold_daily_area_summary"),
])
def gold_daily_area_summary(context: dg.AssetExecutionContext, lakehouse: SparkResource) -> dg.MaterializeResult:
    day = date.fromisoformat(context.partition_key)
    spark = lakehouse.session()
    build_gold(spark, lakehouse.database, day)

    gaps = [g for g in incomplete_area_days(spark, lakehouse.database) if g["day_utc"] == day.isoformat()]
    return dg.MaterializeResult(
        metadata={"incomplete_areas": len(gaps)},
        check_results=[
            dg.AssetCheckResult(
                check_name="day_complete",
                passed=not gaps,
                severity=dg.AssetCheckSeverity.WARN,
                metadata={"incomplete_area_days": gaps},
            )
        ],
    )


# In-process executor: the local Spark metastore (Derby) allows one JVM at a time, so every step
# shares one Spark session instead of each subprocess starting its own.
lakehouse_daily = dg.define_asset_job(
    "lakehouse_daily",
    selection=dg.AssetSelection.assets(bronze_energinet_prod, silver_production, gold_daily_area_summary),
    partitions_def=DAILY,
    executor_def=dg.in_process_executor,
)


def landing_run_requests(landing_dir: str | Path) -> list[dg.RunRequest]:
    """One run per landing file. A changed file (new mtime) gets a new run key, so it reruns."""
    requests = []
    for path in sorted(Path(landing_dir).glob(f"{LANDING_PREFIX}*.json")):
        day = path.stem.removeprefix(LANDING_PREFIX)
        try:
            date.fromisoformat(day)
        except ValueError:
            continue
        requests.append(
            dg.RunRequest(run_key=f"{path.name}:{path.stat().st_mtime_ns}", partition_key=day, job_name=lakehouse_daily.name)
        )
    return requests


@dg.sensor(job=lakehouse_daily, minimum_interval_seconds=30)
def landing_sensor(context: dg.SensorEvaluationContext):
    return landing_run_requests(config.LANDING_DIR)


defs = dg.Definitions(
    assets=[bronze_energinet_prod, silver_production, gold_daily_area_summary],
    asset_checks=[silver_keys_unique],
    jobs=[lakehouse_daily],
    sensors=[landing_sensor],
    resources={"lakehouse": SparkResource()},
)
