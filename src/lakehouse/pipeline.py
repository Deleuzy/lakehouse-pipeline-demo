"""Run the whole pipeline: bronze -> silver -> gold, then write a run report.

    python -m lakehouse.pipeline --source sample
    python -m lakehouse.pipeline --source live --start 2026-11-01T00:00 --end 2026-11-02T00:00
"""

import argparse
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from lakehouse import config
from lakehouse.gold import build_gold, incomplete_area_days
from lakehouse.ingest import (
    fetch_energinet_records,
    load_bronze_energinet,
    load_bronze_price_areas,
    load_records_file,
)
from lakehouse.silver import build_silver_price_areas, build_silver_production
from lakehouse.sql_templates import run_sql


@dataclass
class RunReport:
    batch_id: str
    started_at: str
    source: str
    bronze_rows: int
    silver_valid_rows: int
    quarantined_rows: int
    rule_counts: dict[str, int]
    incomplete_area_days: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def run_pipeline(
    spark: SparkSession,
    *,
    database: str = config.DEFAULT_DATABASE,
    source: str = "sample",
    start: str | None = None,
    end: str | None = None,
    areas: tuple[str, ...] = config.PRICE_AREAS,
    records: list[dict] | None = None,
    batch_id: str | None = None,
    price_areas_file: Path = config.PRICE_AREAS_FILE,
    report_dir: Path = config.REPORT_DIR,
) -> RunReport:
    """Run one batch. Passing the same batch_id again leaves bronze, silver and gold unchanged."""
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    batch_id = batch_id or uuid.uuid4().hex[:12]

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    run_sql(spark, "create_tables.sql", database=database)

    if records is None:
        if source == "sample":
            records = load_records_file(config.SAMPLE_PRODUCTION_FILE)
            source_name = f"sample:{config.SAMPLE_PRODUCTION_FILE.name}"
        else:
            records = fetch_energinet_records(start, end, areas)
            source_name = f"live:{config.ENERGINET_URL} {start}..{end}"
    else:
        source_name = source

    load_bronze_energinet(spark, database, records, source_name, batch_id)
    load_bronze_price_areas(spark, database, price_areas_file, batch_id)

    build_silver_price_areas(spark, database)
    counts = build_silver_production(spark, database, batch_id)
    build_gold(spark, database)

    report = RunReport(
        batch_id=batch_id,
        started_at=started_at,
        source=source_name,
        bronze_rows=counts.bronze_rows,
        silver_valid_rows=counts.valid_rows,
        quarantined_rows=counts.quarantined_rows,
        rule_counts=counts.rule_counts,
        incomplete_area_days=incomplete_area_days(spark, database),
    )

    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / f"run_{batch_id}.json").write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the lakehouse demo pipeline.")
    parser.add_argument("--source", choices=["sample", "live"], default="sample")
    parser.add_argument("--start", help="Live only, e.g. 2026-11-01T00:00")
    parser.add_argument("--end", help="Live only, e.g. 2026-11-02T00:00")
    parser.add_argument("--database", default=config.DEFAULT_DATABASE)
    args = parser.parse_args(argv)
    if args.source == "live" and not (args.start and args.end):
        parser.error("--source live needs --start and --end")

    from lakehouse.spark_session import get_spark

    spark = get_spark()
    report = run_pipeline(
        spark,
        database=args.database,
        source=args.source,
        start=args.start,
        end=args.end,
    )
    print(json.dumps(report.to_dict(), indent=2))


if __name__ == "__main__":
    main()
