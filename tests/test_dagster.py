import json
import shutil

import dagster as dg

from lakehouse import config
from lakehouse.dagster_defs import (
    SparkResource,
    bronze_energinet_prod,
    gold_daily_area_summary,
    landing_run_requests,
    silver_keys_unique,
    silver_production,
)

ASSETS = [bronze_energinet_prod, silver_production, gold_daily_area_summary, silver_keys_unique]


def counts(spark, database):
    return {
        "bronze": spark.table(f"{database}.bronze_energinet_prod_5min").count(),
        "silver": spark.table(f"{database}.silver_production_5min").count(),
        "quarantine": spark.table(f"{database}.quarantine_production_5min").count(),
        "gold": spark.table(f"{database}.gold_daily_area_summary").count(),
    }


def materialize_day(database, landing_dir, day):
    return dg.materialize(
        ASSETS,
        partition_key=day,
        resources={"lakehouse": SparkResource(database=database, landing_dir=str(landing_dir))},
    )


def test_rerunning_a_partition_gives_the_same_counts(spark, database, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copy(config.SAMPLE_PRODUCTION_FILE, landing)

    first = materialize_day(database, landing, "2026-11-01")
    before = counts(spark, database)
    second = materialize_day(database, landing, "2026-11-01")
    after = counts(spark, database)

    assert first.success and second.success
    assert before == after == {"bronze": 576, "silver": 576, "quarantine": 0, "gold": 2}
    checks = {e.check_name: e.passed for e in second.get_asset_check_evaluations()}
    assert checks == {"bronze_reconciles": True, "day_complete": True, "silver_keys_unique": True}


def test_backfill_runs_each_partition_in_a_range(spark, database, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copy(config.SAMPLE_PRODUCTION_FILE, landing)

    # Synthetic earlier day: the saved sample with its timestamps moved back one day.
    # Partitions only cover past days, so the range is 2026-10-31..2026-11-01.
    payload = json.loads(config.SAMPLE_PRODUCTION_FILE.read_text(encoding="utf-8"))
    for record in payload["records"]:
        for key in ("Minutes5UTC", "Minutes5DK"):
                record[key] = record[key].replace("2026-11-01", "2026-10-31")
    (landing / "energinet_prodex5min_2026-10-31.json").write_text(json.dumps(payload), encoding="utf-8")

    for day in ("2026-10-31", "2026-11-01"):
        assert materialize_day(database, landing, day).success

    gold = spark.sql(f"SELECT day_utc, price_area FROM {database}.gold_daily_area_summary ORDER BY day_utc, price_area").collect()
    assert [(str(r["day_utc"]), r["price_area"]) for r in gold] == [
        ("2026-10-31", "DK1"), ("2026-10-31", "DK2"), ("2026-11-01", "DK1"), ("2026-11-01", "DK2"),
    ]


def test_sensor_requests_one_run_per_landing_file(tmp_path):
    (tmp_path / "energinet_prodex5min_2026-11-01.json").write_text("{}", encoding="utf-8")
    (tmp_path / "energinet_prodex5min_notadate.json").write_text("{}", encoding="utf-8")
    (tmp_path / "other.csv").write_text("", encoding="utf-8")

    requests = landing_run_requests(tmp_path)

    assert [r.partition_key for r in requests] == ["2026-11-01"]
    assert requests[0].run_key.startswith("energinet_prodex5min_2026-11-01.json:")
