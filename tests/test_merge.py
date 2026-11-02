from conftest import make_record
from lakehouse import config
from lakehouse.ingest import load_records_file
from lakehouse.pipeline import run_pipeline


def test_rerunning_the_same_sample_adds_no_duplicates(spark, database, tmp_path):
    records = load_records_file(config.SAMPLE_PRODUCTION_FILE)

    first = run_pipeline(spark, database=database, source="sample", records=records, report_dir=tmp_path)
    second = run_pipeline(spark, database=database, source="sample", records=records, report_dir=tmp_path)

    assert first.silver_valid_rows == second.silver_valid_rows == 2 * config.INTERVALS_PER_DAY
    silver_rows = spark.table(f"{database}.silver_production_5min").count()
    assert silver_rows == 2 * config.INTERVALS_PER_DAY
    gold_rows = spark.table(f"{database}.gold_daily_area_summary").count()
    assert gold_rows == len(config.PRICE_AREAS)
    assert second.incomplete_area_days == []


def test_newer_batch_updates_an_existing_interval(spark, database, tmp_path):
    first = [make_record(utc="2026-11-01T10:00:00", ProductionLt100MW=100.0)]
    run_pipeline(spark, database=database, source="a", records=first, report_dir=tmp_path)

    second = [make_record(utc="2026-11-01T10:00:00", ProductionLt100MW=175.0)]
    run_pipeline(spark, database=database, source="b", records=second, report_dir=tmp_path)

    rows = spark.table(f"{database}.silver_production_5min").collect()
    assert len(rows) == 1
    assert rows[0]["production_lt_100_mw"] == 175.0
