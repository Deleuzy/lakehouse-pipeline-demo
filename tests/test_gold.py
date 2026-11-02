from conftest import make_record
from lakehouse import config
from lakehouse.pipeline import run_pipeline


def test_gold_totals_renewable_share_and_gaps(spark, database, tmp_path):
    # Each interval: total = 100 + 200 + 50 + 30 + 20 = 400 MW, renewable = 50 + 30 + 20 = 100 MW.
    records = [
        make_record(utc="2026-11-01T10:00:00"),
        make_record(utc="2026-11-01T10:05:00"),
    ]

    report = run_pipeline(spark, database=database, source="test", records=records, report_dir=tmp_path)

    row = spark.sql(
        f"SELECT * FROM {database}.gold_daily_area_summary WHERE price_area = 'DK1'"
    ).collect()[0]
    assert row["intervals_observed"] == 2
    assert row["intervals_expected"] == config.INTERVALS_PER_DAY
    assert row["total_mwh"] == round(800 * 5 / 60, 2)
    assert row["renewable_mwh"] == round(200 * 5 / 60, 2)
    assert row["renewable_share_pct"] == 25.0
    assert row["area_name"] == "Western Denmark"
    assert report.incomplete_area_days == [
        {"price_area": "DK1", "day_utc": "2026-11-01", "intervals_observed": 2}
    ]
