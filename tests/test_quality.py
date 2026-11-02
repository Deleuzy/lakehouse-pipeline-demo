from conftest import make_record
from lakehouse.pipeline import run_pipeline
from lakehouse.quality import RULE_DESCRIPTIONS


def test_each_rule_quarantines_its_row_and_nothing_is_dropped(spark, database, tmp_path):
    records = [
        make_record(utc="2026-11-01T09:00:00"),  # clean
        make_record(utc="2026-11-01T09:05:00", ProductionLt100MW=150.0),  # duplicate pair, first copy
        make_record(utc="2026-11-01T09:05:00", ProductionLt100MW=160.0),  # duplicate pair, second copy
        make_record(utc=None),  # not_null_key
        make_record(utc="2026-11-01T09:10:00", area="DK9"),  # valid_price_area
        make_record(utc="2099-01-01T00:00:00"),  # not_future_interval
        make_record(utc="2026-11-01T09:15:00", SolarPower=None),  # not_null_production
        make_record(utc="2026-11-01T09:20:00", ProductionLt100MW=-5.0),  # production_non_negative
        make_record(utc="2026-11-01T09:25:00", ExchangeGermany=99999.0),  # value_out_of_range
    ]

    report = run_pipeline(spark, database=database, source="test", records=records, report_dir=tmp_path)

    assert report.bronze_rows == len(records)
    assert report.silver_valid_rows + report.quarantined_rows == report.bronze_rows
    assert report.rule_counts == {
        "duplicate_superseded": 1,
        "not_null_key": 1,
        "valid_price_area": 1,
        "not_future_interval": 1,
        "not_null_production": 1,
        "production_non_negative": 1,
        "value_out_of_range": 1,
    }
    assert set(report.rule_counts) <= set(RULE_DESCRIPTIONS)

    kept = spark.sql(
        f"SELECT production_lt_100_mw FROM {database}.silver_production_5min WHERE interval_utc = '2026-11-01T09:05:00'"
    ).collect()
    assert len(kept) == 1
