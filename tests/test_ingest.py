from unittest.mock import MagicMock

from lakehouse import config
from lakehouse.ingest import fetch_energinet_records, load_records_file
from lakehouse.pipeline import run_pipeline


def test_fetch_asks_api_for_requested_areas_and_window(monkeypatch):
    response = MagicMock()
    response.json.return_value = {"records": [{"PriceArea": "DK1"}]}
    get = MagicMock(return_value=response)
    monkeypatch.setattr("lakehouse.ingest.requests.get", get)

    records = fetch_energinet_records("2026-11-01T00:00", "2026-11-02T00:00", ("DK1",))

    assert records == [{"PriceArea": "DK1"}]
    _, kwargs = get.call_args
    assert kwargs["params"]["start"] == "2026-11-01T00:00"
    assert kwargs["params"]["end"] == "2026-11-02T00:00"
    assert kwargs["params"]["filter"] == '{"PriceArea": ["DK1"]}'


def test_saved_sample_has_a_full_day_for_both_areas():
    records = load_records_file(config.SAMPLE_PRODUCTION_FILE)

    assert len(records) == 2 * config.INTERVALS_PER_DAY
    for area in config.PRICE_AREAS:
        assert sum(1 for r in records if r["PriceArea"] == area) == config.INTERVALS_PER_DAY


def test_pipeline_on_saved_sample_loads_every_row(spark, database, tmp_path):
    report = run_pipeline(
        spark,
        database=database,
        source="sample",
        report_dir=tmp_path,
    )

    assert report.bronze_rows == 2 * config.INTERVALS_PER_DAY
    assert report.silver_valid_rows == 2 * config.INTERVALS_PER_DAY
    assert report.quarantined_rows == 0
    assert report.incomplete_area_days == []
