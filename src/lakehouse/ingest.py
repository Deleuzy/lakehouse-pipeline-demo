"""Bronze layer: land raw data as it arrived, with batch id and ingest time.

Bronze is append-only. Nothing is cleaned or typed here; silver does that.
"""

import csv
import json
import re
from pathlib import Path

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import config

_SAFE_BATCH_ID = re.compile(r"^[A-Za-z0-9_-]+$")


def _clear_batch(spark: SparkSession, table: str, batch_id: str) -> None:
    """Remove rows from an earlier load of the same batch, so reruns do not double bronze."""
    if not _SAFE_BATCH_ID.match(batch_id):
        raise ValueError(f"unsafe batch id: {batch_id!r}")
    spark.sql(f"DELETE FROM {table} WHERE batch_id = '{batch_id}'")


def fetch_energinet_records(
    start: str,
    end: str,
    areas: tuple[str, ...] = config.PRICE_AREAS,
    timeout: int = 60,
) -> list[dict]:
    """Fetch 5-minute production records from the Energinet open data API."""
    params = {
        "start": start,
        "end": end,
        "filter": json.dumps({"PriceArea": list(areas)}),
        "sort": "Minutes5UTC ASC",
        "limit": 100000,
    }
    response = requests.get(config.ENERGINET_URL, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()["records"]


def load_records_file(path: Path) -> list[dict]:
    """Read records saved from the same API, so the pipeline runs offline."""
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)["records"]


def load_bronze_energinet(
    spark: SparkSession,
    database: str,
    records: list[dict],
    source: str,
    batch_id: str,
) -> int:
    rows = [(json.dumps(record, sort_keys=True), source) for record in records]
    frame = (
        spark.createDataFrame(rows, "record_json STRING, source STRING")
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.current_timestamp())
    )
    _clear_batch(spark, f"{database}.bronze_energinet_prod_5min", batch_id)
    frame.write.format("delta").mode("append").saveAsTable(f"{database}.bronze_energinet_prod_5min")
    return len(rows)


def load_bronze_price_areas(
    spark: SparkSession,
    database: str,
    path: Path,
    batch_id: str,
) -> int:
    with open(path, newline="", encoding="utf-8") as handle:
        rows = [
            (line["price_area"], line["area_name"], line["region"], path.name)
            for line in csv.DictReader(handle)
        ]
    frame = (
        spark.createDataFrame(rows, "price_area STRING, area_name STRING, region STRING, source STRING")
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.current_timestamp())
    )
    _clear_batch(spark, f"{database}.bronze_price_areas", batch_id)
    frame.write.format("delta").mode("append").saveAsTable(f"{database}.bronze_price_areas")
    return len(rows)
