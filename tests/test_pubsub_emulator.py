"""Runs only when the Pub/Sub emulator is up (see docker-compose.yml). Skipped in CI."""

import json
import os
import uuid

import pytest

from lakehouse.events import load_contract
from lakehouse.pubsub import ensure_topic_and_subscription, publish, pull_and_ingest
from lakehouse.sql_templates import run_sql

pytestmark = pytest.mark.skipif(
    not os.environ.get("PUBSUB_EMULATOR_HOST"), reason="Pub/Sub emulator not running (PUBSUB_EMULATOR_HOST unset)"
)


def event_bytes(event_id, **overrides):
    body = {
        "schema_version": 1,
        "event_id": event_id,
        "event_time": "2026-11-01T10:05:00",
        "price_area": "DK2",
        "interval_utc": "2026-11-01T10:00:00",
        "production_lt_100_mw": 10.0,
        "production_ge_100_mw": 20.0,
        "offshore_wind_mw": 5.0,
        "onshore_wind_mw": 3.0,
        "solar_mw": 1.0,
    }
    body.update(overrides)
    return json.dumps(body).encode("utf-8")


def test_publish_pull_and_ingest_through_the_emulator(spark, database):
    project = "demo-project"
    topic = f"events-{uuid.uuid4().hex[:6]}"
    subscription = f"{topic}-sub"
    ensure_topic_and_subscription(project, topic, subscription)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    run_sql(spark, "create_tables.sql", database=database)

    publish(project, topic, [event_bytes("e1"), event_bytes("e2"), event_bytes("e1"), b"not json"])
    result = pull_and_ingest(spark, database, project, subscription, batch_id="emulator-1")

    assert result.accepted == 2
    assert result.duplicates == 1
    assert result.dead_lettered == 1
    assert spark.table(f"{database}.bronze_events").count() == 2
    assert spark.table(f"{database}.dead_letter_events").count() == 1
    assert load_contract()["title"] == "energinet.production.5min"
