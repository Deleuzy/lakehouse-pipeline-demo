import json

from lakehouse.events import EventMessage, ingest_events, validate_message, load_contract
from lakehouse.sql_templates import run_sql

CONTRACT = load_contract()


def event(event_id="e1", **overrides):
    body = {
        "schema_version": 1,
        "event_id": event_id,
        "event_time": "2026-11-01T10:05:00",
        "price_area": "DK1",
        "interval_utc": "2026-11-01T10:00:00",
        "production_lt_100_mw": 100.0,
        "production_ge_100_mw": 200.0,
        "offshore_wind_mw": 50.0,
        "onshore_wind_mw": 30.0,
        "solar_mw": 20.0,
    }
    body.update(overrides)
    return json.dumps(body).encode("utf-8")


def message(message_id, data):
    return EventMessage(message_id=message_id, data=data, publish_time="2026-11-01T10:05:01Z")


def setup_tables(spark, database):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    run_sql(spark, "create_tables.sql", database=database)


def test_valid_event_is_accepted_and_a_redelivery_is_a_duplicate(spark, database):
    setup_tables(spark, database)

    first = ingest_events(spark, database, [message("m1", event("e1"))], batch_id="b1", contract=CONTRACT)
    # Pub/Sub redelivers m1 (same message id) and a producer retry arrives as m3 with the same event_id.
    second = ingest_events(
        spark,
        database,
        [message("m1", event("e1")), message("m3", event("e1")), message("m2", event("e2"))],
        batch_id="b2",
        contract=CONTRACT,
    )

    assert (first.accepted, first.duplicates) == (1, 0)
    assert (second.accepted, second.duplicates) == (1, 2)
    assert spark.table(f"{database}.bronze_events").count() == 2


def test_invalid_messages_go_to_the_dead_letter_table_with_a_reason(spark, database):
    setup_tables(spark, database)
    bad = [
        message("bad-json", b"{not json"),
        message("v2", event("e2", schema_version=2)),
        message("area", event("e3", price_area="DK9")),
        message("time", event("e4", interval_utc="yesterday")),
        message("extra", event("e5", surprise="field")),
    ]

    result = ingest_events(spark, database, bad, batch_id="b1", contract=CONTRACT)

    assert result.accepted == 0
    assert result.dead_lettered == 5
    assert result.reasons == {"not_json": 1, "unsupported_schema_version": 1, "contract": 3}
    reasons = {r["message_id"]: r["reason"] for r in spark.table(f"{database}.dead_letter_events").collect()}
    assert reasons["v2"].startswith("unsupported_schema_version")
    assert reasons["area"].startswith("contract: price_area")
    assert reasons["time"].startswith("contract: interval_utc")
    assert spark.table(f"{database}.bronze_events").count() == 0


def test_dead_letter_is_not_written_twice_on_redelivery(spark, database):
    setup_tables(spark, database)
    poison = [message("poison", event("e9", solar_mw=-1.0))]

    ingest_events(spark, database, poison, batch_id="b1", contract=CONTRACT)
    ingest_events(spark, database, poison, batch_id="b2", contract=CONTRACT)

    assert spark.table(f"{database}.dead_letter_events").count() == 1


def test_contract_is_versioned_and_strict():
    assert CONTRACT["properties"]["schema_version"] == {"const": 1}
    assert CONTRACT["additionalProperties"] is False
    event_body, reason = validate_message(event(), CONTRACT)
    assert reason is None and event_body["event_id"] == "e1"
