"""Event ingestion: validate messages against a versioned contract, land valid ones in bronze,
and send everything else to a dead-letter table.

At-least-once delivery means the same event can arrive more than once. Bronze keeps one row
per event_id, so a redelivered event is counted as a duplicate and not written again.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import jsonschema
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CONTRACT_FILE = Path(__file__).parent / "contracts" / "energinet_production_event.v1.json"
SUPPORTED_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class EventMessage:
    message_id: str
    data: bytes
    publish_time: str = ""
    attributes: dict = field(default_factory=dict)


@dataclass
class IngestResult:
    accepted: int = 0
    duplicates: int = 0
    dead_lettered: int = 0
    reasons: dict[str, int] = field(default_factory=dict)


def load_contract(path: Path = CONTRACT_FILE) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_message(data: bytes, contract: dict) -> tuple[dict | None, str | None]:
    """Return (event, None) when valid, or (None, reason) when the message must be dead-lettered."""
    try:
        event = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        return None, f"not_json: {error}"
    if not isinstance(event, dict):
        return None, "not_an_object"
    if event.get("schema_version") != SUPPORTED_SCHEMA_VERSION:
        return None, f"unsupported_schema_version: {event.get('schema_version')!r}"

    errors = sorted(jsonschema.Draft202012Validator(contract).iter_errors(event), key=lambda e: list(e.path))
    if errors:
        first = errors[0]
        where = "/".join(str(part) for part in first.path) or "$"
        return None, f"contract: {where}: {first.message}"

    for name in ("event_time", "interval_utc"):
        try:
            datetime.fromisoformat(event[name])
        except ValueError:
            return None, f"contract: {name}: not an ISO 8601 timestamp"
    return event, None


def ingest_events(
    spark: SparkSession,
    database: str,
    messages: list[EventMessage],
    batch_id: str,
    contract: dict | None = None,
) -> IngestResult:
    """Land one batch of messages. Safe to call again with redelivered messages."""
    contract = contract or load_contract()
    result = IngestResult()

    valid_rows, dead_rows = [], []
    for message in messages:
        event, reason = validate_message(message.data, contract)
        if reason:
            dead_rows.append((message.message_id, batch_id, reason, message.data.decode("utf-8", "replace")))
            result.dead_lettered += 1
            result.reasons[reason.split(":")[0]] = result.reasons.get(reason.split(":")[0], 0) + 1
        else:
            valid_rows.append((event["event_id"], message.message_id, batch_id, json.dumps(event, sort_keys=True), message.publish_time))

    existing = {row["event_id"] for row in spark.table(f"{database}.bronze_events").select("event_id").collect()}
    seen_in_batch = set()
    fresh_rows = []
    for row in valid_rows:
        if row[0] in existing or row[0] in seen_in_batch:
            result.duplicates += 1
        else:
            seen_in_batch.add(row[0])
            fresh_rows.append(row)
    result.accepted = len(fresh_rows)

    if fresh_rows:
        frame = spark.createDataFrame(
            fresh_rows, "event_id STRING, message_id STRING, batch_id STRING, payload STRING, publish_time STRING"
        ).withColumn("ingested_at", F.current_timestamp())
        frame.createOrReplaceTempView("incoming_events")
        spark.sql(f"""
            MERGE INTO {database}.bronze_events AS t
            USING incoming_events AS s ON t.event_id = s.event_id
            WHEN NOT MATCHED THEN INSERT *
        """)

    if dead_rows:
        frame = spark.createDataFrame(
            dead_rows, "message_id STRING, batch_id STRING, reason STRING, payload STRING"
        ).withColumn("received_at", F.current_timestamp())
        frame.createOrReplaceTempView("incoming_dead")
        spark.sql(f"""
            MERGE INTO {database}.dead_letter_events AS t
            USING incoming_dead AS s ON t.message_id = s.message_id
            WHEN NOT MATCHED THEN INSERT *
        """)
    return result
