"""Google Pub/Sub adapter for the event pipeline.

Locally this talks to the Pub/Sub emulator in docker-compose.yml, so no Google account is needed.
Set PUBSUB_EMULATOR_HOST=localhost:8085 and the client library uses the emulator automatically.

Messages are acknowledged only after the batch is committed to bronze or dead-letter. If the
pipeline fails first, Pub/Sub redelivers the same messages, and the event_id check absorbs them.
"""

from google.cloud import pubsub_v1

from lakehouse.events import EventMessage, IngestResult, ingest_events


def ensure_topic_and_subscription(project_id: str, topic_id: str, subscription_id: str) -> None:
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic = publisher.topic_path(project_id, topic_id)
    subscription = subscriber.subscription_path(project_id, subscription_id)
    try:
        publisher.create_topic(request={"name": topic})
    except Exception:  # already exists
        pass
    try:
        subscriber.create_subscription(request={"name": subscription, "topic": topic})
    except Exception:  # already exists
        pass


def publish(project_id: str, topic_id: str, payloads: list[bytes]) -> list[str]:
    publisher = pubsub_v1.PublisherClient()
    topic = publisher.topic_path(project_id, topic_id)
    futures = [publisher.publish(topic, data) for data in payloads]
    return [future.result() for future in futures]


def pull_and_ingest(
    spark,
    database: str,
    project_id: str,
    subscription_id: str,
    batch_id: str,
    max_messages: int = 100,
) -> IngestResult:
    subscriber = pubsub_v1.SubscriberClient()
    subscription = subscriber.subscription_path(project_id, subscription_id)
    response = subscriber.pull(request={"subscription": subscription, "max_messages": max_messages}, timeout=10)

    messages = [
        EventMessage(
            message_id=received.message.message_id,
            data=received.message.data,
            publish_time=received.message.publish_time.isoformat(),
            attributes=dict(received.message.attributes),
        )
        for received in response.received_messages
    ]
    if not messages:
        return IngestResult()

    result = ingest_events(spark, database, messages, batch_id=batch_id)
    subscriber.acknowledge(
        request={"subscription": subscription, "ack_ids": [received.ack_id for received in response.received_messages]}
    )
    return result
