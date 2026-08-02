"""End-to-end tests for the ``soft_delete`` input connector property.

A soft-delete connector pushes every record it receives to the table as an
insertion and reports the original polarity of the record in the ``is_delete``
metadata attribute, so the table represents the stream of updates it receives
instead of the current contents of that stream.

The current contents are recovered by a query that ranks the changes of each
key by the time the connector received them, keeps the most recent one, and
returns it only when it is an insertion. The table below is fed by two
soft-delete connectors, one reading JSON and one reading Avro, since the
polarity of a record is reported by the connector rather than by the data
format.
"""

import io
import json
import uuid
from typing import Any, Optional

import fastavro
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from feldera import Pipeline, PipelineBuilder
from tests import KAFKA_BOOTSTRAP, TEST_CLIENT
from tests.platform.helper import wait_for_condition

# Debezium envelope schema of the Avro stream.  It has no `is_delete` or `ts`
# field: those columns are populated from connector metadata, not from the
# record.
AVRO_SCHEMA: dict[str, Any] = {
    "type": "record",
    "name": "Envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Value",
                    "fields": [
                        {"name": "id", "type": "long"},
                        {"name": "s", "type": "string"},
                    ],
                },
            ],
            "default": None,
        },
        {"name": "after", "type": ["null", "Value"], "default": None},
        {"name": "op", "type": "string"},
    ],
}

# Message timestamps, milliseconds since the epoch.  Each change of a key gets a
# later timestamp than the previous one, which is what orders them in the `live`
# view.
FIRST_CHANGE = 1_700_000_000_000


def _random_topic(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _create_topic(admin: AdminClient, topic: str) -> None:
    futures = admin.create_topics(
        [NewTopic(topic=topic, num_partitions=1, replication_factor=1)]
    )
    futures[topic].result(timeout=30)


def _delete_topic_best_effort(admin: AdminClient, topic: str) -> None:
    try:
        futures = admin.delete_topics([topic], operation_timeout=10)
        futures[topic].result(timeout=10)
    except Exception:
        # Topic deletion can be disabled on some brokers; cleanup is best-effort.
        pass


def _produce(topic: str, messages: list[bytes]) -> None:
    """Produce `messages` oldest first, stamped one second apart."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for index, message in enumerate(messages):
        producer.produce(topic, value=message, timestamp=FIRST_CHANGE + index * 1_000)
    remaining = producer.flush(timeout=30)
    assert remaining == 0, f"failed to flush Kafka messages, remaining={remaining}"


def _json_messages() -> list[bytes]:
    """Changes in the JSON `insert_delete` format.

    Record 1 is inserted and then deleted, so it is not live; record 2 is only
    inserted, so it is.
    """
    return [
        json.dumps({"insert": {"id": 1, "s": "json-1"}}).encode(),
        json.dumps({"delete": {"id": 1, "s": "json-1"}}).encode(),
        json.dumps({"insert": {"id": 2, "s": "json-2"}}).encode(),
    ]


def _avro_message(
    op: str, before: Optional[dict[str, Any]], after: Optional[dict[str, Any]]
) -> bytes:
    """Encode one Debezium event as a bare Avro datum (no registry header)."""
    buffer = io.BytesIO()
    fastavro.schemaless_writer(
        buffer,
        AVRO_SCHEMA,
        {"before": before, "after": after, "op": op},
    )
    return buffer.getvalue()


def _avro_messages() -> list[bytes]:
    """Debezium change events.

    Record 3 is inserted, deleted, and inserted again with a new value, so the
    latest of its three changes is what is live; record 4 is only deleted, so
    it is not live. Record 5 is updated, which is one message that deletes the
    old value and inserts the new one: both changes carry the timestamp of that
    message, so only the tie-break in the `live` view keeps the record.
    """
    return [
        _avro_message("c", None, {"id": 3, "s": "avro-3"}),
        _avro_message("d", {"id": 3, "s": "avro-3"}, None),
        _avro_message("c", None, {"id": 3, "s": "avro-3-again"}),
        _avro_message("d", {"id": 4, "s": "avro-4"}, None),
        _avro_message("c", None, {"id": 5, "s": "avro-5"}),
        _avro_message("u", {"id": 5, "s": "avro-5"}, {"id": 5, "s": "avro-5-updated"}),
    ]


def _connectors(json_topic: str, avro_topic: str) -> list[dict[str, Any]]:
    return [
        {
            "name": "kafka_json",
            "soft_delete": True,
            "transport": {
                "name": "kafka_input",
                "config": {
                    "topic": json_topic,
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    "start_from": "earliest",
                    "include_timestamp": True,
                },
            },
            "format": {
                "name": "json",
                "config": {"update_format": "insert_delete"},
            },
        },
        {
            "name": "kafka_avro",
            "soft_delete": True,
            "transport": {
                "name": "kafka_input",
                "config": {
                    "topic": avro_topic,
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    "start_from": "earliest",
                    "include_timestamp": True,
                },
            },
            "format": {
                "name": "avro",
                "config": {
                    "update_format": "debezium",
                    "schema": json.dumps(AVRO_SCHEMA),
                    "skip_schema_id": True,
                },
            },
        },
    ]


def test_soft_delete_kafka_json_and_avro(pipeline_name):
    """Deletions from either connector land as rows with `is_delete` set, and
    the latest change of each key determines what is live."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    json_topic = _random_topic("soft-delete-json")
    avro_topic = _random_topic("soft-delete-avro")
    _create_topic(admin, json_topic)
    _create_topic(admin, avro_topic)

    connectors = json.dumps(_connectors(json_topic, avro_topic))
    sql = f"""
    CREATE TABLE changes(
        id BIGINT,
        s VARCHAR,
        ts TIMESTAMP DEFAULT CAST(CONNECTOR_METADATA()['kafka_timestamp'] AS TIMESTAMP),
        is_delete BOOLEAN DEFAULT CAST(CONNECTOR_METADATA()['is_delete'] AS BOOLEAN)
    ) WITH (
      'connectors' = '{connectors}'
    );

    CREATE MATERIALIZED VIEW history AS SELECT id, s, is_delete FROM changes;

    -- The latest change of each key, kept only when it is an insertion.  An
    -- update arrives as one message that deletes the old value and inserts the
    -- new one, so both changes carry the same timestamp: rank the insertion
    -- first to keep the updated record.
    CREATE MATERIALIZED VIEW live AS
    SELECT id, s
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY id ORDER BY ts DESC, is_delete NULLS FIRST
        ) AS rn
        FROM changes
    )
    WHERE rn = 1 AND is_delete IS NOT TRUE;

    -- Every change must carry the time the connector received it, since that
    -- is what orders the changes of a key.
    CREATE MATERIALIZED VIEW undated AS SELECT id FROM changes WHERE ts IS NULL;
    """.strip()

    pipeline: Pipeline = PipelineBuilder(
        TEST_CLIENT, name=pipeline_name, sql=sql
    ).create_or_replace()
    pipeline.start()

    def history() -> list[dict[str, Any]]:
        # `query` streams results, so materialize them before comparing.  An
        # insertion and the deletion of the same record differ only in
        # `is_delete`, which therefore has to order them.
        return list(
            pipeline.query(
                "SELECT id, s, is_delete FROM history"
                " ORDER BY id, s, is_delete NULLS FIRST"
            )
        )

    try:
        _produce(json_topic, _json_messages())
        _produce(avro_topic, _avro_messages())

        wait_for_condition(
            "all ten changes ingested",
            lambda: len(history()) == 10,
            timeout_s=120.0,
            poll_interval_s=1.0,
        )

        assert history() == [
            {"id": 1, "s": "json-1", "is_delete": None},
            {"id": 1, "s": "json-1", "is_delete": True},
            {"id": 2, "s": "json-2", "is_delete": None},
            {"id": 3, "s": "avro-3", "is_delete": None},
            {"id": 3, "s": "avro-3", "is_delete": True},
            {"id": 3, "s": "avro-3-again", "is_delete": None},
            {"id": 4, "s": "avro-4", "is_delete": True},
            {"id": 5, "s": "avro-5", "is_delete": None},
            {"id": 5, "s": "avro-5", "is_delete": True},
            {"id": 5, "s": "avro-5-updated", "is_delete": None},
        ]

        assert list(pipeline.query("SELECT id FROM undated")) == []

        # Record 1 ends deleted and record 4 was never inserted, so neither is
        # live; record 3 is live with the value of its latest insertion, and
        # record 5 with the value the update gave it.
        assert list(pipeline.query("SELECT id, s FROM live ORDER BY id")) == [
            {"id": 2, "s": "json-2"},
            {"id": 3, "s": "avro-3-again"},
            {"id": 5, "s": "avro-5-updated"},
        ]
    finally:
        pipeline.stop(force=True)
        _delete_topic_best_effort(admin, json_topic)
        _delete_topic_best_effort(admin, avro_topic)
