"""End-to-end tests for the Kafka input connector's header filter.

Each test creates a pipeline whose Kafka input connector carries a
``header_filter``, produces messages with various headers, and checks that only
the messages satisfying the filter reach the table.  A final admitted
"sentinel" message (produced last on a single-partition topic, so it is
processed last) marks completeness: once it appears, every earlier message has
been decided, so the observed row set is final.
"""

import json
import uuid
from typing import Any, Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from feldera import Pipeline, PipelineBuilder
from tests import KAFKA_BOOTSTRAP, TEST_CLIENT
from tests.platform.helper import wait_for_condition

SENTINEL_ID = 100


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


def _produce(
    topic: str, records: list[tuple[int, Optional[list[tuple[str, bytes]]]]]
) -> None:
    """Produce ``{"id": <id>}`` messages, each with the given headers (or none)."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for record_id, headers in records:
        producer.produce(
            topic,
            value=json.dumps({"id": record_id}).encode("utf-8"),
            headers=headers,
        )
    remaining = producer.flush(timeout=30)
    assert remaining == 0, f"failed to flush Kafka messages, remaining={remaining}"


def _run_filter_test(
    pipeline_name: str,
    header_filter: dict[str, Any],
    records: list[tuple[int, Optional[list[tuple[str, bytes]]]]],
    expected_ids: set[int],
) -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    topic = _random_topic("header-filter-in")
    _create_topic(admin, topic)

    input_connector = {
        "name": "kafka_in",
        "transport": {
            "name": "kafka_input",
            "config": {
                "topic": topic,
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "start_from": "earliest",
                "header_filter": header_filter,
            },
        },
        "format": {"name": "json", "config": {"update_format": "raw", "array": False}},
    }

    sql = f"""
    CREATE TABLE input_t(id INT) WITH (
      'connectors' = '{json.dumps([input_connector])}'
    );
    CREATE MATERIALIZED VIEW output_v AS SELECT * FROM input_t;
    """.strip()

    pipeline: Pipeline = PipelineBuilder(
        TEST_CLIENT, name=pipeline_name, sql=sql
    ).create_or_replace()
    pipeline.start()

    def ingested_ids() -> set[int]:
        return {row["id"] for row in pipeline.query("SELECT id FROM output_v")}

    try:
        # The sentinel is produced last; the filter must admit it.
        assert SENTINEL_ID in expected_ids
        _produce(topic, records)

        wait_for_condition(
            "sentinel message ingested",
            lambda: SENTINEL_ID in ingested_ids(),
            timeout_s=60.0,
            poll_interval_s=1.0,
        )

        # The sentinel is last on a single-partition topic, so ingestion is now
        # complete: the observed set is final and must equal the expected set.
        assert ingested_ids() == expected_ids
    finally:
        pipeline.stop(force=True)
        _delete_topic_best_effort(admin, topic)


def test_kafka_header_filter_leaf(pipeline_name):
    """A single regex leaf admits only whole-value matches; anchoring and
    missing headers behave as documented."""
    header_filter = {"header": {"name": "event", "pattern": "created|updated"}}
    records = [
        (1, [("event", b"created")]),  # admit
        (2, [("event", b"deleted")]),  # drop: wrong value
        (3, [("event", b"updated")]),  # admit
        (4, None),  # drop: header absent
        (5, [("event", b"created-x")]),  # drop: anchored, no substring match
        (SENTINEL_ID, [("event", b"created")]),  # admit (sentinel)
    ]
    _run_filter_test(pipeline_name, header_filter, records, {1, 3, SENTINEL_ID})


def test_kafka_header_filter_boolean(pipeline_name):
    """A boolean filter combines several header tests with and/or/not."""
    header_filter = {
        "and": [
            {
                "or": [
                    {"header": {"name": "env", "pattern": "prod"}},
                    {"header": {"name": "env", "pattern": "staging"}},
                ]
            },
            {"not": {"header": {"name": "skip", "pattern": "true"}}},
        ]
    }
    records = [
        (1, [("env", b"prod")]),  # admit
        (2, [("env", b"dev")]),  # drop: fails `or`
        (3, [("env", b"staging"), ("skip", b"false")]),  # admit: skip != "true"
        (4, [("env", b"prod"), ("skip", b"true")]),  # drop: fails `not`
        (5, [("env", b"staging")]),  # admit
        (SENTINEL_ID, [("env", b"prod")]),  # admit (sentinel)
    ]
    _run_filter_test(pipeline_name, header_filter, records, {1, 3, 5, SENTINEL_ID})
