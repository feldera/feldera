import json
import os
import uuid
from datetime import datetime
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from feldera import Pipeline, PipelineBuilder
from feldera.connector_stats import InputConnectorStatus, OutputConnectorStatus
from tests import TEST_CLIENT
from tests.platform.helper import gen_pipeline_name, wait_for_condition


def env(name: str, default: str) -> str:
    """Get environment variables for the Kafka broker and Schema registry.
    The default values are only meant for internal development; external users must set them."""
    return os.getenv(name, default)


# Set these before running the test:
# Example(terminal/shell):
#   export KAFKA_BOOTSTRAP=localhost:19092

KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS", "ci-kafka-bootstrap.kafka:9092")


def _random_topic(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _kafka_clients() -> tuple[Any, Any, Any]:
    return Producer, AdminClient, NewTopic


def _create_topic(
    admin: Any, topic: str, new_topic_cls: Any, timeout_s: float = 30.0
) -> None:
    futures = admin.create_topics(
        [new_topic_cls(topic=topic, num_partitions=1, replication_factor=1)]
    )
    futures[topic].result(timeout=timeout_s)


def _delete_topic_best_effort(admin: Any, topic: str) -> None:
    try:
        futures = admin.delete_topics([topic], operation_timeout=10)
        futures[topic].result(timeout=10)
    except Exception:
        # Topic deletion can be disabled on some brokers; cleanup is best-effort.
        pass


def _produce_records(topic: str, values: list[str]) -> None:
    producer_cls, _, _ = _kafka_clients()
    producer = producer_cls({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for value in values:
        producer.produce(topic, value=value.encode("utf-8"))
    remaining = producer.flush(timeout=30)
    if remaining != 0:
        raise RuntimeError(f"Failed to flush all Kafka messages, remaining={remaining}")


def _assert_connector_error_list(errors: Any) -> None:
    if errors is None:
        return
    assert isinstance(errors, list)
    for error in errors:
        assert isinstance(error.timestamp, datetime)
        assert isinstance(error.index, int)
        assert isinstance(error.message, str)
        assert error.tag is None or isinstance(error.tag, str)


def _assert_input_status_fields(status, expected_stream: str) -> None:
    # Top-level fields
    assert isinstance(status.endpoint_name, str)
    assert status.endpoint_name
    assert isinstance(status.fatal_error, str) or status.fatal_error is None
    assert isinstance(status.paused, bool)
    assert isinstance(status.barrier, bool)

    # Config
    assert isinstance(status.config.stream, str)
    assert status.config.stream == expected_stream

    # Metrics
    metrics = status.metrics
    assert isinstance(metrics.total_bytes, int)
    assert isinstance(metrics.total_records, int)
    assert isinstance(metrics.buffered_records, int)
    assert isinstance(metrics.buffered_bytes, int)
    assert isinstance(metrics.num_transport_errors, int)
    assert isinstance(metrics.num_parse_errors, int)
    assert isinstance(metrics.end_of_input, bool)

    # Optional rich fields
    _assert_connector_error_list(status.parse_errors)
    _assert_connector_error_list(status.transport_errors)
    if status.completed_frontier is not None:
        assert isinstance(status.completed_frontier.metadata, object)
        assert isinstance(status.completed_frontier.ingested_at, datetime)
        assert isinstance(status.completed_frontier.processed_at, datetime)
        assert isinstance(status.completed_frontier.completed_at, datetime)


def _assert_output_status_fields(status, expected_stream: str) -> None:
    # Top-level fields
    assert isinstance(status.endpoint_name, str)
    assert status.endpoint_name
    assert isinstance(status.fatal_error, str) or status.fatal_error is None

    # Config
    assert isinstance(status.config.stream, str)
    assert status.config.stream == expected_stream

    # Metrics
    metrics = status.metrics
    assert isinstance(metrics.transmitted_records, int)
    assert isinstance(metrics.transmitted_bytes, int)
    assert isinstance(metrics.queued_records, int)
    assert isinstance(metrics.queued_batches, int)
    assert isinstance(metrics.buffered_records, int)
    assert isinstance(metrics.buffered_batches, int)
    assert isinstance(metrics.num_encode_errors, int)
    assert isinstance(metrics.num_transport_errors, int)
    assert isinstance(metrics.total_processed_input_records, int)
    assert isinstance(metrics.total_processed_steps, int)
    assert isinstance(metrics.memory, int)

    # Optional rich fields
    _assert_connector_error_list(status.encode_errors)
    _assert_connector_error_list(status.transport_errors)


def _status_to_primitive(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_status_to_primitive(item) for item in value]
    if isinstance(value, dict):
        return {k: _status_to_primitive(v) for k, v in value.items()}
    if hasattr(value, "__dict__"):
        return {
            key: _status_to_primitive(val)
            for key, val in value.__dict__.items()
            if not key.startswith("_")
        }
    return value


@gen_pipeline_name
def test_connector_status(pipeline_name):
    """
    Test connector status, and especially error reporting.

    Create a pipeline with one input and one output Kafka connector. Feed some valid and invalid
    JSON records to the input connector. Invalid records are dropped and reported as parse errors,
    valid records are passed to the output connector. The output connector is configured with an
    incorrect Avro schema, so it fails to send all 500 records.

    Both connectors should report error count of 500 errors, but should only list 100 errors in
    the error lists, since we truncate the error lists to 100 errors of each type.

    The test validates connector statuses returned by both connectors and in particular checks
    error counts and error lists.
    """

    _, admin_cls, new_topic_cls = _kafka_clients()
    admin = admin_cls({"bootstrap.servers": KAFKA_BOOTSTRAP})
    input_topic = _random_topic("connector-errors-input")
    output_topic = _random_topic("connector-errors-output")

    _create_topic(admin, input_topic, new_topic_cls)
    _create_topic(admin, output_topic, new_topic_cls)

    wrong_avro_schema = {
        "type": "record",
        "name": "WrongOutputSchema",
        "fields": [{"name": "c1", "type": "string"}],
    }

    input_connector = {
        "name": "kafka_in",
        # Make sure the input connector processes records one by one.
        "max_batch_size": 1,
        "transport": {
            "name": "kafka_input",
            "config": {
                "topic": input_topic,
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "auto.offset.reset": "earliest",
            },
        },
        "format": {
            "name": "json",
            "config": {"update_format": "raw", "array": False},
        },
    }
    output_connector = {
        "name": "kafka_out",
        "transport": {
            "name": "kafka_output",
            "config": {
                "topic": output_topic,
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "auto.offset.reset": "earliest",
            },
        },
        "format": {
            "name": "avro",
            "config": {"update_format": "raw", "schema": json.dumps(wrong_avro_schema)},
        },
    }

    sql = f"""
    CREATE TABLE input_t(c1 INT) WITH (
      'connectors' = '{json.dumps([input_connector])}'
    );
    CREATE MATERIALIZED VIEW output_v WITH (
      'connectors' = '{json.dumps([output_connector])}'
    ) AS SELECT * FROM input_t;
    """.strip()

    pipeline: Pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql=sql,
    ).create_or_replace()
    pipeline.start()

    def input_connector_status() -> InputConnectorStatus:
        status = pipeline.input_connector_stats(
            table_name="input_t", connector_name="kafka_in"
        )
        _assert_input_status_fields(status, expected_stream="input_t")
        return status

    def output_connector_status() -> OutputConnectorStatus:
        status = pipeline.output_connector_stats(
            view_name="output_v", connector_name="kafka_out"
        )
        _assert_output_status_fields(status, expected_stream="output_v")
        return status

    try:
        valid_records = [json.dumps({"c1": i}) for i in range(500)]
        _produce_records(input_topic, valid_records)

        wait_for_condition(
            "output connector encode error count reaches 500",
            lambda: output_connector_status().metrics.num_encode_errors == 500
            and len(output_connector_status().encode_errors) == 100,
            timeout_s=100.0,
            poll_interval_s=1.0,
        )
        input_status = input_connector_status()
        output_status = output_connector_status()

        print(
            f"input_status: {json.dumps(_status_to_primitive(input_status), indent=2)}"
        )
        print(
            f"output_status: {json.dumps(_status_to_primitive(output_status), indent=2)}"
        )

        assert input_status.parse_errors is not None
        assert output_status.encode_errors is not None
        assert len(input_status.parse_errors) == 0
        assert len(output_status.encode_errors) == 100
        assert output_status.metrics.num_encode_errors == 500
        assert input_status.metrics.num_parse_errors == 0

        invalid_records = ["invalid\n"] * 500
        _produce_records(input_topic, invalid_records)

        wait_for_condition(
            "input connector parse error count reaches 500",
            lambda: input_connector_status().metrics.num_parse_errors == 500
            and len(input_connector_status().parse_errors) == 100,
            timeout_s=100.0,
            poll_interval_s=1.0,
        )
        input_status = input_connector_status()
        output_status = output_connector_status()

        assert input_status.parse_errors is not None
        assert len(input_status.parse_errors) == 100
        assert input_status.metrics.num_parse_errors == 500
        assert input_status.metrics.num_transport_errors == 0
        assert not input_status.metrics.end_of_input
        assert input_status.fatal_error is None
        assert input_status.metrics.total_records == 500

        assert output_status.encode_errors is not None
        assert len(output_status.encode_errors) == 100
        assert output_status.metrics.num_encode_errors == 500
        assert output_status.metrics.num_transport_errors == 0
        assert output_status.fatal_error is None

    finally:
        _delete_topic_best_effort(admin, input_topic)
        _delete_topic_best_effort(admin, output_topic)
