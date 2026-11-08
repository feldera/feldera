"""Kafka helpers shared by the tests that drive connectors through a broker.

Topics are named after a random suffix so that concurrent runs, and reruns
after a failure that left a topic behind, do not collide.

The helpers also let a test outlive the restart of one broker, which the CI
cluster goes through whenever Strimzi rolls it. Topics take the cluster's
default replication factor, so a partition whose leader restarts gets a new
leader within seconds instead of going offline. Producers are idempotent, so
the batches they resend to the new leader do not land twice.
"""

import uuid
from contextlib import contextmanager
from typing import Iterator

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from tests import KAFKA_BOOTSTRAP

# Tells the broker to apply its own `default.replication.factor` (KIP-464).
# The CI cluster's default is 3, with `min.insync.replicas` 2, so a topic stays
# writable while one broker restarts. A single-broker development setup
# defaults to 1.
BROKER_DEFAULT_REPLICATION_FACTOR = -1


def random_topic(prefix: str) -> str:
    """A topic name that no other test run uses."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def kafka_admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})


def kafka_producer() -> Producer:
    """Create a producer that writes each record exactly once.

    When a partition's leader changes, the producer resends every batch that
    the old leader did not acknowledge, and the old leader may have written
    some of them already. Idempotence lets the broker drop those copies; a
    plain producer would write them twice and break the record counts that
    tests assert.

    Returns:
        A producer connected to `KAFKA_BOOTSTRAP`.
    """
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "enable.idempotence": True})


def create_topic(
    admin: AdminClient, topic: str, timeout_s: float = 30.0, num_partitions: int = 1
) -> None:
    """Create a topic with the broker's default replication factor.

    A topic with one replica goes offline while its broker restarts, and
    Strimzi rolls such a broker without waiting, because the topic cannot stay
    available anyway.

    Args:
        admin: Client of the cluster to create the topic in.
        topic: Name of the new topic.
        timeout_s: How long to wait for the broker to create the topic.
        num_partitions: Number of partitions of the new topic.

    Returns:
        None, once the broker has created the topic.
    """
    futures = admin.create_topics(
        [
            NewTopic(
                topic=topic,
                num_partitions=num_partitions,
                replication_factor=BROKER_DEFAULT_REPLICATION_FACTOR,
            )
        ]
    )
    futures[topic].result(timeout=timeout_s)


def delete_topic_best_effort(admin: AdminClient, topic: str) -> None:
    try:
        futures = admin.delete_topics([topic], operation_timeout=10)
        futures[topic].result(timeout=10)
    except Exception:
        # Topic deletion can be disabled on some brokers; cleanup is best-effort.
        pass


@contextmanager
def kafka_topics(*prefixes: str, num_partitions: int = 1) -> Iterator[list[str]]:
    """Create one random topic per prefix, each with `num_partitions`
    partitions, and delete them all on exit."""
    admin = kafka_admin()
    topics = [random_topic(prefix) for prefix in prefixes]
    for topic in topics:
        create_topic(admin, topic, num_partitions=num_partitions)
    try:
        yield topics
    finally:
        for topic in topics:
            delete_topic_best_effort(admin, topic)
