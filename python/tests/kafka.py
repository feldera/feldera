"""Kafka helpers shared by the tests that drive connectors through a broker.

Topics are named after a random suffix so that concurrent runs, and reruns
after a failure that left a topic behind, do not collide.
"""

import uuid
from contextlib import contextmanager
from typing import Iterator

from confluent_kafka.admin import AdminClient, NewTopic

from tests import KAFKA_BOOTSTRAP


def random_topic(prefix: str) -> str:
    """A topic name that no other test run uses."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def kafka_admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})


def create_topic(admin: AdminClient, topic: str, timeout_s: float = 30.0) -> None:
    futures = admin.create_topics(
        [NewTopic(topic=topic, num_partitions=1, replication_factor=1)]
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
def kafka_topics(*prefixes: str) -> Iterator[list[str]]:
    """Create one random topic per prefix, and delete them all on exit."""
    admin = kafka_admin()
    topics = [random_topic(prefix) for prefix in prefixes]
    for topic in topics:
        create_topic(admin, topic)
    try:
        yield topics
    finally:
        for topic in topics:
            delete_topic_best_effort(admin, topic)
