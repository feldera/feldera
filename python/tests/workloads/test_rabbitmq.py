"""End-to-end test for the RabbitMQ AMQP 1.0 connectors (`rabbitmq_input` /
`rabbitmq_output`).

Data flow exercised:

    RabbitMQ stream --(rabbitmq_input)--> table t
        --> view v --(rabbitmq_output)--> exchange --> bound result queue

Publishing into the input stream and reading the output queue is done over the
HTTP management API, so no extra AMQP client dependency is required; Feldera's
connectors do the AMQP 1.0 work.

Gated on `RABBITMQ_URL` the same way the Kafka tests gate on
`KAFKA_BOOTSTRAP_SERVERS`. Set, for a local RabbitMQ 4.x broker:

    export RABBITMQ_URL=amqp://guest:guest@localhost:5672
    # optional, defaults derived from RABBITMQ_URL host:
    export RABBITMQ_MANAGEMENT_URL=http://localhost:15672
"""

import json
import os
import time
import unittest
import uuid
from urllib.parse import urlparse

import requests

from feldera import PipelineBuilder
from feldera.testutils import unique_pipeline_name
from tests import TEST_CLIENT

RABBITMQ_URL = os.getenv("RABBITMQ_URL")


def _parsed():
    return urlparse(RABBITMQ_URL)


def _mgmt_url() -> str:
    if url := os.getenv("RABBITMQ_MANAGEMENT_URL"):
        return url.rstrip("/")
    return f"http://{_parsed().hostname or 'localhost'}:15672"


def _auth():
    p = _parsed()
    return (p.username or "guest", p.password or "guest")


def _broker_available() -> bool:
    if not RABBITMQ_URL:
        return False
    try:
        requests.get(f"{_mgmt_url()}/api/overview", auth=_auth(), timeout=3).raise_for_status()
        return True
    except Exception:
        return False


def _declare_queue(name: str, queue_type: str):
    r = requests.put(
        f"{_mgmt_url()}/api/queues/%2F/{name}",
        auth=_auth(),
        json={"durable": True, "arguments": {"x-queue-type": queue_type}},
        timeout=10,
    )
    r.raise_for_status()


def _declare_exchange(name: str):
    r = requests.put(
        f"{_mgmt_url()}/api/exchanges/%2F/{name}",
        auth=_auth(),
        json={"type": "topic", "durable": True},
        timeout=10,
    )
    r.raise_for_status()


def _bind(exchange: str, queue: str, routing_key: str):
    r = requests.post(
        f"{_mgmt_url()}/api/bindings/%2F/e/{exchange}/q/{queue}",
        auth=_auth(),
        json={"routing_key": routing_key},
        timeout=10,
    )
    r.raise_for_status()


def _publish_to_queue(queue: str, payloads: list[str]):
    """Publish via the default exchange, routing_key == queue name."""
    for payload in payloads:
        r = requests.post(
            f"{_mgmt_url()}/api/exchanges/%2F/amq.default/publish",
            auth=_auth(),
            json={
                "properties": {},
                "routing_key": queue,
                "payload": payload,
                "payload_encoding": "string",
            },
            timeout=10,
        )
        r.raise_for_status()
        assert r.json().get("routed") is True, f"message to {queue} not routed"


def _drain_queue(queue: str, want: int, timeout_s: int = 60) -> list[str]:
    """Poll the management get API until `want` messages are collected."""
    out: list[str] = []
    deadline = time.time() + timeout_s
    while len(out) < want and time.time() < deadline:
        r = requests.post(
            f"{_mgmt_url()}/api/queues/%2F/{queue}/get",
            auth=_auth(),
            json={"count": want, "ackmode": "ack_requeue_false", "encoding": "auto"},
            timeout=10,
        )
        r.raise_for_status()
        out.extend(m["payload"] for m in r.json())
        if len(out) < want:
            time.sleep(1)
    return out


def _delete(kind: str, name: str):
    try:
        requests.delete(f"{_mgmt_url()}/api/{kind}/%2F/{name}", auth=_auth(), timeout=10)
    except Exception:
        pass


@unittest.skipUnless(_broker_available(), "RABBITMQ_URL not set or broker unreachable")
class TestRabbitMq(unittest.TestCase):
    def test_input_view_output_roundtrip(self):
        p = _parsed()
        host = p.hostname or "localhost"
        port = p.port or 5672
        user = p.username or "guest"
        password = p.password or "guest"

        suffix = uuid.uuid4().hex[:8]
        in_stream = f"feldera_py_in_{suffix}"
        out_exchange = f"feldera_py_ex_{suffix}"
        out_queue = f"feldera_py_out_{suffix}"
        routing_key = "results.v1"

        n = 5
        _declare_queue(in_stream, "stream")
        _declare_exchange(out_exchange)
        _declare_queue(out_queue, "classic")
        _bind(out_exchange, out_queue, routing_key)
        _publish_to_queue(in_stream, [json.dumps({"id": i}) for i in range(n)])

        creds = {"host": host, "port": port, "username": user, "password": password}
        raw_json = {"name": "json", "config": {"update_format": "raw", "array": False}}

        input_connector = json.dumps(
            [{
                "transport": {
                    "name": "rabbitmq_input",
                    "config": {**creds, "queue": in_stream, "offset": {"policy": "first"}},
                },
                "format": raw_json,
            }]
        )
        output_connector = json.dumps(
            [{
                "transport": {
                    "name": "rabbitmq_output",
                    "config": {**creds, "exchange": out_exchange, "routing_key": routing_key},
                },
                "format": raw_json,
            }]
        )

        sql = f"""
create table t (
    id int
) with (
  'connectors' = '{input_connector}'
);

create materialized view v with (
  'connectors' = '{output_connector}'
) as select id from t;
"""

        pipeline = PipelineBuilder(
            TEST_CLIENT, unique_pipeline_name("test_rabbitmq"), sql
        ).create_or_replace()
        try:
            pipeline.start()
            payloads = _drain_queue(out_queue, n)
            got = sorted(json.loads(x)["id"] for x in payloads)
            self.assertEqual(got, list(range(n)), f"expected ids 0..{n - 1}, got {got}")
        finally:
            pipeline.stop(force=True)
            _delete("queues", in_stream)
            _delete("queues", out_queue)
            _delete("exchanges", out_exchange)


if __name__ == "__main__":
    unittest.main()
