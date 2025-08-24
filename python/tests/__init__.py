from feldera.rest import FelderaClient
import os
import unittest

API_KEY = os.environ.get("FELDERA_API_KEY")
BASE_URL = (
    os.environ.get("FELDERA_BASE_URL")  # deprecated
    or os.environ.get("FELDERA_HOST")
    or "http://localhost:8080"
)
KAFKA_SERVER = os.environ.get("FELDERA_KAFKA_SERVER", "localhost:19092")
PIPELINE_TO_KAFKA_SERVER = os.environ.get(
    "FELDERA_PIPELINE_TO_KAFKA_SERVER", "redpanda:9092"
)

TEST_CLIENT = FelderaClient(
    BASE_URL, api_key=API_KEY, connection_timeout=10, requests_verify=False
)


def enterprise_only(fn):
    fn._enterprise_only = True
    return unittest.skipUnless(
        TEST_CLIENT.get_config().edition.is_enterprise(),
        f"{fn.__name__} is enterprise only, skipping",
    )(fn)
