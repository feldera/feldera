import logging
import os

from feldera.testutils import (
    API_KEY,
    BASE_URL,
    FELDERA_REQUESTS_VERIFY,
    TEST_CLIENT,
    enterprise_only,
    unique_pipeline_name,
)

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)

__all__ = [
    "TEST_CLIENT",
    "unique_pipeline_name",
    "enterprise_only",
    "API_KEY",
    "BASE_URL",
    "FELDERA_REQUESTS_VERIFY",
]
