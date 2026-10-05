"""Read a Databricks deletion-vector table over raw S3 and check the rows.

Regression test for delta-io/delta-rs#4692, which the fork carries as a
backport. The per-file deletion-vector keep masks live in a map shared by the
scan's streams and are drained positionally as batches arrive, so a stream can
consume a mask that is not its own. The right *number* of rows is dropped at
the wrong offsets: the count stays correct and the membership does not.

The read has to be partitioned to catch it. An unpartitioned table of the same
data was correct in every attempt against the unfixed reader, at any size and
scan width; see the fixture for what the table needs.

Which rows come back varies run to run, because it depends on the order the
streams reach the mask. This table resurrected 1917 rows through the connector,
and the stock Python client returned 0, 43, 4978 and 5021 on the same table
across six runs. A single green run against an unfixed reader therefore proves
nothing -- repeat it.

The failure is quiet: the count is right and the records are wrong, so a
count-only assertion sees nothing. The test asserts the deleted rows are absent.

Gated on ``DELTA_TABLE_TEST_UNITY_DV_TABLE``; build the fixture with::

    source ~/.feldera-dbx-test.env
    cd python && .venv/bin/python -m tests.platform.fixtures.unity_deletion_vectors
"""

from __future__ import annotations

import json
import os

import pytest

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS

from tests import TEST_CLIENT
from tests.platform.fixtures import unity_api, unity_deletion_vectors as fixture

REQUIRED_ENV = ["DELTA_TABLE_TEST_UNITY_DV_TABLE", "DELTA_TABLE_TEST_UNITY_HOST"]
MISSING_ENV = [name for name in REQUIRED_ENV if not os.environ.get(name)]
# feldera-ci has no standing client_secret (see infra#239); CI mints a bearer
# token per run instead. A client_secret still works for a by-hand run.
if not os.environ.get("DELTA_TABLE_TEST_UNITY_TOKEN"):
    MISSING_ENV += [
        name
        for name in (
            "DELTA_TABLE_TEST_UNITY_CLIENT_ID",
            "DELTA_TABLE_TEST_UNITY_CLIENT_SECRET",
        )
        if not os.environ.get(name)
    ]

# A silent skip in CI is exactly how this test went dormant the first time
# (see infra#239): IN_CI is set on every CI run, so a skip there is always a
# bug (a missing repo variable or federation policy), never an expected
# local-dev state, and must fail loudly instead of leaving CI green.
if MISSING_ENV and os.environ.get("IN_CI"):
    pytest.fail(
        f"CI must have {', '.join(MISSING_ENV)} set; a skip here would silently "
        "stop running this test instead of failing the build",
        pytrace=False,
    )

pytestmark = pytest.mark.skipif(
    bool(MISSING_ENV),
    reason=(
        f"unset: {', '.join(MISSING_ENV)}. Build the table with "
        "`.venv/bin/python -m tests.platform.fixtures.unity_deletion_vectors`."
    ),
)

TABLE = "dv_rows"
CONNECTOR = "dv_in"


def _s3_connector_config() -> dict[str, object]:
    """Resolve the table's S3 location and a temporary credential from Unity.

    The connector is pointed at the raw ``s3://`` location rather than
    ``uc://``: the defect lives in how the object-store reader delivers batches,
    so the test has to exercise that path.
    """
    host = os.environ["DELTA_TABLE_TEST_UNITY_HOST"].rstrip("/")
    token = unity_api.token_from_env(host)
    full_name = os.environ["DELTA_TABLE_TEST_UNITY_DV_TABLE"].removeprefix("uc://")
    return {
        **unity_api.table_location_and_credentials(host, token, full_name),
        "mode": "snapshot",
        "aws_region": os.environ.get("DELTA_TABLE_TEST_UNITY_REGION", "us-west-1"),
        "timeout": "1000 secs",
    }


def test_delta_input_deletion_vectors_over_s3(pipeline_name):
    """No row a deletion vector deletes may be read back.

    The count is checked too, but it is the weaker half: the defect this guards
    against returned exactly the right number of rows.
    """
    connectors = json.dumps(
        [
            {
                "name": CONNECTOR,
                "transport": {
                    "name": "delta_table_input",
                    "config": _s3_connector_config(),
                },
            }
        ]
    ).replace("'", "''")
    sql = (
        f"CREATE TABLE {TABLE} (id BIGINT, half INT, name VARCHAR)"
        f" WITH ('materialized' = 'true', 'connectors' = '{connectors}');\n"
        f"CREATE MATERIALIZED VIEW {TABLE}_summary AS SELECT COUNT(*) AS total,"
        f" COUNT(*) FILTER (WHERE {fixture.DELETED_PREDICATE}) AS resurrected"
        f" FROM {TABLE};"
    )

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
        ),
    ).create_or_replace()
    pipeline.start()
    try:
        pipeline.wait_for_completion(force_stop=False, timeout_s=1800)
        row = list(pipeline.query(f"SELECT * FROM {TABLE}_summary"))[0]
    finally:
        pipeline.stop(force=True)

    resurrected, total = int(row["resurrected"]), int(row["total"])
    assert resurrected == 0, (
        f"{resurrected} rows the deletion vectors delete were read back; the "
        "keep mask was applied at the wrong offsets. The row count alone can "
        "still be right when this happens."
    )
    assert total == fixture.EXPECTED_ROWS, (
        f"expected {fixture.EXPECTED_ROWS} rows, got {total}"
    )
