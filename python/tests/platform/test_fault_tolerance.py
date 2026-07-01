"""Fault-tolerant restart-recovery tests (enterprise only).

A fault-tolerant pipeline (`fault_tolerance: latest_checkpoint`) must replay its
logged input *deterministically* across restarts: on resume the engine restores
the latest checkpoint and replays the input log, and every replayed transaction must
contain the same records (count + hash) as were originally logged.

``test_ft_input_replay_determinism`` exercises this by repeatedly force-stopping
(crashing) and restarting a single-host FT pipeline while a datagen connector is
actively producing, so each restart replays a partially-logged step. On a buggy
build the pipeline crashes because validation code in controller.rs detects the
mismatch in the recorded vs replay input digest.
"""

import json
import os
import time
from http import HTTPStatus

from tests import enterprise_only
from .helper import (
    api_url,
    get,
    post_json,
    wait_for_program_success,
    start_pipeline,
    stop_pipeline,
    wait_for_condition,
    gen_pipeline_name,
)

# A large datagen limit keeps input flowing across many restart cycles, so each
# force-stop interrupts an in-progress step whose input is in the log but not yet
# checkpointed -- the precondition for exercising input replay.
_DATAGEN_CONNECTOR = json.dumps(
    [
        {
            "transport": {
                "name": "datagen",
                "config": {
                    "seed": 12345,
                    "plan": [
                        {
                            "limit": 200000,
                            "rate": 500,
                            "fields": {
                                "id": {"strategy": "increment", "range": [0, 2000000]},
                                "name": {"strategy": "word"},
                                "grp": {"strategy": "uniform", "range": [0, 50]},
                            },
                        }
                    ],
                },
            }
        }
    ]
)

_SQL = f"""
CREATE TABLE example1 (
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    grp INT NOT NULL
) WITH (
    'materialized' = 'true',
    'connectors' = '{_DATAGEN_CONNECTOR}'
);

CREATE MATERIALIZED VIEW view1 AS (SELECT * FROM example1);
"""

# Number of crash -> resume cycles. The bug reproduces within the first few
# cycles; the extra cycles give margin against its intermittency.
_CYCLES = 15

# The bug needs the input sharded across >= 2 workers (a single worker replays
# deterministically). 2 workers is the most reliable trigger -- it reproduces
# within a few cycles, whereas higher worker counts need more cycles -- so the
# test pins 2 rather than using FELDERA_TEST_NUM_WORKERS.
_WORKERS = 2


def _create_ft_pipeline(name: str, workers: int):
    """Create a single-host fault-tolerant pipeline (mirrors helper.create_pipeline
    but with a fault-tolerant runtime config)."""
    payload = {
        "name": name,
        "program_code": _SQL,
        "runtime_config": {
            "fault_tolerance": "latest_checkpoint",
            "storage": True,
            "workers": workers,
            "logging": "debug",
        },
    }
    runtime_version = os.environ.get("FELDERA_RUNTIME_VERSION")
    if runtime_version:
        payload["program_config"] = {"runtime_version": runtime_version}
    r = post_json(api_url("/pipelines"), payload)
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(name, 1)


def _deployment(name: str):
    d = get(api_url(f"/pipelines/{name}")).json()
    return d.get("deployment_status"), d.get("deployment_error")


@enterprise_only
@gen_pipeline_name
def test_ft_input_replay_determinism(pipeline_name):
    _create_ft_pipeline(pipeline_name, _WORKERS)
    start_pipeline(pipeline_name)

    for cycle in range(1, _CYCLES + 1):
        wait_for_condition(
            f"running before crash (cycle {cycle})",
            lambda: _deployment(pipeline_name)[0] == "Running",
            timeout_s=90.0,
            poll_interval_s=0.5,
        )
        # Let datagen feed a partial, not-yet-checkpointed step into the log.
        time.sleep(1.0)

        stop_pipeline(pipeline_name, force=True)  # crash: no final checkpoint
        start_pipeline(pipeline_name, wait=False)  # resume: restore + replay log

        def recovered():
            status, error = _deployment(pipeline_name)
            assert not (status == "Stopped" and error), (
                f"fault-tolerant recovery failed on cycle {cycle}: the pipeline "
                f"self-terminated on resume -- most likely non-deterministic input "
                f"replay ('Logged and replayed step N contained different numbers "
                f"of records or hashes'). deployment_error={error}"
            )
            return status == "Running"

        wait_for_condition(
            f"recovered to Running after crash (cycle {cycle})",
            recovered,
            timeout_s=90.0,
            poll_interval_s=0.5,
        )
