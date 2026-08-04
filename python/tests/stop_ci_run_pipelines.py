#!/usr/bin/env python3
"""Stop every pipeline the current CI run left behind on a shared instance.

Per-test teardown only runs while the test process is alive. A cancelled
workflow run, an evicted runner or a job timeout kills pytest outright, and
every pipeline that was running at that moment keeps burning compute on the
shared instance until the daily sweep finds it. Run this as an `if: always()`
step at the end of a job that talks to a shared instance.

The sweep matches on the prefix `feldera.testutils.unique_pipeline_name`
stamps on every test pipeline, so it only ever touches this run's own
pipelines. It never fails the job: a red cleanup step on an otherwise green
run tells nobody anything the warnings do not, and the daily sweep is still
the backstop for a runner that dies before this step runs.

It stops pipelines and clears their storage; it never deletes them. Stopped
and cleared, a pipeline consumes nothing, and its record is what you read to
work out why the run failed. Deleting is the daily sweep's job.

Run it as a module, not as a path: `tests/` holds a `platform` package that
shadows the standard library one for anything imported by a script living
there.

Usage:
    PYTHONPATH=$PWD uv run python -m tests.stop_ci_run_pipelines [--prefix P]
"""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor

from feldera.testutils import (
    BASE_URL,
    TEST_CLIENT,
    reclaim_pipeline,
    unique_pipeline_name,
)

# A cancelled job gets a short grace window before the runner kills it, so the
# sweep works on several pipelines at once rather than serially.
MAX_CONCURRENT_RECLAIMS = 8


def ci_run_prefix() -> str:
    """The prefix `unique_pipeline_name` gives every pipeline of this run."""
    return unique_pipeline_name("")


def warn(message: str) -> None:
    if os.environ.get("GITHUB_ACTIONS"):
        print(f"::warning::{message}", flush=True)
    else:
        print(f"WARNING: {message}", flush=True)


def is_reclaimed(deployment_status: str | None, storage_status: str | None) -> bool:
    """True once a pipeline holds neither compute nor storage."""
    return deployment_status == "Stopped" and storage_status == "Cleared"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="Pipeline name prefix to sweep. Defaults to this run's own prefix, "
        "the first five characters of GITHUB_SHA plus FELDERA_TEST_TAG_SUFFIX.",
    )
    args = parser.parse_args()

    prefix = args.prefix or ci_run_prefix()
    print(f"Sweeping pipelines named '{prefix}*' on {BASE_URL}", flush=True)

    try:
        pipelines = TEST_CLIENT.pipelines()
    except Exception as error:
        warn(f"could not list pipelines on {BASE_URL}: {error}")
        return 0

    to_reclaim = [
        pipeline.name
        for pipeline in pipelines
        if pipeline.name.startswith(prefix)
        and not is_reclaimed(pipeline.deployment_status, pipeline.storage_status)
    ]

    if not to_reclaim:
        print("No leaked pipelines.", flush=True)
        return 0

    warn(
        f"{len(to_reclaim)} pipeline(s) outlived their test and are being reclaimed: "
        + ", ".join(sorted(to_reclaim))
    )

    def reclaim(name: str) -> tuple[str, list[str]]:
        return name, reclaim_pipeline(name)

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_RECLAIMS) as pool:
        for name, failures in pool.map(reclaim, to_reclaim):
            if failures:
                warn("; ".join(failures))
            else:
                print(f"Reclaimed {name}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
