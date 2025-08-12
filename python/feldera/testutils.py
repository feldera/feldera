"Utility functions for writing tests against a Feldera instance."

import os
import re
import time
import json
import unittest
from typing import cast

from feldera.enums import CompilationProfile
from feldera.pipeline import Pipeline
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.rest import FelderaClient

API_KEY = os.environ.get("FELDERA_API_KEY")
BASE_URL = (
    os.environ.get("FELDERA_HOST")
    or os.environ.get("FELDERA_BASE_URL")
    or "http://localhost:8080"
)
KAFKA_SERVER = os.environ.get("FELDERA_KAFKA_SERVER", "localhost:19092")
PIPELINE_TO_KAFKA_SERVER = os.environ.get(
    "FELDERA_PIPELINE_TO_KAFKA_SERVER", "redpanda:9092"
)
FELDERA_TLS_INSECURE = True if os.environ.get("FELDERA_TLS_INSECURE") else False
FELDERA_HTTPS_TLS_CERT = os.environ.get("FELDERA_HTTPS_TLS_CERT")


class _LazyClient:
    "Construct the FelderaClient only when accessed as opposed to when imported."

    __slots__ = ("_client",)

    def __init__(self):
        self._client = None

    def _ensure(self):
        requests_verify = not FELDERA_TLS_INSECURE
        if requests_verify and FELDERA_HTTPS_TLS_CERT is not None:
            requests_verify = FELDERA_HTTPS_TLS_CERT

        if self._client is None:
            self._client = FelderaClient(
                BASE_URL,
                api_key=API_KEY,
                connection_timeout=10,
                requests_verify=requests_verify,
            )
        return self._client

    def __getattr__(self, name):
        return getattr(self._ensure(), name)

    def __call__(self, *a, **kw) -> FelderaClient:
        return self._ensure()


TEST_CLIENT = cast(FelderaClient, _LazyClient())


def unique_pipeline_name(base_name: str) -> str:
    """
    In CI, multiple tests of different runs can run against the same Feldera instance, we
    make sure the pipeline names they use are unique by appending the first 5 characters
    of the commit SHA or 'local' if not in CI.
    """
    ci_tag = os.getenv("GITHUB_SHA", "local")[:5]
    return f"{ci_tag}_{base_name}"


def enterprise_only(fn):
    fn._enterprise_only = True
    return unittest.skipUnless(
        TEST_CLIENT.get_config().edition.is_enterprise(),
        f"{fn.__name__} is enterprise only, skipping",
    )(fn)


def datafusionize(query: str) -> str:
    sort_array_pattern = re.compile(re.escape("SORT_ARRAY"), re.IGNORECASE)
    truncate_pattern = re.compile(re.escape("TRUNCATE"), re.IGNORECASE)
    timestamp_trunc_pattern = re.compile(
        r"TIMESTAMP_TRUNC\s*\(\s*MAKE_TIMESTAMP\s*\(\s*([^)]+)\s*\)\s*,\s*([A-Z]+)\s*\)",
        re.IGNORECASE,
    )

    result = sort_array_pattern.sub("array_sort", query)
    result = truncate_pattern.sub("trunc", result)
    result = timestamp_trunc_pattern.sub(r"DATE_TRUNC('\2', TO_TIMESTAMP(\1))", result)
    return result


def validate_view(
    pipeline: Pipeline, view_name: str, view_query: str | tuple[str, str]
):
    print(f"Validating view '{view_name}'")

    # We have two modes to verify the view, either we run the same SQL as the view against datafusion
    # by `datafusionizing` the query, or a weaker form where we pass a hash of what the result
    # should look like and check that the hash hasn't changed
    if isinstance(view_query, tuple):
        _view_definition, original_hash = view_query
        view_query = f"select * from {view_name}"
        computed_hash = pipeline.query_hash(view_query)
        if computed_hash != original_hash:
            raise AssertionError(
                f"View {view_name} hash {computed_hash} was but expected hash {original_hash}"
            )
    else:
        # TODO: count records
        view_query = datafusionize(view_query)
        try:
            extra_rows = list(
                pipeline.query(f"(select * from {view_name}) except ({view_query})")
            )
            missing_rows = list(
                pipeline.query(f"({view_query}) except (select * from {view_name})")
            )

            if extra_rows:
                print(
                    "Extra rows in Feldera output, but not in the ad hoc query output"
                )
                print(json.dumps(extra_rows))

            if missing_rows:
                print(
                    "Extra rows in the ad hoc query output, but not in Feldera output"
                )
                print(json.dumps(missing_rows))
        except Exception as e:
            print(f"Error querying view '{view_name}': {e}")
            print(f"Ad-hoc Query: {view_query}")
            raise

        if extra_rows or missing_rows:
            raise AssertionError(f"Validation failed for view {view_name}")


def run_workload(pipeline_name: str, tables: dict, views: dict):
    """
    Helper to run a pipeline to completion and validate the views afterwards using ad-hoc queries.

    Use this for large-scale workload and standard benchmarks (like TPC-H etc.) where you plan to
    ingest a lot of data and validate the results. For testing more specific functionality, see
    frameworks in the `tests` directory.
    """

    sql = ""
    for table_sql in tables.values():
        sql += f"{table_sql}\n"

    for view_name, view in views.items():
        if isinstance(view, tuple):
            view_query, _hash = view
            sql += f"create materialized view {view_name} as {view_query};\n\n"
        else:
            sql += f"create materialized view {view_name} as {view};\n\n"

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        unique_pipeline_name(pipeline_name),
        sql=sql,
        compilation_profile=CompilationProfile.OPTIMIZED,
        runtime_config=RuntimeConfig(provisioning_timeout_secs=60),
    ).create_or_replace()

    pipeline.start()
    start_time = time.monotonic()

    try:
        pipeline.start_transaction()
    except Exception as e:
        print(f"Error starting transaction: {e}")

    pipeline.wait_for_completion(force_stop=False, timeout_s=3600)
    elapsed = time.monotonic() - start_time
    print(f"Data ingested in {elapsed}")

    try:
        start_time = time.monotonic()
        pipeline.commit_transaction(transaction_id=None, wait=True, timeout_s=None)
    except Exception as e:
        print(f"Error committing transaction: {e}")
    finally:
        elapsed = time.monotonic() - start_time
        print(f"Commit took {elapsed}")

    for table in tables.keys():
        row_count = list(pipeline.query(f"select count(*) from {table}"))
        print(f"Table '{table}' count(*):\n{row_count}")

    for view_name, view_query in views.items():
        validate_view(pipeline, view_name, view_query)

    pipeline.stop(force=True)
