"Utility functions for writing tests against a Feldera instance."

import os
import re
import time
import json
import unittest
from typing import List, Optional, cast
from datetime import datetime

from feldera.enums import CompilationProfile
from feldera.pipeline import Pipeline
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import Resources, RuntimeConfig
from feldera.rest import FelderaClient
from feldera.rest._helpers import requests_verify_from_env

API_KEY = os.environ.get("FELDERA_API_KEY")


# OIDC authentication support
def _get_oidc_token():
    """Get OIDC token if environment is configured, otherwise return None"""
    try:
        from feldera.testutils_oidc import get_oidc_test_helper

        oidc_helper = get_oidc_test_helper()
        if oidc_helper is not None:
            return oidc_helper.obtain_access_token()
    except ImportError:
        pass
    return None


def _get_effective_api_key():
    """Get effective API key - OIDC token takes precedence over static API key"""
    oidc_token = _get_oidc_token()
    return oidc_token if oidc_token else API_KEY


BASE_URL = os.environ.get("FELDERA_HOST") or "http://localhost:8080"
FELDERA_REQUESTS_VERIFY = requests_verify_from_env()


class _LazyClient:
    "Construct the FelderaClient only when accessed as opposed to when imported."

    __slots__ = ("_client",)

    def __init__(self):
        self._client = None

    def _ensure(self):
        if self._client is None:
            self._client = FelderaClient(
                connection_timeout=10,
                api_key=_get_effective_api_key(),
            )
        return self._client

    def __getattr__(self, name):
        return getattr(self._ensure(), name)

    def __call__(self, *a, **kw) -> FelderaClient:
        return self._ensure()


TEST_CLIENT = cast(FelderaClient, _LazyClient())


# SQL index definition.
class IndexSpec:
    def __init__(self, name: str, columns: List[str]):
        self.name = name
        self.columns = columns

    def __repr__(self):
        return f"IndexSpec(name={self.name!r},columns={self.columns!r})"


class ViewSpec:
    """
    SQL view definition consisting of a query that can run in Feldera or
    datafusion, optional connector spec and aux SQL statements, e.g., indexes
    and lateness clauses following view definition.
    """

    def __init__(
        self,
        name: str,
        query: str,
        indexes: List[IndexSpec] = [],
        connectors: Optional[str] = None,
        aux: Optional[str] = None,
        expected_hash: Optional[str] = None,
    ):
        if not isinstance(query, str):
            raise TypeError("query must be a string")
        self.name = name
        self.query = query
        self.connectors = connectors
        self.indexes = indexes
        self.aux = aux
        self.expected_hash = expected_hash

    def __repr__(self):
        return f"ViewSpec(name={self.name!r}, query={self.query!r}, indexes={self.indexes!r}, connectors={self.connectors!r}, aux={self.aux!r}, expected_hash={self.expected_hash!r})"

    def clone(self):
        return ViewSpec(
            self.name,
            self.query,
            self.indexes,
            self.connectors,
            self.aux,
            self.expected_hash,
        )

    def clone_with_name(self, name: str):
        return ViewSpec(name, self.query, self.indexes, self.connectors, self.aux)

    def sql(self) -> str:
        sql = ""

        if self.connectors:
            with_clause = f"\nwith('connectors' = '{self.connectors}')\n"
        else:
            with_clause = ""

        sql += (
            f"create materialized view {self.name}{with_clause} as\n{self.query};\n\n"
        )

        for index in self.indexes:
            columns = ",".join(index.columns)
            sql += f"create index {index.name} on {self.name}({columns});\n"

        if self.aux:
            sql += f"{self.aux}\n"

        sql += "\n"

        return sql


def log(*args, **kwargs):
    """Print like built-in print(), but prefix each line with current time."""
    prefix = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    print(prefix, *args, **kwargs)


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


def validate_view(pipeline: Pipeline, view: ViewSpec):
    log(f"Validating view '{view.name}'")

    # We have two modes to verify the view, either we run the same SQL as the view against datafusion
    # by `datafusionizing` the query, or a weaker form where we pass a hash of what the result
    # should look like and check that the hash hasn't changed
    if view.expected_hash:
        view_query = f"select * from {view.name}"
        computed_hash = pipeline.query_hash(view_query)
        if computed_hash != view.expected_hash:
            raise AssertionError(
                f"View {view.name} hash {computed_hash} was but expected hash {view.expected_hash}"
            )
    else:
        # TODO: count records
        view_query = datafusionize(view.query)
        try:
            extra_rows = list(
                pipeline.query(f"(select * from {view.name}) except ({view_query})")
            )
            missing_rows = list(
                pipeline.query(f"({view_query}) except (select * from {view.name})")
            )

            if extra_rows:
                log("Extra rows in Feldera output, but not in the ad hoc query output")
                log(json.dumps(extra_rows, default=str))

            if missing_rows:
                log("Extra rows in the ad hoc query output, but not in Feldera output")
                log(json.dumps(missing_rows, default=str))
        except Exception as e:
            log(f"Error querying view '{view.name}': {e}")
            log(f"Ad-hoc Query: {view_query}")
            raise

        if extra_rows or missing_rows:
            raise AssertionError(f"Validation failed for view {view.name}")


def generate_program(tables: dict, views: List[ViewSpec]) -> str:
    sql = ""

    for table_sql in tables.values():
        sql += f"{table_sql}\n"

    for view in views:
        sql += view.sql()

    return sql


def build_pipeline(
    pipeline_name: str,
    tables: dict,
    views: List[ViewSpec],
    resources: Optional[Resources] = None,
) -> Pipeline:
    sql = generate_program(tables, views)

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        compilation_profile=CompilationProfile.OPTIMIZED,
        runtime_config=RuntimeConfig(
            provisioning_timeout_secs=60,
            resources=resources,
        ),
    ).create_or_replace()

    return pipeline


def validate_outputs(pipeline: Pipeline, tables: dict, views: List[ViewSpec]):
    for table in tables.keys():
        row_count = list(pipeline.query(f"select count(*) from {table}"))
        log(f"Table '{table}' count(*):\n{row_count}")

    for view in views:
        validate_view(pipeline, view)


def check_end_of_input(pipeline: Pipeline) -> bool:
    return all(
        input_endpoint.metrics.end_of_input
        for input_endpoint in pipeline.stats().inputs
    )


def wait_end_of_input(pipeline: Pipeline, timeout_s: Optional[int] = None):
    start_time = time.monotonic()
    while not check_end_of_input(pipeline):
        if timeout_s is not None and time.monotonic() - start_time > timeout_s:
            raise TimeoutError("Timeout waiting for end of input")
        time.sleep(3)


def transaction(pipeline: Pipeline, duration_seconds: int):
    """Run a transaction for a specified duration."""

    log(f"Running transaction for {duration_seconds} seconds")
    pipeline.start_transaction()
    time.sleep(duration_seconds)
    log("Committing transaction")
    commit_start = time.monotonic()
    pipeline.commit_transaction()
    log(f"Transaction committed in {time.monotonic() - commit_start} seconds")


def checkpoint_pipeline(pipeline: Pipeline):
    """Create a checkpoint and wait for it to complete."""

    log("Creating checkpoint")
    checkpoint_start = time.monotonic()
    pipeline.checkpoint(wait=True)
    log(f"Checkpoint complete in {time.monotonic() - checkpoint_start} seconds")


def check_for_endpoint_errors(pipeline: Pipeline):
    """Check for errors on all input and output endpoints."""

    for input_endpoint_status in pipeline.stats().inputs:
        input_endpoint_status.metrics
        if input_endpoint_status.metrics.num_transport_errors > 0:
            raise RuntimeError(
                f"Transport errors detected on input endpoint: {input_endpoint_status.endpoint_name}"
            )
        if input_endpoint_status.metrics.num_parse_errors > 0:
            raise RuntimeError(
                f"Parse errors on input endpoint: {input_endpoint_status.endpoint_name}"
            )
        log(f"  Input endpoint {input_endpoint_status.endpoint_name} OK")

    for output_endpoint_status in pipeline.stats().outputs:
        output_endpoint_status.metrics
        if output_endpoint_status.metrics.num_transport_errors > 0:
            raise RuntimeError(
                f"Transport errors detected on output endpoint: {output_endpoint_status.endpoint_name}"
            )
        if output_endpoint_status.metrics.num_encode_errors > 0:
            raise RuntimeError(
                f"Encode errors on output endpoint: {output_endpoint_status.endpoint_name}"
            )
        log(f"  Output endpoint {output_endpoint_status.endpoint_name} OK")


def number_of_processed_records(pipeline: Pipeline) -> int:
    """Get the total_processed_records metric."""

    return pipeline.stats().global_metrics.total_processed_records


def run_workload(
    pipeline_name: str, tables: dict, views: List[ViewSpec], transaction: bool = True
):
    """
    Helper to run a pipeline to completion and validate the views afterwards using ad-hoc queries.

    Use this for large-scale workload and standard benchmarks (like TPC-H etc.) where you plan to
    ingest a lot of data and validate the results. For testing more specific functionality, see
    frameworks in the `tests` directory.
    """

    pipeline = build_pipeline(pipeline_name, tables, views)

    pipeline.start()
    start_time = time.monotonic()

    if transaction:
        try:
            pipeline.start_transaction()
        except Exception as e:
            log(f"Error starting transaction: {e}")

    if transaction:
        wait_end_of_input(pipeline, timeout_s=3600)
    else:
        pipeline.wait_for_completion(force_stop=False, timeout_s=3600)

    elapsed = time.monotonic() - start_time
    log(f"Data ingested in {elapsed}")

    if transaction:
        start_time = time.monotonic()
        try:
            pipeline.commit_transaction(transaction_id=None, wait=True, timeout_s=None)
            log(f"Commit took {time.monotonic() - start_time}")
        except Exception as e:
            log(f"Error committing transaction: {e}")

        log("Waiting for outputs to flush")
        start_time = time.monotonic()
        pipeline.wait_for_completion(force_stop=False, timeout_s=3600)
        log(f"Flushing outputs took {time.monotonic() - start_time}")

    validate_outputs(pipeline, tables, views)

    pipeline.stop(force=True)
