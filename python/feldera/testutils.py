"Utility functions for writing tests against a Feldera instance."

import base64
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
import platform
import re
import time
import json
import unittest
from typing import List, Optional, cast
from datetime import datetime

from feldera._long_operation_warning import LongOperationWarning
from feldera.enums import CompilationProfile
from feldera.pipeline import Pipeline
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import Resources, RuntimeConfig
from feldera.rest import FelderaClient, RetryConfig
from feldera.rest._helpers import requests_verify_from_env

logger = logging.getLogger(__name__)

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


# Audience -> (token, seconds since the epoch after which it is re-minted).
_oidc_token_cache: dict[str, tuple[str, float]] = {}

# Re-mint a token this many seconds before its own expiry, so a request issued
# just before the check cannot arrive after it.
_OIDC_REFRESH_MARGIN_SECONDS = 120.0

# GitHub's own token endpoint occasionally 503s or drops the connection; these
# are as transient as the pipeline-side errors FelderaClient already retries,
# so retry the same way rather than letting one blip fail the whole run.
_OIDC_MINT_RETRYABLE_STATUS_CODES = frozenset({429, 502, 503, 504})
_OIDC_MINT_MAX_RETRIES = 3
_OIDC_MINT_INITIAL_BACKOFF_SECONDS = 1.0
_OIDC_MINT_BACKOFF_MULTIPLIER = 2.0


def _mint_github_oidc_token(request: urllib.request.Request) -> str:
    """Issue the token-mint request, retrying transient failures."""
    backoff = _OIDC_MINT_INITIAL_BACKOFF_SECONDS
    for attempt in range(_OIDC_MINT_MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)["value"]
        except urllib.error.HTTPError as e:
            if (
                e.code not in _OIDC_MINT_RETRYABLE_STATUS_CODES
                or attempt == _OIDC_MINT_MAX_RETRIES
            ):
                raise
        except urllib.error.URLError:
            if attempt == _OIDC_MINT_MAX_RETRIES:
                raise
        time.sleep(backoff)
        backoff *= _OIDC_MINT_BACKOFF_MULTIPLIER
    raise AssertionError("unreachable")  # loop always returns or raises


def _github_oidc_token() -> str:
    """A GitHub Actions ID token, re-minted shortly before it expires.

    The SDK resolves this before every request, so the implementation has to be
    cheap. Minting per request adds a round trip to GitHub each time, and a
    suite that polls in loops across parallel workers sends enough of them to be
    throttled, which results in a connection timeout rather than an error.
    """
    audience = os.environ.get("FELDERA_OIDC_AUDIENCE", "")
    cached = _oidc_token_cache.get(audience)
    if cached is not None and time.time() < cached[1]:
        return cached[0]

    request_url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
    if audience:
        request_url += "&audience=" + urllib.parse.quote(audience, safe="")
    request = urllib.request.Request(request_url)
    request.add_header(
        "Authorization", f"bearer {os.environ['ACTIONS_ID_TOKEN_REQUEST_TOKEN']}"
    )
    token = _mint_github_oidc_token(request)

    _oidc_token_cache[audience] = (
        token,
        _token_expiry(token) - _OIDC_REFRESH_MARGIN_SECONDS,
    )
    return token


def _token_expiry(token: str) -> float:
    """`exp` from a JWT payload, in seconds since the epoch.

    Returns the current time if the payload cannot be read, which re-mints on
    every call rather than serving a token past its expiry.
    """
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return float(json.loads(base64.urlsafe_b64decode(payload))["exp"])
    except Exception:
        return time.time()


def feldera_bearer_token() -> Optional[str]:
    """The bearer token to send with a request issued now.

    A configured OIDC login flow wins, then a GitHub Actions ID token, then the
    static API key. None when nothing is configured, which is how a local
    instance without authentication runs. Callers that build their own requests
    resolve this per request: under Actions the token expires well inside a test
    run, and one read at import time starts returning 401 partway through.
    """
    if os.environ.get("OIDC_TEST_ISSUER"):
        return _get_effective_api_key()
    if os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL"):
        return _github_oidc_token()
    return API_KEY


BASE_URL = os.environ.get("FELDERA_HOST") or "http://localhost:8080"
FELDERA_REQUESTS_VERIFY = requests_verify_from_env()
FELDERA_TEST_NUM_WORKERS = int(os.environ.get("FELDERA_TEST_NUM_WORKERS", "8"))
FELDERA_TEST_NUM_HOSTS = int(os.environ.get("FELDERA_TEST_NUM_HOSTS", "1"))


class _LazyClient:
    "Construct the FelderaClient only when accessed as opposed to when imported."

    __slots__ = ("_client",)

    def __init__(self):
        self._client = None

    def _ensure(self):
        if self._client is None:
            # Under Actions the token expires inside a run, so the SDK gets the
            # resolver itself: it re-resolves per request and retries once on
            # 401. Elsewhere the credential is fixed for the process.
            self._client = FelderaClient(
                connection_timeout=10,
                # Shared CI instances see infrastructure churn (node
                # replacement, pipeline pod rescheduling) that outlasts the
                # default attempt-based retry budget of ~14 seconds. Retry on
                # a wall-clock budget sized to ride out a node replacement.
                # CI only: `enterprise_only` gates call `get_config` at
                # import time, and a 5-minute retry against an instance that
                # is not running would hang local test collection.
                retry_config=(
                    RetryConfig(deadline_seconds=300.0)
                    if os.environ.get("CI")
                    else RetryConfig()
                ),
                api_key=(
                    feldera_bearer_token
                    if os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL")
                    else feldera_bearer_token()
                ),
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


# Cap on rows logged when a view validation fails. A failing TPC-H or test_now run
# can otherwise emit tens of megabytes on a single line, which blows up CI log capture.
_ROW_DIFF_LOG_LIMIT = 20


def _log_row_diff(label: str, rows: list) -> None:
    if not rows:
        return
    log(
        f"{label} ({len(rows)} rows; showing first {min(len(rows), _ROW_DIFF_LOG_LIMIT)}):"
    )
    for row in rows[:_ROW_DIFF_LOG_LIMIT]:
        log(json.dumps(row, default=str))


def unique_pipeline_name(base_name: str) -> str:
    """
    In CI, multiple tests of different runs can run against the same Feldera instance, we
    make sure the pipeline names they use are unique by appending the first 5 characters
    of the commit SHA or 'local' if not in CI. FELDERA_TEST_TAG_SUFFIX distinguishes
    test suites of the same commit running concurrently against one instance.
    """
    ci_tag = os.getenv("GITHUB_SHA", "local")[:5] + os.getenv(
        "FELDERA_TEST_TAG_SUFFIX", ""
    )
    name = f"{ci_tag}_{base_name}"
    # The pipeline name becomes a Kubernetes label value (max 63 chars). Fail here
    # with a clear message rather than letting provisioning hit a cryptic 422.
    assert len(name) <= 62, (
        f"Generated pipeline name '{name}' is {len(name)} chars, exceeding the 62-char "
        f"limit; shorten the test name '{base_name}' to at most {62 - len(ci_tag) - 1} chars."
    )
    return name


# Teardown must not block on one wedged pipeline: the SDK otherwise polls for
# `Stopped` forever, and when the runner finally kills the job every pipeline
# the run started keeps consuming compute on the shared instance.
RECLAIM_TIMEOUT_SECONDS = 60.0


def reclaim_pipeline(name: str, delete: bool = False) -> List[str]:
    """Force-stop `name`, clear its storage, and optionally delete it.

    Every step runs even when the one before it raised: a pipeline that never
    reports `Stopped` can still release its storage, and abandoning the rest on
    the first error leaves them running on a shared instance. Returns one
    message per failed step, empty when the pipeline is fully reclaimed, so the
    caller decides whether a cleanup failure is worth failing a test over.
    """
    failures = []

    try:
        TEST_CLIENT.stop_pipeline(name, force=True, timeout_s=RECLAIM_TIMEOUT_SECONDS)
    except Exception as error:
        failures.append(f"{name}: stop: {error}")

    try:
        TEST_CLIENT.clear_storage(name, timeout_s=RECLAIM_TIMEOUT_SECONDS)
    except Exception as error:
        failures.append(f"{name}: clear storage: {error}")

    if delete:
        try:
            TEST_CLIENT.delete_pipeline(name)
        except Exception as error:
            failures.append(f"{name}: delete: {error}")

    return failures


def enterprise_only(fn):
    fn._enterprise_only = True
    return unittest.skipUnless(
        TEST_CLIENT.get_config().edition.is_enterprise(),
        f"{fn.__name__} is enterprise only, skipping",
    )(fn)


def single_host_only(fn):
    fn._single_host_only = True
    return unittest.skipUnless(
        FELDERA_TEST_NUM_HOSTS == 1,
        f"multihost not yet supported for {fn.__name__}, skipping",
    )(fn)


def skip_on_arm64(fn):
    """Skip the test on Linux aarch64.

    Some native Python wheels (e.g. `deltalake`) bundle a jemalloc built
    for 4 KB pages and abort on import on Ubuntu's 64 KB-page aarch64
    kernel used by some CI runners. Tests that depend on such wheels can
    use this decorator to opt out of running there.
    """
    fn._skip_on_arm64 = True
    return unittest.skipIf(
        platform.machine() == "aarch64",
        f"{fn.__name__} skipped on aarch64 (incompatible native wheel)",
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

            _log_row_diff(
                "Extra rows in Feldera output, but not in the ad hoc query output",
                extra_rows,
            )
            _log_row_diff(
                "Extra rows in the ad hoc query output, but not in Feldera output",
                missing_rows,
            )
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
    dev_tweaks: Optional[dict] = None,
    datafusion_memory_mb: Optional[int] = None,
) -> Pipeline:
    sql = generate_program(tables, views)

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        compilation_profile=CompilationProfile.OPTIMIZED,
        runtime_config=RuntimeConfig(
            # Covers node auto-provisioning: a pipeline that needs a fresh
            # node shape waits for node boot plus image pull, and parallel
            # test workers can request several fresh nodes at once.
            provisioning_timeout_secs=300,
            resources=resources,
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            dev_tweaks=dev_tweaks,
            datafusion_memory_mb=datafusion_memory_mb,
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
    # Reused by the warning message below so a stalled ingest can be told
    # apart from a slow one; check_end_of_input() would fetch and discard it.
    latest_stats = None
    wait_for_input_end = LongOperationWarning(
        logger,
        lambda elapsed: (
            f"still waiting for end of input on pipeline {pipeline.name}, "
            f"waited {elapsed:.1f} seconds ({latest_stats.global_metrics.progress_summary()})"
        ),
        lambda elapsed: (
            f"end of input reached on pipeline {pipeline.name} "
            f"after {elapsed:.1f} seconds"
        ),
    )

    while True:
        latest_stats = pipeline.stats()
        if all(
            input_endpoint.metrics.end_of_input
            for input_endpoint in latest_stats.inputs
        ):
            wait_for_input_end.done()
            return

        if timeout_s is not None and time.monotonic() - start_time > timeout_s:
            raise TimeoutError("Timeout waiting for end of input")

        wait_for_input_end.check()
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


def transaction_num_records(pipeline: Pipeline, num_records: int):
    """Run a transaction until it ingests a record count or reaches end of input."""

    log(f"Running transaction for {num_records} records or end of input")
    initial_records = number_of_processed_records(pipeline)
    pipeline.start_transaction()

    while not check_end_of_input(pipeline):
        processed_records = number_of_processed_records(pipeline) - initial_records
        if processed_records >= num_records:
            break
        time.sleep(3)

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


def number_of_input_records(pipeline: Pipeline) -> int:
    """Get the total_input_records metric."""

    return pipeline.stats().global_metrics.total_input_records


def run_workload(
    pipeline_name: str,
    tables: dict,
    views: List[ViewSpec],
    transaction: bool = True,
    stop: bool = True,
    resources: Optional[Resources] = None,
) -> Pipeline:
    """
    Helper to run a pipeline to completion and validate the views afterwards using ad-hoc queries.

    Use this for large-scale workload and standard benchmarks (like TPC-H etc.) where you plan to
    ingest a lot of data and validate the results. For testing more specific functionality, see
    frameworks in the `tests` directory.
    """

    pipeline = build_pipeline(pipeline_name, tables, views, resources)

    try:
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
                pipeline.commit_transaction(
                    transaction_id=None, wait=True, timeout_s=None
                )
                log(f"Commit took {time.monotonic() - start_time}")
            except Exception as e:
                log(f"Error committing transaction: {e}")

            log("Waiting for outputs to flush")
            start_time = time.monotonic()
            pipeline.wait_for_completion(force_stop=False, timeout_s=3600)
            log(f"Flushing outputs took {time.monotonic() - start_time}")

        validate_outputs(pipeline, tables, views)
    finally:
        if stop:
            try:
                pipeline.stop(force=True)
                pipeline.clear_storage()
            except Exception as e:
                log(f"Error during pipeline cleanup: {e}")

    return pipeline
