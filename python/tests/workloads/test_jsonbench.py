"""Port of ClickHouse JSONBench (https://github.com/ClickHouse/JSONBench).

The benchmark ingests 1M-row files of newline-delimited JSON Bluesky events
into a VARIANT column and evaluates the five JSONBench queries as
materialized views. Raw lines enter through the raw format; a materialized
view converts them with PARSE_JSON, and the query views build on it.

By default the pipeline reads the gzip-compressed files straight from the
benchmark's public S3 bucket (s3://clickhouse-public-datasets/bluesky) with
Feldera's s3 connector; a user-defined gunzip preprocessor decompresses the
byte stream in front of the parser. With --input-mode file the test instead
downloads and decompresses the files into a local cache and reads them with
the file connector.

The primary purpose is comparing the two VARIANT runtime representations:

    uv run python tests/workloads/test_jsonbench.py --variant 1   # enum Variant
    uv run python tests/workloads/test_jsonbench.py --variant 2   # FlatVariant

The flag pins the representation with SET feldera_flat_variant, overriding the
manager's FELDERA_FLAT_VARIANT environment default in either direction. The test
reports ingest wall time and rows/s and cross-checks the query outputs
(the table row count must equal the sum of per-event counts in q1).
"""

import argparse
import gzip
import os
import sys
import time
import unittest
import urllib.request
from typing import List, Optional

from feldera import PipelineBuilder
from feldera.enums import CompilationProfile
from feldera.pipeline import Pipeline
from feldera.runtime_config import Resources, RuntimeConfig
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    TEST_CLIENT,
    ViewSpec,
    check_for_endpoint_errors,
    generate_program,
    log,
    number_of_processed_records,
    unique_pipeline_name,
    wait_end_of_input,
)

DATASET_BUCKET = "clickhouse-public-datasets"
DATASET_REGION = "eu-central-1"
DATASET_KEY = "bluesky/file_{index:04d}.json.gz"
DATASET_URL = f"https://{DATASET_BUCKET}.s3.amazonaws.com/{DATASET_KEY}"
DEFAULT_CACHE_DIR = "/mnt/data/jsonbench"

# Gunzip preprocessor: decompresses the connector's byte stream in front of
# the raw-format parser. The passthrough splitter hands arbitrary slices of
# the compressed stream to process(); one decoder per fork handles
# concatenated gzip members, so consecutive objects decode seamlessly.
# process() emits whole lines only, holding back the partial trailing line,
# so the preprocessor is message oriented; records must therefore be
# newline-terminated, which holds for the benchmark files.
GUNZIP_UDF = """
use std::io::Write;

use feldera_adapterlib::format::{ParseError, Splitter};
use feldera_adapterlib::preprocess::{
    Preprocessor, PreprocessorCreateError, PreprocessorFactory,
};
use feldera_types::preprocess::PreprocessorConfig;
use flate2::write::MultiGzDecoder;

struct PassthroughSplitter;

impl Splitter for PassthroughSplitter {
    fn input(&mut self, data: &[u8]) -> Option<usize> {
        // None on empty input: Some(0) would make the transport's chunking
        // loop spin on empty chunks forever.
        if data.is_empty() {
            None
        } else {
            Some(data.len())
        }
    }

    fn clear(&mut self) {}
}

pub struct GunzipPreprocessor {
    decoder: MultiGzDecoder<Vec<u8>>,
}

impl GunzipPreprocessor {
    fn new() -> Self {
        GunzipPreprocessor {
            decoder: MultiGzDecoder::new(Vec::new()),
        }
    }
}

impl Preprocessor for GunzipPreprocessor {
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<ParseError>) {
        let result = self
            .decoder
            .write_all(data)
            .and_then(|()| self.decoder.flush());
        match result {
            Ok(()) => {
                // Emit whole lines only; the partial trailing line stays
                // buffered until its remainder is decompressed.
                let out = self.decoder.get_mut();
                match out.iter().rposition(|&b| b == b'\\n') {
                    Some(pos) => {
                        let emit = out[..=pos].to_vec();
                        out.drain(..=pos);
                        (emit, vec![])
                    }
                    None => (Vec::new(), vec![]),
                }
            }
            Err(e) => (
                Vec::new(),
                vec![ParseError::text_envelope_error(
                    format!("gzip decode error: {e}"),
                    "",
                    None,
                )],
            ),
        }
    }

    fn fork(&self) -> Box<dyn Preprocessor> {
        Box::new(GunzipPreprocessor::new())
    }

    fn splitter(&self) -> Option<Box<dyn Splitter>> {
        Some(Box::new(PassthroughSplitter))
    }
}

pub struct GunzipPreprocessorFactory;

impl PreprocessorFactory for GunzipPreprocessorFactory {
    fn create(
        &self,
        _config: &PreprocessorConfig,
    ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError> {
        Ok(Box::new(GunzipPreprocessor::new()))
    }
}
"""

GUNZIP_TOML = """
flate2 = { version = "1.1" }
"""


class JSONBenchConfig:
    def __init__(
        self,
        mode: str = "transaction",
        input_mode: str = "s3",
        variant: Optional[int] = None,
        num_files: int = 1,
        cache_dir: str = DEFAULT_CACHE_DIR,
        resources: Optional[Resources] = None,
    ):
        if mode not in ("transaction", "stream"):
            raise ValueError(f"Unknown mode: {mode}")
        if input_mode not in ("s3", "file"):
            raise ValueError(f"Unknown input mode: {input_mode}")
        if variant not in (None, 1, 2):
            raise ValueError("variant must be 1, 2, or None (manager default)")
        if not 1 <= num_files <= 1000:
            raise ValueError("num_files must be between 1 and 1000")
        self.mode = mode
        self.input_mode = input_mode
        self.variant = variant
        self.num_files = num_files
        self.cache_dir = cache_dir
        self.resources = resources


def download_file(index: int, cache_dir: str) -> str:
    """Fetch one 1M-row benchmark file into the cache, decompressed.
    Returns the local path."""
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, f"file_{index:04d}.json")
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path

    url = DATASET_URL.format(index=index)
    log(f"Downloading {url}")
    tmp_path = path + ".tmp"
    start_time = time.monotonic()
    # The S3 endpoint occasionally resets TLS handshakes; retry a few times.
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(url) as response:
                with gzip.GzipFile(fileobj=response) as decompressed:
                    with open(tmp_path, "wb") as out:
                        while chunk := decompressed.read(1 << 20):
                            out.write(chunk)
            break
        except OSError as e:
            if attempt == 3:
                raise
            log(f"Download attempt {attempt} failed ({e}), retrying")
            time.sleep(2 * attempt)
    os.replace(tmp_path, path)
    log(f"Downloaded {path} in {time.monotonic() - start_time:.1f}s")
    return path


def settings_sql(variant: Optional[int]) -> str:
    if variant is None:
        return ""
    value = "ON" if variant == 2 else "OFF"
    return f"SET feldera_flat_variant = {value};\n"


def s3_connector(index: int) -> str:
    key = DATASET_KEY.format(index=index)
    return f"""{{
        "transport": {{
            "name": "s3_input",
            "config": {{
                "bucket_name": "{DATASET_BUCKET}",
                "key": "{key}",
                "region": "{DATASET_REGION}",
                "no_sign_request": true
            }}
        }},
        "format": {{
            "name": "raw",
            "config": {{ "mode": "lines" }}
        }},
        "preprocessor": [{{
            "name": "gunzip",
            "message_oriented": true,
            "config": {{}}
        }}]
    }}"""


def file_connector(path: str) -> str:
    return f"""{{
        "transport": {{
            "name": "file_input",
            "config": {{ "path": "{path}" }}
        }},
        "format": {{
            "name": "raw",
            "config": {{ "mode": "lines" }}
        }}
    }}"""


def bluesky_tables(config: JSONBenchConfig) -> dict:
    if config.input_mode == "s3":
        connectors = ",".join(
            s3_connector(index) for index in range(1, config.num_files + 1)
        )
    else:
        connectors = ",".join(
            file_connector(download_file(index, config.cache_dir))
            for index in range(1, config.num_files + 1)
        )
    return {
        "settings": settings_sql(config.variant),
        "bluesky_raw": f"""
CREATE TABLE bluesky_raw (
    line VARCHAR NOT NULL
) WITH (
    'connectors' = '[{connectors}]'
);
""",
    }


# The five JSONBench queries (clickhouse/queries.sql), translated to Feldera
# SQL over a VARIANT column. The bluesky view converts raw lines once;
# hour_of_day is computed arithmetically from the microsecond timestamp,
# matching toHour(fromUnixTimestamp64Micro(..)) in UTC; q5's activity span is
# milliseconds, matching date_diff('milliseconds').
def jsonbench_views() -> List[ViewSpec]:
    return [
        ViewSpec(
            "bluesky",
            """
    select PARSE_JSON(line) as data from bluesky_raw
    """,
        ),
        ViewSpec(
            "q1",
            """
    select
        CAST(data['commit']['collection'] AS VARCHAR) as event,
        count(*) as count
    from bluesky
    group by CAST(data['commit']['collection'] AS VARCHAR)
    order by count desc
    """,
        ),
        ViewSpec(
            "q2",
            """
    select
        CAST(data['commit']['collection'] AS VARCHAR) as event,
        count(*) as count,
        count(distinct CAST(data['did'] AS VARCHAR)) as users
    from bluesky
    where CAST(data['kind'] AS VARCHAR) = 'commit'
      and CAST(data['commit']['operation'] AS VARCHAR) = 'create'
    group by CAST(data['commit']['collection'] AS VARCHAR)
    order by count desc
    """,
        ),
        ViewSpec(
            "q3",
            """
    select
        CAST(data['commit']['collection'] AS VARCHAR) as event,
        MOD(CAST(data['time_us'] AS BIGINT) / 3600000000, 24) as hour_of_day,
        count(*) as count
    from bluesky
    where CAST(data['kind'] AS VARCHAR) = 'commit'
      and CAST(data['commit']['operation'] AS VARCHAR) = 'create'
      and CAST(data['commit']['collection'] AS VARCHAR)
          in ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like')
    group by
        CAST(data['commit']['collection'] AS VARCHAR),
        MOD(CAST(data['time_us'] AS BIGINT) / 3600000000, 24)
    order by hour_of_day, event
    """,
        ),
        ViewSpec(
            "q4",
            """
    select
        CAST(data['did'] AS VARCHAR) as user_id,
        min(CAST(data['time_us'] AS BIGINT)) as first_post_ts
    from bluesky
    where CAST(data['kind'] AS VARCHAR) = 'commit'
      and CAST(data['commit']['operation'] AS VARCHAR) = 'create'
      and CAST(data['commit']['collection'] AS VARCHAR) = 'app.bsky.feed.post'
    group by CAST(data['did'] AS VARCHAR)
    order by first_post_ts asc
    limit 3
    """,
        ),
        ViewSpec(
            "q5",
            """
    select
        CAST(data['did'] AS VARCHAR) as user_id,
        (max(CAST(data['time_us'] AS BIGINT)) - min(CAST(data['time_us'] AS BIGINT)))
            / 1000 as activity_span_ms
    from bluesky
    where CAST(data['kind'] AS VARCHAR) = 'commit'
      and CAST(data['commit']['operation'] AS VARCHAR) = 'create'
      and CAST(data['commit']['collection'] AS VARCHAR) = 'app.bsky.feed.post'
    group by CAST(data['did'] AS VARCHAR)
    order by activity_span_ms desc
    limit 3
    """,
        ),
    ]


def build_jsonbench_pipeline(
    pipeline_name: str, config: JSONBenchConfig, views: List[ViewSpec]
) -> Pipeline:
    sql = generate_program(bluesky_tables(config), views)
    needs_gunzip = config.input_mode == "s3"
    return PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        udf_rust=GUNZIP_UDF if needs_gunzip else "",
        udf_toml=GUNZIP_TOML if needs_gunzip else "",
        compilation_profile=CompilationProfile.OPTIMIZED,
        runtime_config=RuntimeConfig(
            provisioning_timeout_secs=60,
            resources=config.resources,
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
        ),
    ).create_or_replace()


# Exact q4/q5 outputs for the 1M dataset (file_0001), identical under both
# VARIANT representations.
EXPECTED_1M = {
    "q4": [
        {
            "user_id": "did:plc:yj3sjq3blzpynh27cumnp5ks",
            "first_post_ts": 1732206349000167,
        },
        {
            "user_id": "did:plc:l5o3qjrmfztir54cpwlv2eme",
            "first_post_ts": 1732206349001905,
        },
        {
            "user_id": "did:plc:s4bwqchfzm6gjqfeb6mexgbu",
            "first_post_ts": 1732206349003907,
        },
    ],
    "q5": [
        {
            "user_id": "did:plc:tsyymlun4eqjuw7hqrhmwagd",
            "activity_span_ms": 813006,
        },
        {
            "user_id": "did:plc:3ug235sfy2pz7cawmpsftb65",
            "activity_span_ms": 811602,
        },
        {
            "user_id": "did:plc:doxhhgtxqiv47tmcovpbcqai",
            "activity_span_ms": 811404,
        },
    ],
}


def validate_views(pipeline, views: List[ViewSpec], num_files: int):
    """q1 must partition the input; q4/q5 must match the known 1M outputs."""
    table_rows = next(pipeline.query("select count(*) as cnt from bluesky"))["cnt"]
    q1_rows = list(pipeline.query("select * from q1 order by count desc"))
    log(f"q1 (event distribution over {table_rows} rows):")
    for row in q1_rows:
        log(f"  {row}")
    q1_total = sum(row["count"] for row in q1_rows)
    if q1_total != table_rows:
        raise AssertionError(
            f"q1 counts sum to {q1_total}, but bluesky has {table_rows} rows"
        )

    for view in views:
        rows = list(pipeline.query(f"select count(*) as cnt from {view.name}"))
        log(f"View {view.name}: {rows[0]['cnt']} rows")

    for name in ("q4", "q5"):
        rows = sorted(
            pipeline.query(f"select * from {name}"), key=lambda row: row["user_id"]
        )
        log(f"{name}: {rows}")
        if num_files == 1:
            expected = sorted(EXPECTED_1M[name], key=lambda row: row["user_id"])
            if rows != expected:
                raise AssertionError(f"{name} produced {rows}, expected {expected}")
        elif len(rows) != 3:
            raise AssertionError(f"{name} must produce 3 rows, got {len(rows)}")


def jsonbench_test(config: JSONBenchConfig):
    views = jsonbench_views()

    variant_label = {None: "default", 1: "variant1", 2: "flat_variant"}[config.variant]
    pipeline = build_jsonbench_pipeline(
        unique_pipeline_name(f"jsonbench-{variant_label}"), config, views
    )

    try:
        pipeline.start()
        start_time = time.monotonic()

        # check_end_of_input() over an empty endpoint list is vacuously true;
        # wait for the connectors to register before waiting for their eoi.
        deadline = time.monotonic() + 60
        while not pipeline.stats().inputs:
            if time.monotonic() > deadline:
                raise TimeoutError("input endpoints did not register")
            time.sleep(0.1)

        if config.mode == "transaction":
            pipeline.start_transaction()
            wait_end_of_input(pipeline, timeout_s=3600)
            ingest_elapsed = time.monotonic() - start_time
            commit_start = time.monotonic()
            pipeline.commit_transaction(transaction_id=None, wait=True, timeout_s=None)
            pipeline.wait_for_completion(force_stop=False, timeout_s=3600)
            commit_elapsed = time.monotonic() - commit_start
        else:
            pipeline.wait_for_completion(force_stop=False, timeout_s=3600)
            ingest_elapsed = time.monotonic() - start_time
            commit_elapsed = 0.0

        check_for_endpoint_errors(pipeline)

        # In transaction mode the circuit processes most of the data at
        # commit, so the variant-dependent work shows up there; total is the
        # number to compare.
        total_elapsed = ingest_elapsed + commit_elapsed
        processed = number_of_processed_records(pipeline)
        log(
            f"JSONBench [{variant_label}, {config.input_mode}]: {processed} records, "
            f"ingest {ingest_elapsed:.1f}s + commit {commit_elapsed:.1f}s = "
            f"{total_elapsed:.1f}s ({processed / total_elapsed:,.0f} records/s)"
        )

        validate_views(pipeline, views, config.num_files)
    finally:
        try:
            pipeline.stop(force=True)
            pipeline.clear_storage()
        except Exception as e:
            log(f"Error during pipeline cleanup: {e}")


def run_cli():
    parser = argparse.ArgumentParser(description="JSONBench for Feldera")
    parser.add_argument(
        "--mode",
        default="transaction",
        choices=["transaction", "stream"],
        help="'transaction' (default) ingests all data in a single transaction; "
        "'stream' ingests without a transaction.",
    )
    parser.add_argument(
        "--input-mode",
        default="s3",
        choices=["s3", "file"],
        help="'s3' (default) reads the gzip-compressed files directly from the "
        "benchmark's public bucket through the s3 connector with a gunzip "
        "preprocessor; 'file' downloads and decompresses them into a local "
        "cache and reads them with the file connector.",
    )
    parser.add_argument(
        "--variant",
        type=int,
        choices=[1, 2],
        default=None,
        help="VARIANT runtime representation: 1 pins the enum Variant, 2 pins the "
        "FlatVariant. Default: whatever the pipeline manager defaults to.",
    )
    parser.add_argument(
        "--files",
        type=int,
        default=1,
        help="Number of 1M-row benchmark files to ingest (1-1000). Default: 1.",
    )
    parser.add_argument(
        "--cache-dir",
        default=DEFAULT_CACHE_DIR,
        help=f"Local dataset cache directory for --input-mode file. "
        f"Default: {DEFAULT_CACHE_DIR}.",
    )
    parser.add_argument("--memory-mb", type=int, default=None, help="Memory size in MB")
    args = parser.parse_args()

    resources = Resources(
        memory_mb_max=args.memory_mb,
        memory_mb_min=args.memory_mb,
    )
    config = JSONBenchConfig(
        mode=args.mode,
        input_mode=args.input_mode,
        variant=args.variant,
        num_files=args.files,
        cache_dir=args.cache_dir,
        resources=resources,
    )
    jsonbench_test(config)


class TestJSONBench(unittest.TestCase):
    def test_jsonbench_1m(self):
        jsonbench_test(JSONBenchConfig())


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_cli()
    else:
        unittest.main()
