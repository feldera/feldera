#!/usr/bin/env python3
"""Push CDC data from S3 to a running Feldera pipeline and measure processing time.

Usage:
    # Push one hour of CDC data (all 4 tables pushed to Feldera in parallel)
    python push_changes.py --hour 2025-11-30T00

Options:
    --pipeline      Pipeline name (default: ecommerce-medallion-architecture)
    --feldera       Feldera URL (default: from FELDERA_URL env or http://localhost:8080)
    --chunk-input   Split each table's payload into sub-nginx-limit chunks before pushing

The CDC data is read from a public S3 bucket using anonymous (unsigned) access,
so no AWS account or credentials are required. Set FELDERA_S3_SIGNED=1 to use
normal AWS credentials instead (e.g. when pointing at a private bucket).
"""

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from dotenv import load_dotenv
from feldera import FelderaClient

load_dotenv()

CDC_TABLES = ["orders", "order_items", "clickstream_events", "inventory_events"]

# nginx default client_max_body_size is 1MB; chunk below that
MAX_CHUNK_BYTES = 900_000

# Feldera's documented end-to-end latency metric, exposed as a Prometheus
# histogram on /metrics, per input connector:
#   "End-to-end latency, from ingestion of a batch by the input connector until
#    all resulting outputs have been sent to all sinks."
# https://docs.feldera.com/pipelines/latency/#latency-metrics
# We read this instead of timing the push from the client, which only sees the
# blocking HTTP round-trip and conflates transport with server-side processing.
COMPLETION_METRIC = "input_connector_completion_latency_seconds"

SCALE_FACTOR = 0.01
S3_BUCKET = "feldera-demos"
S3_PREFIX = f"ecommerce-cdc-{str(SCALE_FACTOR).replace('.', '-')}"
CDC_S3_PREFIX = f"{S3_PREFIX}/cdc"


def read_cdc_file(s3_client, table, hour_str):
    """Read a CDC NDJSON file from S3. Returns (content_str, line_count)."""
    key = f"{CDC_S3_PREFIX}/{table}/{hour_str}.json"
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        content = obj["Body"].read().decode("utf-8").strip()
        line_count = content.count("\n") + 1 if content else 0
        return content, line_count
    except s3_client.exceptions.NoSuchKey:
        return None, 0


def parse_ndjson(content):
    """Parse NDJSON change records into a list of {"insert"/"delete": {...}} dicts."""
    return [json.loads(line) for line in content.split("\n") if line.strip()]


def chunk_records(records, max_bytes=MAX_CHUNK_BYTES):
    """Split a list of change records into chunks whose JSON-array body fits
    under the nginx client_max_body_size limit."""
    chunks = []
    current = []
    current_size = 2  # enclosing "[" and "]"
    for rec in records:
        rec_size = len(json.dumps(rec).encode("utf-8")) + 1  # +1 for the comma
        if current and current_size + rec_size > max_bytes:
            chunks.append(current)
            current = []
            current_size = 2
        current.append(rec)
        current_size += rec_size
    if current:
        chunks.append(current)
    return chunks


def push_table(
    client, pipeline, table, content, count, chunk_input=False, verbose=True, wait=True
):
    """Push one table's NDJSON `content` (split into chunks when chunk_input is set).
    Returns the push elapsed time in seconds."""
    table_name = f"bronze_{table}"
    # Parse the JSON up front, then chunk the list of records. Each chunk is
    # pushed as a JSON array (array=True): the Feldera client's insert_delete
    # validation requires parsed objects, not a raw NDJSON string.
    records = parse_ndjson(content)
    chunks = chunk_records(records) if chunk_input else [records]
    t_push = time.time()
    for chunk in chunks:
        client.push_to_pipeline(
            pipeline_name=pipeline,
            table_name=table_name,
            format="json",
            data=chunk,
            update_format="insert_delete",
            array=True,
            wait=wait,
        )
    push_elapsed = time.time() - t_push

    if verbose:
        chunks_note = f" ({len(chunks)} chunks)" if len(chunks) > 1 else ""
        print(
            f"    {table_name:40s} {count:>6,} rows  →  {push_elapsed:.3f}s{chunks_note}"
        )
    return push_elapsed


def push_hour(
    s3_client, make_client, pipeline, hour_str, chunk_input=False, verbose=True
):
    """Push one hour of CDC data for all tables in parallel — one thread and one
    Feldera client per table — and return (elapsed_seconds, total_rows).

    `elapsed` is the wall-clock to process the whole hourly batch. The per-table
    times reported below are measured concurrently, so they overlap each other and
    are NOT additive — they do not sum to `elapsed`."""
    # Read every table's data up front (S3 reads are not part of the timed push).
    if verbose:
        print(f"  Downloading CDC for {hour_str} from S3 (s3://{S3_BUCKET})...")
    t_read = time.time()
    payloads = []  # (table, content, count)
    for table in CDC_TABLES:
        content, count = read_cdc_file(s3_client, table, hour_str)
        if content:
            payloads.append((table, content, count))

    if not payloads:
        if verbose:
            print(f"  No CDC data for {hour_str}.")
        return 0.0, 0

    if verbose:
        read_rows = sum(count for _, _, count in payloads)
        read_mb = sum(len(content) for _, content, _ in payloads) / 1_000_000
        print(
            f"  Download complete: {read_rows:,} rows / {read_mb:.1f} MB across "
            f"{len(payloads)} table(s) in {time.time() - t_read:.1f}s"
        )

    # One client per thread (the Feldera client isn't assumed thread-safe), built
    # before timing so client setup doesn't count toward the push time.
    clients = [make_client() for _ in payloads]

    if verbose:
        print(f"  Pushing to pipeline '{pipeline}' (all tables in parallel)...")

    def work(client, table, content, count):
        secs = push_table(
            client, pipeline, table, content, count, chunk_input, verbose=False
        )
        return table, count, secs

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=len(payloads)) as executor:
        futures = [
            executor.submit(work, clients[i], table, content, count)
            for i, (table, content, count) in enumerate(payloads)
        ]
        results = [f.result() for f in futures]
    elapsed = time.time() - t0

    total_rows = sum(count for _, count, _ in results)

    if verbose:
        print("  Per-table push latency (concurrent — these overlap):")
        for table, count, secs in results:
            print(f"    {('bronze_' + table):40s} {count:>6,} rows  →  {secs:.3f}s")
        print(
            f"  All tables and views updated in {elapsed:.3f}s "
            f"({total_rows:,} rows across {len(results)} tables)"
        )

    return elapsed, total_rows


def main():
    parser = argparse.ArgumentParser(description="Push CDC data to Feldera pipeline")
    parser.add_argument(
        "--hour", required=True, help="Hour of CDC to push (e.g., 2025-11-30T00)"
    )
    parser.add_argument(
        "--pipeline", default="ecommerce-medallion-architecture", help="Pipeline name"
    )
    parser.add_argument("--feldera", default=None, help="Feldera URL")
    parser.add_argument(
        "--chunk-input",
        action="store_true",
        help=f"Split each table's NDJSON payload into <{MAX_CHUNK_BYTES}-byte chunks before pushing (for nginx body-size limits)",
    )
    args = parser.parse_args()

    feldera_url = args.feldera or os.getenv("FELDERA_URL", "http://localhost:8080")
    feldera_api_key = os.getenv("FELDERA_API_KEY")

    # Factory so each parallel push thread gets its own client (thread-safety).
    def make_client():
        return FelderaClient(url=feldera_url, api_key=feldera_api_key)

    # The demo CDC data lives in a public S3 bucket, so read it anonymously
    # (unsigned) by default — no AWS account or credentials required. Set
    # FELDERA_S3_SIGNED=1 to fall back to normal AWS credentials (e.g. if you
    # repoint S3_BUCKET at a private bucket).
    if os.getenv("FELDERA_S3_SIGNED"):
        s3_client = boto3.client("s3", region_name="us-west-1")
    else:
        s3_client = boto3.client(
            "s3", region_name="us-west-1", config=Config(signature_version=UNSIGNED)
        )

    print(f"\nPushing CDC hour: {args.hour}")
    print("-" * 60)
    push_hour(
        s3_client, make_client, args.pipeline, args.hour, chunk_input=args.chunk_input
    )


if __name__ == "__main__":
    main()
