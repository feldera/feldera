# Measures end-to-end latency for the Delta Lake input connectors using the
# completed_frontier timestamps exposed on /stats.
# See https://docs.feldera.com/pipelines/latency/#measuring-end-to-end-latency-with-input-frontiers
#
# For each Delta connector the frontier reports three RFC 3339 timestamps:
#   ingested_at  -- data for this frontier arrived at the connector
#   processed_at -- the IVM engine finished processing it
#   completed_at -- outputs reached all sinks
# Latency is the delta between these, plus freshness = now - completed_at.
import os
from argparse import ArgumentParser
from datetime import datetime, timezone

from dotenv import load_dotenv
from feldera import FelderaClient

PIPELINE = "ecommerce-medallion-architecture"

# Bronze tables to report on (user-facing names map to bronze_<name>).
TABLES = [
    "bronze_clickstream_events",
    "bronze_inventory_events",
    "bronze_orders",
    "bronze_order_items",
]


def parse_ts(value: str) -> datetime:
    # Frontier timestamps are RFC 3339 with a trailing Z; normalize for fromisoformat.
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def delta_connector(inputs: list[dict], table: str) -> dict | None:
    # A Delta table has both an api-ingress and a delta_table_input connector on
    # the same stream; only the Delta one populates completed_frontier.
    for i in inputs:
        if i["endpoint_name"].startswith(f"{table}.") and i.get("completed_frontier"):
            return i
    return None


def report(client: FelderaClient) -> None:
    stats = client.get_pipeline_stats(PIPELINE)
    now = datetime.now(timezone.utc)

    header = (
        f"{'table':<26}{'version':>8}{'ingest→proc':>14}{'proc→done':>12}{'e2e':>10}"
    )
    print(header)
    print("-" * len(header))

    for table in TABLES:
        conn = delta_connector(stats["inputs"], table)
        if conn is None:
            print(f"{table:<26}{'no delta frontier yet':>56}")
            continue

        f = conn["completed_frontier"]
        version = f.get("metadata", {}).get("version")
        ingested = parse_ts(f["ingested_at"])
        processed = parse_ts(f["processed_at"])
        completed = parse_ts(f["completed_at"])

        ingest_to_proc = (processed - ingested).total_seconds()
        proc_to_done = (completed - processed).total_seconds()
        e2e = (completed - ingested).total_seconds()

        print(
            f"{table:<26}{version:>8}"
            f"{ingest_to_proc:>13.2f}s{proc_to_done:>11.2f}s"
            f"{e2e:>9.2f}s"
        )


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Measure Delta input latency via /stats frontiers"
    )
    parser.add_argument("--pipeline", default=PIPELINE)
    args = parser.parse_args()
    PIPELINE = args.pipeline

    load_dotenv()
    client = FelderaClient(
        url=os.getenv("FELDERA_URL"), api_key=os.getenv("FELDERA_API_KEY")
    )
    report(client)
