import simplejson as json
import os
import re
import time
from feldera.enums import CompilationProfile
from feldera.pipeline import Pipeline
from feldera.pipeline_builder import PipelineBuilder
from feldera.rest.feldera_client import FelderaClient
from feldera.runtime_config import RuntimeConfig


API_KEY = os.environ.get("FELDERA_API_KEY")
BASE_URL = (
    os.environ.get("FELDERA_BASE_URL")  # deprecated
    or os.environ.get("FELDERA_HOST")
    or "http://localhost:8080"
)
TEST_CLIENT = FelderaClient(BASE_URL, api_key=API_KEY)


def datafusionize(query: str) -> str:
    sort_array_pattern = re.compile(re.escape("SORT_ARRAY"), re.IGNORECASE)
    truncate_pattern = re.compile(re.escape("TRUNCATE"), re.IGNORECASE)
    result = sort_array_pattern.sub("array_sort", query)
    result = truncate_pattern.sub("trunc", query)
    return result


def validate_view(pipeline: Pipeline, view_name: str, view_query: str):
    print(f"Validating view '{view_name}'")

    # TODO: count records
    view_query = datafusionize(view_query)
    # print(f"query: {view_query}")

    extra_rows = list(
        pipeline.query(f"(select * from {view_name}) except ({view_query})")
    )
    missing_rows = list(
        pipeline.query(f"({view_query}) except (select * from {view_name})")
    )

    if extra_rows:
        print("Extra rows in Feldera output, but not in the ad hoc query output")
        print(json.dumps(extra_rows))

    if missing_rows:
        print("Extra rows in the ad hoc query output, but not in Feldera output")
        print(json.dumps(missing_rows))

    if extra_rows or missing_rows:
        raise AssertionError(f"Validation failed for view {view_name}")


def run_pipeline(pipeline_name: str, tables: dict, views: dict):
    sql = ""

    for table_sql in tables.values():
        sql += f"{table_sql}\n"

    for view_name, view_query in views.items():
        sql += f"create materialized view {view_name} as {view_query};\n\n"

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        compilation_profile=CompilationProfile.OPTIMIZED,
        runtime_config=RuntimeConfig(provisioning_timeout_secs=60),
    ).create_or_replace()

    pipeline.start()
    start_time = time.monotonic()

    pipeline.start_transaction()

    pipeline.wait_for_completion(force_stop=False, timeout_s=3600)
    elapsed = time.monotonic() - start_time
    print(f"Data ingested in {elapsed}")

    start_time = time.monotonic()
    pipeline.commit_transaction(transaction_id=None, wait=True, timeout_s=None)

    elapsed = time.monotonic() - start_time
    print(f"Commit took {elapsed}")

    for table in tables.keys():
        row_count = list(pipeline.query(f"select count(*) from {table}"))
        print(f"Table '{table}' count(*):\n{row_count}")

    for view_name, view_query in views.items():
        validate_view(pipeline, view_name, view_query)

    pipeline.stop(force=True)
