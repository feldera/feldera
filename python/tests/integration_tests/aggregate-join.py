import json
import os
import re
import time
from feldera.enums import CompilationProfile
from feldera.pipeline import Pipeline
from feldera.pipeline_builder import PipelineBuilder
from feldera.rest.feldera_client import FelderaClient
from feldera.runtime_config import RuntimeConfig

BASE_URL = os.environ.get("FELDERA_BASE_URL", "http://localhost:8080")
TEST_CLIENT = FelderaClient(BASE_URL)

def datafusionize(query: str) -> str:
    pattern = re.compile(re.escape("SORT_ARRAY"), re.IGNORECASE)
    return pattern.sub("array_sort", query)

def validate_view(pipeline: Pipeline, view_name: str, view_query: str):
    print(f"Validating view {view_name}")

    # TODO: count records
    view_query = datafusionize(view_query)
    # print(f"query: {view_query}")

    extra_rows = list(pipeline.query(f"(select * from {view_name}) except ({view_query})"))
    missing_rows = list(pipeline.query(f"({view_query}) except (select * from {view_name})"))

    if extra_rows:
        print("Extra rows in Feldera output, but not in the ad hoc query output")
        print(json.dumps(extra_rows))

    if missing_rows:
        print("Extra rows in the ad hoc query output, but not in Feldera output")
        print(json.dumps(missing_rows))

    if extra_rows or missing_rows:
        raise AssertionError(f"Validation failed for view {view_name}")

def run_pipeline(pipeline_name: str, tables_sql: str, tables: list[str], views: dict):
    sql = tables_sql
    for view_name, view_query in views.items():
        sql += f"\ncreate materialized view {view_name} as\n{view_query};"

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
    pipeline.commit_transaction(transaction_id = None, wait = True, timeout_s = None)

    elapsed = time.monotonic() - start_time
    print(f"Commit took {elapsed}")

    for table in tables:
        row_count = list(pipeline.query(f"select count(*) from {table}"))
        print(f"Table {table} count(*):\n{row_count}")

    for view_name, view_query in views.items():
        validate_view(pipeline, view_name, view_query)

    pipeline.stop(force=True)


t1 = """
create table t1(
    id bigint not null primary key,
    group_id bigint,
    s string
) with (
  'materialized' = 'true',
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 10000000,
            "fields": {
                "group_id": { "range": [1, 10000] },
                "s": { "strategy": "word" }
            }
        }]
      }
    }
  }]');
"""

t2 = """
create table t2(
    id bigint not null primary key,
    group_id bigint,
    s string
) with (
  'materialized' = 'true',
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 10000000,
            "fields": {
                "group_id": { "range": [1, 10000] },
                "s": { "strategy": "word" }
            }
        }]
      }
    }
  }]');
"""

q_t1_aggregate = """
select
    group_id,
    count(*) as cnt,
    SORT_ARRAY(array_agg(s)) as arr
from t1
group by group_id
"""

q_t2_aggregate = """
select
    group_id,
    count(*) as cnt,
    SORT_ARRAY(array_agg(s)) as arr
from t2
group by group_id
"""

q_result = """
select
    t1_aggregate.group_id,
    t1_aggregate.cnt as cnt1,
    t1_aggregate.arr as arr1,
    t2_aggregate.cnt as cnt2,
    t2_aggregate.arr as arr2
from
    t1_aggregate join t2_aggregate
on
    t1_aggregate.group_id = t2_aggregate.group_id
"""

tables_sql = f"""
{t1}
{t2}
"""

run_pipeline("aggregate-join-test", tables_sql, ["t1", "t2"], {"t1_aggregate": q_t1_aggregate, "t2_aggregate": q_t2_aggregate, "result": q_result})