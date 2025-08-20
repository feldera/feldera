import time
from util import build_pipeline, validate_outputs

# Add now() value to a table in a transaction.

INPUT_RECORDS = 5000000

tables = {
    "t": f"""
    create table t(
        id bigint not null primary key,
        company_id bigint,
        name string
    ) with (
    'materialized' = 'true',
    'connectors' = '[{{
        "name": "datagen",
        "transport": {{
            "name": "datagen",
            "config": {{
                "plan": [{{
                    "limit": {INPUT_RECORDS},
                    "fields": {{
                        "name": {{ "strategy": "sentence" }}
                    }}
                }}]
            }}
        }}
    }}]');
    """
}

views = {
    # Have a deterministic now() for testing against datafusion.
    "v_now": """
    select
        now() as now_ts
    """,
    "v": """
    select
        t.*,
        v_now.now_ts as now_ts
    from t, v_now
    """,
}

pipeline = build_pipeline("now-test", tables, views)

pipeline.start()
start_time = time.monotonic()

pipeline.start_transaction()

print(f"inputs: {pipeline.stats().inputs}")

while (
    pipeline.stats().global_metrics.total_input_records < INPUT_RECORDS
    or pipeline.stats().global_metrics.buffered_input_records > 0
):
    print(f"Waiting for {INPUT_RECORDS} records to be ingested...")
    time.sleep(1)

elapsed = time.monotonic() - start_time
print(f"Data ingested in {elapsed}")

# Freeze the value of now().
pipeline.pause()
pipeline.wait_for_idle()

start_time = time.monotonic()
pipeline.commit_transaction(transaction_id=None, wait=True, timeout_s=600)

elapsed = time.monotonic() - start_time
print(f"Commit took {elapsed}")

# Don't validate v_now which depends on the real-time clock.
views.pop("v_now")
validate_outputs(pipeline, tables, views)

# Process more clock ticks
pipeline.resume()
time.sleep(5)

validate_outputs(pipeline, tables, views)

assert next(pipeline.query("select count(*) as cnt from v_now"))["cnt"] == 1

pipeline.stop(force=True)
