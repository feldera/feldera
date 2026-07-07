# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 — Ingest CDC into the Bronze Delta tables
# MAGIC
# MAGIC First task of the `compute_silver_gold` Databricks Workflow. It does two things:
# MAGIC
# MAGIC 1. **(optional) `clean_start`** — when the job parameter `clean_start = true`, the
# MAGIC    working bronze Delta tables are deleted and re-seeded from the read-only source
# MAGIC    snapshot at `s3://feldera-demos/ecommerce-cdc-{scale}/snapshot`. The silver and
# MAGIC    gold materialized views are then recomputed by the pipeline refresh step — a
# MAGIC    normal refresh, which Enzyme automatically turns into a full recompute once it
# MAGIC    sees bronze was rewritten.
# MAGIC 2. **CDC ingest** — read the Feldera-format CDC NDJSON file for the single hour
# MAGIC    `hour` and apply it to the working bronze Delta tables on S3. `bronze_orders`
# MAGIC    carries status updates (delete-of-old + insert-of-new), so it is MERGEd by
# MAGIC    `order_id` keeping the latest row; the other three fact tables are append-only
# MAGIC    event streams, so their inserts are simply appended.
# MAGIC
# MAGIC Run the hours in order (`...T00`, `...T01`, ...): each run applies exactly one
# MAGIC hour of change, the same granularity at which `push_changes.py --hour` feeds
# MAGIC Feldera.
# MAGIC
# MAGIC After this notebook finishes, the workflow refreshes the silver/gold materialized
# MAGIC views defined in `silver_gold_pipeline.py`.
# MAGIC
# MAGIC **Feldera equivalent:** there is no separate ingest step — Feldera follows the
# MAGIC bronze Delta tables and propagates each change through the view DAG incrementally.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# CDC spans 7 days x 24 hours (generate_snapshot_cdc.py): 2025-11-30T00 .. 2025-12-06T23.
# Offer those as a picklist, plus "" for a seed-only run (no CDC).
from datetime import datetime, timedelta

CDC_FIRST_DAY = datetime(2025, 11, 30)
CDC_NUM_DAYS = 7
HOUR_CHOICES = [""] + [
    (CDC_FIRST_DAY + timedelta(hours=h)).strftime("%Y-%m-%dT%H")
    for h in range(CDC_NUM_DAYS * 24)
]

dbutils.widgets.dropdown("hour", "", HOUR_CHOICES, "hour (empty = seed only)")
dbutils.widgets.text("scale_factor", "0.01", "scale_factor")
dbutils.widgets.dropdown("clean_start", "false", ["false", "true"], "clean_start")
dbutils.widgets.text("source_bucket", "s3://feldera-demos", "source_bucket")
dbutils.widgets.text("warehouse_bucket", "s3://feldera-demos", "warehouse_bucket")

HOUR = dbutils.widgets.get("hour").strip() or None
SCALE_FACTOR = float(dbutils.widgets.get("scale_factor"))
CLEAN_START = dbutils.widgets.get("clean_start").strip().lower() == "true"
SOURCE_BUCKET = dbutils.widgets.get("source_bucket").strip()
WAREHOUSE_BUCKET = dbutils.widgets.get("warehouse_bucket").strip()

SCALE_STR = str(SCALE_FACTOR).replace(".", "-")

# Read-only source: the snapshot + CDC produced by generate_snapshot_cdc.py.
# SOURCE_BUCKET comes from the source_bucket job parameter (read above).
SOURCE_PREFIX = f"ecommerce-cdc-{SCALE_STR}"
SNAPSHOT_ROOT = f"{SOURCE_BUCKET}/{SOURCE_PREFIX}/snapshot"
CDC_ROOT = f"{SOURCE_BUCKET}/{SOURCE_PREFIX}/cdc"

# Writable working warehouse: the bronze tables this workflow ingests into and the
# silver/gold materialized-view pipeline reads from. Kept separate from the read-only
# source so a clean_start can rebuild it from scratch without touching the source.
# WAREHOUSE_BUCKET comes from the warehouse_bucket job parameter (read above), the same
# value the pipeline reads via spark.conf — single source of truth across both tasks.
WAREHOUSE_PREFIX = f"ecommerce-pipeline-{SCALE_STR}"
BRONZE_ROOT = f"{WAREHOUSE_BUCKET}/{WAREHOUSE_PREFIX}/bronze"


def s3a(path: str) -> str:
    return path.replace("s3://", "s3a://")


print(f"clean_start:  {CLEAN_START}")
print(f"hour:         {HOUR or '(none — seed only)'}")
print(f"scale_factor: {SCALE_FACTOR}")
print(f"source:       {SNAPSHOT_ROOT}")
print(f"cdc:          {CDC_ROOT}")
print(f"bronze (rw):  {BRONZE_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table metadata
# MAGIC
# MAGIC All seven bronze tables are re-seeded on `clean_start`. Only the four fact tables
# MAGIC receive CDC, and they split into two ingest modes:
# MAGIC
# MAGIC - **`merge`** — `bronze_orders` only. Orders go pending -> confirmed -> shipped ->
# MAGIC   delivered; each transition is a delete-of-old + insert-of-new, so the hour's
# MAGIC   inserts are deduped to the latest `updated_at` per `order_id` and MERGEd.
# MAGIC - **`append`** — order items, clickstream and inventory are append-only event
# MAGIC   streams that never update an existing row, so the hour's inserts are appended
# MAGIC   directly (no join against the target, no file rewrites).

# COMMAND ----------

# All bronze tables. Used for the clean_start re-seed (DEEP CLONE from snapshot).
BRONZE_TABLES = [
    "bronze_suppliers",
    "bronze_products",
    "bronze_customers",
    "bronze_orders",
    "bronze_order_items",
    "bronze_clickstream_events",
    "bronze_inventory_events",
]

# Fact tables that stream CDC -> how to apply one hour of change.
#   merge:  upsert keyed on `pk`, keeping the latest `order_by` row per key.
#   append: insert-only; append the hour's new rows.
CDC_TABLES = {
    "bronze_orders": {
        "short": "orders",
        "mode": "merge",
        "pk": "order_id",
        "order_by": "updated_at",
    },
    "bronze_order_items": {"short": "order_items", "mode": "append"},
    "bronze_clickstream_events": {"short": "clickstream_events", "mode": "append"},
    "bronze_inventory_events": {"short": "inventory_events", "mode": "append"},
}


def bronze_path(name: str) -> str:
    return s3a(f"{BRONZE_ROOT}/{name}")


def snapshot_path(name: str) -> str:
    return s3a(f"{SNAPSHOT_ROOT}/{name}")


def bronze_exists(name: str) -> bool:
    """True if a Delta table is present at the working bronze path for `name`
    (checks for its _delta_log). Uses the s3:// form for dbutils.fs."""
    try:
        dbutils.fs.ls(f"{BRONZE_ROOT}/{name}/_delta_log")
        return True
    except Exception:  # noqa: BLE001 — path absent => table not seeded
        return False


# COMMAND ----------

# MAGIC %md
# MAGIC ## clean_start — delete and re-seed bronze from the source snapshot
# MAGIC
# MAGIC Deletes the working bronze path, then copies each bronze table fresh from the
# MAGIC read-only source snapshot via DEEP CLONE. Silver/gold are materialized views owned
# MAGIC by the Lakeflow pipeline (not stored at these warehouse paths), so they aren't
# MAGIC touched here — they recompute on the pipeline refresh step, which Enzyme turns into
# MAGIC a full recompute once it sees bronze was rewritten.

# COMMAND ----------

if CLEAN_START:
    print("clean_start = true -> deleting bronze re-seed")
    layer_path = f"{WAREHOUSE_BUCKET}/{WAREHOUSE_PREFIX}/bronze"
    try:
        dbutils.fs.rm(layer_path, recurse=True)
        print(f"  deleted {layer_path}")
    except Exception as e:  # noqa: BLE001 — best-effort delete of a possibly-absent path
        print(f"  skip {layer_path}: {e}")

    for name in BRONZE_TABLES:
        src = snapshot_path(name)
        dst = bronze_path(name)
        # DEEP CLONE copies the source data files directly (and their Delta stats)
        # instead of reading every row back through Spark and re-encoding it on write.
        # The row count comes from the operation metrics, so there is no extra scan.
        # Enable row tracking on the working bronze: the silver/gold pipeline's incremental
        # materialized views require row tracking on their source tables, else the refresh
        # fails with ROW_TRACKING_NOT_ENABLED. Set on create so the later MERGE/append
        # writes preserve it.
        res = spark.sql(
            f"CREATE OR REPLACE TABLE delta.`{dst}` "
            f"DEEP CLONE delta.`{src}` "
            "TBLPROPERTIES ('delta.enableRowTracking' = 'true')"
        ).collect()
        copied = res[0]["num_copied_files"] if res else 0
        print(f"  re-seeded {name}: cloned {copied} data files")
    print(
        "clean_start complete — bronze tables are a fresh copy of the source snapshot"
    )
else:
    print("clean_start = false -> keeping existing bronze tables, applying CDC on top")
    # clean_start=false means we apply CDC on top of existing bronze, so both an hour to
    # ingest and pre-seeded bronze tables are required. Fail fast with a clear message
    # rather than no-op'ing here and erroring later in the pipeline's bronze reads.
    if HOUR is None:
        raise ValueError(
            "hour is required when clean_start=false (nothing to ingest otherwise). "
            "Pass hour=YYYY-MM-DDThh, or set clean_start=true to seed bronze first."
        )
    missing = [t for t in BRONZE_TABLES if not bronze_exists(t)]
    if missing:
        raise FileNotFoundError(
            f"Bronze tables missing under {BRONZE_ROOT}: {missing}. "
            "Run once with clean_start=true to seed bronze from the source snapshot "
            "before applying CDC."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC ingest — apply one hour of Feldera-format change records to bronze
# MAGIC
# MAGIC Each CDC line is `{"insert": {...}}` / `{"delete": {...}}`
# MAGIC ([Feldera JSON format](https://docs.feldera.com/formats/json)); a status update is
# MAGIC a delete of the old row followed by an insert of the new one. We keep the inserted
# MAGIC state (the new/updated row) and drop the deleted state — an update re-inserts the
# MAGIC new row, which wins via the orders MERGE key, so the inserts alone reconstruct the
# MAGIC current table. Only one hour's file (`{hour}.json`) is read per table.

# COMMAND ----------


def read_hour_inserts(table_short_name, target_schema, hour):
    """Read the single CDC file for `hour` and return its inserted row state, cast to
    the target (bronze) schema. Returns None if that hour has no file for this table."""
    path = s3a(f"{CDC_ROOT}/{table_short_name}/{hour}.json")
    try:
        # spark.read.json on a missing path throws; a present-but-empty file yields no
        # rows. Either way we treat it as "no change this hour".
        raw = spark.read.json(path)
    except Exception:  # noqa: BLE001 — no file for this hour/table
        return None
    if "insert" not in raw.columns:
        return None

    records = raw.filter(F.col("insert").isNotNull()).select("insert.*")
    if not records.head(1):
        return None

    # JSON serializes decimals/timestamps as strings — cast back to the bronze schema.
    for field in target_schema.fields:
        if field.name in records.columns:
            records = records.withColumn(
                field.name, F.col(field.name).cast(field.dataType)
            )

    return records.select([f.name for f in target_schema.fields])


def ingest_hour(table_name, hour):
    """Apply one hour of CDC to `table_name`: MERGE for orders, append otherwise."""
    cfg = CDC_TABLES[table_name]
    dst = bronze_path(table_name)
    target_schema = spark.read.format("delta").load(dst).schema

    cdc = read_hour_inserts(cfg["short"], target_schema, hour)
    if cdc is None:
        print(f"  {table_name}: no CDC for {hour} — unchanged")
        return

    if cfg["mode"] == "append":
        # Insert-only stream: append the hour's rows. No join against the target and no
        # file rewrites — the cheapest possible Delta write.
        cdc.write.format("delta").mode("append").save(dst)
        print(f"  {table_name}: appended {cdc.count():,} rows")
        return

    # merge mode (orders): the hour may carry several transitions for one order_id;
    # keep the latest updated_at, then upsert. MERGE returns a metrics row, so the
    # counts come for free without a second pass over the source DAG.
    pk, order_by = cfg["pk"], cfg["order_by"]
    w = Window.partitionBy(pk).orderBy(F.col(order_by).desc())
    latest = (
        cdc.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    latest.createOrReplaceTempView("cdc_src")
    metrics = (
        spark.sql(f"""
        MERGE INTO delta.`{dst}` AS t
        USING cdc_src AS s
        ON t.{pk} = s.{pk}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
        .collect()[0]
        .asDict()
    )
    print(
        f"  {table_name}: {metrics.get('num_inserted_rows', 0):,} inserted, "
        f"{metrics.get('num_updated_rows', 0):,} updated (keyed on {pk})"
    )


# COMMAND ----------

if HOUR is None:
    print("hour is empty — skipping CDC ingest (bronze left at its seeded state).")
else:
    print(f"Applying CDC hour {HOUR} to bronze...")
    for table_name in CDC_TABLES:
        ingest_hour(table_name, HOUR)
    print("Bronze ingest complete. Silver/gold materialized views refresh next.")
