# Part 4: Compare Feldera and Databricks Lakeflow Declarative Pipelines (Enzyme)

Part [1](./part1.md) deploys the Feldera pipeline, which reads from a public bucket. [Part 2](./part2.md) demonstrates change data processing by pushing data to the Feldera pipeline via HTTP. [Part 3](./part3.md) compares Feldera and Spark, showcasing the difference between incremental view maintenance and batch refreshes. Part 4 compares Feldera with Databricks Enzyme engine. In this section, you will deploy the Databricks assets into **your own** workspace
and data in **your own** S3 bucket, then repoint the Feldera pipeline at your bucket bucket. Two things
run against the same Bronze Delta tables:

- A **Lakeflow Declarative Pipeline** powered by Databricks Enzyme engine, maintains the Silver and Gold layers as Databricks
  materialized views. Lakeflow Declarative Pipelines are the Databricks implementation of Incremental View Maintenance. The Declarative Pipeline uses the same SQL as the Feldera Pipeline. To fairly compare what Databricks **can** incrementalize, the [`refresh_policy`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy) for views in the pipeline is set to `incremental` where possible. Databricks currently cannot incrementalize the `gold_product_demand_surge` view, so the policy for this view is `auto`.
- An **`ingest_bronze` job** acts as a **data-generating process**: each run writes one
  hour of CDC into the Bronze Delta tables. Point Feldera at those same tables and it
  follows the Delta log, updating Gold sub-second after each write. The
  Databricks pipeline runs immediately after ingestion.

`ingest_bronze` stands in for a real operational source continually landing data. Instead
of `push_changes.py` pushing JSON to Feldera's HTTP ingress (Part 2), the writer here is a
Databricks job writing to Delta on S3, and Feldera consumes those Delta commits directly.

## Files

All four assets live in
[`medallion_architecture/databricks/`](https://github.com/feldera/feldera/tree/main/docs.feldera.com/docs/use_cases/medallion_architecture/databricks).

| File | Role |
|---|---|
| `silver_gold_pipeline.py` | Pipeline source notebook — 8 Silver + 7 Gold materialized views (Bronze exposed as views). |
| `pipeline.yaml` | Settings for the Lakeflow pipeline that runs `silver_gold_pipeline.py` (Step 1). |
| `ingest_bronze.py` | Job notebook — optional `clean_start` re-seed, then apply one hour of CDC to the Bronze Delta tables. |
| `job.yaml` | Settings for the `compute_silver_gold` job (Step 2). |
| `measure_latency.py` | Reports Feldera's per-connector processing and end-to-end latency for changes (Step 4). |

## Prerequisites
To run this demo yourself, you will need:

- A Databricks workspace with **Unity Catalog** and **serverless** enabled.
- Read access to the read-only source bucket `s3://feldera-demos/ecommerce-cdc-{scale}`
  (the snapshot and CDC files, anonymous read in `us-west-1`).
- **A writable S3 bucket you provision** for the working Bronze tables. The Databricks
  job writes here and the Feldera pipeline reads from here, so both Databricks and Feldera
  need access to it. This bucket is yours — you cannot write to `s3://feldera-demos`.

## Storage layout

| Layer | Location | Written by |
|---|---|---|
| Source snapshot + CDC (read-only) | `s3://feldera-demos/ecommerce-cdc-{scale}/{snapshot,cdc}` ||
| Working Bronze (read/write) | `s3://<your-bucket>/ecommerce-pipeline-{scale}/bronze` | `ingest_bronze.py` |
| Silver / Gold materialized views | Unity Catalog `{catalog}.{schema}` | the Lakeflow pipeline, `silver_gold_pipeline.py`|

`{scale}` is the `scale_factor` with the dot replaced by a dash, e.g. `0.01` → `0-01`. The
working Bronze is kept separate from the read-only source so a `clean_start` can rebuild it
from scratch without touching the source snapshot.

## Step 1 — Create the Lakeflow pipeline

Create the pipeline first: the job's second task references it by ID.

1. **Import the pipeline source.** Workspace → your folder → **Import** → add
   `silver_gold_pipeline.py`. It imports as a notebook (the `# Databricks notebook source`
   header makes the `# COMMAND ----------` cells render).
2. **Create the pipeline.** Sidebar → **Jobs & Pipelines** → **Create → ETL pipeline**
   (Lakeflow Declarative Pipeline):
   - **Source code**: the imported `silver_gold_pipeline.py`.
   - **Serverless**: on.
   - **Destination**: Unity Catalog → set **catalog** and **schema** where the Silver/Gold
     materialized views publish (e.g. `main` / `ecommerce_demo`).
   - **Advanced → Configuration**: add the keys the notebook reads via `spark.conf`:
     - `scale_factor` = `0.01`
     - `warehouse_bucket` = `s3://<your-bucket>`
   - The full settings are in [**`pipeline.yaml`**](https://github.com/feldera/feldera/tree/main/docs.feldera.com/docs/use_cases/medallion_architecture/databricks/pipeline.yaml) (paste into *Settings → YAML*, or
     `POST /api/2.0/pipelines`, instead of filling the form by hand).
3. **Save and copy the pipeline ID** (Pipeline details, or the URL) — Step 2 needs it. You
   don't have to run it standalone; the job triggers it.

## Step 2 — Upload `ingest_bronze.py` and create the job

1. **Import the ingest notebook.** Workspace → **Import** → add `ingest_bronze.py`.
2. **Create the job.** Sidebar → **Jobs & Pipelines** → **Create job**, name it
   `compute_silver_gold`. Full settings are in [**`job.yaml`**](https://github.com/feldera/feldera/tree/main/docs.feldera.com/docs/use_cases/medallion_architecture/databricks/job.yaml) (or use *Edit as YAML* / the
   jobs API). Configure:
   - **Job parameters**: `hour` = `""`, `clean_start` = `false`, `scale_factor` = `0.01`,
     `source_bucket` = `s3://feldera-demos`, `warehouse_bucket` = `s3://<your-bucket>`.
   - **Task 1 — `ingest_bronze`** (Notebook): select `ingest_bronze.py`, compute
     **Serverless**. Under the task **Parameters**, map each to the job parameter:
     `hour` = `{{job.parameters.hour}}`, `clean_start` = `{{job.parameters.clean_start}}`,
     `scale_factor` = `{{job.parameters.scale_factor}}`,
     `source_bucket` = `{{job.parameters.source_bucket}}`,
     `warehouse_bucket` = `{{job.parameters.warehouse_bucket}}`.
   - **Task 2 — `refresh_silver_gold`** (Pipeline): **Depends on** `ingest_bronze`; select
     the pipeline from Step 1 (paste its **pipeline ID** into `job.yaml`'s
     `pipeline_task.pipeline_id`). Full refresh off.

`scale_factor` and `warehouse_bucket` appear in both the pipeline configuration (Step 1)
and the job parameters (Step 2). The job passes them to the ingest notebook and, because
the keys match the pipeline's configuration, overrides the pipeline's values at run time —
so keep the two in sync when you change them.

## Step 3 — `ingest_bronze` as a data-generating process

Each run of the `ingest_bronze` task writes one hour of change into the Bronze Delta tables
under `s3://<your-bucket>/ecommerce-pipeline-{scale}/bronze`:

- `bronze_orders` carries status updates (delete-of-old + insert-of-new), so the hour's
  rows are deduped to the latest `updated_at` per `order_id` and **merged**.
- `bronze_order_items`, `bronze_clickstream_events`, and `bronze_inventory_events` are
  append-only event streams, so the hour's rows are **appended**.

Every run commits new versions to the Delta log. That log is exactly what Feldera follows.

**Run now → Run with parameters:**

- **First run** — seed Bronze from the source snapshot into your bucket, then the
  materialized views compute on the refresh step:
  `clean_start = true`.
- **Each later run** — write ONE hour of CDC, in order:
  `hour = 2025-11-30T00` (`clean_start = false`), then `2025-11-30T01`, and so on.

The hours available are 7 days × 24, starting with `2025-11-30T00`. Run hours in order, once each — see [Notes](#notes).

## Step 4 — Point the Feldera pipeline at your bucket

The Feldera pipeline from Part 1 reads the Bronze snapshot from the public demo bucket in
`snapshot` mode. Repoint it at the Bronze tables that `ingest_bronze` writes and switch to
`snapshot_and_follow`, so Feldera loads the seeded snapshot and then follows the Delta log
as each `ingest_bronze` run commits a new hour. See the
[Delta input connector](/connectors/sources/delta) reference for the mode options.

Edit each Bronze table's connector in the pipeline SQL. For the four fact tables that
receive CDC, change the `uri` to your bucket and set `mode` to `snapshot_and_follow`:

```json
{
  "transport": {
    "name": "delta_table_input",
    "config": {
      "uri": "s3://<your-bucket>/ecommerce-pipeline-0-01/bronze/bronze_orders",
      "mode": "snapshot_and_follow",
      "aws_region": "<your-bucket-region>",
      "transaction_mode": "catchup"
    }
  }
}
```

| Bronze table | Mode | Why |
|---|---|---|
| `bronze_orders`, `bronze_order_items`, `bronze_clickstream_events`, `bronze_inventory_events` | `snapshot_and_follow` | Receive CDC every `ingest_bronze` run — follow the Delta log. |
| `bronze_suppliers`, `bronze_products`, `bronze_customers` | `snapshot` | Dimensions, seeded once and not updated by CDC. |

Point every table's `uri` at `s3://<your-bucket>/ecommerce-pipeline-0-01/bronze/<table>`
(the `0-01` segment is `scale_factor` `0.01` with the dot replaced by a dash).

### Configure AWS authentication for a private bucket

The demo bucket is public, so Parts 1–3 use `"aws_skip_signature": "true"`. If your
bucket is private, **drop `aws_skip_signature`** and supply credentials instead. The
simplest option is an access key on the connector config (`aws_region` is required — the
Delta library does not auto-detect it):

```json
"aws_access_key_id": "<AWS_ACCESS_KEY_ID>",
"aws_secret_access_key": "<AWS_SECRET_ACCESS_KEY>",
"aws_region": "<your-bucket-region>"
```

For all supported options — access keys, session tokens, instance/container roles, KMS
encryption, custom endpoints — see
[configuring AWS authentication for the Delta connector](/connectors/sources/delta#storage-parameters)
and the [Setting AWS credentials example](/connectors/sources/delta#example-setting-aws-credentials).

### Run order

1. Run `ingest_bronze` once with `clean_start = true` (Step 3, first run) to seed Bronze in
   your bucket. Feldera's `snapshot_and_follow` needs the snapshot to exist before it
   starts.
2. Start (or restart) the Feldera pipeline. It backfills the seeded snapshot, then begins
   following the Delta log.
3. Run `ingest_bronze` for each later hour. As each run commits, Feldera ingests the change
   and updates Gold sub-second. Watch a Gold view from the **Ad-hoc query** or
   **Change stream** tab, exactly as in [Part 2](./part2.md#watch-a-gold-view-update-in-real-time).
4. Measure the latency. Run `measure_latency.py` against the pipeline to see, per Bronze
   Delta connector, how long Feldera took to ingest and fully process the latest Delta
   commit (see [Result: Feldera's latency](#result-feldera) below):

   ```bash
   uv pip install feldera python-dotenv
   # or
   pip install feldera python-dotenv
   ```

   Then run:

   ```bash
   uv run measure_latency.py --pipeline ecommerce-medallion-architecture
   # or
   python measure_latency.py --pipeline ecommerce-medallion-architecture
   ```

## Notes {#notes}

- **Run hours in order, once each.** The append-only tables use plain appends (the cheapest
  Delta write, and what keeps them insert-only), so re-running an already-applied hour
  double-inserts into them. `bronze_orders` is idempotent (MERGE). For replayable hours,
  switch the append tables to an idempotent MERGE on their primary key or use a
  `txnAppId`/`txnVersion` marker on the append.
- **`scale_factor` and `warehouse_bucket` are set in two places** — the job parameters
  (drive the Bronze S3 path in `ingest_bronze.py`) and the pipeline `configuration` (drives
  the path the materialized views read from). The job values override the pipeline's at run
  time via matching keys; keep the defaults in sync.
- **Databricks incremental refresh is best-effort.** Databricks (Enzyme) incrementalizes a
  materialized view when the query shape allows and otherwise falls back to a full
  recompute — unlike Feldera, which maintains the entire DAG incrementally on every commit.
  That difference is the point of the comparison.

## Result: the Databricks refresh for the first CDC hour

The numbers below are the `refresh_silver_gold` pipeline task rebuilding the Silver and Gold
materialized views defined in `silver_gold_pipeline.py` after the first CDC hour
(`2025-11-30T00`) lands in Bronze. Databricks reports each update as a sequence of phases;
these are the phases for that single refresh:

| Phase | Time | What it is |
|---|---|---|
| Created | 1s | Update queued. |
| Waiting for resources | 8s | Serverless compute provisioning. |
| Initializing | 18s | Pipeline graph + environment setup. |
| Setting up tables | 1s | Reconciling materialized-view metadata. |
| **Running** | **58s** | **Refreshing the 15 Silver/Gold views.** |
| **Total** | **86s** | End-to-end, data-landed to Gold-current. |

Read the phases as two distinct costs:

- **Compute — 58s.** The actual work of refreshing the views for *one hour* of change.
  Work is not proportional to the size of the change. The clean start with full recomputation took about as long as the incremental run. In addition, `gold_product_demand_surge` full
  recomputes every run (see below), and every Lakeflow flow carries fixed per-update
  overhead. This is the same SQL Feldera runs.
- **Freshness — 86s.** What a consumer waits from the moment Bronze receives the hour to
  the moment Gold reflects it. Serverless re-provisions and re-initializes on every
  triggered run, so the 28s of `Waiting for resources` + `Initializing` is paid again each
  time.

Feldera pays neither cost the same way. It is always on, so there is no per-run
provisioning, and it maintains every view incrementally, so the update is proportional to
the size of the change. Updates are reflected in less than a second for one CDC hour, not 58s.

### The view Databricks cannot incrementalize

`gold_product_demand_surge` compares each clickstream event against a trailing 24-hour
window measured from the latest event:

```sql
WHERE ce.event_timestamp >
    (SELECT MAX(event_timestamp) FROM silver_enriched_clickstream) - INTERVAL 1 DAY
```

Databricks' Enzyme incremental planner cannot handle a `SUBQUERY_EXPRESSION` inside a Filter/And operator.
Feldera maintains identical SQL, and any other SQL you might write incrementally.

You can confirm which views incrementalized and which full recomputed in the pipeline's
**event log** (each flow reports its refresh type per update).

## Result: Feldera's latency for the first CDC hour {#result-feldera}

The same first CDC hour (`2025-11-30T00`), measured on the Feldera side. When
`ingest_bronze` commits the hour, Feldera follows the Delta log and propagates the change
through every Silver and Gold view. `measure_latency.py` reads the `completed_frontier`
timestamps each Delta connector exposes on `/stats` and reports three numbers per Bronze
table (see [measuring end-to-end latency with input frontiers](/pipelines/latency/#measuring-end-to-end-latency-with-input-frontiers)):

- **ingest→proc** — from the change arriving at the connector to the IVM engine finishing it.
- **proc→done** — from processing to the outputs reaching all sinks.
- **e2e** — the full ingest-to-sink latency (their sum).

`version` is the Bronze table's Delta version; version `1` is the first CDC commit on top
of the seeded snapshot:

```
table                      version   ingest→proc   proc→done       e2e
----------------------------------------------------------------------
bronze_clickstream_events        1         0.08s       0.05s     0.13s
bronze_inventory_events          1         0.08s       0.05s     0.13s
bronze_orders                    1         0.29s       0.08s     0.37s
bronze_order_items               1         0.08s       0.07s     0.15s
```

Feldera reflected the first CDC hour end-to-end in **0.37s at most** (`bronze_orders`, whose
delete-and-insert updates cost more than the append-only streams) against Databricks' 86s
for the identical SQL. There is no per-run startup: Feldera is already running, so the only
cost is processing the change itself.

## Feldera vs. the Databricks Lakeflow Declarative Pipelines

| | Databricks workflow | Feldera |
|---|---|---|
| Silver / Gold | Materialized views, incremental *where possible* (14/15) | All 15 views, always incremental |
| `gold_product_demand_surge` | Full recompute every refresh | Incremental |
| Orchestration | Job schedule + pipeline refresh | None — change-driven |
| Per-run startup | ~28s serverless provisioning | None — always on |
| Freshness for the first CDC hour | ~86s end-to-end | 0.37s at most |

Both consume the identical Bronze Delta tables in your bucket and run the identical Silver
and Gold SQL. Databricks maintains 14 of the 15 views incrementally and full recomputes
`gold_product_demand_surge` on every refresh, taking 86s end-to-end (58s of it compute) to
reflect the first CDC hour. Feldera follows the Delta log and maintains every view
incrementally, reflecting the same hour end-to-end in 0.37s at most, without any per-run
startup.
