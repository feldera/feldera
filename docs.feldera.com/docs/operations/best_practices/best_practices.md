# Best Practices

This page collects the settings we recommend for every production Feldera
pipeline.

| # | Practice | Where it is set | Reference |
| --- | --- | --- | --- |
| 1 | [Configure fault tolerance](#configure-fault-tolerance) | Runtime config | [Fault Tolerance] |
| 2 | [Provision 2-4x your input data size for storage](#provision-2-4x-your-input-data-size-for-storage) | Resource config | [Pipeline Settings] |
| 3 | [Declare intermediate views `LOCAL`](#declare-intermediate-views-local) | SQL | [Creating Views] |
| 4 | [Use transactions with Delta and Iceberg](#use-transactions-with-delta-and-iceberg) | Connector config | [Transactions] |
| 5 | [Use the flat `VARIANT` representation](#use-the-flat-variant-representation) | SQL | [VARIANT Type] |
| 6 | [Set a clock resolution when `NOW()` is used](#set-a-clock-resolution-when-now-is-used-outside-of-a-filter) | Runtime config | [NOW()] |

## Configure fault tolerance

Fault tolerance is a Feldera Enterprise feature, and it is **not enabled by
default**.  Without fault tolerance enabled, a pipeline restart replays from whatever position its
connectors can recover on their own, which for many sources means reprocessing
from the beginning.  With it, the pipeline resumes from its last checkpoint.

Enabling it takes three steps, covered in full in [Fault Tolerance]:

1. Confirm every connector on the pipeline supports fault tolerance.
2. Enable storage (new pipelines enable it by default).
3. Choose a fault-tolerance model.


| Model | Guarantee after a restart | Cost |
| --- | --- | --- |
| `none` (default) | No checkpoint or resume. | None. |
| `at_least_once` | Every input record is processed at least once; some inputs near the failure point may be processed twice. | Lower. Requires [connectors that support fault tolerance](/pipelines/fault-tolerance/#fault-tolerant-connectors)|
| `exactly_once` | Every input record is processed exactly once. | Higher; requires connectors that support it. |

A shorter `checkpoint_interval_secs` is a bound on the amount of redundant work performed when a pipeline restarts,
at the cost of more checkpoint writes.  Sixty seconds is the default and a
reasonable starting point.

For cross-region recovery or blue-green deployment strategies, also see [Checkpoint Sync] and
[Fault Tolerance & Disaster Recovery].

## Provision 2-4x your input data size for storage

Start with `storage_mb_max` set to **two to four times the size of your input
data**, then measure.  The `resources` section is enforced only in Feldera
Enterprise.

```json
{
  "resources": {
    "storage_mb_max": 400000,
    "storage_class": "gp3"
  }
}
```

The multiplier is a guideline, not a bound: the pipeline's SQL determines it,
and some programs need less than 2x while others need more than 4x.

| Closer to 2x | Closer to 4x, or beyond |
| --- | --- |
| Filters, projections, `LOCAL` views | Joins and aggregates with large state |
| `LATENESS` annotations that let Feldera garbage-collect | Materialized views, tables with primary keys |
| Few or no materialized views | Many large or complex materialized views |

`storage_mb_max` cannot be edited while a stopped pipeline still holds
storage, so size it generously up front. [Storage can be expanded out of band](/operations/guide/#expand-existing-pipeline-storage) but will result in a mismatch between the volume size and the reported `storage_mb_max`. `storage_class` determines IOPS and throughput; backfill is
frequently storage-bound. Use a volume with [fast IO and throughput](/get-started/enterprise/helm-guide/#persistent-volume-sizing) when performing a large backfill.

## Declare intermediate views `LOCAL`

Declare every view `LOCAL` unless it is an output of the pipeline:

```sql
-- Intermediate result: nothing outside the pipeline reads it.
CREATE LOCAL VIEW enriched AS
SELECT o.amount, c.region
FROM orders o JOIN customers c ON o.customer_id = c.id;

-- Pipeline output: output connector and can be read by ad-hoc queries.
CREATE MATERIALIZED VIEW regional_totals AS
SELECT region, SUM(amount) FROM enriched GROUP BY region;

-- Emits only deltas, usually configured with an output connector but cannot be queried ad-hoc
CREATE VIEW regional_totals AS
SELECT region, SUM(amount) from enriched group by region
```

A non-`LOCAL` view is an output of the computation: Feldera produces its change
stream, buffers those changes for output connectors, and supports indexes
declared on it.  A `LOCAL` view exists only as an intermediate calculation, so none of
that work happens.

A view should be `MATERIALIZED` only if an ad-hoc query reads it, or you are debugging it.
Materializing it adds a full copy of its contents on top.  See
[Creating Views] and [Materialized Tables and Views].

## Use transactions with Delta and Iceberg

In continuous mode, Feldera processes input in engine-chosen batches and emits
a change to every view after each batch. By default, the batch size is 10,000
records per worker as set by [max_worker_batch_size](/connectors/#generic-attributes).
During a backfill this produces a long stream of intermediate
updates that may cancel each other out.

Transaction allow input updates to be grouped into a single batch and processed atomically.
Queries still evaluate incrementally, but views update once, at commit.
Delta Lake and Iceberg sources can open and commit transactions on their own through
`transaction_mode`. Feldera will use the Delta's log, or the Iceberg table's snapshot lineage to set transaction boundaries.


```json
{
  "transport": {
    "name": "delta_table_input",
    "config": {
      "uri": "s3://bucket/table",
      "mode": "snapshot_and_follow",
      "transaction_mode": "catchup"
    }
  }
}
```

| `transaction_mode` | Behavior | Use when |
| --- | --- | --- |
| `none` (default) | No transactions. | Low-latency streaming with small commits. |
| `snapshot` | The initial snapshot is one transaction, or one per `LATENESS` range if `timestamp_column` is set.  Follow-phase changes are not grouped. | Backfill once, then stream. |
| `catchup` | Batches all input updates already available into one transaction: many per transaction while catching up, roughly one per commit once caught up. | Backfill and steady-state following.  Best default for both. |
| `always` | One transaction per input update. | Each input update must be applied atomically. |

Transactions also benefit outputs.  Because views change only at
commit, a Delta or Iceberg sink writes one batch per transaction rather than
many intermediate versions, so downstream readers never observe a partial
result.

Three caveats to consider: at most one transaction runs at a time, checkpoints
are deferred until the transaction commits, and the transaction cannot be rolled back.  See
[Transactions], [Delta Transactions], and [Iceberg Transactions].

## Use the flat `VARIANT` representation

Any program that uses [`VARIANT`][VARIANT Type] values, including any program
that processes JSON, should opt in to the flat representation:

```sql
SET feldera_flat_variant = 'on';
```

The flat representation stores variants more compactly.

## Set a clock resolution when `NOW()` is used outside of a filter

`clock_resolution_usecs` controls how often the pipeline advances its clock and
re-evaluates everything that depends on [`NOW()`][NOW()].  The default is one
second:

```json
{
  "clock_resolution_usecs": 60000000
}
```

The setting's cost depends on the SQL program structure:

| `NOW()` appears in | Cost per clock tick |
| --- | --- |
| A temporal filter, e.g. `WHERE ts > NOW() - INTERVAL 1 HOUR` | Relatively inexpensive.  Feldera uses the filter to garbage-collect expired records. |
| Outside of a filter: a projection, an aggregate, a join condition, a `CASE` | Every dependent operator recomputes once per tick, whether or not any input arrived. |

If `NOW()` outside of a temporal filter, set `clock_resolution_usecs` to the coarsest resolution
your business logic tolerates. The compiler will emit a warning if `NOW()` is used outside of a filter.

If `NOW()` appears only inside temporal filters, lowering the clock resolution can still be helpful for performance.

If a query does not use `NOW()` at all, the pipeline suppresses clock updates
and ignores this setting.

[Pipeline Settings]: /pipelines/configuration#runtime-configuration
[Fault Tolerance]: /pipelines/fault-tolerance
[Fault Tolerance & Disaster Recovery]: /pipelines/fault-tolerance-overview
[Checkpoint Sync]: /pipelines/checkpoint-sync
[Creating Views]: /sql/grammar#creating-views
[Materialized Tables and Views]: /sql/materialized
[Transactions]: /pipelines/transactions
[Delta Transactions]: /connectors/sources/delta#transactions
[Iceberg Transactions]: /connectors/sources/iceberg#transactions
[VARIANT Type]: /sql/json#the-variant-type
[NOW()]: /sql/datetime#now
