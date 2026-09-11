# Feldera Best Practices

This page collects the settings we recommend for every production Feldera
pipeline.  Each rule states what to set, why it matters, and where the full
reference lives.  Apply the checklist before you promote a pipeline.

| # | Practice | Where it is set | Reference |
| --- | --- | --- | --- |
| 1 | [Enable adaptive joins](#enable-adaptive-joins) | Runtime config | [Pipeline Settings] |
| 2 | [Configure fault tolerance](#configure-fault-tolerance) | Runtime config | [Fault Tolerance] |
| 3 | [Provision 2-4x your input data size for storage](#provision-2-4x-your-input-data-size-for-storage) | Resource config | [Pipeline Settings] |
| 4 | [Declare intermediate views `LOCAL`](#declare-intermediate-views-local) | SQL | [Creating Views] |
| 5 | [Use transactions with Delta and Iceberg](#use-transactions-with-delta-and-iceberg) | Connector config | [Transactions] |
| 6 | [Use the flat `VARIANT` representation](#use-the-flat-variant-representation) | SQL | [VARIANT Type] |
| 7 | [Set a clock resolution when `NOW()` is used outside of a filter](#set-a-clock-resolution-when-now-is-used-outside-of-a-filter) | Runtime config | [NOW()] |

## Enable adaptive joins

Set `dev_tweaks.adaptive_joins` to `true` in the pipeline's
[runtime configuration][Pipeline Settings]:

```json
{
  "dev_tweaks": {
    "adaptive_joins": true
  }
}
```

A plain hash join partitions rows by a hash of the join key, so every row that
shares a key lands on the same worker.  One hot key pins a whole
join to a single worker while the others idle.  Adaptive joins change the
partitioning policy at runtime when they detect skew, spreading a hot key
across workers.

Adaptive joins are off by default.  Turn them on unless you are using multi-host. Performance improvements for adaptive joins with multi-host are [in progress](https://github.com/feldera/feldera/issues/7113).

## Configure fault tolerance

Fault tolerance is a Feldera Enterprise feature, and it is **not enabled by
default**.  Without it, a pipeline restart replays from whatever position its
connectors can recover on their own, which for many sources means reprocessing
from the beginning.  With it, the pipeline resumes from its last checkpoint.

Enabling it takes three steps, covered in full in [Fault Tolerance]:

1. Confirm every connector on the pipeline supports fault tolerance.
2. Enable storage (new pipelines enable it by default).
3. Choose a fault-tolerance model.

```bash
fda set-config <pipeline> fault_tolerance exactly_once
fda set-config <pipeline> checkpoint_interval 60
```

| Model | Guarantee after a restart | Cost |
| --- | --- | --- |
| `none` (default) | No checkpoint or resume. | None. |
| `at_least_once` | Every input is processed; some inputs near the failure point may be processed twice. | Lower. |
| `exactly_once` | Every input is processed exactly once. | Higher; requires connectors that support it. |

A shorter `checkpoint_interval_secs` bounds how much work a restart repeats,
at the cost of more checkpoint writes.  Sixty seconds is the default and a
reasonable starting point.

For cross-region recovery or blue-green deployment strategies, also see [Checkpoint Sync] and
[Fault Tolerance & Disaster Recovery].

## Provision 2-4x your input data size for storage

Start with `storage_mb_max` set to **two to four times the size of your input
data**, then measure.  The `resources` section is enforced only in Feldera
Cloud; elsewhere, size the volume you attach to the pipeline the same way.

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
storage, so size it generously up front: an over-provisioned volume is cheaper
than a rebuild.  `storage_class` determines IOPS and throughput; backfill is
frequently storage-bound, so do not put a large backfill on the slowest class
available.

## Declare intermediate views `LOCAL`

Declare every view `LOCAL` unless it is an output of the pipeline:

```sql
-- Intermediate step: nothing outside the pipeline reads it.
CREATE LOCAL VIEW enriched AS
SELECT o.*, c.region
FROM orders o JOIN customers c ON o.customer_id = c.id;

-- Actual output: read by a sink and by ad-hoc queries.
CREATE MATERIALIZED VIEW regional_totals AS
SELECT region, SUM(amount) FROM enriched GROUP BY region;
```

A non-`LOCAL` view is an output of the computation: Feldera produces its change
stream, buffers those changes for output connectors, and supports indexes
declared on it.  A `LOCAL` view exists only as an intermediate calculation, so none of
that work happens.

A view should be non-`LOCAL` only if an output connector is
configured for it, an ad-hoc query reads it, or you are debugging it.
Materializing it adds a full copy of its contents on top.  See
[Creating Views] and [Materialized Tables and Views].

## Use transactions with Delta and Iceberg

In continuous mode, Feldera processes input in engine-chosen chunks and emits
a change to every view after each chunk.  During a backfill this produces a
long stream of intermediate updates that may cancel each other out, and
pushes all of them at your sinks.

Transactions group the input into batches you define.  Queries still evaluate
incrementally, but views update once, at commit.  Delta Lake and Iceberg
sources can open and commit transactions on their own through
`transaction_mode`, so you get this declaratively:

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
| `none` (default) | No grouping. | Low-latency streaming with small commits. |
| `snapshot` | The initial snapshot is one transaction, or one per `LATENESS` range if `timestamp_column` is set.  Follow-phase changes are not grouped. | Backfill once, then stream. |
| `catchup` | Batches all commits already available into one transaction: many per transaction while catching up, roughly one per commit once caught up. | Backfill and steady-state following.  Best default for both. |
| `always` | One transaction per source commit. | Each source commit must be applied atomically. |

Setting `timestamp_column` on a snapshot splits it into one transaction per
`LATENESS` range instead of a single enormous one, which keeps peak memory
bounded during backfill.

Outputs benefit without extra configuration.  Because views change only at
commit, a Delta or Iceberg sink writes one batch per transaction rather than
many intermediate versions, so downstream readers never observe a partial
result.

Three limitations apply: at most one transaction runs at a time, checkpoints
are deferred until it commits, and it cannot be rolled back.  See
[Transactions], [Delta Transactions], and [Iceberg Transactions].

## Use the flat `VARIANT` representation

Any program that uses [`VARIANT`][VARIANT Type] values, including any program
that processes JSON, should opt in to the flat representation:

```sql
SET feldera_flat_variant = 'on';
```

The flat representation stores a document in one contiguous buffer instead of a
tree of separately allocated nodes, so documents cost less to allocate, store,
and serialize.  The difference grows with document size and nesting depth.

Put the `SET` statement at the top of the program, before any DDL.  It applies
to the whole program.

## Set a clock resolution when `NOW()` is used outside of a filter

`clock_resolution_usecs` controls how often the pipeline advances its clock and
re-evaluates everything that depends on [`NOW()`][NOW()].  The default is one
second:

```json
{
  "clock_resolution_usecs": 60000000
}
```

The setting matters because of where `NOW()` appears:

| `NOW()` appears in | Cost per clock tick |
| --- | --- |
| A temporal filter, e.g. `WHERE ts > NOW() - INTERVAL 1 HOUR` | Cheap.  Feldera uses the filter to garbage-collect expired records. |
| Outside of a filter: a projection, an aggregate, a join condition, a `CASE` | Every dependent operator recomputes once per tick, whether or not any input arrived. |

If `NOW()` appears only inside temporal filters, leave the default.  If it
appears anywhere else, set `clock_resolution_usecs` to the coarsest resolution
your business logic tolerates.

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
