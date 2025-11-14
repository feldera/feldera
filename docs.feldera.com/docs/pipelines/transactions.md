import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Efficient Bulk Data Processing using Transactions

:::warning
Transaction support is an experimental feature and may undergo significant
changes, including non-backward-compatible modifications in future releases of
Feldera.
:::

Transactions enable Feldera pipelines to ingest and process large volumes of data atomically—in
one logical unit of work rather than piece-by-piece.  Transactions are used to achieve:

* **Efficient backfill**: ingest and process large historical datasets.

* **Atomicity**: process multiple inputs atomically without emitting intermediate results.

## Overview

By default, Feldera pipelines run in **continuous mode**: they ingest a chunk of
data from input connectors, process it to completion and produce updates to all
views before processing the next chunk.  This mode is optimal for low-latency
incremental view maintenance; but less suited for bulk data ingest.

A common scenario is when a pipeline must ingest a large volume of historical data
accumulated over years before processing new real-time inputs. This is known as
the **backfill** problem. While backfill can be performed in continuous mode,
this is likely to lead to two issues:

* **Performance:** computing all intermediate updates can be more expensive than
  computing the cumulative update. The exact performance difference depends on
  your SQL queries and can be very significant in some cases.

* **Atomicity:** in continuous mode the pipeline produces a stream of intermediate
  updates, which often cancel each other out.  Consider for instance a `COUNT(*)`
  query whose output changes any time new records are ingested.  These updates
  expose intermediate computation results to downstream systems while placing
  excessive load on the data sinks.

**Transactions** solve these challenges by allowing input updates to be grouped into
a single batch and processed atomically. In transactional mode,
Feldera still executes SQL queries incrementally and produces a delta of changes
to output views. The key difference is in who controls the batch size: instead of the
engine automatically deciding chunk boundaries, the user explicitly defines the scope
of each transaction.

The following table summarizes the two modes:

|                               | Continuous Mode                                                                                                                                | Transactions                                                                                                                                                         |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **When to use**               | <p>Process real-time inputs with low latency</p>                                                                                               | <ul><li>Efficiently process bulk data</li><li>Process multiple updates atomically</li></ul>                                                                                                                                 |
| **When query evaluation is triggered** | <p>When an input chunk is ingested</p>                                                                                          | <p>On transaction commit</p>                                                                                                                                         |
| **Strong consistency**        | <p>Yes: after processing an input chunk, the contents of all views matches all the inputs received so far.</p>                                 | <p>Yes: after committing a transaction the contents of all matches all the inputs received so far, including inputs received as part of the transaction.</p>         |

## Transaction API

### Start a transaction

Use the [`start_transaction`](/api/begin-transaction) API to start a transaction. The API returns a transaction ID:

<Tabs>
    <TabItem value="rest" label="REST API">
    ```shell
    $ curl -X POST http://localhost:8080/v0/pipelines/my_pipeline/start_transaction

    {"transaction_id":1}
    ```
    </TabItem>

    <TabItem value="python" label="Python SDK">
    ```python
    from feldera.pipeline import Pipeline

    transaction_id = pipeline.start_transaction()
    ```
    </TabItem>

    <TabItem value="fda" label="fda CLI">
    ```shell
    $ fda start-transaction my_pipeline

    Transaction started successfully with ID: 1
    ```
    </TabItem>
</Tabs>

* The `start_transaction` call will fail if there is already a transaction in progress.

* During a transaction, the pipeline ingests incoming data without producing output, performing only minimal processing such as resolving primary keys and indexing inputs.

* Any inputs received by the pipeline between `start_transaction` and `commit_transaction`, along with data buffered by input connectors but not yet processed at the time `start_transaction` is called, are processed as part of the transaction.

### Commit a transaction

Once the pipeline has ingested all inputs that must be processed as part of the transaction,
commit the transaction using the [`commit_transaction`](/api/commit-transaction) API.

<Tabs>
    <TabItem value="rest" label="REST API">
    ```shell
    $ curl -X POST http://localhost:8080/v0/pipelines/my_pipeline/commit_transaction
    "Transaction commit initiated"
    ```
    </TabItem>

    <TabItem value="python" label="Python SDK">
    ```python
    transaction_id = pipeline.commit_transaction(transaction_id, wait = True)
    ```
    </TabItem>

    <TabItem value="fda" label="fda CLI">
    ```shell
    $ fda commit-transaction my_pipeline

    Transaction committed successfully.
    ```
    </TabItem>
</Tabs>

* The `commit_transaction` REST API initiates the commit. The commit can take some time and complete later.
  Use the [`/stats` endpoint](#monitoring-transaction-status) to monitor transaction status.

* The Python SDK and `fda` CLI provide both blocking and non-blocking variants of the commit operation.
  The blocking variant waits until the commit is complete before returning control to the caller.

* During a transaction commit, the pipeline computes updates to all views in the
  program. Depending on the volume of ingested data and the complexity of the
  views, this process can take a significant amount of time. While the commit is
  in progress, the pipeline neither ingests new inputs nor produces outputs. To
  provide visibility, the pipeline periodically (every 10 seconds) logs the
  progress of the current commit operation.

* Once the commit is complete, the pipeline outputs a batch of updates for every view in the program.
  These updates are processed by the output connectors, which send them to their associated data sinks.

When the transaction is complete, the pipeline goes back into continuous processing mode. The user can start a new transaction any time.

### Monitoring transaction status

The user can monitor the transaction handling status of the pipeline using the [`/stats`](/api/get-pipeline-stats) endpoint. The status can be one of:

| Status                  | Description                                     |
|-------------------------|-------------------------------------------------|
| `NoTransaction`         | There is currently no active transaction. The pipeline is running in continuous mode.|
| `TransactionInProgress` | There is an active transaction in progress.     |
| `CommitInProgress`      | The current transaction is being committed.     |


When the status is `TransactionInProgress` or `CommitInProgress`, the `transaction_id` attribute contains the current transaction ID.

<Tabs>
    <TabItem value="rest" label="REST API">
    ```shell
    $ curl -s http://localhost:8080/v0/pipelines/my_pipeline/stats | jq -r '.global_metrics.transaction_status'
    TransactionInProgress

    $ curl -s http://localhost:8080/v0/pipelines/my_pipeline/stats | jq -r '.global_metrics.transaction_id'
    1
    ```
    </TabItem>

    <TabItem value="python" label="Python SDK">
    ```python
    transaction_status = pipeline.transaction_status()
    transaction_id = pipeline.transaction_id()
    ```
    </TabItem>

    <TabItem value="fda" label="fda CLI">
    ```shell
    $ fda stats my_pipeline --format json | jq -r '.global_metrics.transaction_status'
    CommitInProgress

    $ fda stats my_pipeline --format json | jq -r '.global_metrics.transaction_id'
    1
    ```
    </TabItem>
</Tabs>

## Automatic Transaction Orchestration

In addition to initiating transactions programmatically via the API, certain input connectors can also be configured to initiate transactions automatically.
This feature enables users to leverage transactions **declaratively** within the pipeline’s SQL definition.

Use cases:

- **Backfill** — When a pipeline starts, input connectors often start with ingesting historical data from their associated data sources.
  It is beneficial to ingest and process this data as part of a single transaction as explained in the previous sections.

- **Atomic processing of related updates** — Some data sources, such as Delta Lake, partition the input stream into groups of updates that must be applied together as a single atomic unit.
  By processing each of these groups as one Feldera transaction, the system preserves atomicity end-to-end, ensuring that all related updates are applied together.

Multiple connectors attached to the same or different SQL tables may initiate transactions concurrently.
In this case, Feldera automatically combines these individual transaction requests into a single coordinated transaction:

- The transaction **begins** when the first connector initiates it.
- The transaction **commits** when the last participating connector commits the transaction.

For example, if several connectors performing backfill are configured to use transactions, their operations will be merged into one composite transaction, effectively
equivalent to initiating a single transaction through the REST API that ingests all historical inputs together.

### Supported Connectors

Automatic transaction orchestration is currently supported only for the **Delta Lake connector**.
Refer to the [Delta Lake connector documentation](/connectors/sources/delta#transactions) for details.

## Limitations

* Concurrent transactions are not supported. At most one transaction can run at a time. All inputs ingested by the pipeline while the transaction is active are processed as part of the transaction.

* Checkpointing is disabled during a transaction. A checkpoint initiated during a transaction gets delayed until the transaction has committed.

* A transaction currently cannot be aborted or rolled back. Let us know if this feature is important for your use case by [leaving a comment](https://github.com/feldera/feldera/issues/4710).

## Example

```shell
# Create a simple pipeline that copies all records in table `t` to view `v`.
$ echo 'create table t(x int); create materialized view v as select * from t;' | fda create transaction_test --stdin
Pipeline created successfully.

$ fda start transaction_test
Pipeline started successfully.

# Update the table.
$ fda query transaction_test "insert into t values(1)"
+-------+
| count |
+-------+
| 1     |
+-------+

# The change is instantly reflected in the output view.
$ fda query transaction_test "select * from v"
+---+
| x |
+---+
| 1 |
+---+

# Start a transaction.
$ fda start-transaction transaction_test
Transaction started successfully with ID: 1

# Insert more records.
$ fda query transaction_test "insert into t values(2)"
+-------+
| count |
+-------+
| 1     |
+-------+

$ fda query transaction_test "insert into t values(3)"
+-------+
| count |
+-------+
| 1     |
+-------+

# The view remains unmodified since the transaction has not been committed.
$ fda query transaction_test "select * from v"
+---+
| x |
+---+
| 1 |
+---+

# Commit the transaction.
$ fda commit-transaction transaction_test
Transaction committed successfully.

# Updates performed during the transaction are now propagated to all views.
$ fda query transaction_test "select * from v"
+---+
| x |
+---+
| 2 |
| 3 |
| 1 |
+---+
```
