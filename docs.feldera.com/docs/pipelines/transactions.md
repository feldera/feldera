import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Efficient Bulk Data Processing using Transactions

:::warning
Transaction support is an experimental feature and may undergo significant
changes, including non-backward-compatible modifications, in future releases of
Feldera.
:::

Transactions let Feldera pipelines to ingest and process large volumes of data atomically—in
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
engine automatically  deciding chunk boundaries, the scope of
each transaction is explicitly specified by the user.

The following table summarizes the two modes:

|                               | Continuous Mode                                                                                                                                | Transactions                                                                                                                                                         |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **When to use**               | <p>Process real-time inputs with low latency</p>                                                                                               | <p>Efficiently process bulk data</p>                                                                                                                                 |
| **When query evaluation is triggered** | <p>When an input chunk is ingested</p>                                                                                          | <p>On transaction commit</p>                                                                                                                                         |
| **Strong consistency**        | <p>Yes: after processing an input chunk, the contents of all views matches all the inputs received so far.</p>                                 | <p>Yes: after committing a transaction the contents of all matches all the inputs received so far, including inputs received as part of the transaction.</p>         |

## Transaction API

### Start a transaction

Use the [`start_transaction`](https://docs.feldera.com/api/start-a-transaction) API to start a transaction. The API returns a transaction ID:

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

### Commit a transaction

Once the pipeline has ingested all inputs that must be processed as part of the transaction,
commit the transaction using the [`commit_transaction`](https://docs.feldera.com/api/commit-the-current-transaction) API.

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
  provide visibility, the pipeline periodically (every 10 seconds) reports the
  progress of the current commit operation in the log.

* Once the commit is complete, the pipeline outputs a batch of updates for every view in the program.
  These updates are processed by the output connectors, which send them to their associated data sinks.

When the transaction is complete, the pipeline goes back into continuous processing mode. The user can start a new transaction any time.

### Monitoring transaction status

The user can monitor the transaction handling status of the pipeline using the [`/stats`](https://docs.feldera.com/api/retrieve-statistics-e-g-performance-counters-of-a-running-or-paused-pipeline) endpoint. Possible statuses are summarized
in the following table:


| Status                  | Description                                     |
|-------------------------|-------------------------------------------------|
| `NoTransaction`         | There is currently no active transaction.       |
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

## Limitations

* Concurrent transactions are not supported. At most one transaction can run at a time. All inputs ingested by the pipeline while the transaction is active are processed as part of the transaction.

* Checkpointing is disabled during a transaction. A checkpoint initiated during a transaction gets delayed until the transaction has committed.

* Transaction rollback is currently not supported.

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