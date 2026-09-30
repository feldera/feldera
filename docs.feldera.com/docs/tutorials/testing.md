# Testing Pipelines with the Python SDK

This guide describes how to test Feldera SQL pipelines
using the Feldera [Python SDK](pathname:///python/).  This is an
introductory guide only; we try to keep the examples as simple
and readable as possible.  The examples use plain Python functions
that check their results with `assert` and do not rely on a
Python unit testing framework.

A test builds a pipeline from a SQL program, sends input
changes to the connectors of its tables, and compares the contents or
the changes of its views with the expected results.

To run the examples, you need:

* Feldera, running locally or remotely.  The examples connect to its
  API at `http://localhost:8080`.  See [Get started](/get-started) to start it.
* The SDK: `pip install feldera`.

## Run Feldera locally

You can run Feldera from a checkout of the
[Feldera repository](https://github.com/feldera/feldera), without
Docker or Kubernetes.  First install the prerequisites listed under
"Running Feldera from sources" in the repository's `README.md`.  Then
build the SQL compiler, and start the pipeline manager from the root of
the repository:

```bash
(cd sql-to-dbsp-compiler && ./build.sh)
./scripts/start_manager.sh
```

The pipeline manager listens on `http://localhost:8080` and runs the pipelines
on the same machine as the test.  Thus a program can read inputs
from a local file with the [file connector](/connectors/sources/file),
for example with `"path": "file:///tmp/orders.json"`.

## Core concepts

The Python SDK gives access to the following abstractions:

| Concept | Meaning | Operations |
|---------|---------|------------|
| Pipeline | A compiled SQL program together with its runtime. | Compile, start, start paused, pause, resume, stop, discard state.  Read the status, the statistics, the errors, and the logs. |
| Table | An input of the program.  A table holds a multiset of rows.  A table with a `PRIMARY KEY` holds at most one row for each key. | Insert rows, delete rows, replace the row with a given key. |
| View | An output of the program.  The pipeline keeps each view up to date. | Follow its change stream.  Read its snapshot if it is materialized. |
| Materialized table or view | A table or view that the pipeline stores in full.  Tables with a `PRIMARY KEY` are always materialized.  See [Materialized tables and views](/sql/materialized). | Read its snapshot with an ad-hoc SQL query. |
| Snapshot | The current contents of a materialized table or view. | Read. |
| Connector | A link between a table or view and an external system.  An input connector feeds a table; an output connector receives the data from a view.  The SQL program declares most connectors.  In addition, any HTTP client can send input to a table through its HTTP input connector, and can follow the changes of a table or view through an HTTP output connector.  The SDK uses these connectors to push input and to listen.  See [Connectors](/connectors). | Pause, start, read the statistics. |
| Change | A data row together with an integer weight: the number of copies inserted (positive) or deleted (negative).  An update is a deletion of the old row and an insertion of the new row.  The SDK represents a change of weight *n* as *n* changes of weight +1 or -1, each with an `insert_delete` column that holds the weight. | Send to a table as input.  Receive from a change stream as output. |
| Step | The pipeline processes a batch of input changes and computes the resulting changes of all views during a step.  After each step, every view agrees with all input received so far.  The pipeline can process one input in several steps. | Wait until all steps that process an input are complete. |
| Transaction | A group of input changes that the pipeline processes as one unit.  All input changes that the pipeline receives between the start and the commit belong to the transaction.  The views change once, at commit.  See [Transactions](/pipelines/transactions). | Start, commit, read the status. |
| Change stream | The changes of a table or view, in the order of the steps that produce them. | Connect a listener, read the changes received so far. |

## Lifecycle of a test

A test takes a pipeline through these phases:

| Phase | Test does | SDK calls | Pipeline state |
|-------|-----------|-----------|----------------|
| 1 | Compile the program | `PipelineBuilder(client, name, sql).create_or_replace()` returns a `Pipeline` object | Stopped |
| 2 | Start the pipeline paused, connect listeners | `pipeline.start_paused()`, `pipeline.listen(view)` | Paused |
| 3 | Resume the pipeline | `pipeline.resume()` | Running |
| 4 | Push input, then wait until the pipeline processes it | `pipeline.input_json(table, changes)` | Running |
| 5 | Read snapshots and changes | `pipeline.query(sql)`, `listener.to_dict()` | Running |
| 6 | Stop the pipeline, discard its state | `pipeline.stop(force=True)`, `pipeline.clear_storage()` | Stopped |
| 7 | Delete the pipeline | `pipeline.delete(clear_storage=True)` | Deleted |

A compiled pipeline can go through phases 2 to 6 many times, so many
tests can use one compilation.

Compilation is usually the slowest phase, and Feldera compiler server can only compile a
limited number of programs at the same time.
The compiler server may be the bottleneck of the test suite.  Compile each program
once and try to reuse a pipeline in multiple tests.  Several
tests can also be implemented as a single big SQL program.

A test that reads only snapshots can combine phases 2 and 3: it can
start the pipeline directly with `pipeline.start()`, as in
[A first test](#a-first-test).  `stop(force=True)` stops the pipeline
at once, without making a checkpoint.  `clear_storage()` discards the
state of the pipeline, enabling the next test to start with empty tables.

## A first test

This test is a Python program that does the following:

1. It compiles a SQL program that computes the total of the orders of each customer.
2. It starts a pipeline that runs the SQL program.
3. It inserts three orders.
4. It reads the totals and compares them with the expected values.
5. It stops the pipeline and deletes it.

Save the program in a file called `test_orders.py`:

```python
from feldera import FelderaClient, PipelineBuilder

SQL = """
CREATE TABLE orders (
    id BIGINT NOT NULL PRIMARY KEY,
    customer VARCHAR NOT NULL,
    amount INT NOT NULL
);

-- MATERIALIZED: the pipeline stores the entire view, so the test
-- can read all of its contents at any time with an ad-hoc query.
CREATE MATERIALIZED VIEW customer_totals AS
SELECT customer, SUM(amount) AS total, COUNT(*) AS num_orders
FROM orders
GROUP BY customer;
"""


def check_totals_per_customer() -> None:
    client = FelderaClient("http://localhost:8080")
    pipeline = PipelineBuilder(client, name="test-orders", sql=SQL).create_or_replace()
    pipeline.start()
    try:
        # Blocks until the pipeline has processed the rows and the results are visible.
        pipeline.input_json(
            "orders",
            [
                {"id": 1, "customer": "alice", "amount": 10},
                {"id": 2, "customer": "bob", "amount": 5},
                {"id": 3, "customer": "alice", "amount": 7},
            ],
        )

        rows = list(pipeline.query("SELECT * FROM customer_totals ORDER BY customer"))

        assert rows == [
            {"customer": "alice", "total": 17, "num_orders": 2},
            {"customer": "bob", "total": 5, "num_orders": 1},
        ]
    finally:
        pipeline.stop(force=True)
        pipeline.delete(clear_storage=True)


if __name__ == "__main__":
    check_totals_per_customer()
```

You can run this with `python test_orders.py`.  If the check fails, the program
stops with an `AssertionError`.  The program goes through the following phases:

| Phase | Code | Result |
|-------|------|--------|
| 1 | `PipelineBuilder(...).create_or_replace()` | Feldera compiles the program.  The first compilation can be slow, but later compilations are much faster. |
| 2 | `pipeline.start()` | The pipeline starts with empty tables. |
| 3 | `pipeline.input_json("orders", [...])` | The pipeline inserts three rows into `orders` and updates `customer_totals`.  The `pipeline.input_json()` call returns only after the pipeline has processed the rows and sent the resulting changes to all its outputs, so the new contents of `customer_totals` are visible. |
| 4 | `pipeline.query("SELECT ...")` | The test reads the current snapshot of `customer_totals` and compares it with the expected rows. |
| 5 | `pipeline.stop(force=True)`, `pipeline.delete(clear_storage=True)` | The pipeline stops.  Feldera discards its state and deletes it.  The `finally` block makes sure that this also occurs when the test fails. |

You can query the data in a materialized view with
[ad-hoc queries](/sql/ad-hoc).  Ad-hoc queries use a different SQL
dialect than Feldera SQL programs, so we recommend keeping them simple,
for example `SELECT * FROM customer_totals`.  Ad-hoc queries also
use the same computational resources as the pipeline, so running
very expensive ad-hoc queries on large data can interfere or even
crash the pipeline.

Because `input_json()` returns only after
the pipeline has processed the rows, the new contents of
`customer_totals` are visible as soon as the call completes.
Thus the ad-hoc query in phase 4 sees the totals of all three orders.

## Send and receive data

A test sends data to the input connectors of the tables, and receives
data from the tables and views.  Rows are Python dictionaries whose
keys are the column names.  These examples use the program from
[A first test](#a-first-test):

```python
# A row of the 'orders' table:
{"id": 1, "customer": "alice", "amount": 10}

# A list of input changes using the "insert_delete" format.  These changes delete a row and
# insert another one: together, the two changes update order 1.
[
    {"delete": {"id": 1, "customer": "alice", "amount": 10}},
    {"insert": {"id": 1, "customer": "alice", "amount": 12}},
]

# A change of the 'customer_totals' view, as returned by a listener:
{"customer": "alice", "total": 10, "num_orders": 1, "insert_delete": 1}
```

These methods can be used to send data to a pipeline:

| Method | Sends | Notes |
|--------|-------|-------|
| `pipeline.input_json(table, changes)` | A list of changes | When using `update_format="insert_delete"`, each element is `{"insert": row}` or `{"delete": row}`.  See [Updates and deletes](#updates-and-deletes). |
| `pipeline.input_pandas(table, df)` | The rows of a pandas DataFrame | Inserts the rows (delete is unsupported). |
| `pipeline.execute("INSERT INTO ...", wait=True)` | The rows of an ad-hoc `INSERT` statement | Inserts only: ad-hoc queries cannot delete rows.  To delete rows, use `pipeline.input_json()`. |
| A connector declared in SQL | Data from an external system, for example a file or a Kafka topic | The test writes the data to the external system, which sends it to the pipeline. |
| The [datagen connector](/connectors/sources/datagen), declared in SQL | Rows that the connector generates from a plan in its configuration | See [Tests with fixed inputs](#tests-with-fixed-inputs). |

These methods receive data:

| Method | Receives | Notes |
|--------|----------|-------|
| `pipeline.query("SELECT ...")` | The snapshot: the current contents of the queried tables and views | Only for materialized tables and views. |
| `pipeline.listen(view)` | A listener: an `OutputHandler` object that receives the change stream of one table or view | For all tables and views.  See the listener methods below. |
| A connector that the program declares | Changes that the pipeline writes to an external system | The test reads from the external system. |

`pipeline.listen()` returns a listener.  The listener is essentially a queue,
which automatically receives changes from the monitored table or view.
The listener interacts with the pipeline using an HTTP output connector,
which is automatically attached to every table and view.
The listener has two methods that return the changes that it received so far:

| Listener method | Returns |
|-----------------|---------|
| `listener.to_dict()` | The changes as a list of dictionaries.  Each change contains the columns of its row and an additional `insert_delete` column, with the value +1 or -1. |
| `listener.to_pandas()` | The same changes as a pandas DataFrame. |

Both methods return only the changes that have been received since the previous
call, and remove them from the listener's queue.  These two calls
return immediately.

Each change contains the columns of the inserted or deleted row, and
an `insert_delete` column with the value 1 for an insertion and -1 for
a deletion.  For example, when a second order of `alice` updates
her total, `listener.to_pandas()` on `customer_totals` returns a
deletion of the old row and an insertion of the new row:

```text
  customer  total  num_orders  insert_delete
0    alice     10           1             -1
1    alice     17           2              1
```

`listener.to_dict()` returns the same changes as dictionaries.  A
change of weight *n* appears as *n* identical changes of weight +1 or
-1.  When no change has arrived,
`listener.to_pandas()` returns an empty DataFrame without columns.

The listener returns every change that it receives, in the order of
arrival, and does not combine them.  For example, if one step inserts a row and
a later step deletes it, `listener.to_dict()` returns both the insertion
and the deletion.  Within one step, however, the pipeline combines the
changes, so the changes of one step never contain
both an insertion and a deletion of the same row.

A `listener` uses a background thread to read data from the pipeline.
The pipeline waits for this thread, so a thread that cannot read fast
enough slows the pipeline down.  The thread stores every change in the
listener's queue, which has no size limit, so the Python test may run
out of memory when the pipeline produces changes faster than the
listener can absorb them.

The next section explains when the data that a test receives reflects
a particular set of input changes.

## Synchronization

A test usually starts a pipeline that runs on a different machine.  The
pipeline then runs in its own process, independently of the test: its
input connectors feed its tables, it processes the input in steps, and
it sends the results to its outputs.  The test does not write to the
tables directly.  It writes to input connectors, either through the
SDK or through an external system that a connector reads.

The computation model of the pipeline is strongly consistent.  The
pipeline processes its input in steps, and after each step the
contents of every view agree with all the input that the pipeline has
received up to that step.  See
[A synchronous streaming model](https://www.feldera.com/blog/synchronous-streaming).

However, this guarantee applies to the state of the pipeline, and does not
necessarily apply to the interaction of the test with the pipeline.  The test observes the
pipeline through separate operations, and each operation can observe
the state of the pipeline at a different moment:

| When running | Observe |
|--------------|---------|
| One ad-hoc query | The state after one step, for all the tables and views that the query reads. |
| Two ad-hoc queries | The two queries may observe the same or different steps. |
| A listener | The changes of one table or view, in the order of the steps that produce them, delivered after the listener has connected.  The changes of one step can arrive in several chunks; a chunk never contains changes of two steps. |
| Independent listeners | Separate change streams. |

Thus the data that a test gets from the pipeline reflects a consistent
snapshot only when the test synchronizes with the pipeline.
Synchronization makes sure that:

* The observed state includes all the input that the test sent, and
  no input that the test has not sent yet.
* Separate reads observe the same state.  After the pipeline has
  processed all its input, and while no connector delivers new input,
  the contents of the tables and views do not change.

Without proper synchronization, tests may be flaky.

For this reason, a test usually sends all the input itself, so that it
controls when input arrives.  The real-time clock, expressed in SQL by the `NOW()` function,
is implemented as a table with a clock connector, which by default produces new values
periodically.  A test can use a deterministic clock by controlling
the data delivered by this connector as described in
[Testing programs that use the real-time clock `NOW()`](#testing-programs-that-use-the-real-time-clock-now).

Feldera pipelines are deterministic: given the same input, a pipeline
produces the same output.  `NOW()` counts as an input, and Feldera SQL has
no non-deterministic functions, such as random number generators.
[User-defined functions](/sql/udf) are required to be deterministic too.  Thus a
test that sends the same input always gets the same result, which makes
tests repeatable.  However, the ad-hoc queries used to query tables or
views are *not* necessarily deterministic.

We recommend against using elapsed time for synchronization: a test
that sleeps for a fixed time and then reads fails when the pipeline is
slower than expected.  Wait for a deterministic condition instead.
Some conditions are enforced by a blocking SDK call.  For other conditions, the test polls the
condition, as the [helper functions](#helper-functions) do.  Timeouts are
used to deal with unresponsive pipelines or unresponsive systems under test (e.g.,
Kubernetes does not allocate enough resources for the pipeline to start):

| To read | Method | Example |
|------|--------|---------|
| A snapshot | Call a method that waits until the pipeline has processed the input, and query when it returns.  See the table below. | [A first test](#a-first-test) |
| Change stream | Connect the listeners before the first step of the pipeline, then wait for a known number of changes. | [Check the contents of a view as a change stream](#check-the-contents-of-a-view-as-a-change-stream) |
| Absence of a change | Push a second input with a known effect, and wait for its change. | [Check that a change does not occur](#check-that-a-change-does-not-occur) |
| Result of `NOW()` | Wait until a view that shows `NOW()` has the new value. | [Testing programs that use the real-time clock `NOW()`](#testing-programs-that-use-the-real-time-clock-now) |

These calls return only after the pipeline has processed the input:

| Call | Returns when | After it returns |
|------|--------------|------------------|
| `pipeline.input_json(table, changes)`, `pipeline.input_pandas(table, df)` | The pipeline processed the input and sent the resulting changes to all its outputs. | `pipeline.query()` shows the effect of the input. |
| `pipeline.execute("INSERT ...", wait=True)` | Same as `pipeline.input_json()`. | Same as `pipeline.input_json()`. |
| `pipeline.commit_transaction()` | The commit is complete. | `pipeline.query()` shows the effect of the transaction. |
| `pipeline.wait_for_completion()` | Every input connector signaled the end of its input, and the pipeline processed all of it.  See below. | `pipeline.query()` shows the effect of all input. |

If you want to test programs without materialized views:

* Connect the listeners while the pipeline is paused, before it
  processes any input: call `start_paused()`, then `listen()`, then
  `resume()`.  This ensures that listeners receive all changes, including
  from the first step.
* Wait for the changes.  The pipeline sends each change to the
  listener over the network, and a background thread of the test
  receives it.  Thus the pipeline can send a change, and
  `input_json()` can return, before the corresponding output change reaches the listener.
  `to_dict()` returns only the changes already in the queue.  The `read_changes()` function in
  [Helper functions](#helper-functions) waits until a specified number of
  changes arrive.

### End of input

Some input connectors have a notion of "end of input", for example
a file connector or the datagen connector.  A
connector whose source can produce an unbounded data stream, for example a
Kafka topic, may never reach an "end of input".  Some
connectors support both bounded and unbounded stream modes: a file connector
with configuration `follow: true` does not emit an end of input
(see [File input connector configuration](/connectors/sources/file#file-input-connector-configuration)).
A Delta Lake connector signals "end of input" only after it reads a snapshot
(`mode: snapshot`), or after it reaches the specified `end_version` in the
connector configuration while following the transaction log
(see [Delta Lake input connector configuration](/connectors/sources/delta#delta-lake-input-connector-configuration)).
The HTTP connectors do not signal "end of input".

`pipeline.wait_for_completion()` returns when every input connector
except the HTTP connectors has signaled "end of input", and the pipeline
has processed all its input and sent the resulting changes to all its
outputs.  It never returns if a connector does not signal "end of
input", or if the program uses `NOW()` and the clock follows the system
clock.

## Helper functions

The following functions are not part of the SDK, but may be handy.
`wait_for_condition()` polls a condition specified as a Python predicate
function, `read_changes()` waits until a listener has received a number
of changes, and `sorted_dicts()` sorts rows or changes.

```python
import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from feldera.output_handler import OutputHandler


def wait_for_condition(
    description: str,
    predicate_func: Callable[[], bool],
    timeout_s: float | None = None,
    poll_interval_s: float = 0.1,
) -> None:
    """Keep re-evaluating `predicate_func` until it returns True or the timeout elapses.

    :param description: Human-readable description used in the timeout error.
    :param predicate_func: Callable returning True when a condition is met.
    :param timeout_s: Maximum wait time in seconds.  None means wait forever.
    :param poll_interval_s: How frequently to call the predicate, in seconds.
    :raises TimeoutError: If the condition is not met within `timeout_s`.
    """
    timestamp_deadline_s = (
        time.monotonic() + timeout_s if timeout_s is not None else float("inf")
    )
    while not predicate_func():
        if time.monotonic() > timestamp_deadline_s:
            raise TimeoutError(
                f"timeout ({timeout_s:.1f}s) waiting for condition '{description}'"
            )
        time.sleep(poll_interval_s)


def read_changes(
    listener: OutputHandler, count: int, timeout_s: float | None = None
) -> list[dict[str, Any]]:
    """Wait until `listener` has received at least `count` changes; return all
    received changes (could be more than count).

    Each change has an `insert_delete` value of +1 or -1; a change of weight n
    arrives as n changes.

    :param listener: The listener that `Pipeline.listen()` returned.
    :param count: Number of changes to wait for.
    :param timeout_s: Maximum wait time in seconds.  None means wait forever.
    :raises TimeoutError: If fewer than `count` changes arrive within `timeout_s`.
    """
    wait_for_condition(
        f"{count} change(s) on the '{listener.view_name}' listener",
        lambda: len(listener.to_pandas(clear_buffer=False)) >= count,
        timeout_s,
    )
    return listener.to_dict()


def sorted_dicts(items: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Return `items`, a list of dictionaries such as rows or changes, sorted
    using their natural order."""
    return sorted(items, key=lambda item: sorted(item.items()))
```

## Test scenarios

Here are solutions to some common test scenarios.
Each scenario is implemented in a Python function that receives a
compiled pipeline.  It starts the pipeline, and at the end it stops
the pipeline and discards its state, preparing the pipeline for a
subsequent test.  Unless a scenario shows its own program, it uses the
`SQL` program from [A first test](#a-first-test).  A scenario with its
own program builds its pipeline the same way.

The following program creates the pipeline and invokes two scenarios:

```python
from feldera import FelderaClient, Pipeline, PipelineBuilder

client = FelderaClient("http://localhost:8080")
pipeline = PipelineBuilder(client, name="test-orders", sql=SQL).create_or_replace()
try:
    check_totals_in_any_order(pipeline)
    check_second_order_updates_total(pipeline)
finally:
    pipeline.stop(force=True)
    pipeline.delete(clear_storage=True)
```

### Check the expected contents of a view

Read the snapshot of a view using the `query()` method after the `input_json()` call returns.
`query()` returns the rows in no defined order.  To compare them with an expected result, you can:

* Use an ad-hoc query that sorts the rows using `ORDER BY`, and compare them with a
  list in the same order, as [A first test](#a-first-test) does.
* Sort the rows with `sorted_dicts()` above.

```python
def check_totals_in_any_order(pipeline: Pipeline) -> None:
    pipeline.start()
    pipeline.input_json(
        "orders",
        [
            {"id": 1, "customer": "alice", "amount": 10},
            {"id": 2, "customer": "bob", "amount": 5},
        ],
    )
    rows = list(pipeline.query("SELECT * FROM customer_totals"))
    pipeline.stop(force=True)
    pipeline.clear_storage()

    assert sorted_dicts(rows) == sorted_dicts(
        [
            {"customer": "alice", "total": 10, "num_orders": 1},
            {"customer": "bob", "total": 5, "num_orders": 1},
        ]
    )
```

`query()` can only be used with materialized tables and views.

### Check the contents of a view as a change stream

To inspect a view that is not materialized, you need to capture all the
changes that it produces.

Start the pipeline paused, connect the listener, and then resume the
pipeline, so that the listener connects before the first step.  Then
wait for the changes with `read_changes()`.

```python
def check_second_order_updates_total(pipeline: Pipeline) -> None:
    # Connect the listener before the first step.
    pipeline.start_paused()
    totals = pipeline.listen("customer_totals")
    pipeline.resume()

    pipeline.input_json("orders", [{"id": 1, "customer": "alice", "amount": 10}])
    pipeline.input_json("orders", [{"id": 2, "customer": "alice", "amount": 7}])
    changes = read_changes(totals, 3)
    pipeline.stop(force=True)
    pipeline.clear_storage()

    assert sorted_dicts(changes) == sorted_dicts(
        [
            {"customer": "alice", "total": 10, "num_orders": 1, "insert_delete": 1},
            {"customer": "alice", "total": 10, "num_orders": 1, "insert_delete": -1},
            {"customer": "alice", "total": 17, "num_orders": 2, "insert_delete": 1},
        ]
    )
```

The pipeline processes each `input_json()` call in one or more steps
of its own; the next call starts only after the pipeline has processed
the previous one.  The first call inserts the row for `alice`.  The second call
updates the row: it deletes the old row and inserts the new row.

`read_changes()` returns all changes that the listener received, which
can be more than `count`.

### Updates and deletes

The SDK accepts these input changes:

| Change | `update_format` | Sample element of the `data` list |
|--------|-----------------|----------------------------|
| Insert | `"raw"` (default) | `{"id": 1, "customer": "alice", "amount": 10}` |
| Insert | `"insert_delete"` | `{"insert": {"id": 1, ...}}` |
| Delete | `"insert_delete"` | `{"delete": {"id": 1, ...}}` |
| Replace a row (table with a primary key) | any | An insert with the key of an existing row. |
| Change some columns (table with a primary key) | `"insert_delete"` | `{"update": {"id": 1, "amount": 12}}`.  See [the JSON format](/formats/json#the-insertdelete-format). |

```python
def check_replace_and_delete(pipeline: Pipeline) -> None:
    pipeline.start()
    pipeline.input_json(
        "orders",
        [
            {"id": 1, "customer": "alice", "amount": 10},
            {"id": 2, "customer": "bob", "amount": 5},
        ],
    )
    # This insert has the key of order 1, so it replaces order 1.
    pipeline.input_json("orders", [{"id": 1, "customer": "alice", "amount": 25}])
    pipeline.input_json(
        "orders",
        [{"delete": {"id": 2, "customer": "bob", "amount": 5}}],
        update_format="insert_delete",
    )
    rows = list(pipeline.query("SELECT * FROM customer_totals"))
    pipeline.stop(force=True)
    pipeline.clear_storage()

    assert rows == [{"customer": "alice", "total": 25, "num_orders": 1}]
```

:::warning

Deleting a row of a table without a primary key when the row is not in the
table produces unpredictable results, and the pipeline may crash.

:::

### Check that a change does *not* occur

While Feldera has a reliable mechanism to track the completion of steps,
called a completion token, unfortunately the HTTP connector (which is the basis
for the listener) does not expose this information.  We hope to improve this
API: see [issue 7360](https://github.com/feldera/feldera/issues/7360).

Some input changes produce no output changes (e.g., updating a record will
not change a `COUNT`).  To check that an input produces no output
changes, a test has to use a different approach:

* For materialized views, use an ad-hoc query executed after `input_json()` returns.
  The snapshot will include all effects of the input.
* For a non-materialized view, push a subsequent input which produces an
  expected output change.  Since inputs are processed in order, receiving the change
  produced by the second input guarantees that the first one has been processed.

```python
def check_identical_row_causes_no_change(pipeline: Pipeline) -> None:
    # Connect the listener before the first step.
    pipeline.start_paused()
    totals = pipeline.listen("customer_totals")
    pipeline.resume()

    pipeline.input_json("orders", [{"id": 1, "customer": "alice", "amount": 10}])
    # The input under test: replace order 1 with an identical row.
    pipeline.input_json("orders", [{"id": 1, "customer": "alice", "amount": 10}])
    # The second input: its effect is known.
    pipeline.input_json("orders", [{"id": 2, "customer": "bob", "amount": 5}])
    changes = read_changes(totals, 2)
    pipeline.stop(force=True)
    pipeline.clear_storage()

    assert changes == [
        {"customer": "alice", "total": 10, "num_orders": 1, "insert_delete": 1},
        {"customer": "bob", "total": 5, "num_orders": 1, "insert_delete": 1},
    ]
```

The pipeline processes the three `input_json()` calls in separate
steps, in the order that the test makes them, and the listener receives
the changes of `customer_totals` in the same order:

| Call | Input | Expected change of `customer_totals` |
|------|-------|--------------------------------------|
| 1 | Insert order 1 for `alice` | Insert `alice, 10, 1` |
| 2 | The input under test: replace order 1 with an identical row | None |
| 3 | Insert order 2 for `bob` | Insert `bob, 5, 1` |

`read_changes(totals, 2)` returns when the `totals` listener has
received at least two changes.

### Tests with transactions

A transaction makes the pipeline process a group of inputs as one
unit.  The views change once, at commit, without exposing intermediate results.

```python
def check_transaction_changes_view_once(pipeline: Pipeline) -> None:
    # Connect the listener before the first step.
    pipeline.start_paused()
    totals = pipeline.listen("customer_totals")
    pipeline.resume()

    pipeline.start_transaction()
    # The pipeline processes the input only at commit, so do not wait for it.
    pipeline.input_json("orders", [{"id": 1, "customer": "alice", "amount": 10}], wait=False)
    pipeline.input_json("orders", [{"id": 2, "customer": "alice", "amount": 7}], wait=False)
    before_commit = list(pipeline.query("SELECT * FROM customer_totals"))
    pipeline.commit_transaction()
    changes = read_changes(totals, 1)
    pipeline.stop(force=True)
    pipeline.clear_storage()

    assert before_commit == []
    assert changes == [
        {"customer": "alice", "total": 17, "num_orders": 2, "insert_delete": 1}
    ]
```

Compare this result with the test in
[Check the contents of a view as a change stream](#check-the-contents-of-a-view-as-a-change-stream): the same
two orders without a transaction cause three changes.

Before the commit, `query()` shows the contents of the views from
*before* the start of the transaction.

### Testing programs that use the real-time clock `NOW()`

The `NOW()` function returns the current time of the system clock, in
UTC unless the `clock_timezone_offset` runtime setting gives another time
zone.  This makes programs using `NOW()` non-deterministic: if the same
program is run twice over the same input, the results can be different.
As described above, Feldera pipelines are fully deterministic, and the
value of the clock is supplied to a pipeline using a compiler-synthesized
table which has a single column of type `TIMESTAMP`.  By default a built-in
clock connector overwrites the sole value in this table periodically.

With these runtime settings, the clock connector stops following the
system clock, and the test sets the value of `NOW()`:

| Setting | Effect |
|---------|--------|
| `dev_tweaks.now_offset` | Initial value of the `NOW()` timestamp. |
| `dev_tweaks.now_http_driven` | `NOW()` advances only when the test calls `pipeline.advance_clock(delta_ms)`. |
| `clock_resolution_usecs` | The step of the clock, in microseconds.  `NOW()` is always a multiple of this value: the pipeline rounds `now_offset` down to it, and rounds the result of `pipeline.advance_clock(delta_ms)` up to it.  `pipeline.advance_clock()` with no argument moves `NOW()` forward by this value. |


```sql
CREATE TABLE events (
    id BIGINT NOT NULL,
    t TIMESTAMP NOT NULL
);

-- Events of the last hour.
CREATE MATERIALIZED VIEW recent AS
SELECT id FROM events WHERE t >= NOW() - INTERVAL 1 HOUR;

-- The clock value of the latest step, in milliseconds since the epoch.
CREATE MATERIALIZED VIEW clock AS SELECT CAST(NOW() AS BIGINT) AS now_ms;
```

The settings are part of the runtime configuration, so the program is
compiled with them:

```python
from feldera.runtime_config import RuntimeConfig

CLOCK_CONFIG = RuntimeConfig(
    clock_resolution_usecs=1_000_000,
    dev_tweaks={"now_offset": "2030-01-01T00:00:00Z", "now_http_driven": True},
)

pipeline = PipelineBuilder(
    client, name="test-recent", sql=SQL, runtime_config=CLOCK_CONFIG
).create_or_replace()
```

```python
MINUTE_MS = 60_000


def clock_ms(pipeline: Pipeline) -> int | None:
    rows = list(pipeline.query("SELECT now_ms FROM clock"))
    return rows[0]["now_ms"] if rows else None


def advance_clock(pipeline: Pipeline, delta_ms: int) -> None:
    """Move NOW() forward and wait until all views use the new value."""
    target_ms = pipeline.advance_clock(delta_ms)["now_ms"]
    wait_for_condition(
        f"NOW() is {target_ms} ms", lambda: clock_ms(pipeline) == target_ms
    )


def check_event_leaves_window(pipeline: Pipeline) -> None:
    pipeline.start()
    pipeline.input_json("events", [{"id": 1, "t": "2030-01-01 00:00:00"}])
    assert list(pipeline.query("SELECT id FROM recent")) == [{"id": 1}]

    advance_clock(pipeline, 30 * MINUTE_MS)
    assert list(pipeline.query("SELECT id FROM recent")) == [{"id": 1}]

    advance_clock(pipeline, 31 * MINUTE_MS)
    assert list(pipeline.query("SELECT id FROM recent")) == []
    pipeline.stop(force=True)
    pipeline.clear_storage()
```

`advance_clock()` returns before the pipeline completes a step with
the new value of `NOW()`.  The `clock` view shows the new value only
after that step completes.  All views agree after each step.

### Tests with fixed inputs

#### The datagen connector

The [datagen connector](/connectors/sources/datagen) generates the same rows
each time the pipeline starts, so it can be used to test programs with
fixed input data.  The following program generates 100 orders:

```sql
CREATE TABLE orders (
    id BIGINT NOT NULL PRIMARY KEY,
    customer VARCHAR NOT NULL,
    amount INT NOT NULL
) WITH (
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": { "plan": [{ "limit": 100 }] }
    }
  }]'
);

CREATE MATERIALIZED VIEW customer_totals AS
SELECT customer, SUM(amount) AS total, COUNT(*) AS num_orders
FROM orders
GROUP BY customer;
```

The generator gives each column the values 0, 1, ..., 99, so each
order has a different customer.

```python
def check_generated_orders(pipeline: Pipeline) -> None:
    pipeline.start()
    pipeline.wait_for_completion()
    rows = list(
        pipeline.query(
            "SELECT COUNT(*) AS customers, SUM(total) AS total FROM customer_totals"
        )
    )
    pipeline.stop(force=True)
    pipeline.clear_storage()

    assert rows == [{"customers": 100, "total": 4950}]
```

After it generates the `limit` rows of its plan, the connector signals
the end of its input.  Without a `limit` setting, the connector generates rows
forever, and `wait_for_completion()` never returns.

#### Reading S3 files

A test can also read a fixed input from a file in an object store.  The following
program uses the [S3 connector](/connectors/sources/s3) to read a JSON
file with three vendors from a public bucket:

```sql
CREATE TABLE vendor (
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR,
    address VARCHAR
) WITH (
  'connectors' = '[{
    "transport": {
      "name": "s3_input",
      "config": {
        "bucket_name": "feldera-basics-tutorial",
        "key": "vendor.json",
        "region": "us-west-1",
        "no_sign_request": true
      }
    },
    "format": { "name": "json" }
  }]'
);

CREATE MATERIALIZED VIEW vendor_names AS SELECT id, name FROM vendor;
```

```python
def check_vendors_from_s3(pipeline: Pipeline) -> None:
    pipeline.start()
    pipeline.wait_for_completion()
    rows = list(pipeline.query("SELECT id, name FROM vendor_names ORDER BY id"))
    pipeline.stop(force=True)
    pipeline.clear_storage()

    assert rows == [
        {"id": 1, "name": "Gravitech Dynamics"},
        {"id": 2, "name": "HyperDrive Innovations"},
        {"id": 3, "name": "DarkMatter Devices"},
    ]
```

As with datagen, `wait_for_completion()` returns after the connector has
read the whole file.  The test needs network access to the bucket.

### Compare with another SQL engine

Instead of writing the expected results by hand, a test can compute
them with another SQL engine from the same input.  This test generates
1000 orders, pushes them to the program of [A first test](#a-first-test),
and compares `customer_totals` with the result of the same query in
SQLite, which is part of the Python standard library:

```python
import random
import sqlite3


def expected_totals(orders: list[dict]) -> list[dict]:
    """Computes customer_totals with SQLite."""
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE orders (id INTEGER, customer TEXT, amount INTEGER)")
    db.executemany("INSERT INTO orders VALUES (:id, :customer, :amount)", orders)
    rows = db.execute(
        "SELECT customer, SUM(amount), COUNT(*) FROM orders "
        "GROUP BY customer ORDER BY customer"
    )
    return [
        {"customer": customer, "total": total, "num_orders": num_orders}
        for customer, total, num_orders in rows
    ]


def check_totals_match_sqlite(pipeline: Pipeline) -> None:
    rng = random.Random(1)
    orders = [
        {"id": i, "customer": rng.choice(["alice", "bob", "carol"]), "amount": rng.randint(1, 100)}
        for i in range(1000)
    ]
    pipeline.start()
    pipeline.input_json("orders", orders)
    rows = list(pipeline.query("SELECT * FROM customer_totals ORDER BY customer"))
    pipeline.stop(force=True)
    pipeline.clear_storage()

    assert rows == expected_totals(orders)
```

The fixed seed of `random.Random` makes the input the same in each run.
Use this approach only for queries that both engines support and
evaluate in the same way.  SQL dialects differ in many details.

The second engine can also be the one that runs the
[ad-hoc queries](/sql/ad-hoc) of the pipeline.  The test runs the query
of a view as an ad-hoc query, and compares the two results inside the
pipeline:

```python
def check_view_matches_adhoc_query(pipeline: Pipeline, view: str, query: str) -> None:
    """Fails if `view` and the ad-hoc `query` return different rows."""
    extra = list(pipeline.query(f"(SELECT * FROM {view}) EXCEPT ALL ({query})"))
    missing = list(pipeline.query(f"({query}) EXCEPT ALL (SELECT * FROM {view})"))
    assert extra == [] and missing == [], (extra, missing)


# After the pipeline has processed the input:
check_view_matches_adhoc_query(
    pipeline,
    "customer_totals",
    "SELECT customer, SUM(amount) AS total, COUNT(*) AS num_orders "
    "FROM orders GROUP BY customer",
)
```

`EXCEPT ALL` also finds a row that the two results contain a different
number of times.  The ad-hoc query reads the tables of the program, so
they must be materialized.
Ad-hoc queries use a different SQL engine than the pipeline, so the
same caveat applies: the query must produce the same results in
both dialects.  See
[Differences between Feldera SQL and ad-hoc queries](/sql/ad-hoc#differences-between-feldera-sql-and-ad-hoc-queries).

### Negative tests

#### Malformed programs

`PipelineBuilder(...).create_or_replace()` raises a `RuntimeError` when the program does
not compile.  The message contains the errors of the compiler.

```python
def check_unknown_column_is_rejected(client: FelderaClient) -> None:
    sql = "CREATE TABLE t (x INT);\nCREATE VIEW v AS SELECT y FROM t;"
    try:
        PipelineBuilder(client, name="test-bad-sql", sql=sql).create_or_replace()
    except RuntimeError as error:
        assert "failed to compile" in str(error)
    else:
        raise AssertionError("the program compiled")
    finally:
        client.delete_pipeline("test-bad-sql")
```

To check only whether a program compiles, `client.validate_program(sql)`
is much faster: it runs the SQL compiler, but it does not create a
pipeline or build the pipeline binary.  It does not raise an exception
for an error in the program.  It returns `{"Success": ...}`, or
`{"SqlError": ...}` with one message for each error:

```python
def check_unknown_column_is_rejected_quickly(client: FelderaClient) -> None:
    sql = "CREATE TABLE t (x INT);\nCREATE VIEW v AS SELECT y FROM t;"
    result = client.validate_program(sql)
    messages = result["SqlError"]["info"]["messages"]
    assert "Column 'y' not found in any table" in messages[0]["message"]
```

#### Runtime errors

Some errors happen only at runtime, for example an
arithmetic overflow.  When a pipeline crashes, it
stops, and `pipeline.deployment_error()` describes the error.

```sql
CREATE TABLE numbers (a TINYINT NOT NULL, b TINYINT NOT NULL);

CREATE MATERIALIZED VIEW sums AS SELECT a + b AS total FROM numbers;
```

```python
from feldera.enums import PipelineStatus
from feldera.rest.errors import FelderaAPIError


def check_overflow_stops_pipeline(pipeline: Pipeline) -> None:
    pipeline.start()
    try:
        # 120 + 100 does not fit in a TINYINT.
        pipeline.input_json("numbers", [{"a": 120, "b": 100}])
    except FelderaAPIError:
        pass
    wait_for_condition(
        "the pipeline stopped",
        lambda: pipeline.status() == PipelineStatus.STOPPED,
        timeout_s=60,
    )
    error = pipeline.deployment_error()
    pipeline.stop(force=True)
    pipeline.clear_storage()

    assert error["error_code"] == "RuntimeError.WorkerPanic"
    assert "'120 + 100' causes overflow for type TINYINT" in error["message"]
```

## Common problems

| Symptom | Cause | Fix |
|---------|-------|-----|
| `pipeline.query()` fails for a view. | The view is not materialized. | Declare it `CREATE MATERIALIZED VIEW`, or read its change stream instead of using `query`. |
| The listener misses changes, or receives changes from before it connected. | The test connected the listener to a running pipeline. | Connect the listeners while the pipeline is paused: `pipeline.start_paused()`, `pipeline.listen()`, `pipeline.resume()`. |
| The listener has fewer changes than expected. | The test read the listener before all changes arrived. | Wait with `read_changes()`. |
| A test fails at random. | The test sleeps for a fixed time. | Synchronize as shown in [Synchronization](#synchronization). |
| Tests fail at random when several tests run concurrently. | Pipeline names are unique within one account of a Feldera API.  `create_or_replace()` stops a running pipeline with the same name and replaces it, so one run destroys the pipeline of another. | Give the pipelines of each run different names, for example by adding a suffix that is unique to the run. |
| A comparison fails, but the rows or changes are correct. | Their order is not defined. | Use `ORDER BY` in the query, or `sorted_dicts()`. |
| A view with `LIMIT` and an ad-hoc query with the same `LIMIT` may return different rows. | When rows are tied on the `ORDER BY` columns, or there is no `ORDER BY`, `LIMIT` can keep any of the tied rows.  The pipeline and [ad-hoc queries](/sql/ad-hoc) use different SQL engines, which choose different rows. | Add columns to `ORDER BY`, for example the primary key. |
| `pipeline.input_json()` does not return during a transaction. | The pipeline processes the input only at commit. | Pass `wait=False`. |
| A floating-point value does not compare equal. | `pipeline.query()` returns floating-point values as `Decimal`, and rounding differs between engines. | Round the value, or `CAST` it to `VARCHAR` in the view. |
| A value has an unexpected type. | Listeners return pandas values, for example `pandas.Timestamp` for `TIMESTAMP`.  `pipeline.query()` returns JSON values, for example strings for `TIMESTAMP`. | Compare with values of the same type. |
| Column names do not match. | Feldera converts names that are not quoted to lowercase. | Use lowercase names in the expected rows. |
| A view shows wrong results after a delete. | The test deleted a row that a table without a primary key does not contain. | Delete only rows that the test inserted, or add a primary key. |
| A check passes although it should fail. | Python runs with `-O`, which removes `assert` statements. | Run the tests without `-O`. |

## Avoiding hanging programs

The examples above wait without a time limit: each blocking call
returns only when the pipeline has done its work.  If the pipeline is
stuck or does not even start, the test waits forever.  A test that runs unattended, for
example a unit test in a continuous integration job, must fail instead
of hanging.  Most blocking SDK calls accept a time limit:

| Call | Time limit | Error when the time is up |
|------|------------|---------------------------|
| `pipeline.input_json()` | `wait_timeout_s=...` | `FelderaTimeoutError` |
| `pipeline.start()`, `pipeline.start_paused()`, `pipeline.resume()` | `timeout_s=...` | `TimeoutError` |
| `pipeline.commit_transaction()` | `timeout_s=...` | `TimeoutError` |
| `pipeline.wait_for_completion()` | `timeout_s=...` | `TimeoutError` |
| `pipeline.stop()`, `pipeline.clear_storage()` | `timeout_s=...` | `FelderaTimeoutError` |
| Each HTTP request of a client | `FelderaClient(url, timeout=...)` | `FelderaTimeoutError` |
| `pipeline.input_pandas()`, `pipeline.execute(..., wait=True)`, compilation in `PipelineBuilder(...).create_or_replace()` | None | |

`FelderaTimeoutError` (from `feldera.rest.errors`) is not a subclass
of `TimeoutError`, so a test must catch both.

The polling helpers from [Helper functions](#helper-functions) take a
limit argument: pass `timeout_s` to `wait_for_condition()` or to
`read_changes()`, for example `read_changes(totals, 3, timeout_s=60)`.
They raise `TimeoutError` when the time is up.

```python
from feldera.rest.errors import FelderaTimeoutError

TIMEOUT_S = 60


def check_totals_with_time_limits(pipeline: Pipeline) -> None:
    pipeline.start(timeout_s=TIMEOUT_S)
    try:
        pipeline.input_json(
            "orders",
            [{"id": 1, "customer": "alice", "amount": 10}],
            wait_timeout_s=TIMEOUT_S,
        )
        rows = list(pipeline.query("SELECT * FROM customer_totals"))
    except (TimeoutError, FelderaTimeoutError):
        print("status:", pipeline.status(), "errors:", pipeline.errors())
        raise
    finally:
        pipeline.stop(force=True, timeout_s=TIMEOUT_S)
        pipeline.clear_storage(timeout_s=TIMEOUT_S)

    assert rows == [{"customer": "alice", "total": 10, "num_orders": 1}]
```

These methods can provide additional diagnostics:

| Operation | Shows |
|-----------|-------|
| `pipeline.status()` | The state of the pipeline, for example `RUNNING` or `PAUSED` |
| `pipeline.errors()` | The compilation errors, and the error that stopped the pipeline, if any |
| `pipeline.stats()` | The statistics, for example the number of input records received and processed |
| `pipeline.logs()` | The log of the pipeline, one line at a time |

## Further reading

* [Python SDK reference](pathname:///python/)
* [Ad-hoc SQL queries](/sql/ad-hoc)
* [Transactions](/pipelines/transactions)
* [JSON format](/formats/json)
* [Time series analysis](/tutorials/time-series)
