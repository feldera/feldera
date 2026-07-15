# Ad-hoc SQL Queries

You can run ad-hoc SQL queries on a running or paused pipeline. Unlike Feldera SQL programs that define pipelines and
are evaluated incrementally, ad-hoc queries are evaluated in batch mode,
using the [datafusion engine](https://datafusion.apache.org).

Ad-hoc queries provide a way to query the state of [materialized](/sql/materialized) tables and views. They are designed to aid
development and debugging, so you need to be aware of their limitations to avoid potential confusion.

## Limitations

:::info

Tables and views are only accessible to `SELECT` ad-hoc queries if they are declared as [materialized](/sql/materialized).

:::

As of now, there are differences between the SQL dialects of Feldera SQL programs and ad-hoc queries.
This is because they use different engines (Apache Calcite for Feldera SQL vs. Apache Datafusion for ad-hoc queries).
See below for some examples.

Currently, only `SELECT` and `INSERT` statements are supported. You cannot create or alter tables and views using ad-hoc SQL.

### Differences between Feldera SQL and Ad-hoc Queries

For the common subset of SQL, results should be consistent, aside from minor differences
like floating-point rounding or decimal precision handling.

However, there are some known, notable differences in the SQL dialect between Feldera SQL and
ad-hoc queries that need to be taken into account:

- The order of output rows is non-deterministic in both dialects without the `ORDER BY` clause.
- Feldera SQL ignores the outermost order by clause.
- Feldera SQL's `TIMESTAMP_TRUNC(x, MINUTE)` is `DATE_TRUNC('MINUTE', x)` in ad-hoc queries.
- Feldera SQL's `SORT_ARRAY()` is `ARRAY_SORT()` in ad-hoc queries.
- Casting integers to timestamp conversion is interpreted as seconds in ad-hoc SQL and as milliseconds
  in Feldera SQL.
  (`SELECT 1729595568::TIMESTAMP;` will yield `2024-10-22T11:12:48` in ad-hoc queries and
  `1970-01-21 00:26:35` in Feldera SQL).
- Ad-hoc SQL cannot perform as-of joins.
- Feldera SQL reads fields of `VARIANT` values with subscripts (`doc['scores'][1]`, array indexes
  are 1-based); ad-hoc queries use
  [JSON functions](#querying-variant-columns-with-json-functions) instead
  (`json_get(doc, 'scores', 0)`, array indexes are 0-based).

We will continue to improve the consistency between the two engines in future releases.

## Usage

Ad-hoc queries can be executed via different Feldera tools both when the pipeline is running or paused.

### Feldera Web Console

You can issue ad-hoc queries by opening the "Ad-hoc query" tab of the pipeline and typing a SQL `SELECT` or `INSERT`
query in the input text field. To submit the query, press `Enter` or the Play <icon icon="bx:play" /> button next
to the query. To start a new line, press `Shift + Enter`. After successful execution of the query you will see a table
containing the results. You can abort a long-running query by clicking the Stop <icon icon="bx:stop" /> button or
pressing `Ctrl + C`.


![Browsing a materialized view in the Web Console](materialized-1.png)

### Feldera CLI

```bash
fda exec pipeline-name "SELECT * FROM materialized_view;"
```

```bash
cat query.sql | fda exec pipeline-name -s
```

Alternatively, you can enter the `fda shell` command to open an interactive shell and execute queries.

```bash
fda shell pipeline-name
```

Refer to [CLI docs](/interface/cli) for more details.

### Feldera Python SDK

You can execute ad-hoc queries via the Python SDK using the [.query](pathname:///python/feldera.html#feldera.pipeline.Pipeline.query) method, which returns a generator of Python Dictionaries:
```py
gen_obj = pipeline.query("SELECT * FROM materialized_view;")
output = list(gen_obj)
```

There are variations of the `.query` method that return response in different formats:
- [.query_tabular](pathname:///python/feldera.html#feldera.pipeline.Pipeline.query_tabular)
  Returns a generator of `String`.
- [.query_parquet](pathname:///python/feldera.html#feldera.pipeline.Pipeline.query_parquet)
  Saves the output of this query to the parquet file.

For `INSERT` queries it is recommended to use the [execute](pathname:///python/feldera.html#feldera.pipeline.Pipeline.execute) method:

```py
pipeline.execute("INSERT INTO tbl VALUES(1, 2, 3);")
```

### REST API

Consult the [query endpoint](/api/execute-ad-hoc-sql) reference to run ad-hoc queries directly through the API.

## Architecture

Ad-hoc queries are evaluated using the Apache Datafusion SQL engine against a consistent snapshot of the pipeline's
tables and views. This is achieved using a form of [Multiversion concurrency control](https://en.wikipedia.org/wiki/Multiversion_concurrency_control).
The datafusion engine reads data directly from the pipeline's storage layer, which is shared with the
Feldera SQL engine.

![Architectural Diagram Showing the Datafusion SQL engine in the Feldera pipeline](datafusion.png)

Ad-hoc queries can use CPU resources, memory and, to a lesser extent, storage (for intermediate results),
especially if they are complex or involve scanning large datasets. Since these resources are shared with the
Feldera SQL engine, such queries may reduce pipeline performance during ad-hoc query execution.

### Read-after-write within a multi-statement ad-hoc query

A multi-statement ad-hoc request reads from a single consistent snapshot
captured at the start of the request. Intermediate `INSERT`s in the same
request are applied to the pipeline, but a trailing `SELECT` does not
observe them:

```sql
INSERT INTO t VALUES (1);
SELECT COUNT(*) FROM t;  -- returns the count before the INSERT
```

The same rule applies inside a user transaction: the request observes the
state of all tables and materialized views as of the start of that
transaction, even when earlier statements in the same request inserted
into source tables.

## Examples

### Inserting Complex Data Types

Given the following Feldera SQL program:

```sql
create type struct_typ as (
  a int,
  b varchar
);

create table complex_types (
    a int array not null,
    b struct_typ not null,
    json variant not null,
    m map<varchar, int>,
    tup row(one int not null, two int not null)
) with ('materialized' = 'true');
```

An ad-hoc query to insert data into the `complex_types` table would look like this:

```sql
insert into complex_types values ([1,2,3], struct(2, 'b'), '{"field": 3}', MAP(['answer'], [42]), struct(2, 3));
```

### Querying VARIANT Columns with JSON Functions

Datafusion has no equivalent of the Feldera SQL [`VARIANT`](/sql/json) type. An ad-hoc query
receives a `VARIANT` column as a string that holds the JSON-encoded value, so the Feldera SQL
subscript syntax (`doc['name']`) does not work in ad-hoc queries. Instead, ad-hoc queries
take JSON strings apart with the
[datafusion-functions-json](https://github.com/datafusion-contrib/datafusion-functions-json)
function family. These functions also apply to string columns that hold JSON text.

| Function                          | Return type     | Description                                                            |
|-----------------------------------|-----------------|------------------------------------------------------------------------|
| `json_get(json, path...)`         | union           | Value at `path`; cast the result to select a concrete type (see below) |
| `json_get_str(json, path...)`     | `VARCHAR`       | String value at `path`                                                 |
| `json_get_int(json, path...)`     | `BIGINT`        | Integer value at `path`                                                |
| `json_get_float(json, path...)`   | `DOUBLE`        | Float value at `path`                                                  |
| `json_get_bool(json, path...)`    | `BOOLEAN`       | Boolean value at `path`                                                |
| `json_get_json(json, path...)`    | `VARCHAR`       | Raw JSON text of the value at `path`                                   |
| `json_get_array(json, path...)`   | array           | Array at `path`, each element as raw JSON text                         |
| `json_as_text(json, path...)`     | `VARCHAR`       | Value at `path` as text; also available as the `->>` operator          |
| `json_contains(json, path...)`    | `BOOLEAN`       | Whether a value exists at `path`                                       |
| `json_length(json, path...)`      | `BIGINT`        | Length of the object or array at `path`                                |
| `json_object_keys(json, path...)` | `VARCHAR` array | Keys of the object at `path`                                           |

A path is a sequence of object keys and 0-based array indexes:
`json_get_int(doc, 'scores', 0)` reads element 0 of the array under the `scores` key.
A missing key, a mismatched type, or a `NULL` document yields `NULL`.

Given

```sql
CREATE TABLE json_docs (id INT, doc VARIANT) WITH ('materialized' = 'true');
```

where `doc` holds documents such as `{"name": "Bob", "scores": [8, 10], "active": true}`,
the following ad-hoc query returns the names of active users whose first score exceeds 5:

```sql
SELECT id, json_get_str(doc, 'name') AS name
FROM json_docs
WHERE json_get_bool(doc, 'active') = TRUE
  AND json_get_int(doc, 'scores', 0) > 5;
```

Notes:

- `json_get` returns a value of a union type that only the `text` and `arrow_ipc` output
  formats can render. To use `json_get` results in the `json` or `parquet` output format,
  cast them to a concrete type; the cast is rewritten to the matching typed getter, so
  `json_get(doc, 'scores', 0)::BIGINT` and `CAST(json_get(doc, 'scores', 0) AS BIGINT)`
  both execute as `json_get_int(doc, 'scores', 0)`.
- Of the operators provided by the crate, only `->>` (alias of `json_as_text`) is
  available; `->` and `?` do not parse in the SQL dialect used by ad-hoc queries.

### Parameterized Queries with PREPARE / EXECUTE

Ad-hoc requests accept a `PREPARE` statement followed by a single
`EXECUTE` that binds positional parameters (`$1`, `$2`, ...) to literal
values. The two statements must be submitted together in the same
request, separated by a semicolon:

```sql
PREPARE q AS SELECT * FROM materialized_view WHERE v = $1;
EXECUTE q('2');
```

Only the final `EXECUTE` produces the result set; the preceding
`PREPARE` is consumed during planning. Prepared statement names do not
persist across requests, and `EXECUTE` parameters must be literal
values.

## See also

- Blog post [on inspecting Feldera Pipelines](https://www.feldera.com/blog/inspecting-feldera-pipelines).
- Tutorial using the [Feldera Web Console](/tutorials/basics/part1) to run ad-hoc queries.
