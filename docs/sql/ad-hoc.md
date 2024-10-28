# Ad-hoc SQL queries

You can run ad-hoc SQL queries on a running pipeline.
Unlike SQL programs that define pipelines and are evaluated incrementally, ad-hoc queries are evaluated using a batch engine against the current snapshot of pipeline's tables and views. Ad-hoc queries can be executed both when the pipeline is running or paused.

Ad-hoc queries provide a very cheap way to query the state of a materialized view. They are designed to aid development and debugging, so you need to be aware of their limitations to avoid potential confusion.

## Limitations

:::note

Tables and views are only accessible to `SELECT` ad-hoc queries if they are declared as [materialized](/sql/materialized).

:::

Feldera does not guarantee consistency between SQL dialects of Feldera programs and ad-hoc queries as the former is used in an incremental view maintenance (IVM) context and the latter is not, so more complex ad-hoc queries may produce different results than an SQL program.

Currently, only `SELECT` and `INSERT` statements are supported. You can not create or alter tables and views using ad-hoc SQL.

## Usage

Ad-hoc queries can be executed via different Feldera tools both when the pipeline is running or paused.

### Feldera Web Console

You can issue ad-hoc queries by opening the "Ad-hoc query" tab of the pipeline and typing a SQL SELECT query in the input text field. To submit the query, press `Enter` or the Play <icon icon="bx:play" /> button next to the query. To start a new line, press `Shift + Enter`. After successful execution of the query you will see a table containing the results. You can abort a long-running query by clicking the Stop <icon icon="bx:play" /> button or pressing `Ctrl + C`.

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

Refer to [CLI docs](/reference/cli) for more details.

### Feldera Python SDK

You can execute adhoc queries via the Python SDK using the [.query](pathname:///python/feldera.html#feldera.pipeline.Pipeline.query) method, which returns a generator of Python Dictionaries:
```py
gen_obj = pipeline.query("SELECT * FROM materialized_view;")
output = list(gen_obj)
```

There are variations of the `.query` method that return response in different formats:
- [.query_tabular](pathname:///python/feldera.html#feldera.pipeline.Pipeline.query_tabular)
  Returns a generator of `String`.
- [.query_parquet](pathname:///python/feldera.html#feldera.pipeline.Pipeline.query_parquet)
  Saves the output of this query to the parquet file.

For `INSERT` and `DELETE` queries it is recommended to use the [.execute](pathname:///python/feldera.html#feldera.pipeline.Pipeline.execute) method:

```py
pipeline.execute("INSERT INTO tbl VALUES(1, 2, 3);")
```

### REST API
Consult the [query endpoint](/api/execute-an-ad-hoc-query-in-a-running-or-paused-pipeline) reference to run ad-hoc queries directly through the API.