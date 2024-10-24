# Ad-hoc SQL queries

Unlike SQL programs that define pipelines and are evaluated incrementally, ad-hoc queries are evaluated against the current snapshot of pipeline's tables and views. Ad-hoc queries can be executed both when the pipeline is running or paused.

Feldera does not guarantee consistency between SQL dialects of Feldera programs and ad-hoc queries as the former is used in an incremental view maintenance (IVM) context and the latter is not, so more complex ad-hoc queries may produce different results than an SQL program.

Ad-hoc queries are designed to aid exploring data, development and debugging, so you need to be aware of these limitations to avoid potential confusion.

Different Feldera tools provide you with ability to run ad-hoc queries:

### Feldera Web Console

You can issue ad-hoc queries by opening the "Ad-hoc query" tab of the pipeline and typing a SQL SELECT query in the input text field. To submit the query, press `Enter` or the Play <icon icon="bx:play" /> button next to the query. To start a new line, press `Shift + Enter`. After successful execution of the query you will see a table containing the results. You can abort a long-running query by pressing the Stop <icon icon="bx:play" /> button or pressing Ctrl + C

![Browsing a materialized view in the Web Console](materialized-1.png)

### Feldera CLI

You can execute ad-hoc queries using the Feldera CLI. The command `fda exec` allows you to execute a SQL query
against the pipeline's materialized tables and views.

```bash
fda exec pipeline-name "SELECT * FROM materialized_view;"
cat query.sql | fda exec pipeline-name -s
```

Alternatively, you can enter the `fda shell` command to open an interactive shell and execute queries.

```bash
fda shell pipeline-name
```

### Feldera Python SDK

You can execute adhoc queries via the Python SDK using the [.query](/python/feldera.html#feldera.pipeline.Pipeline.query) method, which returns a generator of Python Dictionaries:
```py
gen_obj = pipeline.query("SELECT * FROM materialized_view;")
output = list(gen_obj)
```

There are variations of the `query` method that return response in different formats:
- [.query_tabular](/python/feldera.html#feldera.pipeline.Pipeline.query_tabular)
  Returns a generator of `String`.
- [.query_parquet](/python/feldera.html#feldera.pipeline.Pipeline.query_parquet)
  Saves the output of this query to the parquet file.

#### Insert & Delete

If you want to execute `INSERT` and `DELETE` queries, use the method [execute](/python/feldera.html#feldera.pipeline.Pipeline.execute).
Unlike `query`, this method processes the query **eagerly** and discards the result.

Example:
```py
pipeline.execute("INSERT INTO tbl VALUES(1, 2, 3);")
```

### Rest API
Consult the [API reference](/api/execute-an-ad-hoc-query-in-a-running-or-paused-pipeline) to run ad-hoc queries directly through HTTP requests