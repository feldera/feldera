# Materialized Tables and Views

By default, Feldera does not maintain the complete contents of tables and views; it only
stores the data necessary to compute future outputs. However, in some cases, users
may need to inspect or query the entire contents of a relation.  This can
be useful in the following scenarios:

* **Debugging**. The user may want to inspect the current contents of tables and views
  to validate their SQL program.
* **Retrieve full state snapshot**.  This is useful, for instance, to sync the output of Feldera with an external
  database on demand.
* **Ad hoc queries**.  In some applications, users may not want to store a complete copy of the data,
  but instead query it on demand.

Feldera supports such use cases by allowing users to label tables and views as **materialized**.
To declare a materialized table, use the materialized attribute:

```sql
CREATE TABLE my_table (...) WITH ('materialized' = 'true');
```

To declare a materialized view, use the `CREATE MATERIALIZED VIEW` syntax:

```sql
CREATE MATERIALIZED VIEW my_view as SELECT * from my_table;
```

These declarations instruct Feldera to maintain a complete snapshot of the table or view.

## Using materialized tables and views

### Web Console

You can browse materialized tables and views in the Feldera Web Console by clicking on the "eye"
icon next to the table or view of a running pipeline:

![Browsing a materialized view in Web Console](../tutorials/basics/preferred-vendor1.png)

### Ad hoc queries

:::caution Under Construction

We are implementing ad hoc querying support for materialized tables and views.

:::


## Usage considerations

Materialized relations can significantly increase the storage used by the program.
For example, Feldera can evaluate simple programs with no joins or aggregates without keeping
any state.  However, Mmaterializing inputs or outputs of such programs will make them
**stateful**, requiring storage proportional to the size of the materialized
relations.

Feldera takes advantage of [`LATENESS` annotations](streaming.md#lateness-expressions)
to garbage collect old records in time series tables and views (i.e., tables and views with
monotonically or near-monotonically growing timestamps).  This allows evaluating complex queries
over unbounded streams using bounded storage.  Materializing these tables forces Feldera to keep
their entire history, resulting in unbounded storage growth.

Finally note that materialized tables and views are **not a performance optimization**.
Feldera automatically maintains all the state needed to incrementally evaluate user queries
efficiently.  Materializing additional relations will not make it faster, but can actually
slow it down, as it needs to write more data to storage.
