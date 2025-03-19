---
slug: /
---

# What is Feldera?

Feldera is a fast query engine for *incremental computation*. It has the [unique](/literature/papers) ability to evaluate arbitrary SQL programs incrementally, making it more powerful, expressive and performant than existing alternatives like batch engines, warehouses, stream processors or streaming databases.

Our approach to incremental computation is simple. A Feldera pipeline is a set of SQL tables and views. Views can be deeply nested. Users start, stop or pause pipelines to manage and advance a computation. Pipelines continuously process changes, which are any number of inserts, updates or deletes to a set of tables. When the pipeline receives changes, Feldera incrementally updates all the views by only looking at the changes and it completely avoids recomputing over older data. While a pipeline is running, users can inspect the results of the views at any time.

Our approach to incremental computation makes Feldera incredibly fast (millions of events per second on a laptop). It also enables **unified offline and online compute** over both live and historical data. Feldera users have built batch and real-time feature engineering pipelines, ETL pipelines, various forms of incremental and periodic analytical jobs over batch data, and more.

Our defining features:

1.  **Full SQL support and more**. Our engine is the only one in existence that can evaluate full SQL syntax and semantics completely incrementally. This includes joins and aggregates, group by, correlated subqueries, window functions, complex data types, time series operators, UDFs, and recursive queries. Pipelines can process deeply nested hierarchies of views.

2. **Fast out-of-the-box performance**. Feldera users have reported getting complex use cases implemented in 30 minutes or less, and hitting millions of events per second in performance on a laptop without any tuning.

3. **Datasets larger than RAM**. Feldera is designed to handle datasets that exceed the available RAM by spilling efficiently to disk, taking advantage of recent advances in NVMe storage.

4. **Strong guarantees on consistency and freshness**. Feldera is strongly consistent: it [guarantees](https://www.feldera.com/blog/synchronous-streaming/) that the state of the views always corresponds to what you'd get if you ran the queries in a batch system for the same input.

5. **Connectors for your favorite data sources and destinations**. Feldera connects to myriad batch and streaming data sources, like Kafka, HTTP, CDC streams, S3, Data Lakes, Warehouses and more. If you need a connector that we don't yet support, [let us know](https://github.com/feldera/feldera/issues/new/choose).

6. [**Fault tolerance**](/pipelines/fault-tolerance). Feldera can gracefully restart from the exact point of an abrupt shutdown or crash, picking up from where it left off without dropping or duplicating input or output. Fault tolerance is a preview feature that requires support from input and output connectors.

7. **Seamless ad-hoc queries**. You can run ad-hoc SQL queries on a running or paused pipeline to inspect or debug the
   state of materialized views. While these queries are evaluated in batch mode using Apache Datafusion, their
   results are consistent with the incremental engine's output for the same queries, aside from minor dialect and
   rounding differences.
