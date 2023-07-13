# What is DBSP?

DBSP, which stands for "DataBase Stream Processor," is a fast, open
source streaming query engine with a rich SQL feature set.  With DBSP,
sophisticated SQL written for databases such as Postgres and MySQL can
operate on streaming data with few changes.  This site explains the
concepts behind DBSP and how to use it through its user interface and
API.

## Concepts

A database stores data and allows it to be updated and queried.  DBSP,
in contrast, processes and analyzes streams of data.  DBSP provides
results to queries "on-line": as input data arrives, in small or large
batches, DBSP outputs updates to all the queries given by the user.
DBSP queries are written in SQL, so users who have an existing
investment in analyzing data at rest with a SQL database can use much
the same code to analyze streaming data with DBSP.

### Incremental processing

DBSP is fundamentally **incremental** in how it handles input,
computation, and output.

For input, being incremental means that DBSP processes data as it
arrives from input streams.  DBSP does not require all of the data to
be on hand before beginning computation.  Unlike a database, DBSP does
not durably store data.

For computation, being incremental means that when new data arrives,
DBSP does a minimal amount of work to update query results, rather
than by fully recomputing them.  This speeds up processing.

For output, being incremental means that DBSP streams query results to
output streams, in a similar way to how it accepts its input.  When a
query result changes, DBSP writes only the changes, also called an
"update" or "delta", to the query's output stream.

### Programs

In DSBP, a SQL streaming **program** is a collection of SQL table and
view definitions:

* SQL table definitions with `CREATE TABLE` specify the format of
  data.  DBSP does not store tables, since it is not a database, so
  table definitions do not reserve disk space or other storage.

* SQL view definitions with `CREATE VIEW` specify analyses.  Views may
  draw data from tables and from other views.  DBSP provides powerful
  SQL analysis features, including time-series operators.

A program defines a computation, but it doesn't specify the source or
destination for data.  Those are the province of **connectors** and
**pipelines**, described below.

### Connectors

A **connector** gives a DBSP computation access to data.  There are
two classes of connectors: **input connectors** that connect a source
of input records to a DBSP table, and **output connectors** that
connect a DSBP view to a destination.

DBSP includes input and output connectors for [Kafka], open source
event streaming software from the [Apache Software
Foundation][Apache].  Kafka's API is widely adopted, which means that
these connectors also allow DBSP to work directly with [RedPanda] and
other software that use the same API.

DBSP has a plug-in connector model.  This means that connectors can be
written to interface with whatever data sources and sinks a user would
find most convenient.

[Kafka]: https://kafka.apache.org/
[Apache]: https://www.apache.org/
[RedPanda]: https://redpanda.com/

### Pipelines

A **pipeline** combines a SQL program with a mapping from input
connectors to tables and from views to output connectors.  A DBSP
pipeline is the top-level unit of execution.  Once a DBSP user
assembles a pipeline from from a program and connectors, they may
start, stop, manage, and monitor it.

## Foundations

DBSP is the pioneering implementation of a new theory that unifies
databases, streaming computation, and incremental view maintenance,
written by the inventors of that theory.  See our
[publications](papers) for all the details.

The DBSP code is available on [Github][DBSP] using an MIT open-source
license.  It consists of a Rust runtime and a SQL compiler.

[DBSP]: https://github.com/feldera/dbsp
