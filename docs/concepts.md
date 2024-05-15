# Concepts

Feldera processes queries and produces output continuously.  When
input arrives, Feldera recomputes query results and sends the changes
to outputs.  Feldera queries are written in SQL, so users who have an
existing investment in analyzing data at rest with a SQL database can
use much of the same code to analyze data continuously with Feldera.

## Incremental processing

Feldera is fundamentally **incremental** in how it handles input,
computation, and output.

For input, being incremental means that Feldera processes data as it
arrives. Feldera does not require all of the data to be on hand before
beginning computation.

For computation, being incremental means that when new data arrives,
Feldera does a minimal amount of work to update query results, rather
than by fully recomputing them. This is much faster than fully
recomputing queries from scratch.

For output, being incremental means that Feldera outputs query results
as sets of changes from the previous output. This improves performance
and reduces output size.

## Programs

In Feldera, a SQL **program** is a collection of SQL DDL (table and
view definitions):

* SQL table definitions with `CREATE TABLE` specify the format of
  data.  Feldera does not store tables, since it is not a database, so
  table definitions do not reserve disk space or other storage.

* SQL view definitions with `CREATE VIEW` specify transformations and
  computations.  Views may draw data from tables and from other views.
  Feldera provides powerful SQL analysis features, including
  time-series operators.

A program defines a computation, but it doesn't specify the source or
destination for data.  Those are the province of **connectors** and
**pipelines**, described below.

## Connectors

A **connector** gives Feldera computation access to data.  There are
two classes of connectors: **input connectors** that connect a source
of input records to a Feldera table, and **output connectors** that
connect a Feldera view to a destination.

Feldera includes input and output connectors for [Kafka], open source
event streaming software from the [Apache Software
Foundation][Apache].  Kafka's API is widely adopted, which means that
these connectors also allow Feldera to work directly with [RedPanda]
and other software that use the same API.

Feldera has a plug-in connector model.  This means that connectors can
be written to interface with whatever data sources and sinks a user
would find most convenient.

[Kafka]: https://kafka.apache.org/
[Apache]: https://www.apache.org/
[RedPanda]: https://redpanda.com/

## Pipelines

A user assembles a **pipeline** by attaching a program's tables to
input connectors and its views to output connectors. A Feldera
pipeline is the top-level unit of execution. Once a Feldera user
assembles a pipeline from a program and connectors, they may start,
stop, manage, and monitor it.

# Foundations

Feldera is the pioneering implementation of a new theory that unifies
databases, streaming computation, and incremental view maintenance,
written by the inventors of that theory. See our
[publications](/docs/papers) for all the details.

Feldera code is available on [Github][Feldera] using an MIT
open-source license. It consists of a Rust runtime and a SQL compiler.

[Feldera]: https://github.com/feldera/feldera
