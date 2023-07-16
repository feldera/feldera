# The Feldera Continuous Analytics Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![CI workflow](https://github.com/feldera/dbsp/actions/workflows/ci.yml/badge.svg)](https://github.com/feldera/dbsp/actions)
[![codecov](https://codecov.io/gh/feldera/dbsp/branch/main/graph/badge.svg?token=0wZcmD11gt)](https://codecov.io/gh/feldera/dbsp)
[![nightly](https://github.com/feldera/dbsp/actions/workflows/containers.yml/badge.svg)](https://github.com/feldera/dbsp/actions/workflows/containers.yml)


The [Feldera Continuous Analytics Platform](https://www.feldera.com/), or Feldera in short, is a
fast computational engine for *continuous analytics* over data in-motion. It
allows users to build data pipelines as SQL programs that are continuously
evaluated as new data arrives from various sources. What makes Feldera
[unique](#theory) is its ability to *evaluate arbitrary SQL programs
incrementally*, making it more expressive and performant than existing
alternatives like streaming engines.

With Feldera, software engineers and data scientists building data pipelines
are not exposed to to the complexities of querying changing data, an otherwise
notoriously hard problem. Instead, they can express their
computations as declarative queries and have Feldera evaluate
these queries incrementally, correctly and efficiently.

To this end we set the following high-level objectives:

1. **Full SQL support and more.** Our goal is to support the complete SQL
   syntax and semantics, including joins and aggregates, correlated subqueries,
   window functions, complex data types, time series operators, UDFs, and
   recursive queries.

1. **Scalability in multiple dimensions.**  The engine scales with the number
   and complexity of queries, input data rate and the amount of state the
   system maintains in order to process the queries.

1. **Performance out of the box.**  The user should be able to focus on the
   business logic of their application, leaving it to the system to evaluate
   this logic efficiently.


## Architecture

With Feldera, users create data pipelines out of SQL programs and data
connectors. An SQL program comprises tables and views. Connectors feed data to
input tables in a program or receive outputs computed by views. Example
connectors we currently support are Kafka, Redpanda and an HTTP API to push/pull
directly to and from tables/views. We are working on more connectors such as
ones for database CDC streams. Let us know of any connectors you'd like us to
cover.

Feldera fundamentally operates on changes to data, i.e., inserts and deletes to
tables. This model covers all kinds of data in-motion use cases, like
insert-only streams of event, log, HTTP and timeseries data, as well as changes
to traditional databases extracted via CDC streams.

The following diagram shows Feldera's architecture.

![Feldera Architecture](architecture.svg)

## What's in this repository?

This repository comprises all the buildings blocks to run continuous analytics pipelines using Feldera.

* [web UI](web-ui): a web interface for writing SQL, setting up connectors, and managing pipelines.
* [pipeline-manager](crates/pipeline_manager): serves the web UI and is the REST API server for building and managing data pipelines.
* [dbsp](crates/dbsp): the core [engine](#theory) that allows us to evaluate arbitrary queries incrementally.
* [SQL compiler](sql-to-dbsp-compiler): translates SQL programs into DBSP programs.
* [connectors](crates/adapters/): to stream data in and out of Feldera pipelines.

## Quick start

First, make sure you have [Docker Compose](https://docs.docker.com/compose/) installed.

Next, run the following command to download a Docker Compose file, and use it to bring up
a DBSP deployment suitable for demos, development and testing:

```text
curl https://raw.githubusercontent.com/feldera/dbsp/main/deploy/docker-compose.yml | docker compose -f - --profile demo up
```

It can take some time for the container images to be downloaded. About ten seconds after that, the DBSP
web interface will become available. Visit [http://localhost:8085](http://localhost:8085) on your browser
to bring it up. We suggest going through our [demos](https://docs.feldera.io/docs/demos) next.

Our [Getting Started](https://docs.feldera.io/docs/intro) guide has more detailed instructions on running the demo.

## Documentation

To learn more about Feldera, we recommend going through the [documentation](https://docs.feldera.io/docs/intro).

* [Getting started](https://docs.feldera.io/docs/intro)
* [UI tour](https://docs.feldera.io/docs/tour/)
* [Demos](https://docs.feldera.io/docs/category/demos)
* [SQL reference](https://docs.feldera.io/docs/sql/intro)
* [API reference](https://docs.feldera.io/docs/api/rest/)

## Theory

Feldera achieves its objectives by building on a solid mathematical
foundation.  The formal model that underpins our system, called DBSP, is
described in the accompanying paper:

- [Budiu, Chajed, McSherry, Ryzhyk, Tannen. DBSP: Automatic
  Incremental View Maintenance for Rich Query Languages, Conference on
  Very Large Databases, August 2023, Vancouver,
  Canada](https://github.com/feldera/dbsp/blob/main/docs/static/vldb23.pdf)

- Here is the [a presentation about DBSP](https://www.youtube.com/watch?v=iT4k5DCnvPU) at the 2023
Apache Calcite Meetup.

The model provides two things:

1. **Semantics.** DBSP defines a formal language of streaming operators and
queries built out of these operators, and precisely specifies how these queries
must transform input streams to output streams.

1. **Algorithm.** DBSP also gives an algorithm that takes an arbitrary query and
generates an incremental dataflow program that implements this query correctly (in accordance
with its formal semantics) and efficiently.  Efficiency here means, in a
nutshell, that the cost of processing a set of input events is proportional to
the size of the input rather than the entire state of the database.
