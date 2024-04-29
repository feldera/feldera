# The Feldera Continuous Analytics Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![CI workflow](https://github.com/feldera/feldera/actions/workflows/ci.yml/badge.svg)](https://github.com/feldera/feldera/actions)
[![nightly](https://github.com/feldera/feldera/actions/workflows/containers.yml/badge.svg)](https://github.com/feldera/feldera/actions/workflows/containers.yml)
[![slack](https://img.shields.io/badge/slack-blue.svg?logo=slack)](https://www.feldera.com/community)
[![discord](https://img.shields.io/badge/discord-blue.svg?logo=discord&logoColor=white)](https://discord.gg/5YBX9Uw5u7)
[![sandbox](https://img.shields.io/badge/feldera_sandbox-blue?logo=CodeSandbox)](https://www.feldera.com/sandbox)

The [Feldera Continuous Analytics Platform](https://www.feldera.com), or Feldera Platform in short, is a
fast computational engine and associated components for *continuous analytics* over data in motion. Feldera Platform
allows users to configure data pipelines as standing SQL programs (DDLs) that are continuously
evaluated as new data arrives from various sources. What makes Feldera's engine
[unique](#theory) is its ability to *evaluate arbitrary SQL programs
incrementally*, making it more expressive and performant than existing
alternatives like streaming engines.

With the Feldera Platform, software engineers and data scientists configuring data pipelines
are not exposed to to the complexities of querying changing data, an otherwise
notoriously hard problem. Instead, they can express their
computations as *standing queries* and have the Feldera Platform evaluate
these queries incrementally, correctly and efficiently.

To this end we set the following high-level objectives:

1. **Full SQL support and more.** Our goal is to support the complete SQL
   syntax and semantics, including joins and aggregates, correlated subqueries,
   window functions, complex data types, time series operators, UDFs, and
   recursive queries.

1. **Scalability in multiple dimensions.**  The platform scales with the number
   and complexity of queries, input data rate and the amount of state the
   system maintains in order to process the queries.

1. **Performance out of the box.**  The user should be able to focus on the
   business logic of their application, leaving it to the system to evaluate
   this logic efficiently.

## Architecture

With Feldera Platform, users create data pipelines out of SQL programs and data
connectors. An SQL program comprises tables and views. Connectors feed data to
input tables in a program or receive outputs computed by views. Example
connectors currently supported are Kafka, Redpanda and an HTTP API to push/pull
directly to and from tables/views. We are working on more connectors such as
ones for database CDC streams. Let us know of any connectors you would like us to
develop.

Feldera Platform fundamentally operates on changes to data, i.e., inserts and deletes to
tables. This model covers all kinds of data in-motion use cases, like
insert-only streams of event, log, HTTP and timeseries data, as well as changes
to traditional databases extracted via CDC streams.

The following diagram shows Feldera Platform's architecture.

![Feldera Platform Architecture](architecture.svg)

## What is in this repository?

This repository comprises all the buildings blocks to run continuous analytics
pipelines using Feldera Platform.

* [web UI](web-console): a web interface for writing SQL, setting up connectors, and managing pipelines.
* [pipeline-manager](crates/pipeline_manager): serves the web UI and is the REST API server for building and managing
  data pipelines.
* [dbsp](crates/dbsp): the core [engine](#theory) that allows us to evaluate arbitrary queries incrementally.
* [SQL compiler](sql-to-dbsp-compiler): translates SQL programs into DBSP programs.
* [connectors](crates/adapters/): to stream data in and out of Feldera Platform pipelines.

## Quick start with Docker

First, make sure you have [Docker Compose](https://docs.docker.com/compose/) installed.

Next, run the following command to download a Docker Compose file, and use it to bring up
a Feldera Platform deployment suitable for demos, development and testing:

```text
curl -L https://github.com/feldera/feldera/releases/latest/download/docker-compose.yml | \
docker compose -f - --profile demo up
```

It can take some time for the container images to be downloaded. About ten seconds after that, the Feldera
web console will become available. Visit [http://localhost:8080](http://localhost:8080) on your browser
to bring it up. We suggest going through our [demo](https://www.feldera.com/docs/demo) next.

Our [Getting Started](https://www.feldera.com/docs/intro) guide has more detailed instructions on running the demo.

## Running Feldera from sources

To run Feldera from sources, first install all the required
[dependencies](CONTRIBUTING.md). This includes the Rust toolchain (at least 1.75), Java (at
least JDK 19), Maven and Typescript.

After that, the first step is to build the SQL compiler:

```
cd sql-to-dbsp-compiler
mvn package -DskipTests
```

Next, from the repository root, run the pipeline-manager:

```
cargo run --bin=pipeline-manager --features pg-embed
```

As with the Docker instructions above, you can now visit
[http://localhost:8080](http://localhost:8080) on your browser to see the
Feldera WebConsole.

## Documentation

To learn more about Feldera Platform, we recommend going through the
[documentation](https://www.feldera.com/docs/intro).

* [Getting started](https://www.feldera.com/docs/intro)
* [UI tour](https://www.feldera.com/docs/tour)
* [Tutorials](https://www.feldera.com/docs/tutorials)
* [Demo](https://www.feldera.com/docs/demo)
* [SQL reference](https://www.feldera.com/docs/sql/intro)
* [API reference](https://www.feldera.com/api)

## Contributing

Most of the software in this repository is governed by an open-source license.
We welcome contributions. Here are some [guidelines](CONTRIBUTING.md).

## Theory

Feldera Platform achieves its objectives by building on a solid mathematical
foundation. The formal model that underpins our system, called DBSP, is
described in the accompanying paper:

- [Budiu, Chajed, McSherry, Ryzhyk, Tannen. DBSP: Automatic
  Incremental View Maintenance for Rich Query Languages, Conference on
  Very Large Databases, August 2023, Vancouver,
  Canada](https://www.feldera.com/vldb23.pdf)

- Here is [a presentation about DBSP](https://www.youtube.com/watch?v=iT4k5DCnvPU) at the 2023
  Apache Calcite Meetup.

The model provides two things:

1. **Semantics.** DBSP defines a formal language of streaming operators and
   queries built out of these operators, and precisely specifies how these queries
   must transform input streams to output streams.

1. **Algorithm.** DBSP also gives an algorithm that takes an arbitrary query and
   generates an incremental dataflow program that implements this query correctly (in accordance
   with its formal semantics) and efficiently. Efficiency here means, in a
   nutshell, that the cost of processing a set of input events is proportional to
   the size of the input rather than the entire state of the database.
