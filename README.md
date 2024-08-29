# Feldera: Incremental Computation Made Easy

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![CI workflow](https://github.com/feldera/feldera/actions/workflows/ci.yml/badge.svg)](https://github.com/feldera/feldera/actions)
[![CI nightly](https://github.com/feldera/feldera/actions/workflows/ci-nightly.yml/badge.svg)](https://github.com/feldera/feldera/actions)
[![Containers](https://github.com/feldera/feldera/actions/workflows/containers.yml/badge.svg)](https://github.com/feldera/feldera/actions/workflows/containers.yml)
[![Slack](https://img.shields.io/badge/slack-blue.svg?logo=slack)](https://www.feldera.com/community)
[![Discord](https://img.shields.io/badge/discord-blue.svg?logo=discord&logoColor=white)](https://discord.gg/5YBX9Uw5u7)
[![Sandbox](https://img.shields.io/badge/feldera_sandbox-blue?logo=CodeSandbox)](https://www.feldera.com/sandbox)

[Feldera](https://www.feldera.com) is a
fast query engine for *incremental computation*.
Feldera has the [unique](#theory) ability to *evaluate arbitrary SQL programs
incrementally*, making it more powerful, expressive and performant than existing
alternatives like batch engines, warehouses, stream processors or streaming databases.

Our approach to incremental computation is simple. A Feldera `pipeline` is a set of SQL tables and views. Views can be
deeply nested.
Users start, stop or pause pipelines to manage and advance a computation.
Pipelines continuously process
**changes**, which are any number of inserts, updates or deletes to a set of tables. When the pipeline receives changes,
Feldera **incrementally** updates all the views by only looking at the changes and it completely avoids recomputing over
older data.
While a pipeline is running, users can inspect the results of the views at any time.

Our approach to incremental computation makes Feldera incredibly fast (millions of events per second on a laptop).
It also enables unified offline and online compute over both live and historical data. Feldera users have built batch
and real-time
feature engineering pipelines, ETL pipelines, various forms of incremental and periodic analytical jobs over batch data,
and more.

Our defining features:

1. **Full SQL support and more.**  Our engine is the only one in existence that can evaluate full SQL
   syntax and semantics completely incrementally. This includes joins and aggregates, group by, correlated subqueries,
   window functions, complex data types, time series operators, UDFs, and
   recursive queries. Pipelines can process deeply nested hierarchies of views.

3. **Fast out-of-the-box performance.**  Feldera users have reported getting complex use cases
   implemented in 30 minutes or less, and hitting millions
   of events per second in performance on a laptop without any tuning.

4. **Datasets larger than RAM.** Feldera is designed to handle datasets
   that exceed the available RAM by spilling efficiently to disk, taking advantage of recent advances in NVMe storage.

5. **Strong guarantees on consistency and freshness.** Feldera is strongly consistent. It
   also [guarantees](https://www.feldera.com/blog/synchronous-streaming/) that the state of the views always corresponds
   to what you'd get if you ran the queries in a batch system for the same input.

6. **Connectors for your favorite data sources and destinations.** Feldera connects to myriad batch and streaming data
   sources, like Kafka, HTTP, CDC streams, S3, Data Lakes, Warehouses and more.
   If you need a connector that we don't yet support, let us [know](https://github.com/feldera/feldera/issues).

## Architecture

The following diagram shows Feldera's architecture

![Feldera Platform Architecture](architecture.svg)

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
to bring it up. We suggest going through our [tutorial](https://www.feldera.com/docs/tutorials/basics/) next.

Our [Getting Started](https://www.feldera.com/docs/get-started) guide has more detailed instructions on running the
demo.

## Running Feldera from sources

To run Feldera from sources, first install all the required
[dependencies](CONTRIBUTING.md). This includes the Rust toolchain (at least 1.75), Java (at
least JDK 19), Maven and Typescript.

After that, the first step is to build the SQL compiler:

```
cd sql-to-dbsp-compiler
./build.sh
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
[documentation](https://www.feldera.com/docs).

* [Getting started](https://www.feldera.com/docs/get-started)
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
