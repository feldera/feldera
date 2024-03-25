# Running Nexmark Benchmarks with Apache Beam

Apache Beam is a layer over other data processing systems with
multiple backends ("runners").  It includes its own implementation of
the Nexmark benchmarks.  These instructions document how to run them
for four of its runners:

  * Direct: This is built into Beam.  This is meant for checking
    correctness and is not optimized for performance.

  * Flink

  * Spark

  * Google Cloud Dataflow

Each runner supports both batch and streaming processing.  In
addition, Beam's Nexmark suite includes an implementation of each
query in as many as three forms.  All of them are implemented using the
Beam native representation in terms of transforms and aggregations.
Some of them are also implemented in two forms of SQL: "standard" SQL
and ZetaSQL.

For a Beam overview, see
https://beam.apache.org/get-started/beam-overview/.  For information
about the Nexmark benchmark for Beam, see
https://beam.apache.org/documentation/sdks/java/testing/nexmark/.

## Prerequisites

Install the Java Development Kit.  These instructions were tested with
OpenJDK 21.0.2.

## Setting up Beam

You can follow the instructions below to build Nexmark, or run
`setup.sh` in this directory.

1. Clone the Beam repository:

   ```
   git clone https://github.com/apache/beam.git
   ```

   If you wish to benchmark a particular version, check it out:

   ```
   (cd beam && git checkout origin/release-2.55.0)
   ```

2. Apply `configurable-spark-master.patch`:

   ```
   (cd beam && git am < ../configurable-spark-master.patch)
   ```

There's no need to run a separate build step.  Beam will build the
first time you run a benchmark.

## Running the benchmarks

Use `run-nexmark.sh` in the parent directory.
