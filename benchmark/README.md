# Comparative Benchmarking

It's useful to be able to run the same benchmark on Feldera and other
systems.  The `run-nexmark.sh` script in this directory supports
running the [Nexmark](https://datalab.cs.pdx.edu/niagara/NEXMark/)
benchmarks in comparable ways across a few different systems,
currently:

  * Feldera.

  * Flink.

  * Apache Beam, with the Flink, Spark, and Google Cloud Dataflow
    runners.

## Running individual benchmarks

Run `run-nexmark.sh --help` to see its options.  The most important
options are:

  * The underlying system to use, with `--runner`:

    - `--runner=feldera` for Feldera.

	- `--runner=flink` for standalone Flink.

	- `--runner=beam/direct` for Apache Beam with its built-in
      "direct" runner.  (This runner isn't optimized for performance.)

	- `--runner=beam/flink` for Apache Beam with Flink as the
      underlying runner.

	- `--runner=beam/spark` for Apache Beam with Spark.

	- `--runner=beam/dataflow` for Apache Beam on top of Google Cloud
      Dataflow.

    Most of the runners require some extra configuration (see below).

  * Streaming analysis is the default.  Use `--batch` for batch
    analysis.

  * By default, only 100k events are generated.  This only takes a few
    seconds.  Add `--events=100M` to analyze 100,000,000 events.

  * The Apache Beam runners support multiple forms of some queries.
    By default, they are implemented in Beam's native Java-based form.
    Add `--language=sql` to use Calcite-based SQL or
    `--language=zetasql` to use ZetaSQL instead.  The two SQL options
    will run fewer queries because not all the queries have been
    formulated that way.

  * Set the number of cores to use with `--cores`.  The default is
    however many cores your system has, but no more than 16.  Feldera
    uses the exact number of cores specified; some of the other
    runners only approximate the number of cores.

`run-nexmark.sh` prints the commands that it runs before it runs them.
You can cut-and-paste those commands to run them directly later, if
you like.  Or you can pass `--dry-run` to make it just print the
commands it would run without running them at all.

## Running benchmarks across multiple runners

You can run multiple benchmarks using `run-nexmark.sh` with different
options, or you can use `suite.mk`, which is a wrapper around
`run-nexmark.sh`, to run several of them together.  Take a look at
`suite.mk` itself for more information.

# Setting up the runners

## Feldera setup

`run-nexmark.sh` supports Feldera for the Nexmark benchmarks
implemented in Rust (the default) and SQL.  The Rust implementation is
more mature and more complete than the SQL implementation.

## Feldera setup for SQL

To use `run-nexmark.sh` for the SQL implementation of Nexmark with
Feldera, first follow the instructions for step 1 in
`feldera-sql/README.md`, that is, start Kafka (or Redpanda) and the
Feldera pipeline manager.  If you start them as suggested there, then
nothing additional is needed; otherwise, you might need to use
`--kafka-broker` and `--api-url` to tell `run-nexmark.sh` where to
find them.

The first time you run `run-nexmark.sh` for a given number of events
with a particular Kafka broker, the script will generate that number
of events into the Kafka broker, which can take a while.  Afterward,
subsequent runs with the same broker and event count will run faster.

## Feldera setup for Rust

The Rust implementation does not need special setup.

Feldera uses temporary files for storage, by default in `/tmp`.  If
`/tmp` is `tmpfs`, as is the default on Fedora and some other
distributions, then these files will really be written into memory.
In that case, consider setting `TMPDIR` in the environment to a
directory on a real filesystem, e.g.:

```
TMPDIR=/var/run ./run-nexmark.sh
```

## Flink setup

To use `run-nexmark.sh` with Flink, first follow the instructions for
building Nexmark in `flink/README.md`.  This can be as simple as:

```
(cd flink && ./build.sh)
```

## Beam setup

To use `run-nexmark.sh` with Beam, first follow the instructions for
building Nexmark in `beam/README.md`.  This can be as simple as:

```
(cd beam && ./setup.sh)
```

### Google Cloud Dataflow on Beam

Benchmarking Beam with Google Cloud Dataflow requires extra setup to
create and configure a GCP project.  These steps only need to be run
once, unless you delete that project.

Use the Google Cloud web UI to set up the project, following the steps
under "Before you begin" at
https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-java
Keep track of the `PROJECT_ID` and `BUCKET_NAME` that you use.
`BUCKET_NAME` is globally visible and must be globally unique, so you
might want to generate it randomly, e.g. `BUCKET_NAME=$(uuidgen)`.

# Using the runners

## Apache Beam

The first run with Beam will take longer, to build the code base.

Warnings during runs that look like the following are normal and
harmless:

```
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [...]
SLF4J: [...more lines like the previous...]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.JDK14LoggerFactory]
```

Messages like the following will also sometimes appear.  Despite the
apparent severity, they do not indicate a real problem:

```
Apr 10, 2023 2:54:03 PM org.apache.beam.sdk.testutils.metrics.MetricsReader getCounterMetric
SEVERE: Failed to get metric fatal, from namespace Query5
```

## Beam with Spark

Streaming runs with Spark on Beam hang.  Use `--batch` to avoid hangs.

## Beam with Google Cloud Dataflow

Provide the project ID and bucket name to `run-nexmark.sh` to use the
Dataflow runner, e.g.:

```
./run-nexmark.sh --runner=beam/dataflow --project=$PROJECT_ID --bucket=$BUCKET_NAME
```

`run-nexmark.sh` tries to approximate the number of cores specified on
`--cores` in terms of 4-core workers (GCP virtual machines), e.g. with
`--cores=16` it will use 4 workers.

Google limits newly created GCP accounts to 24 cores.
`run-nexmark.sh` runs as many tests in parallel as it can within this
limit given the number of workers; for example, with 4 workers, this
is just 1, since 4×4 = 16 is less than 24 but 2×(4×4) = 32 is greater.
If you successfully requested an increased CPU core quota from Google
and want `run-nexmark.sh` to use it, specify the maximum number of
cores to use with `--max-cores=N`.

`run-nexmark.sh` defaults to starting Dataflow in the us-west1 region.
Use `--region=REGION` to specify a different region.

### Notes

Running Nexmark on GCP will take longer than running it locally
because it takes time to spin up the Dataflow VMs.  In a practical
deployment this overhead would be amortized over the run, so the
command reports timings omitting this overhead.

Dataflow runs of Nexmark tend to print a number of warnings along the
lines of "Failed to received monitoring results."  The run will still
complete successfully.

You can monitor Dataflow runs from the Google cloud console at
https://console.cloud.google.com/projectselector2/dataflow/jobs.  When
a Nexmark run finishes, all jobs should be completed.  If not, this is
probably due to an error, and you should cancel the jobs manually so
that you don't end up getting billed for unintentional usage.  If you
cancel a run with Control+C or similar, cancel the underlying jobs the
same way.

To make sure that you don't get billed further, you can delete the
project that you created for benchmarking.  The ongoing cost of the
project is minimal (perhaps $1 per month or less) as long as no jobs
run, so there is not much reason to delete it.

# Sample results

The following table, produced using `suite.mk` and `analysis.sps`,
shows results obtained on one particular system:

```
                              16-core Nexmark Streaming Performance
╭────────┬─────────────────────────────────────────────────────────────────────────────────────╮
│        │                                        runner                                       │
│        ├─────────┬───────────────────┬──────────────────────────┬────────────────────────────┤
│        │ Feldera │       Flink       │       Flink on Beam      │      Dataflow on Beam      │
│        ├─────────┼───────────────────┼──────────────────────────┼────────────────────────────┤
│        │   mode  │        mode       │           mode           │            mode            │
│        ├─────────┼─────────┬─────────┼──────────────────────────┼────────────────────────────┤
│        │  stream │  batch  │  stream │          stream          │           stream           │
│        ├─────────┼─────────┼─────────┼──────────────────────────┼────────────────────────────┤
│        │ language│ language│ language│         language         │          language          │
│        ├─────────┼─────────┼─────────┼────────┬────────┬────────┼─────────┬─────────┬────────┤
│        │ default │ default │ default │ default│   SQL  │ ZetaSQL│ default │   SQL   │ ZetaSQL│
│        ├─────────┼─────────┼─────────┼────────┼────────┼────────┼─────────┼─────────┼────────┤
│        │ events/s│ events/s│ events/s│events/s│events/s│events/s│ events/s│ events/s│events/s│
│        ├─────────┼─────────┼─────────┼────────┼────────┼────────┼─────────┼─────────┼────────┤
│        │   Mean  │   Mean  │   Mean  │  Mean  │  Mean  │  Mean  │   Mean  │   Mean  │  Mean  │
├────────┼─────────┼─────────┼─────────┼────────┼────────┼────────┼─────────┼─────────┼────────┤
│query 0 │6,973,987│2,701,024│2,642,776│ 283,366│ 225,276│ 208,986│  697,837│  679,810│ 197,746│
│      1 │6,595,871│2,417,737│2,604,438│ 316,056│ 305,904│        │1,023,541│  819,001│        │
│      2 │6,748,549│3,176,519│3,122,073│ 517,331│ 500,250│ 417,014│1,824,818│1,773,050│ 329,598│
│      3 │6,608,075│2,089,078│2,039,692│ 555,247│ 415,455│ 402,414│  793,651│  694,444│ 413,394│
│      4 │4,233,521│  499,072│  501,017│  93,712│        │        │   63,068│         │        │
│      5 │6,589,786│  662,708│  661,901│ 251,699│        │        │  114,758│         │        │
│      6 │3,997,921│         │         │  88,488│        │        │   63,577│         │        │
│      7 │3,136,763│  283,085│  281,262│ 194,970│  68,069│        │  102,491│    5,518│        │
│      8 │6,641,870│1,996,446│2,103,271│ 396,511│        │        │  418,760│         │        │
│      9 │  910,680│  206,424│  209,547│  88,715│        │        │   69,701│         │        │
│      10│         │         │         │ 199,681│        │        │  106,758│         │        │
│      11│         │  541,202│  547,783│ 194,326│        │        │   83,731│         │        │
│      12│6,682,259│1,939,488│2,002,162│ 365,631│        │        │  176,149│         │        │
│      13│3,675,255│1,529,824│1,571,314│ 225,734│ 141,844│ 134,445│1,002,004│  564,016│ 110,681│
│      14│6,858,711│2,247,191│2,191,541│ 200,803│        │        │  108,319│         │        │
│      15│3,539,322│  947,679│  912,326│        │        │        │         │         │        │
│      16│  810,701│  233,439│  233,294│ 339,443│        │        │  208,030│         │        │
│      17│3,870,269│1,732,712│1,713,796│        │        │        │         │         │        │
│      18│1,358,216│  499,176│  511,146│        │        │        │         │         │        │
│      19│1,201,216│1,014,693│1,011,419│        │        │        │         │         │        │
│      20│1,352,192│  269,199│  266,441│        │        │        │         │         │        │
│      21│6,634,820│1,254,548│1,229,695│        │        │        │         │         │        │
│      22│6,305,568│1,623,456│1,521,838│        │        │        │         │         │        │
╰────────┴─────────┴─────────┴─────────┴────────┴────────┴────────┴─────────┴─────────┴────────╯
Beam Spark performance omitted because Nexmark hangs in streaming mode.
```
