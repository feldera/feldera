# Checkpoints & Fault Tolerance

:::note Enterprise-only feature
Checkpoints & fault tolerance features are only available in Feldera Enterprise Edition.
:::

Feldera supports the following ways to gracefully stop a pipeline and
later restart it from the same point:

* **Pause and resume.**  A paused pipeline continues to run and occupy
  memory on its host, but it does not ingest any more input until
  resumed.  A paused pipeline does finish processing input it has
  already ingested; afterward, it uses only enough CPU to answer HTTP
  requests.

  In the web UI, the pause button pauses a pipeline and the play button resumes it.

  Every Feldera pipeline supports pause and resume.

* **Checkpoint and resume.**  The Stop button in the UI writes a
  checkpoint to storage and then exits.  When the user resumes the
  pipeline with Start, it uses the checkpoint to resume from the
  exact point where it left off, without dropping or duplicating input
  or output.

  Checkpointing allows a pipeline to resume on a different host or
  with a modified configuration.  With Feldera deployed in Kubernetes,
  when a pipeline exits it terminates its current pod while keeping
  its state in EBS or S3.  The pipeline can then resume in a new pod
  with more or fewer resources.

  Resume does not delete the checkpoint that it uses.  If the pipeline
  crashes or is force-stopped and then later restarts, it resumes from
  the same point.

  For a pipeline to support checkpoints, it must use
  [fault-tolerant connectors](#fault-tolerant-connectors) and have
  storage configured.

* **At-least-once fault tolerance.**  The pipeline regularly (by default,
  every minute) writes a checkpoint to storage.  If the pipeline
  crashes, it resumes from the most recent checkpoint, restarting
  input connectors from the point just before the checkpoint.  Output
  already produced beyond the checkpoint will be produced again,
  meaning that output records are produced at least once.

  Writing checkpoints has some performance cost, so this mode must be
  enabled explicitly.  An at-least-once fault-tolerant pipeline can
  also be checkpointed and resumed.

* **Exactly-once fault tolerance.**  This extends at-least-once fault
  tolerance with input and output journaling so that the pipeline
  restarts from the exact state prior to any unplanned crash.
  Restart does not drop or duplicate input or output, meaning that
  each output record is produced exactly once.

  Journaling adds some performance cost to periodic checkpointing, so
  this mode must be enabled explicitly.  A fault-tolerant pipeline can
  also be checkpointed and resumed.

> Feldera resolves [secrets](../connectors/secret-references.md)
> each time a pipeline starts or resumes, without saving resolved
> secrets to checkpoints or journals.

## Enabling checkpoints and fault tolerance

To enable checkpoint and resume or fault tolerance in an enterprise
Feldera pipeline:

1. Ensure that all of the pipeline's connectors support fault tolerance, as
   described in the [fault-tolerant connectors](#fault-tolerant-connectors)
   section.

2. Recent versions of Feldera enable storage in new pipelines by
   default.  If storage is not yet enabled, enable it in one of the
   following ways:

   - In the web UI, click on the gear icon.  In the dialog box,
     change `storage` to `{}`. Example with additional configuration options:
     ```
      "storage": {
        "backend": {
          "name": "default"
        },
        "min_storage_bytes": null,
        "min_step_storage_bytes": null,
        "compression": "default",
        "cache_mib": null
      }
     ```

     Then click on the Apply button.  (For more storage options, click
     on the gear again to see that `{}` has been expanded to show the
     available settings). For all storage configuration options, refer to [pipeline settings](../pipelines/configuration.mdx#runtime-configuration)

   - Using the `fda` command line tool:

     ```
     fda set-config <pipeline> storage true
     ```

3. To additionally enable fault tolerance:

   - In the web UI, click on the gear icon.  In the dialog box,
     inside `fault_tolerance`:

     - Change `model` to `"at_least_once"` or `"exactly_once"`.

     - Optionally, change `"checkpoint_interval_secs"` to an automatic
       checkpoint interval of your choice, in seconds. The default is 60 seconds. `null` will
       disable automatic checkpoints.

     Then, click on the Apply button.  If clicking Apply does not
     dismiss the dialog box, either storage has not been enabled
     or the running version of Feldera is not the enterprise edition.

   - With the `fda` command line tool, use one of these commands to
     enable fault tolerance:

     ```
     fda set-config <pipeline> fault_tolerance at_least_once
     fda set-config <pipeline> fault_tolerance exactly_once
     ```

     Optionally, use the following command to change the automatic
     checkpointing interval to `<seconds>` (use `0` for `<seconds>` to
     disable automatic checkpoints):

     ```
     fda set-config <pipeline> checkpoint_interval <seconds>
     ```

## Writing checkpoints

When fault tolerance is enabled, by default Feldera automatically
writes a checkpoint every 60 seconds.  This interval can be changed
or disabled as described
[above](#enabling-checkpoints-and-fault-tolerance).

Feldera also has an HTTP API, `/checkpoint`, that allows the user to
request writing a checkpoint immediately.  If automatic checkpoints
are disabled, then the user should occasionally invoke this API to
ensure that the checkpoint feature is useful.

> Writing a checkpoint is ordinarily a fast operation that takes
several seconds.  However, the [Delta Lake input
connector](../connectors/sources/delta.md) can only checkpoint at some
input positions.  When a checkpoint is requested between those points,
Feldera executes steps that draw input only from those connectors
until they advance to a point at which a checkpoint is possible, and
then writes the checkpoint.  In some cases, this can take minutes or
longer.

## Fault-tolerant connectors

For Feldera to checkpoint and resume, or to enable fault tolerance, all
of the pipeline's input connectors must support fault tolerance.  Some
input connectors do not yet support fault tolerance and therefore may
not be part of a pipeline that supports these features.  Input
connectors individually document their support for fault tolerance.

For a pipeline to fully support either feature, its output connectors
must also be fault tolerant. If a fault-tolerant pipeline includes non-fault-tolerant output
connectors, then in the event of a crash and restart, Feldera may send
duplicate output to those connectors, but it will not drop output.

Feldera does not yet support fault tolerance or checkpoint and resume in
pipelines that use the SQL `NOW` function.

The following table documents input connector support for fault
tolerance.

|Input connector|Checkpoint and resume|At-least-once FT|Exactly once FT|
| --------------:|:-------------------:|:----------------:|:---------------:|
|[Datagen]|✅|✅|✅|
|[Debezium]|✅|✅|✅|
|[Delta Lake]|✅|✅|❌|
|[HTTP GET (URL)]|✅|✅|✅|
|[HTTP]|✅|✅|✅|
|[Iceberg]|❌|❌|❌|
|[Kafka]|✅|✅|✅|
|[NATS]|✅|✅|✅|
|[PostgreSQL]|❌|❌|❌|
|[Pub/Sub]|❌|❌|❌|
|[S3]|✅|✅|✅|
|[File]|✅|✅|✅|

[Datagen]: /connectors/sources/datagen.md
[Debezium]: /connectors/sources/debezium.md
[Delta Lake]: /connectors/sources/delta.md
[HTTP GET (URL)]: /connectors/sources/http-get.md
[HTTP]: /connectors/sources/http.md
[Iceberg]: /connectors/sources/iceberg.md
[Kafka]: /connectors/sources/kafka.md
[NATS]: /connectors/sources/nats.md
[PostgreSQL]: /connectors/sources/postgresql.md
[Pub/Sub]: /connectors/sources/pubsub.md
[S3]: /connectors/sources/s3.md
[File]: /connectors/sources/file.md

## Implementation details

Feldera implements checkpoints by writing a **checkpoint** — a consistent
snapshot of the system's state, including computation and the input and
output connectors — to storage.  On resume, Feldera loads its state
from the checkpoint, and then restarts each connector at the point
where it left off.

In addition, exactly-once fault tolerance writes additional data to a
separate **journal** between checkpoints.  For each batch of data that
Feldera processes through the pipeline, it writes enough information
to the journal to obtain another copy of the batch's input data later.

When an exactly-once fault-tolerant pipeline restarts, it loads its
state from the most recent checkpoint, then replays any data from
input connectors previously processed beyond that checkpoint.  If
replay produces output that was previously sent to output connectors,
it discards that output.  After replay completes, the pipeline
continues with new input that has not previously been processed.
