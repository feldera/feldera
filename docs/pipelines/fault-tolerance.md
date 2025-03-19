# Fault tolerance and suspend/resume

Feldera supports three ways to gracefully stop a pipeline and later
restart it from the same point:

* Pause and resume.  A paused pipeline continues to run and occupy
  memory on its host, but it does not ingest any more input until
  resumed.  A paused pipeline does finish processing input it has
  already ingested; afterward, it uses only enough CPU to answer HTTP
  requests.

  In the web UI, the ⏸️ button pauses a pipeline and ▶️ resumes it.

  Every Feldera pipeline supports pause and resume.

* Suspend and resume.  When the user suspends a pipeline, it writes a
  checkpoint to storage and then exits.  When the user resumes the
  pipeline, it uses the checkpoint to resume from the exact point
  where it left off, without dropping or duplicating input or output.

  The "suspend" API suspends a pipeline, and the "start" API (or ▶️ in
  the web UI) resumes it.

  Suspend and resume is in the Feldera enterprise edition only.  For a
  pipeline to support suspend and resume, a pipeline must use
  [fault-tolerant connectors](#fault-tolerant-connectors) and have
  storage configured.

* Fault tolerance.  Fault tolerance extends suspend and resume so
  that, in addition to resuming from the point of a suspend request,
  Feldera can also restart from the exact point of any unplanned,
  abrupt shutdown or crash.

  Fault tolerance is in the Feldera enterprise edition only, with the
  same configuration requirements as suspend and resume.  Fault
  tolerance has some performance cost so, in addition, it must be
  enabled explicitly on a pipeline.

The following sections describe suspend and resume and fault tolerance
in more detail.

## Implementation

Feldera implements suspend and resume by writing a **checkpoint** to
storage, that is, a consistent snapshot of the Feldera system's state,
including computation and the input and output adapters.  On resume,
Feldera loads its state from the checkpoint, and then restarts each of
the connectors at the point where it left off.

Fault tolerance builds on top of checkpointing by periodically writing
a checkpoint to storage.  Between checkpoints, for each batch of data
that Feldera processes through the pipeline, it writes enough
information to a separate **journal** to obtain another copy of the
batch's input data later.

When a fault-tolerant pipeline restarts, it loads its state from the
most recent checkpoint, then it replays any data from input connectors
previously processed beyond that checkpoint.  If replay produces
output that was previously sent to output connectors, it discards that
output.  After replay completes, the pipeline continues with new input
that has not previously been processed.

## Fault-tolerant connectors

For Feldera to suspend and resume, or to enable fault-tolerance, all
of the pipeline's input connectors must support fault tolerance.  Some
input adapters do not yet support fault tolerance and therefore may
not be part of a pipeline that supports these features.  Input
connectors individually document their support for fault tolerance.

For a pipeline to fully support either feature, its output connectors
must also be fault tolerant.  Only the [Kafka output
connector](/connectors/sinks/kafka.md) supports fault tolerance as of
now.  If a fault-tolerant pipeline includes non-fault-tolerant output
connectors, then in the event of a crash and restart, Feldera may send
duplicate output to those connectors, but it will not drop output.

Feldera does not yet support fault tolerance or suspend and resume in
pipelines that use recursive SQL or the SQL `NOW` function.

## Enabling suspend and resume and fault tolerance

To enable suspend and resume or fault tolerance in an enterprise
Feldera pipeline:

1. Ensure that all of the pipeline's connectors support fault tolerance, as
   described in the previous section.

2. Enable storage in one of the following ways:

   - In the web UI, click on the gear icon ⚙️.  In the dialog box,
     change `storage` to `{}`, e.g.:

     ![Fault tolerance configuration](fault-tolerance.png)

     Then click on the Apply button.  (For more storage options, click
     on the ⚙️ again, to see that `{}` has been expanded to show the
     available settings.)

   - Using the `fda` command line tool:

     ```
     fda set-config <pipeline> storage true
     ```

3. To additionally enable fault tolerance:

   - In the web UI, click on the gear icon ⚙️.  In the dialog box,
     change `fault_tolerance` to `{}`, then click on the Apply button.
     If clicking on Apply does not dismiss the dialog box, then either
     storage has not been enabled or the running version of Feldera is
     not the enterprise edition.

     (Again, click on the ⚙️ again afterward to see the available
     fault tolerance settings.)

   - Using the `fda` command line tool:

     ```
     fda set-config <pipeline> fault_tolerance true
     ```
