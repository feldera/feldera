# Fault Tolerance & Disaster Recovery

:::note Enterprise-only feature
Checkpoints, fault tolerance, and disaster recovery features are only available in Feldera Enterprise Edition.
:::

Feldera protects pipelines against data loss through two complementary mechanisms:
**checkpoints** for fault tolerance and **object store sync** for disaster recovery.

## Fault tolerance

Feldera periodically writes **checkpoints** — consistent snapshots of the entire
pipeline state, including computation and connectors — to local storage. If a
pipeline crashes, it automatically restarts from the most recent checkpoint
without operator intervention.

Three modes are available, each building on the previous one:

- **Checkpoint and resume** — A stopped pipeline writes a checkpoint on exit
  and restores from it on restart. This enables graceful restarts and configuration changes without data loss.

- **At-least-once fault tolerance** — The pipeline writes checkpoints
  automatically (every 60 seconds by default). If the pipeline crashes, it will
  resume from the last checkpoint. Output records produced beyond that
  checkpoint may be sent again.

- **Exactly-once fault tolerance** — Extends at-least-once mode with input and
  output journaling. The pipeline replays journaled data and
  suppresses duplicate output, so each output record is produced exactly once.

For configuration, connector requirements, and implementation details, see
[Checkpoints & Fault Tolerance](fault-tolerance.md).

## Disaster recovery

Feldera can synchronize checkpoints to an **S3-compatible object store**,
enabling recovery even when local storage is lost. This turns checkpoints into
a durable backup that supports several disaster recovery patterns:

- **Standby pipelines** — A secondary pipeline continuously pulls the latest
  checkpoint from object store. If the primary fails, the standby activates
  and resumes processing within seconds.

- **Pipeline seeding** — A new pipeline can bootstrap its state from another
  pipeline's checkpoints stored in a read-only bucket, without affecting the
  source.

- **Cross-region recovery** — Checkpoints stored in object storage can be
  restored on infrastructure in a different region or availability zone.

In the case that state cannot be restored from a local or remote checkpoint, the pipeline will need to backfill the data from its source.

For setup, standby configuration, and S3 permissions, see
[Checkpoint Sync to Object Store](checkpoint-sync.md).
