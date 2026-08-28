---
title: Clean shutdown on SIGTERM
sidebar_position: 30
---

# Clean shutdown on SIGTERM

When Feldera stops a pipeline through its own control plane (for example, a
graceful `/stop`), the pipeline checkpoints its state and suspends before the
pods are removed. But a pipeline pod can also be terminated by the
infrastructure with no Feldera involvement — Kubernetes node drain, eviction,
Karpenter consolidation, or `kubectl delete`. These deliver `SIGTERM` to the
pipeline process.

By default the pipeline reacts to `SIGTERM` with a graceful *HTTP* shutdown: it
drains in-flight requests and exits, but it does **not** checkpoint. Any state
accumulated since the last checkpoint is lost and must be recomputed on the next
start.

Setting `FELDERA_CLEAN_SHUTDOWN_ON_SIGTERM` makes the pipeline run the same
checkpoint-and-suspend flow as a graceful stop when it receives `SIGTERM`, so an
infrastructure-initiated termination resumes from a fresh checkpoint.

## Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `FELDERA_CLEAN_SHUTDOWN_ON_SIGTERM` | off | When truthy (`1`, `true`, `yes`, `on`; case-insensitive), checkpoint and suspend on `SIGTERM` before shutting down. |

## Kubernetes: `terminationGracePeriodSeconds`

The feature only has an effect when the pod is given enough time to checkpoint
before the kernel kills it. Kubernetes sends `SIGTERM`, waits
`terminationGracePeriodSeconds`, then sends `SIGKILL`. If the grace period is
`0` (or too short to finish a checkpoint), the pod is `SIGKILL`ed and the
checkpoint is truncated — the same outcome as not enabling the feature.

When enabling `FELDERA_CLEAN_SHUTDOWN_ON_SIGTERM`:

- Set the pipeline pod's `terminationGracePeriodSeconds` comfortably above the
  expected checkpoint time (which scales with state size and object-store
  latency). This is the only deadline: the pipeline waits for the checkpoint to
  finish, and if it wedges, Kubernetes `SIGKILL`s the pod when the grace period
  ends.

## Other signals

With the feature enabled the pipeline installs its own handlers for `SIGTERM`
(checkpoint, then shut down) and `SIGINT` / Ctrl-C (immediate graceful shutdown,
no checkpoint). Default handling of other signals such as `SIGHUP` and `SIGQUIT`
is no longer installed; in a Kubernetes deployment only `SIGTERM` (then
`SIGKILL`) is sent, so this does not affect normal operation.
