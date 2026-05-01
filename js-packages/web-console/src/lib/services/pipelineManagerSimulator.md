# Pipeline Manager Mock Data Simulator — Plan

This document describes the behavior of `pipelineManagerSimulator.ts`, a
hardcoded-flag mock-data generator that taps into a subset of API calls in
`pipelineManager.ts` and produces realistic, time-evolving responses for the
checkpoint and transaction subsystems.

The simulator is intended for development of the web console UI without a
running backend (or with a backend that does not exercise these features
often). It is **off by default**; the flags are flipped manually in the source
file.

## Tapped functions

The simulator can intercept the following functions in `pipelineManager.ts`:

| Function                     | What the simulator returns                                            |
| ---------------------------- | --------------------------------------------------------------------- |
| `getPipelineCheckpoints`     | Simulated `CheckpointMetadata[]`, growing as checkpoints succeed.     |
| `checkpointPipeline`         | Triggers a manual checkpoint, returns the next sequence number.       |
| `getCheckpointStatus`        | `{ success, failure }` reflecting last successful / failed CP.        |
| `syncCheckpoint`             | Triggers a manual sync, returns the synced checkpoint UUID.           |
| `getCheckpointSyncStatus`    | `{ success, failure, periodic }` for the sync subsystem.              |
| `getPipelineStats` (overlay) | Overlays `checkpoint_activity` (root) and a handful of fields under   |
|                              | `global_metrics` (`transaction_status`, `transaction_id`,             |
|                              | `transaction_msecs`, `transaction_records`, `transaction_initiators`, |
|                              | `commit_progress`) onto the real response.                            |

`getPipelineStats` is **overlay-only**: the simulator does not invent a full
`ControllerStatus`. If the real status is `null` or `'not running'`, no
overlay is applied — the simulator is meant to enrich a live pipeline view,
not impersonate one.

## Single source of truth

A module-private `Map<pipelineName, PipelineSimState>` holds all simulator
state. Every tapped function reads from / writes to this map. State is
created lazily the first time a pipeline is observed.

```
PipelineSimState = {
  checkpoint?: CheckpointSimState   // when checkpoint sim is enabled
  transaction?: TransactionSimState // when transaction sim is enabled
}
```

A single `setInterval` tick (250 ms) advances all per-pipeline state
machines. The interval starts on first read/write and stops when no
simulator is enabled. Because `Date.now()` is consulted, the simulator
remains correct even if ticks are delayed by an inactive tab.

## Hardcoded toggles

Two top-of-file constants control the simulator. They are independent:

```ts
export const SIMULATE_CHECKPOINTS = false
export const SIMULATE_TRANSACTIONS = false
```

The wiring in `pipelineManager.ts` checks these at call time, so flipping a
flag and reloading is sufficient — no rebuild of the codegen layer is needed.

## Checkpoint state machine (per pipeline)

Mirrors the real backend's notion of `CheckpointActivity`, the
`/checkpoint_status` endpoint, and the in-memory list returned by
`/checkpoints`.

```
state ∈ { idle, delayed, in_progress }
nextSequenceNumber: number       // next checkpoint #, starts at 1
checkpoints: CheckpointMetadata[] // grows on success
lastSuccess?: number              // sequence # of last successful CP
lastFailure?: CheckpointFailure   // last failed CP
inProgressStartedAt?: ISO string
delayedSince?: ISO string
delayReasons: TemporarySuspendError[]
nextAutoTriggerAt: number         // ms epoch
manualPending: boolean            // a manual checkpoint was requested
                                  //   while non-idle
```

### Cadence

- Periodic auto-trigger every **30 s** (real time).
- A manual `checkpointPipeline()` call:
  - if `state === 'idle'`: starts the checkpoint immediately, returns the
    sequence number that will be used.
  - else: sets `manualPending = true`, returns the in-flight or future
    sequence number, and schedules an immediate checkpoint when state
    returns to `idle`.

### Transitions

- **idle → delayed**: when a periodic/manual trigger fires while a
  transaction is in progress (any non-`NoTransaction` status). The reason
  list is `['TransactionInProgress']`. While delayed, the simulator keeps
  re-evaluating each tick.
- **idle → in_progress** (or **delayed → in_progress**): set
  `inProgressStartedAt = now`. Random duration **2–5 s** (uniform).
- **in_progress → idle**:
  - With probability **5%**: failure path — append a `CheckpointFailure`
    with `sequence_number = nextSequenceNumber`, advance the sequence
    counter, leave `lastSuccess` untouched.
  - Else: success path — append a new `CheckpointMetadata` to
    `checkpoints` with:
    - `uuid`: UUIDv7-like string.
    - `identifier`: optional, omitted by default.
    - `fingerprint`: deterministic-per-pipeline 64-bit value.
    - `processed_records`: a slow-growing counter (≈ +50 k per CP).
    - `size`: random in 5–25 MiB, slowly drifting up.
    - `steps`: `total_completed_steps`-equivalent counter.
    - Update `lastSuccess` to this sequence number.
  - Schedule next periodic trigger 30 s out.

### Failures

A failure does not retry automatically; the next periodic trigger does. The
failure record persists until a later success. The UI hides the failure if
`lastSuccess > failure.sequence_number` — the simulator preserves both so
that natural recovery is visible.

### Sync substate

The sync subsystem is independent of checkpoint creation but operates on
checkpoints once they exist.

```
syncState ∈ { idle, in_progress }
syncStartedAt?: number
syncTargetUuid?: string                // checkpoint being synced
syncKind: 'manual' | 'periodic'
lastManualSuccessUuid?: string         // → /sync_status .success
lastManualFailure?: CheckpointSyncFailure
lastPeriodicSuccessUuid?: string       // → /sync_status .periodic
nextPeriodicSyncAt: number
```

- Periodic sync fires every **120 s** (matches the doc default for
  `push_interval`). It only fires if a checkpoint exists and we have not
  already synced its UUID.
- A manual `syncCheckpoint()`:
  - returns the UUID of the latest _successful_ checkpoint (or the most
    recent if there are no successes yet);
  - if `syncState === 'in_progress'`, returns the in-flight UUID and does
    not start a second sync.
  - sets `syncKind = 'manual'`.
- Sync duration **1–3 s**.
- Failure rate **3%**. On failure, set `lastManualFailure`; do not advance
  `lastManualSuccessUuid`.
- On success: update either `lastManualSuccessUuid` or
  `lastPeriodicSuccessUuid` depending on `syncKind`.

## Transaction state machine (per pipeline)

Mirrors `TransactionStatus`, `transaction_id`, `transaction_msecs`,
`transaction_records`, `transaction_initiators`, and `commit_progress` on
`global_metrics`.

```
state ∈ { NoTransaction, TransactionInProgress, CommitInProgress }
transactionId: number      // 0 when NoTransaction; monotonic
phaseStartedAt: number     // ms epoch — start of current phase
totalRecordsTarget: number // chosen at TransactionInProgress entry
recordsSoFar: number
totalOps: number           // chosen at CommitInProgress entry, 15–30
opsCompleted: number
opsInProgress: number      // 0–3
opInProgressTotal: number  // per-op total records
opInProgressDone: number
nextStartAt: number        // ms epoch — when to start next transaction
```

### Cadence

- A new transaction starts every **75 s** (jittered ±15 s) measured from
  the end of the previous transaction.
- TransactionInProgress phase length: **30–60 s**, uniform.
- CommitInProgress phase length: **10–25 s**, uniform.
- Quiescent gap (NoTransaction): **15–30 s**, uniform.

### TransactionInProgress

- `transactionId` is incremented at phase entry.
- `totalRecordsTarget` = uniform 100 000–1 000 000.
- `recordsSoFar` grows non-linearly toward the target using an ease-out
  curve (records arrive faster early, level off near the end). At any
  tick: `recordsSoFar = floor(target * (1 - (1 - p)^2))` where
  `p = elapsed / phaseLength`.
- `transaction_msecs` = `now - phaseStartedAt`.
- `transaction_records` = `recordsSoFar`.
- `transaction_initiators.initiated_by_api = 'Started'`.
- `transaction_initiators.transaction_id = transactionId`.

### CommitInProgress

- On entry: `recordsSoFar` is locked to the final
  `transaction_records` value; pick `totalOps`.
- Operators progress through the pipeline in waves:
  - 1–3 operators are in flight at once; their per-op total record count
    is uniform 5 000–80 000.
  - Each in-flight op processes its records linearly. When it finishes,
    `opsCompleted++`; a new op is pulled from `remaining`.
- `commit_progress`:
  - `completed = opsCompleted`
  - `in_progress = opsInProgress`
  - `remaining = totalOps - opsCompleted - opsInProgress`
  - `in_progress_total_records` = sum of in-flight ops' per-op totals
  - `in_progress_processed_records` = sum of in-flight ops' processed
- `transaction_initiators.initiated_by_api = 'Committed'`.

### NoTransaction

- `transactionId` resets to 0.
- `transaction_msecs` and `transaction_records` are `null`.
- `commit_progress` is `null`.
- `transaction_initiators` = `{ initiated_by_connectors: {}, transaction_id: null }`.

### Interaction with checkpoints

The transaction simulator only writes transaction state. The checkpoint
state machine **reads** the simulated transaction state (when both
simulators are enabled) to decide whether to enter `delayed` with reason
`TransactionInProgress`. If the transaction simulator is off, checkpoints
never see transaction-induced delays from this simulator.

## Overlaying onto `getPipelineStats`

The wrapper:

1. Calls the real `getPipelineStats`.
2. If neither simulator is enabled, or the real result is
   `'not running'` / has `status === null`, returns the real result.
3. Otherwise produces a shallow-cloned status with the simulated fields
   substituted:
   - root: `checkpoint_activity` (when checkpoint sim is enabled).
   - `global_metrics`: `transaction_status`, `transaction_id`,
     `transaction_msecs`, `transaction_records`, `transaction_initiators`,
     `commit_progress` (when transaction sim is enabled).

`permanent_checkpoint_errors` and `suspend_error` are **not** simulated;
the real values pass through unchanged.

## File layout

- `pipelineManagerSimulator.md` — this plan.
- `pipelineManagerSimulator.ts` — implementation. Exports the toggle
  constants, the override functions, and `overlayPipelineStats`.
- `pipelineManager.ts` — modified to dispatch to the simulator at the call
  sites of the six tapped functions.

## Out of scope

- Persistence across reloads — state is in-memory only.
- Multi-worker / distributed nuances of `transaction_initiators` (only
  `initiated_by_api` is set; `initiated_by_connectors` stays empty).
- Re-creating a full `ControllerStatus` from scratch when no real one
  exists.
- Automated tests — the goal is interactive UI development. Logic that
  warrants tests should be promoted into reusable helpers separately.
