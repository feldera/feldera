# Concurrent bootstrapping

## Background: how bootstrapping works today

Bootstrapping (`CircuitHandle::restore`, `circuit_builder.rs`) proceeds as follows:

1. Determine the operators that require backfill: every node whose `restore()`
   fails with `NotFound` (its persistent-id state file is missing from the
   checkpoint), plus balancer-invalidated clusters (clusters whose checkpointed
   balancing policy becomes inconsistent in the modified circuit) and their transitive
   successors.
2. Walk back from these operators (`compute_replay_nodes_step`) to compute the
   *replay nodes* used to bootstrap the new parts of the circuit and the larger
   set of operators that participate in bootstrapping. The walk stops at streams
   that have a registered replay source (an integral's `Z1Trace`, registered at
   construction via `register_replay_stream`).
3. Activate only these operators (`executor.prepare(circuit,
   Some(participate_in_backfill))`), wire replay nodes to play back their contents
   (`add_replay_edges`), and run the circuit until the new parts are fully
   bootstrapped in one large transaction (the replay source emits one chunk per
   step, so the commit stretches across as many steps as the replay needs).
4. Resume normal operation (`complete_replay` re-prepares the scheduler with all
   nodes active).

During bootstrapping, existing views that are unaffected by the change are not
updated. The resulting downtime may be unacceptable for some applications.

## Design overview

Concurrent bootstrapping keeps old views receiving incremental updates while new
and modified views are populated. The circuit is cloned into two copies: copy 1
runs the old operators as usual; copy 2 bootstraps the new parts. Copy 1 also
records changes to the *boundary streams* that cross between the copies; those
records bring copy 2 up to date in the final stage. The operator sets activated in
the two copies may overlap.

The algorithm:

- **Steps 1–2** (unchanged): compute the replay nodes and the participating set.
  The boundary streams are exactly the (stream → replay source) substitutions the
  backward walk records.
- **Step 3**: clone the circuit into copy 1 and copy 2.
  * **Copy 1** enables only the operators present in the old circuit (everything
    that does not participate in replay, plus its transitive predecessors), plus
    *recorder* operators that accumulate changes to the boundary streams (the same
    streams whose integrals the replay sources play back in copy 2).
  * **Copy 2** enables only the participating operators and wires the replay nodes
    — the exact circuit of the old step 3. Operators that do not participate are
    `clear_state()`d, and their state is not restored in the first place.
- **Step 4 (backfill)**: two circuits run with separate schedulers. Copy 1 is driven
  through the normal interface (`start_transaction`/`step`/`start_commit`/…); copy
  2 runs one large transaction in the background.
- **Step 5 (synchronization)**: replay the recorded changes into copy 2 to catch it
  up with everything that arrived since backfill began. Copy 1 processes no new
  inputs during this phase.
- **Step 6 (cutover)**: transfer the bootstrapped operators' state from copy 2 into
  copy 1, drop copy 2, and enable all operators in copy 1.

Correctness of the cut: copy 2 replays the integrals as of the fork point (the
checkpoint), and the recorders capture every boundary-stream delta from the fork
point on; the fork must therefore precede copy 1's first post-restore commit. The
cut is one-directional — forward propagation of `need_backfill` guarantees that no
backfilled operator feeds an old operator, so nothing flows from copy 2 back into
copy 1 until cutover.

## Key design decisions

1. **Scheduling: one worker pool, lockstep interleaved stepping.** A second
   `Runtime` is not viable: `LockedDirectory` admits one runtime per storage
   directory; `Runtime`/`WORKER_INDEX` are thread-locals baked into spine creation
   and persistent ids; the exchange mailbox matrix is indexed by the global worker
   layout. Instead each worker thread owns a second self-contained `CircuitHandle`
   (own Rc graph, executor, scheduler, tokio runtime) and advances copy 2 by a
   fixed quantum while executing each `DBSPHandle` step/transaction command. Driving
   copy 2 from broadcast commands — never from local timing — preserves the
   invariant that all workers make identical interleaving decisions (every step is
   an all-worker rendezvous through exchange operators and the scheduler's metadata
   broadcast).

2. **State transfer (step 6): in-memory take/install.** A Node/Operator API pair
   moves operator state (spines, scalar Z⁻¹ values, window bounds, …) from copy 2's
   nodes into copy 1's, pairwise by `NodeId` (identical across copies, because both
   are built from the same constructor closure). Same thread, O(#batches) `Arc`
   moves, no I/O.

3. **Synchronization: single pass in v1.** One sync transaction with copy 1 paused;
   downtime is proportional to the changes recorded since backfill began. The
   recorder drain is designed so that an iterative catch-up variant (replay while
   copy 1 keeps running, repeat on the smaller residue, short final pause) can be
   added later without rework.

4. **Recorders: pre-built and gated.** A recorder node is constructed alongside
   every registered replay stream at build time and enabled only on actual boundary
   streams during bootstrap (the `enable_count` pattern used by `Accumulator`). This
   keeps construction — and therefore `Runtime::sequence_next()` id allocation —
   deterministic across workers. Adding nodes post-build (untested path) and
   recording inside the integral operators (high blast radius on
   `AccumulateTraceAppend`, historically replay-fragile) were both rejected.

## Mapping to existing machinery

| Design element | Existing machinery |
|---|---|
| Replay sets, boundary streams | `CircuitHandle::restore`, `compute_replay_nodes_step` — reused unchanged |
| Circuit clone | Constructor closure is `FnOnce + Clone` (`Runtime::init_circuit`); retain a clone and re-run `RootCircuit::build`. NodeIds and persistent ids match across copies; StreamIds and circuit caches are per-copy |
| Per-copy operator subsets | `executor.prepare(circuit, Some(node_set))` |
| Copy 2's large transaction | Today's bootstrap transaction verbatim (chunked replay, `splitter_output_chunk_size` per step) |
| Sync-transaction replay | The same chunked replay protocol, fed from recorder spines |
| Recorder core | `Accumulator` (`operator/dynamic/accumulator.rs`): accumulate batches into a `Spine`, drain by swap — generalized to a multi-transaction lifetime with an explicit drain trigger |
| Cheap state copy / spine fork | Spine batches are immutable `Arc<B>`; fork = new spine + the Arc'd batches, no data copy (hazard: `Spine::consolidate` panics on a shared batch `Arc`, so forked spines must avoid that path) |

## Invariants and risks

- **Checkpointing while two copies exist is undefined** (shared persistent-id
  namespace). Checkpoints are suppressed until cutover. A crash mid-bootstrap
  restarts bootstrapping from the old checkpoint — as today, but the window is
  longer and copy 1 ingests live data; the FT journal stays disabled in v1 (input
  connectors resume from pre-bootstrap offsets after a crash).
- **Step-5 downtime is not zero** — it is proportional to the recorded changes. The
  iterative variant (decision 3) is the eventual mitigation.
- The two copies have **separate StreamId spaces and circuit caches**; all
  cross-copy correlation goes through `NodeId`/persistent id.

## Balanced joins

A balanced (rebalancing) join partitions each input across workers by a
cluster-wide `PartitioningPolicy` that the per-circuit balancer renegotiates at
transaction boundaries, so its state is not self-contained per node and cutover
cannot naively move it. Concurrent bootstrap handles it by making the bootstrap
copy authoritative for the partitioning of whatever it backfills: that copy runs
its balancer to convergence and shards its integral under the resulting policy,
and at cutover the integral node transfers the spine while
`RebalancingExchangeSender::swap_state` moves the policy with it, re-seeding the
live balancer through the same `set_policy_for_stream` hook `restore` uses (the
rebalancing accumulator is empty between transactions, so its `swap_state` is a
checked no-op). With these in place both balanced operators report
`supports_state_transfer() == true` and `concurrent_restore_refusal` no longer
falls back.

The subtlety is a cluster that spans the boundary — a new join over kept, shared
streams (e.g. adding `s2 ⋈ s3` when `s1 ⋈ s2` and `s3 ⋈ s4` already exist) merges
their clusters into one whose partitioning both copies would otherwise solve
independently, risking a post-cutover inconsistency (e.g. broadcast × broadcast).
The live copy must therefore freeze such a cluster's policies until cutover.
`restore_concurrent` hands the bootstrap copy's `participate_in_backfill` set to
the live balancer (`set_bootstrap_backfill_region`); `get_fixed_policy` freezes
any stream whose cluster intersects it, and `prepare(None)` clears it at cutover.
The backfill region — not the live copy's `active_nodes` — is the correct signal:
because per-stream balanced traces are cached and reused, the new join runs
*live* in the main copy yet is *re-evaluated* in the bootstrap copy, so the
cluster is fully active in the live copy and an active-node test would miss it.
Freezing pins kept streams to their checkpoint policy while leaving genuinely new
streams (whose policy is `None` in the live copy) free for the bootstrap copy to
choose. An unsatisfiable boundary-spanning cluster still falls back, as do nested
circuits and boundary streams with no replay source.

## Controller integration

The engine layer builds the API on `DBSPHandle` (`start_concurrent_bootstrap`,
`step_bootstrap_circuit`, `sync_concurrent_bootstrap`,
`complete_concurrent_bootstrap`, the `BootstrapCircuitState` /
`ConcurrentBootstrapPhase` machines, and the `ConcurrentRestoreOutcome` return).
The controller (`crates/adapters/src/controller.rs`) drives it so a pipeline keeps
serving old views while new/modified views backfill.

The key fact: `DBSPHandle::bootstrap_in_progress()` is **false** during a
concurrent bootstrap — it tracks only the stop-the-world path (`bootstrap_info`);
concurrent state lives in `concurrent_bootstrap_info`. So none of today's
stop-the-world plumbing — global input pause, `silent_bootstrap` output
suppression, FT disable, status — fires. The controller adds a parallel driver and
reuses that machinery unchanged only when concurrent bootstrap is off.

### The `concurrent_bootstrap` flag

Concurrent bootstrap is **opt-in**, via a boolean `concurrent_bootstrap` flag
analogous to and carried like the existing `silent_bootstrap` (`BootstrapConfig`),
settable at startup or in the `/approve` body (the `/approve` value is taken as-is,
exactly like `silent_bootstrap`); the existing `BootstrapPolicy`
(Allow/Reject/AwaitApproval) approval flow is reused unchanged. The two flags are
**mutually exclusive: startup fails if both are set.**

Off (the default): today's stop-the-world bootstrap, unchanged. On: attempt a
concurrent bootstrap with **no fallback** — if the engine returns `FellBack`, or
the bootstrap fails or aborts in any way, the pipeline **fails fatally**, and the
user restarts with `concurrent_bootstrap=false` for stop-the-world. A silent
fallback is deliberately excluded: it would pause inputs and stall the old views,
the exact outcome the user opted out of.

### Driver

With the flag on, on startup from a checkpoint with a program diff the controller
calls `start_concurrent_bootstrap(checkpoint_base)` instead of entering
stop-the-world, and branches:

* **`UpToDate`** — run normally.
* **`FellBack { reason, .. }`** — fatal `InitializationError(reason)`; no fallback.
* **`Concurrent`** — drive a state machine mirroring the engine's, as a new arm in
  the `StepTrigger`:
  1. **Backfill** — run main transactions normally (old views stay live, inputs
     flow) and pump `step_bootstrap_circuit()` once after each main step.
  2. **Synchronize** — when that returns `true`, call `sync_concurrent_bootstrap()`;
     the engine now rejects main transactions, so pause inputs and pump to
     completion — a brief stop-the-world bounded by the deltas recorded during
     backfill.
  3. **Cutover** — `complete_concurrent_bootstrap()`, resume inputs, run.

### Gating

* **Inputs.** A `concurrent_backfill_in_progress` flag keeps pre-existing inputs
  flowing during backfill (unlike the `bootstrap_in_progress()` pause) and pauses
  them only during synchronize.
* **New-table connectors.** Out of scope in v1: a new table has no checkpoint, so
  its integral is rejected as a replay source and the backward walk reaches the raw
  input, tripping the `is_input` refusal in `concurrent_restore_refusal` →
  `FellBack` → fatal. So a concurrent bootstrap and a live new-table connector never
  coexist. Supporting them later (e.g. modified/cleared tables that re-ingest from
  connectors) requires keeping such connectors disabled through backfill **and**
  synchronize and starting them at cutover from offset 0 — else their data is
  dropped (the new integral is excluded from the live schedule, rebuilt by copy 2,
  which never saw it) or double-counted — which needs a per-endpoint suppression flag
  distinct from `is_paused_by_user`, keyed off the bootstrap's `need_backfill` set.
* **Outputs.** `silent_bootstrap` stays off (pre-existing deltas are real); new
  views emit nothing during backfill (excluded from the schedule). A new view's
  output at cutover needs no special path: at the end of the bootstrap transaction
  its output accumulator holds the accumulated changes — the full view, computed
  from scratch — which is exactly the first batch a connector emits under normal
  operation. So the **first output is the bootstrap-transaction accumulator (the
  full view) and the second is the catchup-transaction changes**, then normal
  deltas — identical to a from-scratch run.

### Status, checkpoints, fault tolerance

* **Status.** Two new first-class runtime statuses, `ConcurrentBootstrapping` (old
  views live, replay in progress) and `Synchronizing` (cutover window), surfaced
  through `RuntimeStatus` / `CombinedStatus` like `Bootstrapping` today; expose the
  backfilling views (`need_backfill` pids) and progress for the UI.
* **Checkpoints.** Blocked by the engine during and after a concurrent bootstrap, so
  the controller defers scheduled checkpoints until just after cutover, which
  captures the unified circuit.
* **Fault tolerance.** Disabled during concurrent bootstrap (as for stop-the-world)
  and re-enabled after the post-cutover checkpoint. A crash restarts from the
  pre-bootstrap checkpoint and re-runs the bootstrap; the bootstrap circuit and
  recorded deltas are in-memory only and lost on crash — safe, since no new-view
  output was emitted before cutover.

### Failure handling

Any failure or abort is **fatal** — no degraded-but-running state, no fallback.
Teardown mid-backfill, an error from any `*_concurrent_bootstrap` /
`step_bootstrap_circuit` call, a stop mid-bootstrap, or worker divergence
(`ReplayInfoConflict`, fingerprint mismatch) all fail the pipeline; the
`concurrent_bootstrap_aborted` flag keeps checkpoints blocked so no
half-bootstrapped state is captured. On restart the bootstrap re-runs from the
pre-bootstrap checkpoint.
