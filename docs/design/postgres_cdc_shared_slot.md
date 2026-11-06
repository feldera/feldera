# Postgres CDC: one replication slot per publication

Plan for https://github.com/feldera/feldera/issues/7413.

Checked against `main` at `0f3c32534` (v0.379.0). Since the first draft, main has:

- Renamed `crates/adapters` to `crates/connectors` (`f74271ece`).
- Put the controller behind engine traits, with the dbsp engine in `crates/static-connectors` (`be5509ae8`). Connector tests use `#[engine_test(...)]`.
- Moved the connector to upstream supabase/etl at `dbb7c00b`, mirrored by feldera/etl with no patches (`381926d17`). A stop now sets `drain_on_shutdown: false` and parks unanswered results instead of failing them.
- Added the per-connector options `batch` and `memory_backpressure` to `PostgresCdcReaderConfig`.
- Made the connectors initialize in parallel on a thread pool (`controller.rs:8175-8246`).

Section 9 lists what these changes do to the plan.

## 1. Problem

Paths are relative to `crates/connectors/src/integrated/postgres/` unless they are shown in full.

| Where | Behavior today | Cost |
| --- | --- | --- |
| `cdc_input.rs:2988` | `pipeline_id = hash(host:port/db, publication, source_table)` | etl names its apply slot `supabase_etl_apply_<id>`, so N tables use N slots |
| `cdc_input.rs:762` | `worker_task_inner` makes one `etl::Pipeline`, one `PostgresStore`, one tokio runtime and one `TableErrorMonitor` for each connector | O(N) replication connections, O(N) state-store pools, N pollers of `etl.replication_state` |
| `cdc_input.rs:846` | `table_sync_copy: TableSyncCopyConfig::IncludeAllTables` | each connector copies every table of the publication |
| `cdc_input.rs:1681`, `:2033` | `CopyWrite::OtherTable` and `column_names_for_target_schema` drop the rows of other tables | N connectors on an N-table publication copy and decode N^2 tables and keep N |
| etl `crates/etl/src/pipeline.rs:391-404` (pinned rev) | etl sets every table of the publication to `Init` and starts a sync worker for each | one transient `table_sync` slot per table per connector at start |

PostgreSQL decodes the full WAL once per slot. The publication filters only the output. With the default `max_replication_slots = 10`, a program is limited to about 10 CDC tables.

## 2. What other systems do

| System | Slots | Where tables are split | Table added later | Slot advance | One consumer stalls |
| --- | --- | --- | --- | --- | --- |
| PostgreSQL native | 1 per subscription, plus a transient slot per table during copy | pgoutput filters by publication, one apply worker | `ALTER SUBSCRIPTION REFRESH PUBLICATION` copies only the new tables | apply worker feedback | whole subscription |
| Debezium | 1 per connector | one Kafka topic per table | incremental snapshot, stream continues | one offset commit | Kafka decouples sinks |
| Materialize | 1 per source | one subsource per table | `ALTER SOURCE ADD SUBSOURCE` with a temporary slot, other subsources paused | one source LSN | not documented |
| RisingWave | 1 per shared source | one filter fragment per table | per-table backfill with buffering | source offset | per-table buffer |
| Flink CDC | 1 global, plus 1 temporary per subtask during snapshot | regex in one job | savepoint restore | checkpoint, delayed by 3 | whole job |
| PeerDB, Estuary, Airbyte, Fivetran | 1 per mirror, capture or connection | N destination tables | per-table backfill, other tables paused | one sync checkpoint | all together |

All systems share one slot across tables. Per-table slots exist only as transient copy slots. The split happens on the consumer side after one decode. No system advances the slot per table.

Precedents in this repository and in etl:

| Precedent | Location | Lesson |
| --- | --- | --- |
| Nexmark: one generator feeds three tables | `crates/static-connectors/src/nexmark.rs:151` | Waits for all expected tables before it starts. Uses a process-global `Weak`, so two pipelines in one process collide. Rejects late joiners. Keep the first behavior, avoid the other two. |
| No connector shares a client or pool | Kafka, S3, URL transports | The shared CDC pipeline is the first shared connector resource with controller scope. `datafusion_runtime_env` (`crates/connectors/src/controller.rs:7973`) is the only model. |
| etl: one destination per pipeline, routed by `TableId` | etl `crates/etl/src/pipeline.rs`, `crates/etl-destinations` (BigQuery, Iceberg) | Each etl destination groups a batch by table and answers one `Durable` for the whole batch. |
| etl starts a sync worker for any table in `Init`, after each commit and when idle | etl `crates/etl/src/replication/apply/mod.rs:3306` (`get_syncing_tables`), `:3655`, `:3873` (pinned rev) | A table set to `Init` through the shared store while etl runs gets its own transient slot, snapshot and catchup. No restart. |
| Startup rollback of stop errors left by older connectors already covers every table of the publication | `cdc_input.rs:77` (`LEGACY_STOP_ERRORS`) | Shared state needs no new rollback rule. |

## 3. Design

One etl pipeline for each (Feldera pipeline, host, connection identity, publication). Connectors on the same publication are members of it.

```
Postgres publication P ──one slot──▶ SharedCdcPipeline(P)            one per Feldera pipeline and host
                                      ├─ etl::Pipeline, PostgresStore, tokio runtime
                                      ├─ DispatchingDestination: route by (schema, table)
                                      │    ├─ Member(orders)  → InputQueue → Feldera table 1
                                      │    ├─ Member(items)   → InputQueue → Feldera table 2
                                      │    └─ unmapped tables → copied once, events dropped
                                      ├─ one TableErrorMonitor, evaluates every member
                                      └─ one completion watcher, ack fan-in per write
```

| Concern | Decision |
| --- | --- |
| Feature flag | A `dev_tweaks` flag (`crates/feldera-types/src/config/dev_tweaks.rs:48`), off by default. When the flag is off, `PublicationKey` includes `source_table` and leaves out the Feldera pipeline and host. Each shared pipeline then has exactly one member and keeps today's id, so there is no re-snapshot and no slot cleanup. Legacy mode is a key choice, not a second code path. Make the flag default on after a stabilization period. |
| Pipeline id | Flag on: `hash(feldera_pipeline, host_idx, host:port/db, publication)`. `feldera_pipeline` is `PipelineConfig.name` (`pipeline-<uuid>`, does not change on rename, `crates/feldera-types/src/config.rs:225-229`) when set. `host_idx` is `Layout::local_host_idx()` when the pipeline is not solo. Without them, two Feldera pipelines that read different tables of one publication would use one slot name and one set of `etl.replication_state` rows (section 9, R1). |
| Registry | Controller scope. Add a generic `ConnectorRegistry` (`Mutex<HashMap<String, Weak<dyn Any + Send + Sync>>>`) to `ControllerInner` next to `datafusion_runtime_env` (`controller.rs:7973`). Pass it through `create_integrated_input_endpoint` (`crates/connectors/src/integrated.rs:118`, call site `controller.rs:8673`). No field in the controller depends on a feature flag. |
| Member config | `uri` user, TLS, `run_source_migrations`, `batch` and `memory_backpressure` apply to the whole etl pipeline. A member whose values differ from the first member's is rejected at `join`, under the members lock, with an error that names both connectors and the field (section 10.6). Nexmark rejects conflicting options in the same way. |
| Startup | `open` keeps the synchronous per-table checks (publication listing, column validation) and registers the member without starting etl (`cdc_input.rs:231-300`). Connectors initialize in parallel on a pool of `max_parallel_connector_init` threads (`controller.rs:8175-8246`). An `open` that waits for its peers would deadlock when the pool has fewer threads than the publication has members. After `run_on_thread_pool` returns, the controller calls `ConnectorRegistry::initial_connect_done()`, and the shared pipelines start etl on that signal. The signal returns the start result, so a start error is still reported at pipeline start. Members learn of a failure through `consumer.error(fatal)`. Configs are not parsed to predict membership: a `${secret:...}` URI, a connector that starts `paused`, or a member whose checks fail would make such a prediction wrong. |
| Copy scope | `IncludeAllTables`. With sharing, etl copies each table once. A table that no member reads costs one discarded copy, as in Materialize. `IncludeTables` is fixed when etl starts and would block the late join below. A dynamic predicate is an upstream etl follow-up. |
| Late join (API `connect`, journal replay `add_inputs` at `controller.rs:5537`) | Set the new member's table to `Init` through the shared `PostgresStore`, confirm that the state is no longer `Ready`, then arm the member for routing. etl's apply worker starts a sync worker after the next commit or when idle. A member accepts `write_events` only after `drop_table_for_copy` or its first `write_table_rows` marked its copy as started, or when it resumed with a completed copy. Two cases fall back to stopping etl and starting it again with the new member set: a table that was not in the publication when etl started (etl reads the table list once, `pipeline.rs:391`), and etl failing on `Ready -> Init` at runtime. |
| Leave (API `disconnect`, pipeline stop, journal replay `remove_inputs`) | A leave never counts an ack share as done. At pipeline stop the controller drops the readers one by one (`controller.rs:8474`), and a share counted as done would let the last member advance the slot past rows the others never stored. A leave parks the member's open fan-ins, as `ParkedResults` does today (`cdc_input.rs:401`), and stops etl, which with `drain_on_shutdown: false` abandons the pending write and reads it again on the next start. If members remain after a short membership-stable grace, etl starts again for them. A member that joins again snapshots its table again. |
| Pause | The shared stream waits while any member is `Paused`. This replaces the per-endpoint `wait_unpaused` (`cdc_input.rs:2002`). The WAL stream is one ordered sequence, and a push into the queue of a paused member would defeat backpressure. Flink and PostgreSQL native stop the whole job too. Consequences to document: the controller pauses endpoints one by one on backpressure, so one busy Feldera table slows every table of the publication, and a large late-join snapshot holds checkpoints for the whole pipeline through the copy barrier. |
| Acks | A `write_events` batch can contain rows for several members. Fan-in: a counter of the members that got rows; the last queue entry of each member carries one share; the last share answers etl. Steps are pipeline-wide (`controller.rs:10530`), so the fan-in is the same as "wait for the largest `flush_step` among the members". A batch with no member rows answers `Durable` at once. |
| Copy barrier | Stays per member (`CopyBarrier`, `cdc_input.rs:419`). etl's terminal `write_table_rows` is per table. |
| Table errors | One monitor reads the store. An `Errored` member table fails only that member. An etl pipeline error fails all members. |
| Upgrade (flag on) | Resume metadata carries the `pipeline_id` of the slot it was taken against (`resume_for`, `cdc_input.rs:1604`). A member whose metadata names a different id has no usable copy: the new slot starts at the current WAL position, so `reconcile_decision` (`cdc_input.rs:2297`) returns `RedoCopy`, with fault tolerance and without. Otherwise the changes between the checkpoint and the upgrade would be lost. Cleanup: when the metadata names a legacy id, drop the `supabase_etl_apply_*` and `supabase_etl_table_sync_*` slots of that id and delete its `etl.replication_state` rows. Without metadata, drop only inactive slots with the legacy id and log each one. Orphaned slots keep WAL without limit (Materialize and Flink document this). |
| Slot budget | One apply slot plus up to `max_table_sync_workers` transient copy slots per publication. The etl default is 4 (etl `etl-config/src/shared/pipeline.rs:480`, pinned rev), so a shared pipeline can hold 5 slots during the initial copy. CI Postgres allows 4 (`test.rs:3052`). Read `max_replication_slots` and the slots in use at start and set `max_table_sync_workers` to what is free, at least 1. Report an actionable error when not even the apply slot is free. |
| Runtime threads | `RUNTIME_WORKER_THREADS` (`cdc_input.rs:2943`) is sized for one copying table. Use `max_table_sync_workers * max_copy_connections_per_table`, with a cap. |

### 3.1 Types

Names follow the code in `cdc_input.rs` (`InputQueue<QueueAux>`, `CopyBarrier`, `WatcherReceiver`, `Ack`, `ParkedResults`).

```rust
// ---------------------------------------------------------------- identity

/// What one slot is shared by. With the dev_tweaks flag off, `source_table`
/// is set and `feldera_pipeline` and `host_idx` are `None`: the id is then
/// today's (cdc_input.rs:2988).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PublicationKey {
    feldera_pipeline: Option<String>,   // PipelineConfig.name, `pipeline-<uuid>`
    host_idx: Option<usize>,            // Layout::local_host_idx(), when not solo
    host: String,
    port: u16,
    database: String,
    publication: String,
    source_table: Option<String>,       // legacy mode only
}

impl PublicationKey {
    fn new(config: &PostgresCdcReaderConfig, pipeline: &PipelineConfig, layout: &Layout, shared: bool) -> AnyResult<Self>;
    /// etl pipeline id, names the slot `supabase_etl_apply_<id>`.
    fn pipeline_id(&self) -> u64 { xxh3_64(self.identity_string().as_bytes()) }
    /// Id the pre-sharing connector used for `table`; for slot cleanup.
    fn legacy_pipeline_id(&self, table: &str) -> u64;
}

/// Schema-qualified source table, resolved (`orders` -> `public.orders`).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableName { schema: String, name: String }

// ---------------------------------------------------------------- registry

/// Owned by the controller. Lives on `ControllerInner` next to
/// `datafusion_runtime_env` (controller.rs:7973) and reaches the factory
/// through `create_integrated_input_endpoint` (integrated.rs:118).
#[derive(Default)]
pub struct ConnectorRegistry {
    shared: Mutex<HashMap<String, Weak<dyn Any + Send + Sync>>>,
}

impl ConnectorRegistry {
    /// Upgrade or create, as Nexmark does, but scoped to one pipeline.
    pub fn get_or_create<T: Any + Send + Sync>(&self, key: &str, make: impl FnOnce() -> Arc<T>) -> Arc<T>;
    /// Called by the controller after the connector-init pool has returned.
    pub fn initial_connect_done(&self) -> Result<(), ControllerError>;
}

// ---------------------------------------------------------------- shared pipeline

/// `SharedCdcPipeline(P)` in the diagram. One per `PublicationKey`.
/// Members hold an `Arc` to it; the last drop stops etl.
pub struct SharedCdcPipeline {
    key: PublicationKey,
    config: SharedConfig,
    members: Mutex<Members>,
    /// Set by `ConnectorRegistry::initial_connect_done()`. A join before it
    /// registers only; a join after it is a late join.
    initial_connect_done: AtomicBool,
    /// Shared with `DispatchingDestination`; also the late-join entry point
    /// (`update_table_state(id, Init)`, picked up by etl, apply/mod.rs:3306).
    store: OnceLock<PostgresStore>,
    /// The worker thread owns the runtime, as today (cdc_input.rs:267,
    /// `block_on_then_shut_down`); dropping a runtime on its own thread
    /// panics. The struct keeps only the handle.
    etl: Mutex<EtlHandle>,                // NotStarted | Starting | Running { pipeline } | Stopped
    stopping: Arc<AtomicBool>,
    /// Results that cannot be answered when a member leaves or etl stops.
    parked: ParkedResults,
}

/// Options that apply to the whole etl pipeline. Every member must agree.
#[derive(PartialEq)]
struct SharedConfig {
    username: String,
    tls: PostgresTlsConfig,
    run_source_migrations: bool,
    batch: PostgresCdcBatchConfig,
    memory_backpressure: PostgresCdcMemoryBackpressureConfig,
}

#[derive(Default)]
struct Members {
    /// Several Feldera tables can read one source table (today: two slots).
    by_table: BTreeMap<TableName, Vec<Arc<Member>>>,
    /// Members that joined after etl started and still need their copy.
    joined_late: BTreeSet<TableName>,
}

impl SharedCdcPipeline {
    /// Called from `PostgresCdcInputReader::new`, possibly on several
    /// connector-init threads at once. Runs the synchronous per-table checks
    /// (publication_tables, columns), compares `SharedConfig` with the other
    /// members' under the members lock, then registers. A late join resets
    /// the table to `Init` and blocks until it is armed.
    fn join(self: &Arc<Self>, member: Arc<Member>) -> AnyResult<()>;
    /// Called from `PostgresCdcInputReader::drop`. Parks the member's open
    /// fan-ins, stops etl, starts it again for the remaining members after
    /// a membership-stable grace.
    fn leave(self: &Arc<Self>, member: &Member);
    /// Starts etl; returns its start result.
    fn start(self: &Arc<Self>) -> AnyResult<()>;
    /// Any member paused => the whole stream waits.
    fn all_running(&self) -> bool;
    fn fail_all(&self, error: anyhow::Error);
}

// ---------------------------------------------------------------- member

/// One Feldera table fed by the shared pipeline. Replaces the per-table
/// parts of `PostgresCdcInputInner` (cdc_input.rs:621).
pub struct Member {
    table: TableName,
    endpoint_name: String,
    consumer: Box<dyn InputConsumer>,
    queue: Arc<InputQueue<QueueAux>>,
    input_stream: Mutex<Box<dyn DeCollectionStream>>,
    feldera_required_columns: Vec<String>,
    /// Per table: etl's terminal `write_table_rows` is per table.
    copy: Arc<CopyBarrier>,
    pipeline_state: watch::Receiver<PipelineState>,
    /// Strict (checkpoint frontier) or Fast (step frontier); unchanged.
    watcher: Option<WatcherReceiver>,
    acks_tx: Option<mpsc::UnboundedSender<HeldAcks>>,
}

// ---------------------------------------------------------------- destination

/// `DispatchingDestination` in the diagram. One per shared pipeline.
#[derive(Clone)]
struct DispatchingDestination {
    pipeline: Weak<SharedCdcPipeline>,
    stopping: Arc<AtomicBool>,
}

impl DispatchingDestination {
    /// Empty for a table no member reads: its rows are dropped and answered
    /// `Durable`, as `CopyWrite::OtherTable` does today (cdc_input.rs:1681).
    /// A late-joined member is listed only once its copy has started.
    fn members_for(&self, schema: &ReplicatedTableSchema) -> Vec<Arc<Member>>;
}

impl Destination for DispatchingDestination {
    // drop_table_for_copy / write_table_rows: route to the table's members,
    // per-member CopyBarrier unchanged.
    // write_events: split rows by member, one `AckFanIn` per batch.
}

// ---------------------------------------------------------------- acks

/// One etl `write_events` answer, owed by every member that got rows.
/// The last share answers etl. Same contract as etl's own destinations.
struct AckFanIn {
    pending: AtomicUsize,
    answer: Mutex<Option<WriteEventsResult>>,
}

impl AckFanIn {
    fn new(answer: WriteEventsResult, shares: usize) -> Arc<Self>;
    /// Returns the `Ack` a member stores in its last `QueueAux` entry.
    fn share(self: &Arc<Self>) -> Ack;
    /// A leaving member parks the answer: etl, stopped without draining,
    /// abandons the write and reads it again. Answering or failing it would
    /// advance the slot or record a table error.
    fn park(self: &Arc<Self>, parked: &ParkedResults);
}
```

### 3.2 Runtime paths

| Event | Path |
| --- | --- |
| Endpoint `open` (pool thread) | `registry.get_or_create(key)` -> `pipeline.join(member)`; registers and returns |
| Connector-init pool returned | `registry.initial_connect_done()` -> each shared pipeline starts its worker thread: `PostgresStore::new`, one `load_table_states`, legacy cleanup, per-member `reconcile_table_state` (`RedoCopy` when the resume metadata names another `pipeline_id`), slot budget, `Pipeline::new(config, store, DispatchingDestination)`; the call returns the start result |
| Runtime `connect`, or journal replay `add_inputs` | `join` sees etl `Running`, calls `store.update_table_state(oid, Init)`, waits until the state is no longer `Ready`, arms the member; etl starts the sync worker. Table not in etl's schema store: stop and start again |
| `write_events` with rows for members A and B | `AckFanIn::new(answer, 2)`; A and B each queue their last entry with one share; the slot advances when both have fired |
| Member reader drop | `leave`: park its fan-ins, remove it from `by_table`, stop etl as `shutdown_etl_pipeline` does today (`cdc_input.rs:1119`); if members remain after the grace, start again |

## 4. Work breakdown

One commit per row. Each commit body says why.

| # | Step | Files |
| --- | --- | --- |
| 1 | Refactor with no behavior change: split `cdc_input.rs` (5175 lines) into `pipeline.rs` (etl lifecycle), `member.rs` (reader, queue, copy barrier, acks), `destination.rs`, `reconcile.rs`, `monitor.rs`. A pure move: the commit body says so and the diff has no other edits. | `crates/connectors/src/integrated/postgres/cdc/` |
| 2 | `ConnectorRegistry` on the controller, passed to the factory, with `initial_connect_done()` after the connector-init pool | `crates/connectors/src/controller.rs`, `crates/connectors/src/integrated.rs` |
| 3 | `dev_tweaks` flag. `PublicationKey`. `SharedCdcPipeline` with one member in legacy mode: same ids, same behavior | `crates/feldera-types/src/config/dev_tweaks.rs`, `cdc/pipeline.rs` |
| 4 | `DispatchingDestination`: route by table name, ack fan-in, any-paused wait, `SharedConfig` check | `cdc/destination.rs`, `cdc/pipeline.rs` |
| 5 | Leave with park and restart. Late join through `Init` reset with arming. Legacy id detection and cleanup. Slot budget | `cdc/pipeline.rs`, `cdc/reconcile.rs` |
| 6 | Shared `TableErrorMonitor` and completion watcher | `cdc/monitor.rs` |
| 7 | Integration test for `Ready -> Init` at runtime in this repository. Upstream etl test or fix if it fails | `test/cdc_scenarios.rs`, supabase/etl |
| 8 | Docs and rustdoc | section 6 |

Steps 1 and 2 are independent and can be separate PRs.

## 5. Tests

Integration tests use `#[engine_test(serial, services(Postgres))]` in `test.rs`, `test/cdc_scenarios.rs` and `test/cdc_privileges.rs`. CI runs them against `debezium/postgres:15` (`.github/workflows/test-adapters.yml:43`, `POSTGRES_URL` at `:215`). These helpers rebuild the id from `(url, publication, table)` and must use `PublicationKey`: `drop_replication_slots` (`test.rs:3035`), `wait_for_slots_released` (`test.rs:3609`), `slot_token` (`test/cdc_scenarios.rs:1297`). These tests describe per-table slots and change with the flag on: `replication_slots_survive_restart` (`:1125`), `other_tables_in_publication_are_filtered` (`:1169`), `etl_state_lives_in_source_database` (`:892`). Run them with the flag off and with it on.

| Test | Asserts |
| --- | --- |
| more tables in one publication than `max_replication_slots` | the pipeline starts and every table gets its rows (the acceptance test of the issue) |
| two tables, one publication | one `supabase_etl_apply_*` slot while the pipeline runs; both tables get snapshot and streamed rows |
| two tables, one of them with the full type schema (`cdc_all_types_relation`, used by `all_types_replayed_after_restart` at `test/cdc_scenarios.rs:1005`) | every type arrives through the dispatching destination |
| FT, two members, checkpoint and resume | the slot advances only past checkpoints that hold the rows of both members; the resume reads nothing again |
| pause one member | the other stops too; start resumes both |
| disconnect one member | etl starts again once for the other member; it loses no rows; one slot again |
| stop with queued rows in two members | the stop answers no write `Durable`; after restart both tables hold every row (R2, finding 1) |
| resume from a checkpoint of the per-table version, flag turned on | both tables snapshot again; a row changed between the checkpoint and the upgrade is present (finding 2) |
| one member fails its column check at start | the other members start; the failing one reports its error at pipeline start (finding 3) |
| connector-init pool of 1 thread, 3 members | the pipeline starts (R3) |
| two Feldera tables from one source table | both get rows from one slot |
| members with a different URI user or `batch` | the start fails with an error that names both connectors and the field |
| two Feldera pipelines, different tables of one publication | two slots, both pipelines run (R1) |
| late join through `connect` | the new member snapshots; the existing member loses no rows; etl does not start again |
| late join through journal replay | FT resume with `add_inputs` in the journal: same as above (R5) |
| table that no member reads | copied once, no rows reach Feldera, state reaches `Ready` |
| legacy upgrade | the old per-table slot and state rows are gone after start |
| two publications, one database | two slots |
| flag off | today's slot names; all existing CDC tests pass unchanged |
| unit: ack fan-in | fires only after the last share; `park` keeps the sender alive |
| unit: any-paused predicate, dispatch by qualified name, legacy-id detection in resume metadata, `SharedConfig` comparison, slot budget arithmetic | |

Each new test is run against the old behavior by reverting the relevant step, to confirm that it catches the problem.

Deterministic test hooks, so that no test depends on timing:

- The membership-stable grace is a constant that tests set to 0.
- A test-only counter of etl starts, like `STOPPED_PAST_GRACE` (`cdc_input.rs:2937`), proves that etl did or did not start again.
- Rows stay queued with `CircuitShape { max_batch_size: Some(1), .. }` (`test.rs:3436`) while the pipeline is paused.
- Waits use the existing `wait(condition, timeout)` with a condition, never a sleep.

## 6. Docs

`docs.feldera.com/docs/connectors/sources/postgresql-cdc.md`:

- New section on slot sharing per publication, and the flag that enables it.
- Pause semantics across members.
- Slot sizing: 1 apply slot plus up to `max_table_sync_workers` transient copy slots per publication; how the connector sizes `max_table_sync_workers` from the free slots. Recommend `max_slot_wal_keep_size`.
- `batch` and `memory_backpressure` apply to the publication: members must agree. Update "Batch configuration" and "Memory backpressure configuration".
- The one-time snapshot when the flag is turned on.
- "Resume behavior": the identity list with the flag on.
- Changelog entry (`docs.feldera.com/docs/changelog.md`).

Rustdoc, which becomes the OpenAPI text: `PostgresCdcReaderConfig.publication`, `.batch` and `.memory_backpressure` in `crates/feldera-types/src/transport/postgres.rs`, and the new `dev_tweaks` field with its type, default and description.

## 7. Open decisions

1. Upgrade path: closed by the feature flag. With the flag off nothing changes. With the flag on each table snapshots once.
2. Pause: any paused member pauses the publication (recommended), or a paused member buffers without limit.
3. Upstream etl follow-ups: a dynamic copy predicate so tables that no member reads are not copied, and an option that leaves them out of replication to remove their transient slots and sync workers.
4. Gen-2 engine: `feldera/forge` (`crates/crucible`) copies the connector config parsing. Check whether it also creates integrated input endpoints, and if so pass it a `ConnectorRegistry` too.

## 8. Adversarial review

Findings against the first design, each with the change it caused.

| # | Severity | Problem | Resolution in this plan |
| --- | --- | --- | --- |
| 1 | Data loss | A leaving member that counts its ack shares as done lets the last member answer `Durable` at pipeline stop, so the slot passes rows N-1 members never stored | Leave parks the fan-in; etl, stopped without draining, abandons the write and reads it again. Leave stops etl and starts it again for the remaining members |
| 2 | Data loss | Resume from a checkpoint of the old version: reconcile without fault tolerance keeps the copy, but the new slot starts now, so changes since the checkpoint are lost | Resume metadata that names another `pipeline_id` means `RedoCopy` |
| 3 | Liveness | Predicting members from `PipelineConfig.inputs` hangs when a member fails its checks, uses a `${secret:...}` URI, or starts `paused` | The controller signals `initial_connect_done()`; no prediction |
| 4 | Semantics | A fully non-blocking `open` turns start failures into runtime connector errors | The signal that starts etl returns its result |
| 5 | Correctness | A late-joined member that can get rows before its table is back in `Init` receives events before its snapshot | `Init` first, confirm, then arm; route only after the copy started |
| 6 | Correctness | A table added to the publication after etl started has no schema row; an `Init` reset cannot find it | Stop and start again, as PostgreSQL needs `REFRESH PUBLICATION` |
| 7 | Correctness | One member per source table; two Feldera tables from one PG table work today | `Vec<Arc<Member>>` per table |
| 8 | Correctness | Members with one key but different credentials or options share the first member's config without notice | Reject at `join` |
| 9 | Safety | Dropping any inactive legacy slot can hit another pipeline still on the old version | Precise cleanup from resume metadata; otherwise inactive slots only, each logged |
| 10 | Crash | A runtime owned by the shared struct can be dropped on its own thread | The worker thread owns the runtime |
| 11 | Behavior | One paused or backpressured member, or one large late-join snapshot, holds every table of the publication | Accepted; documented |
| 12 | Scale | `load_table_states` once per member per poll | Once per poll |
| 13 | Threads | Runtime sized for one copying table | `max_table_sync_workers * max_copy_connections_per_table`, capped |
| 14 | Opportunity | A source transaction across members is split over steps. Not a regression | Follow-up: connector transactions (`crates/adapterlib/src/transport.rs`, `start_transaction` / `commit_transaction`) |

## 9. Re-assessment after the rebase onto `0f3c32534`

| # | Severity | What changed on main | Effect on the plan |
| --- | --- | --- | --- |
| R1 | Data loss | Not new on main; found while checking the key again. A key of (database, publication) is the same in every Feldera pipeline and on every host. Two pipelines that read different tables of one publication work today (different ids). With sharing they would use one slot name and one set of `etl.replication_state` rows. | The key includes `PipelineConfig.name` and, for multihost, `local_host_idx`. Test with two Feldera pipelines. |
| R2 | Correctness | `381926d17`: a stop sets `drain_on_shutdown: false` and parks results (`cdc_input.rs:401`, `:1119`). `release_pending_answers` and the shutdown-marker error are gone; failing a result now records a `ManualRetry` table error. | Finding 1: leave parks the fan-in instead of failing it. `AckFanIn::poison` became `park`. |
| R3 | Liveness | Connectors initialize in parallel on a pool of `max_parallel_connector_init` threads (`controller.rs:8175-8246`). | `join` runs on several threads at once: compare `SharedConfig` under the members lock. An `open` that waits for peers would deadlock when the pool is small; `initial_connect_done()` after the pool avoids that. Test with a pool of 1. |
| R4 | Correctness | New per-connector options `batch` and `memory_backpressure` (`crates/feldera-types/src/transport/postgres.rs:82`, `:180`) configure the whole etl pipeline. | Part of `SharedConfig`; members must agree. Docs. |
| R5 | Correctness | Journal replay connects and disconnects inputs (`controller.rs:5529-5548`). | Late join and leave also run during replay. Test. |
| R6 | Tests | `#[engine_test]` harness; CDC scenarios in `test/cdc_scenarios.rs` describe per-table slots. | Update the helpers and three scenario tests; run them with the flag off and on. |
| R7 | Process | etl is upstream supabase/etl, mirrored by feldera/etl without patches. | No fork commits. The `Ready -> Init` check is an integration test here; a fix, if needed, goes upstream. |
| R8 | None | Startup rollback of legacy stop errors covers every table of the publication. | Nothing to add for shared state. |

Items that stay open: the length of the membership-stable grace, and whether etl accepts `Ready -> Init` at runtime (work item 7 chooses between the `Init` path and the restart path for late joins).

## 10. PR gates

The `/feldera-pr-checks` gates applied to the plan, so that the PRs pass them.

### 10.1 Testing hierarchy

| Level | Plan |
| --- | --- |
| Manual | A pipeline with 12 tables in one publication against a stock Postgres (`max_replication_slots = 10`): check `pg_replication_slots` while it runs and during the initial copy. Turn the flag on for an existing pipeline and confirm the one-time snapshot and that the old slots are gone. Stop, pause and disconnect one connector, and watch the other tables. Write the results in the PR description. |
| Unit | Section 5, last two rows. |
| Integration | Section 5. Required: the change crosses the Postgres boundary and keeps state over time (slots, etl state, checkpoints). |
| QA / soak | A soak run with 20 tables in one publication, a write load on all of them, periodic checkpoints, pipeline restarts and one `connect` and `disconnect` per hour. Assert that the slot's `confirmed_flush_lsn` advances and that WAL retention stays bounded. |
| Benchmark | WAL decode cost on Postgres: N tables with N slots (flag off) against 1 slot (flag on); measure walsender CPU and ingest throughput. Keep the results for the PR description. |

### 10.2 Flaky tests

| Risk | Rule |
| --- | --- |
| Grace timer before restart | Constant set to 0 in tests; assert on the start counter, not on time. |
| Late join waits for etl's next commit or idle tick | Wait on the table state in `etl.replication_state` with a condition, not a sleep. |
| Parallel init order | Results must not depend on which member registers first; the pool-of-1 and default-pool tests cover both orders. |
| Shared CI Postgres with 4 slots | Each test drops its slots in `Drop` (`test.rs:3035`) with the new id; the slot-budget logic keeps copies within the limit. |

### 10.3 SQL type coverage

The dispatching destination changes how rows reach each table, so one member of the two-table test uses `cdc_all_types_relation`. Type conversion (`row_to_json`, `cell_to_json`) does not change.

### 10.4 Breaking changes

With the flag off: none. With the flag on, these behavior changes are listed in the PR description, the docs and the changelog:

| Change | Rule in `FELDERA_BREAKING_CHANGES.md` |
| --- | --- |
| Slot names change; each table snapshots once | 1: users see the re-snapshot |
| One paused or backpressured table stops the publication | 3: different semantics |
| Disconnecting one connector restarts the others' etl pipeline | 3: different side effects |
| Members with different `uri` user, TLS, `batch` or `memory_backpressure` are rejected | 2: accepted input now rejected |

The connector is experimental and the flag is an opt-in, which matches rule 5.

### 10.5 Feature flag

`dev_tweaks` flag, section 3. Runtime code, so `dev_tweaks` is the right mechanism.

### 10.6 Actionable errors

| Case | Message (what, why, action) |
| --- | --- |
| Config mismatch | `connectors 'orders_cdc' and 'items_cdc' read publication 'shop' of 'db.example.com:5432/app' with different 'batch' settings. They share one replication slot, so they must use the same settings. Make 'batch' the same on both, or give one of them its own publication.` |
| No free slot | `cannot read publication 'shop': the database allows 10 replication slots (max_replication_slots) and all 10 are in use. Free a slot (see pg_replication_slots), or raise max_replication_slots and restart PostgreSQL.` |
| etl start failure reported to the other members | `the replication pipeline for publication 'shop', which this connector shares with 'items_cdc' and 'users_cdc', failed to start: <cause>. Fix the cause and restart the pipeline.` The cause keeps `with_setup_hint` (`cdc_input.rs`). |
| Late join of a table that etl cannot find | `table 'public.items' was added to publication 'shop' after the replication pipeline started; restarting the replication pipeline to copy it.` (log warning, not an error) |

### 10.7 Docs

Section 6, including the rustdoc that becomes OpenAPI and the changelog.

### 10.8 Commits

Section 4: one commit per row, each with a body that explains why. The refactor commit is a pure move. No "address review comments" commits at merge.

### 10.9 Not affected

| Gate | Reason |
| --- | --- |
| New dependencies, licenses, FIPS | No new crates. etl stays at the pinned rev. |
| `unsafe` | None planned. |
| `pull_request_target`, CI workflows | No workflow change; tests run in the existing `test-adapters.yml` job. |
| New crate publishing | No new crate. |
| AGENTS.md / CLAUDE.md | None added. |
| README hygiene | `crates/connectors/README.md` lists transports only; no change. This plan lives in `docs/design/`, not in the repo root. |
