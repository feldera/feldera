# Reading Delta Lake changes through the Change Data Feed

Design for a `change_feed` option on the Delta Lake input connector's `follow`
and `snapshot_and_follow` modes: ingest changes from the table's Change Data
Feed (CDF) instead of reconstructing them from `add`/`remove` file actions.

Status: implemented on branch `delta-cdf-input`, four commits on top of
PR #6921 (`[adapters] delta: fix follow and CDC reads of partitioned tables`),
which supplies the partition-value handling this depends on.

## Summary

Feldera should read `_change_data` itself, inside the existing follow loop, and
fall back to the current `add`/`remove` logic for any commit that carries no
`cdc` action. Neither delta-rs's `CdfLoadBuilder` nor `delta_kernel`'s
`TableChanges` is usable: both drop features the connector already supports
(column mapping, deletion vectors, per-commit schema, `uc://` reads), and the
part they do supply is about forty lines we can write against machinery we
already own.

The work needs one prerequisite: the follow reader used to ignore Hive partition
values, so partition columns came back NULL. CDF cannot be built on top of that,
because change data files are partitioned the same way. PR #6921 fixes it, and
this branch builds on it.

## Why CDF, and where it does not help

Follow mode turns a commit into a ZSet delta at *file* granularity: every
`remove` becomes a retraction of the whole file, every `add` an insertion of the
whole file. The result is algebraically correct - unchanged rows cancel - but
the cost is the size of the rewritten files, not the size of the change. CDF
gives the writer's own row-level statement of the change.

The win is entirely table-shape dependent:

| Commit shape | `change_feed = off` reads | `change_feed = auto` reads |
|---|---|---|
| Blind append (`INSERT`) | added files | added files - **identical**, no `cdc` action exists |
| `DELETE`, deletion vectors on | DV delta rows | DV delta rows - Delta records no change data for it |
| `DELETE`, copy-on-write | removed file + rewritten file | deleted rows only |
| `UPDATE` / `MERGE`, copy-on-write | every removed file + every new file | pre-image + post-image rows only |
| `OPTIMIZE` / compaction | nothing (`data_change = false`) | nothing |
| `RESTORE` | added/removed files | falls back to added/removed files |

So CDF pays off on copy-on-write tables driven by `MERGE` - the standard
Databricks CDC landing pattern, and the shape where a 1 GB file rewritten for
ten changed rows costs 2 GB of reads today. It is neutral on append-only tables
and on deletion-vector tables, where Feldera's existing DV-delta path
(`d832bd8be`) already reads only the changed rows.

Two things CDF does **not** fix, and the design should not claim otherwise:

- **Retention.** `_change_data` files are vacuumed like any other data file
  (delta-io/delta-rs#3392). A connector that lags past the retention window
  breaks in CDF mode exactly as it breaks in follow mode.
- **Writer opt-in.** `delta.enableChangeDataFeed` must be set on the source
  table, and only changes committed after it was enabled are recorded.

## State of CDF support in delta-rs

Pinned at `deltalake` 0.32.3, Feldera fork `78a5d066` (`Cargo.toml:160`).
0.32.4 is the latest upstream release; no newer line exists, and no CDF work
has landed since.

`CdfLoadBuilder` (`crates/core/src/operations/load_cdf.rs`) implements the
protocol's version-walk correctly: for each commit it takes the `cdc` actions if
any, else the `data_change` `add`/`remove` actions, and unions three
`ParquetSource` scans with `_change_type`, `_commit_version`, and
`_commit_timestamp` injected as partition constants. That skeleton is right. The
gaps are in everything around it.

| Requirement | `CdfLoadBuilder` 0.32.3 | Already in the Feldera connector |
|---|---|---|
| Column mapping (`name` / `id`) | none - `create_cdc_schema` reads by logical name against `col-<uuid>` physical columns (`load_cdf.rs:365`) | `physical_read_schema`, `project_physical_to_logical`, nested relabel, field-id realign (`54bc3de55`, `f191aa2a2`) |
| Deletion vectors | none - `create_partition_values` builds bare `PartitionedFile`s, so the `add`/`remove` fallback emits logically deleted rows | `decode_dv`, `filtered_parquet_table`, same-path DV delta (`aa61087d9`, `d832bd8be`) |
| Schema per commit | one snapshot schema for the whole range | `advance_schema` / `pin_schema_to_version` (`input.rs:3227`) |
| `uc://` tables | addresses files by `object_store_url()` | `requires_direct_object_store_read` (`ed9f4299f`) |
| Incremental / resumable | `build()` re-walks the whole commit range per call | the follow loop already reads one commit at a time |
| Retry, health, read semaphore, parse `JobQueue` | none | `retry`, `ConnectorHealth`, `DELTA_READER_SEMAPHORE`, `execute_df_inner` |
| CDF disabled at the start version | hard error `ChangeDataNotEnabled` | can degrade to the follow path |
| `_commit_timestamp` source | `CommitInfo.timestamp`, 0 when absent | n/a (protocol says log-file mtime or `inCommitTimestamp`) |

Upstream tracks the same list in delta-io/delta-rs#4554, which asks for exactly
the feature in this document ("serve snapshot as inserts, then change feed") and
records column mapping as a known correctness issue, deletion vectors and
vacuumed boundary files as unhandled edge cases, and the absence of any
resumable offset.

### `delta_kernel::table_changes` is not an alternative

`buoyant_kernel` 0.22.2 is already a dependency (used only for DV decoding) and
does have a complete `table_changes` module, including `resolve_dvs.rs`. It is
stricter than we can accept:

- column mapping is rejected outright (`table_changes/mod.rs:88`);
- the schema must be *exactly* equal across the whole version range, so any
  schema evolution aborts the read;
- it needs a kernel `Engine`, i.e. a second object-store, credential, and
  runtime stack beside delta-rs's;
- it yields `Box<dyn EngineData>`, not a DataFusion `DataFrame`, so `filter`,
  `skip_unused_columns`, and the parse `JobQueue` would all need a second
  implementation.

### What delta-rs is good for

Its *writer* emits CDC files for `UPDATE`, `DELETE`, `MERGE`, and overwriting
writes (`operations/cdc.rs`, `delete.rs:577`, `update.rs:425`, `merge/mod.rs:855`),
gated on `should_write_cdc`. Rust tests can therefore build CDF fixtures locally
with no Spark. Its delete path is copy-on-write and never writes a deletion
vector, so DV-plus-CDF commits still need hand-built log actions.

## Prerequisite: partition values (PR #6921)

Delta does not store partition columns in the data file. Confirmed on the
delta-rs `cdf-table` fixture:

```
birthday=2023-12-22/part-00000-592a7e14-....parquet    ['id', 'name']
_change_data/birthday=2023-12-22/cdc-00000-59fa51a4-....parquet
                                                       ['id', 'name', '_change_type']
```

`create_parquet_table` declared the full logical schema and set no
`table_partition_cols`, so DataFusion's schema adapter null-filled `birthday`.
Snapshot mode was unaffected - it goes through delta-rs's own table provider -
so the bug was confined to `follow` and `cdc` mode.

PR #6921 fixes it, and this work builds on that branch. It supplies three things
the CDF reader uses directly:

| | |
|---|---|
| `add_partition_columns` | reprojects a frame in table-column order, each partition column a `cast(lit(v) AS <type>)` from the log action, keyed by physical name so column mapping works |
| `file_listing_url` | builds a file's `ListingTableUrl` without decoding the path twice |
| `physical_read_schema` | now excludes partition columns, since no data file carries them |

The CDF reader adds one parameter to `add_partition_columns`: `extra_columns`,
naming columns to carry through that the table schema does not declare. The
reprojection walks the table schema, so without it `_change_type` - the column
that decides each row's polarity - would be dropped before it could be read.

## Design

### Configuration surface

No new modes. `follow` and `snapshot_and_follow` gain an option:

```
change_feed = auto     read change data when a commit records it   (default)
              require  the same, but fail at startup without the table property
              off      never read change data
```

`reads_change_feed()` selects the reader: `follow() && !is_cdc() && change_feed
!= Off`. Every other option keeps its meaning, and `cdc_delete_filter` /
`cdc_order_by` stay rejected outside `cdc` mode.

The first draft used two mode variants, `cdf` and `snapshot_and_cdf`. Three
things settled it the other way:

| | |
|---|---|
| The result is identical | A change feed read and a file-action read produce the same ZSet; `change_feed_matches_file_actions` asserts it. An option that picks how a change is read fits that; a mode, which usually picks what the data means, does not. |
| `cdf` sat one letter from `cdc` | And `cdc` means something else entirely: a change log the user encoded as table rows. |
| An old manager degrades gracefully | It silently strips unknown *fields* but rejects unknown enum *variants*. With `auto` as the default, a new runtime reads the change feed under a manager that has never heard of the option; a new mode name would fail to deserialize. |

That last point is also why the default is `auto` rather than `off`.

**`cdc` mode is excluded, deliberately.** It takes each row's polarity from
`cdc_delete_filter`; Delta's `_change_type` is a second, incompatible answer to
the same question, and an `update_preimage` row read as an append-event is
nonsense. It would also buy nothing: the commits `cdc` mode cares about are
appends, which record no change data at all.

`require` exists because `auto` cannot fail. A connector provisioned for a table
where each `MERGE` rewrites hundreds of gigabytes needs to know at startup that
the feed is on, not infer it from a counter days later.

### Reader

One new branch in `process_log_entry` (`input.rs:2879`), after `advance_schema`:

```
process_log_entry(version, actions)
  ├─ is_cdc()  -> process_cdc_transaction        (unchanged)
  ├─ reads_change_feed() -> process_change_feed_log_entry  (new)
  └─ else      -> the add/remove follow path     (unchanged)
```

`process_cdf_log_entry`:

1. Collect `Action::Cdc(f)` from the commit.
2. **No `cdc` actions** - delegate to the existing follow path verbatim. Per the
   protocol this is an append-only or blind-delete commit, and the follow path is
   both correct and already DV-aware. This is also what makes the mode degrade
   safely if `delta.enableChangeDataFeed` is turned off mid-stream.
3. **Some `cdc` actions** - ignore the commit's `add` and `remove` actions
   entirely. The protocol is explicit: "when CDC actions exist in a version,
   readers must read only those to get the row-level changes, and skip the
   remaining `add` and `remove` actions in this version."
4. Group the change data files by `partition_values`. For each group build a
   `ListingTable` over `change_data_read_schema()` - the physical read schema
   plus `_change_type: Utf8` - then `project_physical_to_logical` and
   `add_partition_columns(.., &[CHANGE_TYPE_COLUMN], ..)`.
5. `UNION ALL` the groups, apply `config.filter`, project to
   `used_columns` + `_change_type`.
6. Execute with `Polarity::ChangeType`.

Steps 4 and 5 are the only genuinely new code. Everything else - schema pinning,
column mapping, retry, health, the read semaphore, the parse `JobQueue`, resume
info, catchup transactions - is reached unchanged.

### Polarity

CDF maps onto ZSet weights without ceremony:

| `_change_type` | weight |
|---|---|
| `insert` | +1 |
| `update_postimage` | +1 |
| `update_preimage` | -1 |
| `delete` | -1 |

`insert_with_polarities` (`adapterlib/src/catalog.rs:166`) already takes a
`&[bool]`, and `execute_df` already computes one for `cdc` mode. Rather than
adding a third positional argument to a function that is already
`#[allow(clippy::too_many_arguments)]`, replace `polarity: bool` and
`cdc_delete_filter: Option<Arc<dyn PhysicalExpr>>` with one parameter:

```rust
enum Polarity {
    /// Every row has the same polarity: snapshot and follow reads.
    Fixed(bool),
    /// `cdc` mode: a predicate over the row decides. True means delete.
    DeleteFilter(Arc<dyn PhysicalExpr>),
    /// A change feed read: `_change_type` decides, and the column is dropped
    /// before the row reaches the input stream.
    ChangeType,
}
```

`take_change_type_polarities` computes the polarity vector from the
`_change_type` string column and removes the column from the batch before the
record deserializer sees it. A value outside the four-word vocabulary is a parse
error naming the value, not a row ingested with a guessed polarity. Dropping the
column keeps a reserved Delta name out of the deserializer; the alternative -
two filtered dataframes, one per polarity - reads each change data file twice,
which is the wrong trade for a large `MERGE`.

### Resume, transactions, end-of-input

Unchanged. `DeltaResumeInfo::follow_mode(version, eoi)` (`input.rs:911`) already
records the last fully ingested commit, and CDF resumes at `version + 1` like
follow. `transaction_mode = catchup` and `always` batch CDF commits exactly as
they batch follow commits, and `end_version` terminates the same way. The
existing suspend/resume tests carry over by construction.

### Enablement checks

`validate_change_data_feed` runs once the table is open. Under `require`, a
table without `delta.enableChangeDataFeed` fails configuration with a message
naming the property; under `auto` it is fine, since the fallback is what `auto`
is for. The same check rejects a table that
declares a column named `_change_type`, which Delta reserves - otherwise
`change_data_read_schema` would declare the column twice and fail with a
DataFusion error naming neither the table nor the cause.

Mid-stream, `warn_if_change_data_feed_disabled` logs once if a commit turns the
property off. The fallback keeps the data correct; only the cost profile
changes.

Not checked: the `changeDataFeed` writer feature. delta-rs exposes the table
`Protocol` only through a `pub(crate)` kernel type, so a table with the property
set but the feature absent - which records nothing - is not distinguishable at
startup. It shows up instead as `..._from_add_remove` rising while
`..._from_change_data` stays at zero.

### Metrics

Two counters, so "why is my CDF connector still slow" is answerable from
`/metrics`:

- `input_connector_delta_commits_from_change_data`
- `input_connector_delta_commits_from_file_actions`

`DeltaPhase::Follow` is reused; the phase gauge's help text needs no change.

These are not only diagnostics. Forcing the fallback leaves the differential
test green, because the fallback is genuinely correct - the counters are what
distinguish "reading the change feed" from "reading files and calling it the
change feed", and the test that asserts on them is the one that fails.

They also answer the question `auto` cannot fail on: a table whose property is
set but whose `writerFeatures` omit `changeDataFeed` records nothing, and shows
up as the second counter rising while the first stays at zero.

### Explicit non-goals

- **Exposing `_change_type` / `_commit_version` / `_commit_timestamp` to SQL.**
  `CONNECTOR_METADATA()` is fed one `Variant` per batch
  (`insert_with_polarities(&batch, &polarities, &metadata)`). The two commit
  columns are constant per commit and would fit; `_change_type` is per row and
  does not. Deferred rather than done halfway.
- **CDF for the initial snapshot.** The snapshot is read through delta-rs's
  table provider, as it always was; the option only affects log following.
- **Making a lagging connector survive `VACUUM`.** Out of reach in either mode.

## Correctness notes

**Filter interaction is exact.** A row whose `UPDATE` moves it across the
`filter` boundary is handled correctly without special casing: the pre-image is
retracted because it had passed the filter and been inserted; the post-image is
filtered out. The reverse direction works symmetrically.

**Projection is consistent.** Pre-image and post-image go through the same
`used_columns` projection as the snapshot did, so a retraction always matches the
row that was originally inserted.

**Half-applied updates are observable between steps.** A pre-image can land in
Feldera step *N* and its post-image in step *N+1*, leaving the row transiently
absent. This is already true of follow mode, where retractions and insertions
are separate `execute_df` calls. Document the remedy: `transaction_mode: always`
(one Feldera transaction per Delta commit) or `catchup`.

**`_change_type` in base data files.** A CDF-enabled table writes a
`_change_type` column into its regular data files too (observed on the fixture
above). The fallback path is safe because `physical_read_schema` derives from
the Delta logical schema, which does not contain it, so DataFusion never
projects it. Worth a regression test rather than a comment.

**Deletion vectors under CDF: the fallback is load-bearing.** Measured against
Delta Spark 4.x with both features on (`fixtures/change_data_feed.py`), a
`DELETE` records **no change data at all** - only a same-path `add`/`remove`
pair carrying deletion vectors, since the vectors already say exactly which rows
left and a pure delete has no new row content. An `UPDATE` on the same table
does record change data, because its post-images are new rows.

So on a Databricks-shaped table, where deletion vectors are the default, every
`DELETE` reaches the connector through the `add`/`remove` fallback. That is not
a defensive corner: it is the common case, and it is what delta-rs's
`CdfLoadBuilder` gets wrong. Its fallback reads the `add` at +1 and the `remove`
at -1 with the vectors unapplied, so the two cancel and the delete vanishes.
Feldera's fallback is the follow path, which computes the vector delta and
retracts exactly the newly-masked rows - the same rows Spark's own change feed
reader derives from that commit.

## Testing

The strongest available oracle is differential: **`change_feed = auto` and
`change_feed = off` must produce the same ZSet for the same table history.**
Both are exercised by the same harness, and the property holds for every commit
shape.

It is not sufficient on its own. Forcing every commit down the `add`/`remove`
fallback leaves the differential test green, because the fallback is correct -
so a second test pins which path each commit shape actually takes.

Fixtures are built with delta-rs's own writer, which emits change data for
`UPDATE`, `DELETE`, and `MERGE`; no Spark is needed.

| Test | What it pins |
|---|---|
| `delta_table_change_feed_matches_follow_test` | append, `UPDATE`, and `DELETE` read in both modes give the same contents |
| `delta_table_change_feed_reads_change_data_test` | an `UPDATE` takes the change feed, an append takes the fallback |
| `delta_table_change_feed_partition_column_test` | an `UPDATE` moving a row between partitions, whose two images sit in different `_change_data` directories |
| `delta_table_change_feed_filter_test` | a row crossing the `filter` boundary in both directions |
| `delta_table_change_feed_suspend_test` | restart across commits made while the connector was down, with no duplicate or lost row |
| `delta_table_change_feed_not_enabled_test` | a table without the property fails at startup, naming it |
| `delta_table_change_feed_skip_unused_columns_test` | a skipped column does not disturb the polarity column |
| `change_type_tests` (unit) | the four-word vocabulary, an unknown value, a NULL, and a missing column |

Each was validated by reverting the code it covers:

| Reverted | Fails |
|---|---|
| `update_preimage` mapped to +1 | `change_feed_matches_file_actions` |
| always take the `add`/`remove` fallback | `change_feed_reads_change_data` only - `change_feed_matches_file_actions` stays green |
| change data file's partition values dropped | `change_feed_partition_column` |
| startup enablement check removed | `change_feed_require_not_enabled` |
| checkpoint recorded one version behind | `change_feed_suspend` |
| `filter` not applied on the change feed path | `change_feed_filter` |

### Platform tests, against Delta Spark

delta-rs cannot write a deletion vector beside a change feed, so the Rust
fixtures cannot reach the shape a Databricks table has. `python/tests/platform/`
adds a layer that can, through the repo's existing `ensure_delta_spark_fixture`
harness:

| Test | What only Spark produces |
|---|---|
| `change_feed_matches_file_actions` | the whole history read both ways |
| `cdf_reads_only_changed_rows` | the 600-against-202 measurement, as an assertion |
| `cdf_deletion_vector_delete` | the `DELETE` that records no change data |
| `change_feed_partition_column` | partition values through both the change feed and the fallback |

All four pass end to end against a manager built from this branch (six pipelines
compiled, six distinct source checksums, no connector errors).

### Unity Catalog

A `uc://` location has no path for a `ListingTable` to resolve, which is why
`add_with_polarity` reads follow-mode files through the object store instead
(`0d3441cde`, `ed9f4299f`). `change_data_group_dataframe` makes the same split:
one provider per change data file for `uc://`, one listing for every other
scheme. A change data file never carries a deletion vector - the protocol gives
`AddCDCFile` no field to carry one - so the bitmap is always empty.

The mechanism is covered by `change_data_file_reads_whole_through_object_store`,
which reads a Spark-shaped change data file (including the `__is_cdc` column the
declared schema prunes) through `filtered_parquet_table`. What that test cannot
cover is the scheme detection and the catalog's credentials together;
`delta_table_unity_change_feed` does, gated on
`DELTA_TABLE_TEST_UNITY_CDF_TABLE` and inert without it.

### MERGE

A `MERGE` is the only operation that records all four change types in one
commit, and the shape a Databricks source is usually maintained by. It is now
the fixture's fifth commit, so every variant covers it, and it records change
data even with deletion vectors on -- unlike a pure `DELETE`, because its
updates and inserts produce genuinely new rows.

`delta_table_change_feed_merge_test` covers it in the Rust suite through
delta-rs's own merge, so a regression in the polarity mapping is caught without
the Spark stack. `test_delta_input_change_feed_merge` covers the Spark-written
one, replaying the merge commit alone against the snapshot before it: four
recorded rows against the hundred-row file a copy-on-write merge rewrites.

### Schema evolution

Adding a column is the only schema change a change-feed table can undergo.
Delta rejects `DROP COLUMN` and `RENAME COLUMN` outright without column mapping
(`DELTA_UNSUPPORTED_DROP_COLUMN`, `DELTA_UNSUPPORTED_RENAME_COLUMN`), and
rejects them again *with* column mapping once a change feed is enabled
(`DELTA_BLOCK_COLUMN_MAPPING_AND_CDC_OPERATION`). So the reader never has to
interpret change data written under a column that has since been renamed away -
only under one that did not exist yet.

`test_delta_input_change_feed_added_column` replays a whole history across an
`ADD COLUMN`: the change data of three earlier commits was written before the
column existed, while the SQL table declares it. Reverting `advance_schema`
leaves the row count right and the column empty (`extra_rows` 0 against 1),
which is the shape of the defect and also the answer to a question worth having:
a SQL column the Delta schema does not yet carry arrives NULL rather than
failing the read, so declaring it ahead of the source is safe.

### skip_unused_columns

`change_data_read_schema` declares every column and leaves the pruning to
projection pushdown, which on this path has more to survive than on the follow
path: the physical-to-logical rename, the partition literals, and a union across
partition groups. Measured rather than assumed -
`change_feed_projection_prunes_scan` builds that shape and reads the plan:

```
Union
  Projection: cdc_0.id, cdc_0.s, Utf8("0") AS grp, cdc_0._change_type
    Filter: cdc_0.region = Utf8("us")
      TableScan: cdc_0 projection=[id, s, region, _change_type]
```

`junk` is pruned, the filter-only `region` is kept, and the partition literal
never reaches the scan. So the declared schema can stay wide; restricting it
would buy nothing and could only drop a column some later node needs.

`VARIANT` is deliberately left until that support lands on its own branch.

## Commit plan

As landed, on top of PR #6921:

| | |
|---|---|
| `[types] delta: add the cdf and snapshot_and_cdf ingest modes` | the first draft's config surface |
| `[adapters] delta: name the polarity rule a read applies` | the `Polarity` enum, no behavior change |
| `[adapters] delta: ingest changes from the change data feed` | the reader, the fallback, the checks, the counters, the tests |
| `[docs] delta: document the cdf and snapshot_and_cdf modes` | connector docs, `openapi.json`, the generated TypeScript client |
| `[docs] delta: design note for the change data feed reader` | this document |
| `[python] delta: end-to-end cdf tests against Spark-written change data` | the PySpark fixture, four platform tests, and the corrected deletion-vector claim |
| `[python] delta: read a column-mapped change feed` | column mapping through a change data file; one commit history for every fixture variant |
| `[adapters] delta: cdf under the catchup and always transaction modes` | transaction-mode coverage |
| `[types]/[adapters]/[docs] delta: replace the cdf modes with a change_feed option` | the surface this document now describes |

The reader and its tests are one commit rather than two: the tests are the
argument that the reader is right, and splitting them makes the first commit
unreviewable on its own.

Two notes on the last commit. `bun run generate-openapi` reformats all of
`js-packages/web-console`; running `bunx openapi-ts` and then prettier over
`src/lib/services/manager` alone keeps the diff to the generated client.
Regenerating also picks up `CheckpointSyncStatus.running`, which `cc64e80fa`
added to `openapi.json` without regenerating the client.

## References

- Delta protocol, [Add CDC File / Change Data Files](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [delta-io/delta-rs#4554](https://github.com/delta-io/delta-rs/issues/4554) - Spark-parity CDF streaming with snapshot bootstrap
- [delta-io/delta-rs#3392](https://github.com/delta-io/delta-rs/issues/3392) - CDF broken by vacuum
- [delta-io/delta-rs#2579](https://github.com/delta-io/delta-rs/issues/2579) - deletes missing from the change feed (fixed by #2721)
- `delta_kernel` CDF: `buoyant_kernel-0.22.2/src/table_changes/mod.rs`
