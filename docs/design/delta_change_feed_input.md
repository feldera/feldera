# Reading Delta Lake changes through the Change Data Feed

Design for a `change_feed` option on the Delta Lake input connector's `follow`
and `snapshot_and_follow` modes: ingest changes from the table's Change Data
Feed (CDF) instead of reconstructing them from `add`/`remove` file actions.

Status: implemented on branch `delta-cdf-input`. PR #6921
(`[adapters] delta: fix follow and CDC reads of partitioned tables`), which
supplies the partition-value handling this depends on, is on main.

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
because change data files are partitioned the same way. PR #6921 fixed it.

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
| `UPDATE` / `MERGE`, copy-on-write | every removed file + every new file | pre-image + post-image rows, read twice (see [Polarity](#polarity)) |
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
| Schema per commit | one snapshot schema for the whole range | `advance_schema` / `pin_schema_to_version` (`input.rs:3687`, `input.rs:3651`) |
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
change_feed = auto     read change data when a commit records it
              require  the same, but fail at startup without the table property
              off      never read change data                     (default)
```

`off` is the default until the manager exposes `change_feed`; the
`DELTA_CHANGE_FEED` environment variable overrides it in the meantime.

`reads_change_feed()` selects the reader: `follow() && !is_cdc() && change_feed
!= Off`. Setting `change_feed` in any other mode is rejected at startup rather
than ignored, since `require` exists to fail loudly and a connector that starts
and reads nothing from the feed is what it rules out. Every other option keeps
its meaning, and `cdc_delete_filter` / `cdc_order_by` stay rejected outside
`cdc` mode.

The first draft used two mode variants, `cdf` and `snapshot_and_cdf`. Three
things settled it the other way:

| | |
|---|---|
| The result is identical | A change feed read and a file-action read produce the same ZSet; `change_feed_matches_file_actions` asserts it. An option that picks how a change is read fits that; a mode, which usually picks what the data means, does not. |
| `cdf` sat one letter from `cdc` | And `cdc` means something else entirely: a change log the user encoded as table rows. |
| An old manager degrades gracefully | It silently strips unknown *fields* but rejects unknown enum *variants*, so a config carrying `change_feed` still round-trips through a manager that has never heard of it; a new mode name would fail to deserialize. |

The first draft defaulted the option to `auto`, on the strength of that last
row: a new runtime would then read the change feed even under an old manager.
The default is now `off`, because a manager that cannot set the option also
cannot clear it, which leaves a pipeline that hits a problem reading the feed
with no way out. `DELTA_CHANGE_FEED` is the temporary way in until the manager
exposes the field.

**`cdc` mode is excluded, deliberately.** It takes each row's polarity from
`cdc_delete_filter`; Delta's `_change_type` is a second, incompatible answer to
the same question, and an `update_preimage` row read as an append-event is
nonsense. It would also buy nothing: the commits `cdc` mode cares about are
appends, which record no change data at all.

`require` exists because `auto` cannot fail. A connector provisioned for a table
where each `MERGE` rewrites hundreds of gigabytes needs to know at startup that
the feed is on, not infer it from a counter days later.

### Reader

One new branch in `process_log_entry` (`input.rs:3072`), after `advance_schema`:

```
process_log_entry(version, actions)
  ├─ is_cdc()  -> process_cdc_transaction        (unchanged)
  ├─ reads_change_feed() -> process_change_feed_log_entry  (new)
  └─ else      -> the add/remove follow path     (unchanged)
```

`process_change_feed_log_entry`:

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
column keeps a reserved Delta name out of the deserializer.

Polarity alone does not settle the order, and a keyed relation needs one. An
upsert stream applies each row as `Update::Insert` or `Update::Delete` keyed by
the row's key, last writer wins, so an insertion that overtakes its own
retraction leaves the key deleted. `process_change_feed_log_entry` therefore
runs two filtered passes over the frame, retractions first, which orders every
retraction of a commit ahead of every insertion of it. The Unity fixture
measures what that is worth: 14 rows of 40 survive without it, 40 with it.

The cost is a second read. Two passes mean two `execute_stream` calls, and
DataFusion caches no scan results, so a commit's change data files are fetched
twice. Measured on one `UPDATE` against a 20,000-row table, total bytes read:

| rows the commit updates | `change_feed = auto` | `change_feed = off` | ratio |
|---|---|---|---|
| 1%   |    19,631 B | 1,822,666 B | 0.01 |
| 50%  |   861,236 B | 1,566,340 B | 0.55 |
| 100% | 1,712,543 B | 1,269,809 B | 1.35 |

The last row inverts the feature: a commit that rewrites every row reads more
through the change feed than through file actions. One scan would put that case
near 0.81. Partitioning each `RecordBatch` by `_change_type` does not recover
it, because that orders rows within a batch and the ordering has to hold across
the commit: a key's two images need not share a batch, and a `MERGE` can delete
a key in one change data file and insert it in another. Reading once needs
either a branch on whether the relation is keyed - the ordering buys a plain
ZSet nothing, since weights add - or buffering one half of the commit. Neither
is done here.

### Resume, transactions, end-of-input

Unchanged. `DeltaResumeInfo::follow_mode(version, eoi)` (`input.rs:947`) already
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

Mid-stream, `warn_if_change_data_feed_disabled` logs when a commit turns the
property off. A `metaData` action carries the whole configuration, so the
property is off both when it reads `false` and when the key is absent, which is
what `ALTER TABLE ... UNSET TBLPROPERTIES` leaves behind; only a commit that
changes the state warns, so a table that never recorded a change feed is silent.
The fallback keeps the data correct; only the cost profile changes.

Not checked: the `changeDataFeed` writer feature. delta-rs exposes the table
`Protocol` only through a `pub(crate)` kernel type, so a table with the property
set but the feature absent - which records nothing - is not distinguishable at
startup. It shows up instead as `..._from_file_actions` rising while
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

A `filter` is a `where` clause over the *Delta* table, so it may name a column
the SQL table never declares, or a partition column that lives only in the log.
Both resolve only because of the order the reader applies things: partition
literals, then filter, then projection to `used_columns`. Follow mode broke on
exactly this once (`786b8a3c9`, "No field named ..."), and the change feed path
is a second copy of that order, so it is pinned separately -
`change_feed_filter_undeclared_column`, `..._struct_field`, and
`..._partition_column`. Projecting before filtering reproduces the old error on
all three; supplying the partition columns after the filter breaks only the
third. A filter over a *declared* column passes under both, which is why the
original filter test was not enough on its own.

**Projection is consistent.** Pre-image and post-image go through the same
`used_columns` projection as the snapshot did, so a retraction always matches the
row that was originally inserted.

**Retractions must precede insertions, and nothing else orders them.** On a
relation with a primary key the connector feeds an upsert handle, where a
retraction is `Update::Delete` - a delete *by key*, not a weight that cancels -
and `input_upsert` applies same-key updates in the order they arrive (the sort
into key order is stable, so the connector's order survives). An update's two
images therefore do not commute: post-image before pre-image leaves no row.

Nothing imposes that order for free. A commit's change data files become
parallel scan partitions, so their batches race, and a partition-key update puts
the two images in different files entirely. So the reader makes two passes,
retractions first, exactly as `process_follow_actions` splits its own and for
the same reason. `cdc` mode's mandatory `cdc_order_by` is the third instance of
this requirement.

A ZSet relation hides all of it, which is why this survived every test until one
used a primary key: -1 and +1 cancel whichever way round they arrive.

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
`DELTA_TABLE_TEST_UNITY_CDF_TABLE` and inert without it. Its fixture is built by
`python/tests/platform/fixtures/unity_change_feed.py` through the SQL Statement
Execution API, so the change data is Databricks' own.

Run against a live workspace it settles what local fixtures cannot. A Databricks
table arrives with `delta.enableDeletionVectors`, `delta.enableRowTracking` and
reader v3 on by default: the read succeeds, so delta-rs accepts that protocol
and row tracking's materialised columns (`_row-id-col-...`) are pruned the way
`__is_cdc` is. The relation is keyed and the fixture's merge moves every row
into an earlier-sorting partition, so it doubles as the retraction-ordering
regression test on real output - 14 rows of 40 survive without that fix, 40
with it.

### Stress

`test_delta_input_unity_stress` is the one test that puts everything on one
table at once, gated on `DELTA_TABLE_TEST_UNITY_STRESS_TABLE` and built by
`unity_change_feed.py --stress`: every column type the connector reads, name-mode
column mapping, partitioning, deletion vectors and row tracking, over a history
that mixes two inserts, a `MERGE` recording all four change types, a `DELETE`,
two `OPTIMIZE`s, an `UPDATE` that moves a tenth of the rows into an
earlier-sorting partition, a plain `UPDATE`, and a block of rows written with the
feed switched off. The relation is keyed, so the retraction-ordering defect stays
reachable at every scale the fixture is run at.

The oracle is aggregates rather than rows, computed by Databricks over the same
table and by a Feldera view over the ingested copy. That is what lets the fixture
grow: 28 aggregates over 21 columns cost one row of output whatever the table's
size. They are exact wherever exactness is free - integers, a decimal scaled to
an integer,
distinct counts, string extremes, and a field reached through a struct, an array
and a map, so column mapping's rename path is checked on each of the three
nested shapes. Floats are counted rather than summed, because a sum over 10^5 of
them depends on arrival order and would fail for a reason that is not a defect.

Four runs: `change_feed` `auto` and `off`, and `transaction_mode` `always` and
`catchup` on top of `auto`. `off` is not a duplicate assertion - it reads the
same table through the file actions alone, so it gives the `auto` runs something
to disagree with, which a fixed expectation written from the change feed's own
output could not.

The one commit that has to be *constructed* is the gap. Databricks reports
`delete` rows from `table_changes` for a deletion-vector delete whether it wrote
a change data file or reconstructed them from the vector, so that commit cannot
be trusted to exercise the file-action fallback. Turning
`delta.enableChangeDataFeed` off for one insert and back on can: it is also the
shape of a table whose owner enabled the feed after the table already had
history.

At 117,000 rows over the eleven commits, all 28 aggregates agree exactly, and
the counters split the history 3 commits from change data against 8 from file
actions. That split is the answer to a question the fixture could not settle
from the Databricks side: `table_changes` reports `delete` rows for v4, but the
connector read v4 through its file actions, so Databricks recorded no change
data for that delete and Delta's own reader reconstructed those rows from the
deletion vector. The table above is right about deletion-vector deletes for
Databricks Runtime as well as for open-source Delta Spark.

Run against Databricks it immediately found a defect that is not the change
feed's and not this branch's: `deletion_vector.rs` applies a deletion vector with
`with_row_selection`, so the Parquet decoder *skips* the deleted rows, and
`DeltaBitPackDecoder::skip` mishandles the 256-value miniblocks Photon writes.
Both directions are broken, so there is no version to move to:

| parquet | skipping a 256-value miniblock |
|---|---|
| 58.2 and earlier | panics once a skip run exceeds the buffer, which it sizes from the physical type (64 for INT64) |
| 58.3 through 59.3 (newest) | `cannot skip miniblock of size 256` |

Downgrading trades an error the connector retries for a panic that unwinds out
of the DataFusion stream, so it is not a workaround. Filed as
apache/arrow-rs#11018; the fix is to size the skip buffer from
`block_size / mini_blocks_per_block` and to test `bit_width == 0` above the
guard, so the O(1) path stays reachable.

Photon delta-encodes when the table carries
`delta.parquet.format.version = 2.12.0`, which Unity Catalog can set as a schema
default -- so tables inherit it without asking, and this will become more common
rather than less. A column whose deltas are constant takes the `bit_width == 0`
fast path and sidesteps both failures, which is why a fixture keyed on a dense
ascending id can pass while the defect is present:
https://github.com/ryzhyk/parquet-photon-miniblock-repro

The connector is exposed only in follow and CDC mode. Snapshot reads apply
deletion vectors through delta-rs, which masks after decode and never skips.

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

As landed, on top of main:

| | |
|---|---|
| `[adapters] delta: ingest changes from the Change Data Feed` | the `change_feed` option, the reader with its two-pass read, the fallback, the checks, the counters, the tests, the connector docs, `openapi.json`, the generated TypeScript client, and this document |
| `[adapters] delta: default change_feed to off` | the default, and `DELTA_CHANGE_FEED` |
| `Address review feedback` | the tests the default flip left covering nothing, the mode check, the unset-property warning, and the wide-type differential read |

Two notes on the generated files. `bun run generate-openapi` reformats all of
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
