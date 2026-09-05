# Delta Lake output connector: merge mode

## Overview

Merge mode keeps a Delta table in sync with the current contents of a Feldera view. The
table holds exactly the view's rows and columns: one row per key, no metadata columns, no
change history.

The connector's existing `cdc` mode instead appends a change log, tagging every insert and
delete with `__feldera_op` and `__feldera_ts`, and leaves users to fold that log into a
state table with a job of their own. Merge mode moves the fold inside the connector.

Each change is applied by appending the new row version and marking the superseded one
deleted:

| Change | What the connector writes |
|--------|---------------------------|
| Insert | The row, appended to a new data file |
| Update | The new row appended, the old row tombstoned by a deletion vector |
| Delete | The old row tombstoned by a deletion vector |

No data file is ever rewritten. One flush produces one commit.

The design targets tables administered by someone else. The connector is the only party
writing data; external parties own the schema, the partitioning, the table properties, and
maintenance. It never rewrites a data file, never repartitions, and never alters the table
protocol.

Two requirements: the view must have a unique key (the connector's `index` property), and
the target table must have `delta.enableDeletionVectors = true`. On a table the connector
creates it sets the property itself; on an existing table it checks the property at startup,
reports the `ALTER TABLE` that enables it, and refuses to run it on the user's behalf.

That refusal is not fussiness. Enabling deletion vectors raises the table to reader version
3 with the `deletionVectors` reader feature, and a consumer that cannot read deletion
vectors then loses access to the table entirely, not just to the new rows. Merge mode is
therefore a choice about who may read the table, which is the table owner's to make.

Two further costs to state plainly, because they are the other side of the saving:

- Read latency grows with the accumulated tombstones. A reader has to apply the vectors,
  and the table holds every superseded row until a compaction reclaims it.
- The Spark merge job the user no longer runs is replaced by an OPTIMIZE they now have to.
  The saving is real -- a compaction is cheaper than a merge and does not have to run on
  the write path -- but it is a saving, not an elimination.

## The problem this solves

Keeping a table in sync means superseding rows, and a Delta data file is immutable. There
are only two ways to make a row stop being visible:

1. Rewrite the file without it. Cost is the whole file, however few rows changed. For keys
   spread across a large table this approaches rewriting the table on every flush.
2. Leave the file alone and record that the row is deleted, in a deletion vector attached
   to the file. Cost is proportional to the change, not to the file.

Merge mode does the second. That choice determines everything else in this document,
because a deletion vector addresses rows by file path and physical row ordinal rather than
by key, so the connector must first work out where each superseded row lives. That lookup
is the only expensive step in a flush and gets most of the attention below.

The cost this buys, on a 100 M row table of 200 byte rows with 1000 changes per second
flushed every 10 seconds:

| Term | Per flush |
|------|-----------|
| Locate the rows to supersede | 400 MB, the key columns of the candidate files |
| Append new row versions | 2 MB |
| Rewrite deletion vectors | 0.04 MB fresh, growing with accumulated tombstones |
| Total | about 400 MB, so 40 MB/s sustained |

### Why not delta-rs's own MERGE

delta-rs ships `DeltaOps::merge`, a full `MERGE INTO` with matched and not-matched
clauses, and it is the obvious thing to reach for. It is option 1 above.

Its implementation contains no reference to deletion vectors. A matched row is superseded
by rewriting the file that holds it, and the rows in that file that did not change are
copied into the replacement -- the operation reports how many, as `num_target_rows_copied`
(`crates/core/src/operations/merge/mod.rs:685` in delta-rs). On the 100 M row table above,
a flush touching 10 000 keys spread across the table rewrites every file those keys land
in. That is the cost this connector exists to avoid.

Two further mismatches, either of which would rule it out on its own:

| Property of MERGE | Why it does not fit |
|-------------------|---------------------|
| It rewrites data files | Merge mode targets tables someone else administers, where the file layout is the administrator's business. Rewriting files changes that layout on every flush, and not doing so is the one thing the ownership contract promises |
| Its source is a DataFusion `DataFrame` (`merge/mod.rs:164`) | The output batch would have to be materialized as a table provider. The connector instead walks it through a single forward cursor and holds only the encoded keys, which is what bounds memory by configuration rather than by batch size |

DataFusion itself is not a reason. `crates/adapters` already enables it, because delta-rs
needs it for the writer's `Invariant` support (`crates/adapters/Cargo.toml:157`).

What MERGE offers beyond this design -- schema evolution, arbitrary match predicates,
several conditional clauses -- merge mode does not need. Its predicate is always key
equality, and the target schema is checked at startup and required to match the view's.

## Requirements and guarantees

All requirements are checked at startup, so a misconfiguration fails before any data
moves.

| Requirement | Reason |
|-------------|--------|
| `index` property naming a unique key | The key identifies the row to supersede. The Postgres sink imposes the same requirement (`crates/adapters/src/integrated/postgres/output.rs:1093`) |
| Key columns are scalars, or `ROW` of scalars | See "Supported key types" |
| Key column physical types match what the connector encodes | Decimal scale, timestamp unit, and binary representation are fixed by the serde config at `crates/adapters/src/integrated/delta_table/output.rs:46-57`. A mismatch would make a round-tripped key compare unequal to itself |
| `delta.enableDeletionVectors` is true | Without it the connector cannot tombstone |
| delta-rs `PROTOCOL.can_write_to` passes | Fails fast on row tracking, column mapping, and identity columns, which delta-rs does not list as supported writer features |
| Change Data Feed is off | A CDF table needs `_change_data` files describing each update and delete. Producing those alongside deletion vector updates needs its own design |
| Target schema matches the view schema | Checked once rather than surfacing as a cast error mid-stream |

Guarantees:

- No data file is rewritten.
- One flush produces one commit, so a reader never sees half a flush.
- Memory is bounded by configuration, not by table size, with one documented exception.
- A replay after a restart converges. A Feldera delta states the final value or a deletion
  per key, so re-applying one tombstones the row it previously appended and appends an
  identical one. The cost is one extra tombstone per replayed row, which the next
  compaction reclaims.

Merge mode is not exactly-once and the connector remains not fault tolerant. The TODO at
`crates/adapters/src/integrated/delta_table/output.rs:1042` stays open.

## Table ownership

The connector is the only party that writes data to the target table. External parties run
maintenance against it: OPTIMIZE, VACUUM, and table property changes. That split is
narrower than exclusive ownership, and it is what makes the design usable against a table
someone else administers.

Being the only data writer does not mean knowing everything in the table. The table can
hold rows written by an earlier run of the same pipeline, which the current run does not
remember.

| Regime | When | Effect |
|--------|------|--------|
| Owned | The connector truncated the table during initialization, or found it empty | Everything in the table was written by this run. A key new to the view is absent from the table |
| Default | Anything else: `mode: append` onto a non-empty table, or a checkpoint resume, which reopens without truncating (`output.rs:287-309`) | The table may hold a row for a key this run never saw |

The regime is derived at startup, not configured, and it is logged. There is no setting to
assert the owned regime, because the connector can already tell: a truncate it performed,
or a snapshot with no files.

In the owned regime an insert skips the lookup entirely, which makes a bootstrap flush pure
appends: no lookup, no tombstones, nothing held in memory. That is the case worth
optimizing for, and it is why a pipeline that can start from a clean table should.

In the default regime every changed key enters the lookup set, inserts included, and any
row found is tombstoned before the new version is appended. Slower, correct without
assumptions, and the default for that reason.

A restart drops a pipeline from the owned regime into the default one, because the resume
path deliberately does not truncate. That is a throughput change, not a correctness one.

## The algorithm

The controller delivers one `encode` call per `batch_end`
(`crates/adapters/src/controller.rs:8057`), so a flush is a well-defined unit.

1. Walk the cursor once. It yields keys in index-key order from a spine snapshot that the
   storage layer already keeps on disk when it is large, so the walk itself costs no memory
   beyond the cursor.
2. Rows to append (inserts and the new side of updates) stream straight into the existing
   `DeltaWriter`, with the table's partition columns rather than the empty list currently
   passed at `output.rs:678`. A partitioned table gets one file per partition present in
   the batch.
3. Keys to remove (deletes, the old side of updates, and in the default regime inserts too)
   accumulate into a lookup chunk of encoded keys, bounded by `lookup_chunk_bytes`. The old
   side carries the old row, so it carries the old partition values.
4. When a chunk fills, and once more at the end of the walk, one lookup pass over the
   candidate row groups turns its keys into (file path, physical row ordinal) pairs.
5. For each data file with new tombstones, read its existing deletion vector, union the new
   ordinals into it, and append the result to a single packed vector file for the whole
   flush.
6. Commit.

Actions per commit:

| Action | Count | Contents |
|--------|-------|----------|
| `Add` for appended data | 1 per touched partition | New rows, `data_change: true`, statistics covering key columns |
| `Remove` plus `Add` per tombstoned file | 2 per touched file | Same path, size, partition values, and statistics as the current `Add`, with the new `deletionVector` |

Log replay keys files by (path, deletion vector id), so the remove and add pair for one
path is unambiguous within a commit.

Object writes per commit: one data file per touched partition, one deletion vector file,
one log entry. The number of files a flush touches drives the number of small log records,
never the number of objects written.

Partition changes need no special handling. The tombstone targets the old row wherever it
lives, and the append writes the new row into its new partition. That matters because on a
table the connector does not administer it also does not choose the partitioning. There is
one restriction, and it is not about which columns may be partition columns but about which
may be both partition and key columns: a key column stored in the log rather than in the
data file cannot be read back by the probe, so such a table is rejected at startup until the
probe reconstructs the column. See "Implementation status".

### Pseudocode

The pseudocode describes the design as specified. Three things below are not what the code
does, each for a reason given under "Implementation status": the summaries are not built
(and the location cache within them is unsound), `prune_parts` is decided only by the
partition columns being a subset of the key columns and not by the regime, and the regime
is derived from the snapshot alone rather than from a truncate flag.

```
# ---------------- startup, once per run ----------------

open_or_create_table()
require(table.property("delta.enableDeletionVectors"))
require(PROTOCOL.can_write_to(snapshot))
require(not table.change_data_feed_enabled())
require(schema_matches(view_schema, table.schema))
require(key_types_supported(index_key))              # scalars, or ROW of scalars
require(key_physical_types_match(index_key, table))  # decimal scale, timestamp unit, ...

regime      = OWNED if snapshot.files().is_empty() else DEFAULT
prune_parts = (regime == OWNED) or partition_cols_subset_of(key_cols)
encoder     = KeyEncoder(index_key)                  # RowConverter, canonical field order
summaries   = Summaries(bloom_cache_bytes, location_cache_bytes)
log(regime, prune_parts)


# ---------------- one flush, one commit ----------------

def flush(batch):                        # batch: spine snapshot, sorted by index key
    appends    = DeltaWriter(table.partition_columns, arrow_schema)   # streams out
    tombstones = {}                      # path -> RoaringBitmap; <= table_rows/8 bytes
    chunk      = EncodedKeys()           # <= lookup_chunk_bytes
    partitions = set()                   # distinct partition tuples in this chunk

    cursor = batch.cursor()
    while cursor.key_valid():
        op = indexed_operation_type(cursor)          # Insert | Upsert | Delete | None
        if op is None:
            cursor.step_key(); continue

        if op in (Insert, Upsert):                   # append side
            cursor.position_at_new_value()
            appends.write_row(cursor)                # flushes every CHUNK_SIZE rows

        if needs_lookup(op):                         # removal side
            if op != Insert:
                cursor.position_at_old_value()       # carries the old partition values
            key = encoder.encode(cursor.key())
            if hit := summaries.locate(key):
                tombstones[hit.path].insert(hit.ordinal)
            else:
                chunk.push(key)
                if prune_parts and op != Insert:
                    partitions.add(cursor.partition_values())
                if chunk.bytes() >= lookup_chunk_bytes:
                    lookup(chunk, partitions, tombstones)
                    chunk.clear(); partitions.clear()

        cursor.step_key()

    if not chunk.is_empty():
        lookup(chunk, partitions, tombstones)

    commit(appends.close() + write_deletion_vectors(tombstones))


def needs_lookup(op):
    if op == Insert:  return regime == DEFAULT       # owned: key cannot already be present
    return True


# ---------------- lookup: one pass over candidate row groups ----------------

def lookup(chunk, partitions, tombstones):
    chunk.sort()                                     # by encoded bytes

    for file in snapshot.files():
        if prune_parts and partitions and file.partition not in partitions:  continue
        if not stats_may_contain(file.stats, chunk):                         continue

        for rg in row_groups(file):                  # footer only; cached by immutable path
            if not stats_may_contain(rg.stats, chunk):                       continue
            if summaries.bloom_excludes(file, rg, chunk):                    continue
            probe(file, rg, chunk, tombstones)


def stats_may_contain(stats, chunk):
    # Sound in one direction only: anything uncertain keeps the unit.
    if any(stats.get(leaf) is None for leaf in key_leaves):  return True
    testable = [l for l in key_leaves if not chunk.has_null_or_nan(l)]
    if not testable:  return True
    leaf = testable[0]                               # exact test on the leading leaf
    i = chunk.lower_bound(leaf, stats[leaf].min)     # one binary search
    return i < chunk.len() and chunk.leaf_at(i, leaf) <= stats[leaf].max


def probe(file, rg, chunk, tombstones):
    base = rg.first_row_index                        # physical, ignores any existing DV
    seen = []
    for b in read_key_columns(file, rg):             # ProjectionMask::leaves, streamed
        for i, key in enumerate(encoder.encode_batch(b)):
            seen.append(key)
            if chunk.contains(key):                  # binary search; no extra index
                ordinal = base + b.offset + i
                tombstones[file.path].insert(ordinal)
                summaries.record_location(key, file.path, ordinal)
    summaries.record_bloom(file, rg, seen)           # free byproduct of the read


# ---------------- deletion vectors: one object per commit ----------------

def write_deletion_vectors(tombstones):
    if not tombstones:  return []
    actions = []
    dv      = new_dv_file()                          # deletion_vector_<uuid>.bin
    writer  = StreamingDeletionVectorWriter(dv)
    for path, new_ordinals in tombstones:            # one file at a time: bounded peak
        add    = snapshot.add_action(path)
        bitmap = read_existing_dv(add) if add.deletion_vector else empty()
        bitmap |= new_ordinals
        r      = writer.write_deletion_vector(bitmap)
        desc   = DeletionVectorDescriptor(UuidRelativePath, dv.encoded_path,
                                          r.offset, r.size_in_bytes, r.cardinality)
        actions += [Remove(add), Add(add.with_deletion_vector(desc))]
        drop(bitmap)
    writer.finalize()
    return actions


# ---------------- commit ----------------

def commit(actions):
    for attempt in retries():
        try:
            CommitBuilder.with_actions(actions).build(snapshot, log_store, Write)
            return
        except ConcurrentDeleteDelete:               # maintenance replaced a tombstoned file
            snapshot = table.update()
            summaries.invalidate_paths_absent_from(snapshot)
            raise RetryFlush                         # paths moved; the lookup must re-run
```

Retry is at flush level rather than commit level: a conflicting compaction changes file
paths, which invalidates the ordinals the lookup produced. Data files already written
become orphans that vacuum reclaims, matching how the existing write path handles partial
failures (`output.rs:496-503`).

## Row addressing

A deletion vector addresses rows by file path and row ordinal, not by key, as the adapter's
existing vector reader already notes
(`crates/adapters/src/integrated/delta_table/deletion_vector.rs:4`). Turning keys into
ordinals is the only expensive step in a flush, and it is a funnel where only the last
stage reads data.

### Stage 1: classification, free

`indexed_operation_type` (`crates/adapters/src/util.rs:82`) already reports, per key,
whether the view held that key before this step: `Insert` means weight `+1` with no `-1`,
so the key is new to the view.

In the owned regime that licenses skipping the lookup, and the lookup set shrinks to the
update and delete volume. An insert-heavy flush does no addressing I/O at all. In the
default regime an insert is demoted to an upsert and every changed key is looked up.

### Stage 2: candidate pruning, free

The chunk's keys are known exactly, so pruning tests each file against the key set itself,
not against a summary of it. Three filters apply in order, all from the snapshot and the
parquet footer.

1. Partition values, when sound. See below.

2. Key min/max statistics. The chunk is sorted by encoded bytes, so per file the question
   "does the chunk intersect this file's key range" is one lower-bound search:

   ```
   for each candidate file f:                           # O(log C) each
       i = chunk.partition_point(|k| k < f.min_key)     # first key >= f.min_key
       keep f  if  i < chunk.len() && chunk[i] <= f.max_key
       prune f otherwise
   ```

   Composite and `ROW` keys give an n-dimensional box of per-leaf statistics, addressed by
   leaf path (`s.a`, `s.b`), and set intersection against a box is not one search. Two
   searches on the leading leaf bound a contiguous slice of the chunk; an empty slice prunes
   the file, and scanning the slice against the remaining leaves tightens the answer if it
   is worth the cycles.

3. Row group statistics inside a surviving file, from the footer the reader parses anyway.

Pruning is sound in one direction only, and the unsound direction is a data bug rather than
a slow flush: wrongly pruning a file that holds a removal key skips the tombstone, appends
the new version anyway, and leaves a duplicate row. Anything uncertain keeps the file.

| Situation | Required behavior |
|-----------|-------------------|
| No statistics for a key column on that file | Keep the file. A missing statistic is not an empty range |
| Null in the chunk | Keep every file. Statistics leave nulls out, so no range rules a null key out |
| NaN in the chunk | Same. Parquet and Delta conventionally exclude NaN from min/max, so a box test would wrongly exclude it |
| String statistics | Delta truncates them, min downward and max upward, so the box stays a superset. Sound as long as the comparison uses the same UTF-8 byte ordering the writer used, which matches Feldera's string `Ord` |

Statistics only exist for the columns the table asks to index, and the connector collects
exactly that set: `delta.dataSkippingStatsColumns` when the table names one, and otherwise
the first `delta.dataSkippingNumIndexedCols` leaves, defaulting to 32. Reading the table's
own configuration rather than choosing a count here is what keeps this connector's files
skippable on the same columns as every other writer's; the resolution uses delta-rs's own
`get_num_idx_cols_and_stats_columns`, so it matches what its write path does.

The count is over parquet leaves, not top-level columns, and partition columns take up no
position because they are not stored in the data files. A nested column early in the schema
therefore pushes a key column past the limit sooner than the column list suggests. A key
with no statistics is not a correctness problem -- the missing statistic keeps the file,
which is the safe direction -- but it turns file pruning off, so the connector checks at
startup and warns, naming `delta.dataSkippingStatsColumns` as the administrator's fix. It
does not set the property itself: statistics collection is the table owner's decision, and a
connector that quietly widened it would write files that disagree with the table's
declaration.

Pruning pays off when a batch is key-local or partition-local. For uniformly distributed
keys over an unclustered table it prunes nothing, which is what the summaries below address
and statistics alone cannot.

#### When partition pruning is sound

| Case | Sound | Why |
|------|-------|-----|
| Owned regime | Yes | Every row was written by this run, and the removal side carries the old row, so its partition values say where the row is |
| Default regime | No | A row left by an earlier run can sit in any partition. Pruning to the old row's partition can miss it, skip the tombstone, and leave a duplicate. The lookup must consider every partition |
| Default regime, partition columns all drawn from key columns | Yes | The partition is a function of the key, so the row for a key is in one known partition no matter who wrote it |

Computed once at startup as `prune_parts` and logged. Note the asymmetry with statistics
pruning: a statistic that is too wide merely fails to prune, while partition pruning is
either sound or wrong, so it is switched off wholesale rather than applied cautiously.

### Stage 3: key-column read, the only I/O

Read the key columns of the surviving row groups with a `ProjectionMask` over
`ParquetObjectReader`, in streamed batches, and binary search each decoded key in the
sorted chunk. The adapter's vector reader establishes the pattern
(`deletion_vector.rs:320`, `:365`). Row groups are probed concurrently up to
`max_concurrent_probes`. Project by leaves rather than roots, so a `ROW` key pulls only its
own leaves.

Searching the chunk rather than indexing the row group is what keeps the probe's peak at
one decoded batch: no auxiliary hash index is built for either side. The two sides are made
comparable by `arrow_row::RowConverter`, which encodes a value of any supported type into
bytes whose comparison matches the logical one. Struct fields are encoded positionally, so
the encoder projects them into a fixed order by name first.

The ordinals produced must be physical row positions in the file, counted as if no deletion
vector were applied, because that is the space a vector addresses. Two things perturb the
count: skipped row groups, whose row counts come from the footer, and rows already
tombstoned by an existing vector, which must not shift the numbering. The probe applies no
row selection for that reason, and checks the rows it read against the footer's declared
count, because a mismatch would silently shift every ordinal after the gap.

Two outcomes are not errors. A key no candidate file contains means the row is not in the
table: a delete becomes a no-op and an update becomes a plain insert. A key found in more
than one file gets every occurrence tombstoned, which converges the table to one row. Both
are counted and exported, because a nonzero rate signals divergence.

### Chunking

A chunk holds encoded key bytes, not the batch and not values: an `arrow_row::Rows` buffer
plus offsets and a sort permutation, about 21 bytes per key for an 8-byte integer key. A
256 MB `lookup_chunk_bytes` holds roughly 12 M keys, more than a steady-state flush
produces, so the normal case is one chunk and one pass over the candidate row groups.

The chunk is sorted explicitly on encoded bytes rather than inheriting the cursor's order.
That costs `C log C` on a buffer already in cache and buys independence from whether the
DBSP key order and the arrow-row order agree, keeping the key-type rule at equality
preservation rather than order preservation.

A flush that exceeds the budget is split into `K` chunks and `K` lookup passes. Because the
cursor is sorted, each chunk is a contiguous key range, so on a key-clustered table
successive chunks prune to disjoint row groups and the passes still add up to about one
pass over the table. On an unclustered table each chunk touches nearly everything and the
cost is `K` full passes. The owned regime removes the largest instance of this, since a
bootstrap there is pure appends with an empty lookup set.

### Summaries

Parquet files are immutable, so once the probe has read a row group's key column that
knowledge never goes stale. Two in-memory summaries turn the lookup from a per-flush cost
into a per-row-group cost paid once. Both are built as byproducts of work already done: the
location cache from appends, the blooms from appends and probes.

| Summary | Bytes per key | 100 M rows | 1 B rows | Answers |
|---------|---------------|------------|----------|---------|
| Per-row-group bloom | 1.2 at 1% FPP | 120 MB | 1.2 GB | Is this row group worth reading |

They compose. The location cache answers for keys this connector wrote, the blooms decide
which of the remaining row groups to open, and a miss in both reads the row group and
populates both.

Both are keyed by file path. Delta paths are immutable and a rewritten file gets a new path,
so validating a key against the current snapshot self-invalidates anything an external
compaction replaced. Row ordinals within a path never change, because tombstoning does not
move rows. The failure mode of a stale entry is a re-probe, never a wrong answer.

The bloom is per row group rather than per file. A file is skipped when every one of its
row-group blooms misses, so file-level pruning falls out and needs no separate structure.
For a file of `R` row groups at per-bloom false positive rate `p`, a file holding no removal
key costs `R * p * (file_bytes / R)`, the same as a single file-level bloom at that rate;
when the file does hold a match, row-group blooms read only the matching groups. Equal in
the miss case, cheaper in the hit case. Row-group granularity also matches the scan unit
that keeps memory bounded and the per-group row counts the ordinal arithmetic needs.

Bloom testing costs one test per chunk key per row group, so it pays while the chunk is
small relative to a row group and should be skipped above that.

## Supported key types

The probe compares a key Feldera holds against the same value decoded back from parquet, so
a type is usable as a key only when equality survives the round trip through the
connector's encoding and the target column's physical type. That is a per-type property.

| Type | Supported | Reason |
|------|-----------|--------|
| Scalars | Yes | |
| `ROW` whose leaves are all supported scalars | Yes | A composite key in disguise. Delta writes statistics per leaf path, so pruning still works, and the row encoding handles nested structs |
| `ARRAY` of scalars | No | Encodable, but arrays carry no statistics, so the probe would read the whole column with no pruning. Add it when a user needs it |
| `MAP`, `VARIANT` | No | Not a mechanical limit. Feldera's map is a `BTreeMap` and canonically ordered, while a parquet map preserves whatever order was written; a variant renders as JSON, and two equal variants can render differently. On a table the connector does not administer there is no way to enforce a canonical physical form, so equality would be silently wrong rather than loudly unsupported |

Users needing an unsupported type should project a scalar surrogate into the view and index
on that.

Nullable key columns are allowed. The probe compares encoded bytes, which treat null as a
distinct, self-equal value, matching Feldera's `Ord` on `Option`. The only consequence is
that a null key switches min/max pruning off for that lookup pass: statistics leave nulls
out, so the recorded range of every file sits above the encoded null and a range test would
prune away the file that holds the row.

## Bounded memory

Every operation runs in memory bounded by configuration rather than by table size, with one
deliberate exception. A pipeline must not be able to exhaust memory because its target table
grew.

The output batch is never materialized. It arrives as a spine snapshot the storage layer
already keeps on disk when large, and the connector reads it through a single forward cursor
pass. Nothing derived from it is held whole either: appended rows stream out, and removal
keys are held only as encoded bytes, only for cache misses, and only one chunk at a time.

| Structure | Bound |
|-----------|-------|
| Output batch | Not materialized. One forward cursor pass over a disk-backed snapshot |
| Lookup chunk | `lookup_chunk_bytes`. A flush exceeding it is split into successive chunks |
| Appended rows | Streamed into the writer, as `CHUNK_SIZE` already does at `output.rs:90` |
| Probe input | One decoded batch per concurrent task, capped by `max_concurrent_probes` |
| Deletion vector bitmaps | One bit per table row across all files, and one file's bitmap at any instant |
| Row group metadata | Offsets and row counts, tens of bytes per row group |
| Per-row-group blooms | Not bounded by configuration. About 1.2 bytes per distinct key |

Deletion vector bitmaps deserve a precise bound. A vector cannot exceed one bit per row in
its file, so every bitmap in a flush together cannot exceed `table_rows / 8` bytes: 12.5 MB
per 100 M rows, and less in practice because a roaring bitmap run-encodes dense regions.
The instantaneous peak is smaller still, because each file's read-modify-write completes
and streams into the packed vector file before the next begins.

The blooms would be the deliberate exception, and are the reason the summaries were left
unbuilt: they scale with the table rather than with a setting. The design gave them two
guardrails, a metric for the footprint and a `bloom_cache_bytes` ceiling that evicts by
file and falls back to probing. Nothing in the connector holds per-table state today, so
this paragraph describes the shape a future summary would have to take, not what runs.

No full-table scan happens at startup, and none on restart: the connector reads the log,
never the data, until a flush asks for a key.

## Cost model

Per flush:

```
bytes = key_column_bytes(candidate row groups)   # lookup, near zero on a cache hit
      + appended_rows * row_size                 # new versions
      + sum over touched files of dv_bytes       # one packed object
```

No term is proportional to the size of the data files being modified.

Two things drive cost, and neither is the shape of the data files. Key column width sets
the lookup, and tombstone accumulation sets both the vector rewrite cost and the compaction
cadence. File size, partition granularity, and file count do not move the numbers.

Batch size matters only indirectly: the output buffer consolidates repeated updates to the
same key before the connector sees them (`crates/adapters/src/controller.rs:6300`), so a
wider window shrinks the change set for hot keys. A 10 second window is a reasonable
default.

The lookup scales with table size and flush frequency, not with change volume. 100 M rows at
a 10 second flush is 40 MB/s and comfortable. A billion rows at the same cadence is 400 MB/s
and is not: widen the flush window, or rely on the summaries.

## Working alongside table maintenance

What the connector never does: rewrite a data file, change the schema, change the
partitioning, alter table properties, or upgrade the protocol.

Concurrency, verified against delta-rs's conflict checker:

| Concurrent event | Outcome |
|------------------|---------|
| OPTIMIZE removes a file being tombstoned | `ConcurrentDeleteDelete`, retry against the new snapshot. The check does not filter on `data_change`, so compaction's `data_change: false` removes still conflict, which is wanted here |
| OPTIMIZE adds compacted files | No conflict. The read and append checks consider only `data_change: true` files under the default isolation level |
| VACUUM | Retains a vector that a live `add` references, and reclaims the rest. True of Spark as written; true of delta-rs only in `Lite` mode until the fork fix described below lands in the pinned rev |
| Schema or property change | The startup checks run once, so a mid-run change is caught by the next flush instead: a metadata change conflicts on commit, and a changed key column fails the key encoding rather than superseding the wrong row |

A retry re-runs the lookup against the new snapshot, because file paths may have changed.
Retries reuse the existing macro and health reporting (`output.rs:209-262`).

Because no other party writes data, the connector does not declare a read predicate on its
commits. `DeltaOperation::Write { predicate }` stays `None` as it is today
(`output.rs:457`).

## Table maintenance obligations

This is the main operational risk and it should be stated plainly to users. Every update
adds a row and tombstones one. Without compaction the table grows without bound and read
cost grows with the number of updates rather than the number of live rows.

By default the table's administrator compacts. Materializing deletion vectors during a
rewrite is a protocol obligation every engine implements, so an existing OPTIMIZE schedule
already does the right thing. The connector defaults its own compaction off for that reason
and exposes `optimize_interval_secs` for tables where Feldera is the only maintainer.

What the connector reports so the administrator can schedule it. All are prefixed
`delta_merge_` and defined in `merge/metrics.rs`.

| Metric | Meaning |
|--------|---------|
| `tombstone_ratio_permille` | Superseded rows per thousand rows in the table. The compaction signal |
| `rows_appended_total`, `rows_superseded_total` | Change volume, split by side |
| `files_appended_total`, `files_dropped_total` | Small-file growth, and files reclaimed because every row in them was superseded |
| `keys_probed_total` | Keys that reached stage 3. Below the changed-key count by what the insert shortcut saves |
| `keys_not_found_total` | Removal keys absent from the table. A sustained rate signals divergence |
| `probe_files_scanned_total`, `probe_files_pruned_total` | Whether file pruning is doing anything |
| `probe_row_groups_scanned_total`, `probe_row_groups_pruned_total` | The same, within the files that are opened |
| `lookup_passes_total` | Above one per flush only when a key set exceeded `lookup_chunk_bytes` |
| `bytes_written_total` | Data files plus deletion vectors |

The connector warns at most once per hour when `tombstone_ratio_permille` exceeds 200, and
names the remedy.

## Configuration

Declared in `crates/feldera-types/src/transport/delta_table.rs`.

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `update_mode` | `cdc \| merge` | `cdc` | Orthogonal to `mode`, which governs what happens to an existing table at startup |
| `lookup_chunk_bytes` | `usize` | 256 MiB | Ceiling on encoded removal keys held at once. Capped at 2 GiB: the chunk addresses its buffer with 32-bit offsets |
| `max_concurrent_probes` | `usize` | 4 | Caps the probe working set and its request concurrency |
| `optimize_interval_secs` | `Option<u64>` | none | Connector-driven compaction. Off by default, because the table administrator normally compacts; set it where Feldera is the only writer. Runs in the background after a flush, one at a time, first run one interval after startup |

Output buffering is a requirement of merge mode rather than a tuning knob, since the pass
over the file list is per flush. The connector does not warn about it; the user
documentation states it instead.

## Code layout

Merge mode lives under `crates/adapters/src/integrated/delta_table/merge/`.

| Module | Role |
|--------|------|
| `key.rs` | Which types may form a key, leaf paths for statistics, and the `RowConverter` encoder used by both sides of the probe |
| `chunk.rs` | The bounded buffer of encoded keys: push, sort, binary search, range intersection |
| `prune.rs` | Which files and row groups can be skipped: the interval test and the partition filter |
| `probe.rs` | The key-column read that produces (path, ordinal) pairs |
| `tombstone.rs` | Accumulating ordinals, reading and unioning existing vectors, packing them, and building the log actions |
| `startup.rs` | What the target table must satisfy before the first row moves |
| `flush.rs` | The cursor walk that drives all of the above, and the commit |
| `metrics.rs` | The exported counters, and the compaction warning |

Changes to the existing connector:

| Location | Change |
|----------|--------|
| `output.rs:94` (`DeltaTableWriter::new`) | Capability and protocol checks, CDF and schema checks, key type and physical type checks, regime detection |
| `output.rs:123` | Arrow schema without the metadata columns in merge mode |
| `output.rs:678` | Pass the table's partition columns to `WriterConfig` instead of `vec![]` |
| `output.rs:688-746` | Single cursor walk feeding the append writer and the lookup chunk; the upsert branch already exposes both the old and new values |
| `output.rs:848` (`batch_end`) | Run the apply algorithm and commit, replacing the plain Add commit |
| `deletion_vector.rs:63` | Reuse `read_deletion_vector` for the read side of a vector update |

Merge mode involves no DataFusion, so the writer needs no `SessionContext` and does not draw
on the pipeline memory pool.

External dependencies are pinned in the workspace `Cargo.toml`: delta-rs at rev
`78a5d066d60feffcc7dcd9bae62d1c537dd9018c`, `delta_kernel` (package `buoyant_kernel`) at
0.22, which supplies the deletion vector file format writer, and `object_store` at 0.14.
`put` lives on `ObjectStoreExt`, so the `object_store` version has to be the one delta-rs's
`Arc<dyn ObjectStore>` implements.

## Test plan

Correctness, in `crates/adapters/src/integrated/delta_table/test.rs` against local
filesystem tables:

| Test | Asserts |
|------|---------|
| Model test over a random operation stream | After each flush the table read back equals a `BTreeMap` model. Covers insert, update, delete, delete of an absent key, reinsert of a deleted key |
| Proptest over batch shapes | Same model, using the existing harness |
| Vector accumulation | Repeated updates to one key leave exactly one live row and a vector whose cardinality equals the number of superseded versions |
| Vector read-modify-write | A file tombstoned across several flushes retains all earlier tombstones |
| Partition change | An update moving a row between partitions leaves exactly one row, in the new partition |
| Replay convergence | Applying the same batch twice yields the same live rows, with one extra tombstone |
| Owned regime skips the lookup | After a truncating start, an insert-only flush reads no data files and reports `keys_probed = 0` |
| Default regime catches a stale key | A pre-populated table reopened without truncating, fed an insert for an existing key, leaves exactly one live row |
| Resume leaves the owned regime | A checkpoint resume logs the default regime and looks up inserts |
| Capability refusal | Missing deletion vectors, CDF on, row tracking on, a `MAP` or `VARIANT` key, or a key whose physical type does not match all fail at startup naming the cause |
| Key round trip, differential | For every supported key type including nested `ROW`, random values written and read back compare equal exactly when Feldera's `Eq` says they are |
| Key round trip, targeted | NaN, signed zero, decimal scale, timestamp unit, struct field reordering, nested nulls, null key columns |
| Empty batch | No commit, table version unchanged |

Concurrent maintenance. The interesting cases commit inside a flush, after the lookup has
produced ordinals and before the commit lands, which needs a `#[cfg(test)]` hook at that
point.

| Test | Asserts |
|------|---------|
| OPTIMIZE between flushes | Deleted rows stay deleted after compaction materializes the vectors, and the next flush re-locates rows in the new files |
| OPTIMIZE commits mid-flush, we lose | The conflict produces `ConcurrentDeleteDelete` and the retry re-runs the lookup rather than reusing stale ordinals. This catches a retry that skips straight back to committing |
| OPTIMIZE commits mid-flush, we win | Our commit lands, the concurrent OPTIMIZE retries, and the table is correct once both settle |
| Compaction of a file with a live vector | Rows tombstoned before compaction do not reappear |
| Summaries invalidate on compaction | Entries for paths absent from the new snapshot are dropped and the next lookup finds the rows in the new files |
| VACUUM during a long flush | A file disappearing mid-probe surfaces as a retryable error, not a wrong answer. Retention must exceed the longest flush |

Retries, using an `ObjectStore` wrapper that fails the Nth operation of a given kind.

| Test | Asserts |
|------|---------|
| Transient probe read failure | Retries and produces the same result as an uninterrupted run |
| Transient data write failure | Extends the existing retry loop to merge mode and confirms the progress counter rolls back exactly what the failed attempt added |
| Vector write failure | Leaves the table unchanged, because nothing is committed, and the retry succeeds |
| Commit failure, transient | Retries and lands once. The flush must not appear applied twice |
| No double apply | After any injected failure and recovery, live rows match the model and no key has two live rows |
| `max_retries` exhaustion | Surfaces an error and marks the connector unhealthy rather than dropping the batch |
| Health transitions | Unhealthy on first failure, healthy after a successful retry |

Efficiency, because the central claims are about cost:

| Test | Asserts |
|------|---------|
| No data file is rewritten | Across a run of flushes every original data file path stays live, only its vector changes |
| Pruning works | A key-local batch on a table with key statistics scans fewer files than a key-random batch of the same size. Negative control included |
| Pruning is sound | A file with no statistics for a key column is never pruned, and neither is one whose range excludes a null or NaN in the chunk |
| Partition pruning switched off when unsound | In the default regime with a partition column outside the key, a stale row in a different partition is still found. Negative control is the same table in the owned regime |
| Ordinals are physical | Tombstoning a row in a file that already has a vector, and in a file whose leading row groups were pruned, removes the intended row and no other. The fixture must span several decoded batches, or a running counter and a per-batch index agree and the test cannot discriminate |
| Insert shortcut skips the lookup | An insert-only batch reports `keys_probed = 0` and reads no data files |
| Summaries remove the re-read | Probing a row group once, then probing again for keys it does not contain, reads the key column exactly once |
| Bloom eviction degrades, not fails | A ceiling below what the table needs still produces correct results, at the cost of re-probing |
| Memory stays bounded | Peak RSS across a flush touching every file stays within the configured ceilings |
| Chunking is transparent | A flush past `lookup_chunk_bytes` produces the same live rows and tombstones as the same flush with the ceiling raised |
| Tombstone warning fires | A table driven past the ratio threshold warns exactly once |

Validate each new test by reverting the change it guards, following the project rule on test
verification.

## Benchmarking plan

Both suites extend `crates/adapters/benches/delta_encoder.rs`, which already drives a
`DeltaTableWriter` with generated indexed batches from `bench_common.rs`.

Wall-clock is reported but never asserted on, because CI timing is too noisy to gate a
merge. The gating numbers are counters the connector exports.

| Metric | Claim it tests |
|--------|----------------|
| Bytes read and written per flush | Cost is the lookup plus new rows plus one vector file |
| Object store operations per flush, GET and PUT separately | One data file per touched partition, one vector file, one log entry on the write side. The read side is the open question |
| Data file paths removed per flush | Must be zero |
| `probe_bytes` against table size, change rate, flush window | The lookup scales with table size and flush frequency, not change volume |
| Bytes written per changed row | Write amplification |
| `tombstone_ratio` growth and the cost of the OPTIMIZE that clears it | Compaction cadence guidance |
| Query latency as `tombstone_ratio` rises | The read-side cost the design accepts |

Dimensions: table size (1 M, 10 M, 100 M rows), row width (5 and 100 columns), key
distribution (monotonic and random), change rate and flush window producing `M` from 10 to
10 M, summaries cold and warm, and `max_concurrent_probes` at 1, 4, 16, 64. Baseline is
`cdc` mode on the same data, the cheapest available writer and therefore a floor.

The local suite runs against a `TempDir` table and gates CI on counter budgets. The S3 suite
runs against a real bucket, with MinIO as a deterministic stand-in, and measures per-request
latency and request counts under concurrency. It is not a CI gate; it runs on demand and
before a release.

One hypothesis needs the S3 suite specifically. The design bounds object writes per commit
at a small constant but says nothing about reads, and the probe issues roughly one GET per
candidate row group plus one footer per file. A thousand row groups is a thousand round
trips, and at `max_concurrent_probes = 4` with 30 ms per request that is about 7 seconds of
latency for a flush moving very few bytes. If confirmed, the default is too low and the
probe needs range coalescing across row groups within a file.

## Implementation status

Branch `delta-merge-mode-v2`, rebased onto `main`.

Merge mode applies and commits correctly, end to end, and prunes. Feature-complete against
this document except for the summaries, which the section at the end argues should wait for
measurements rather than be built on the strength of the estimates here.

| Component | Status |
|-----------|--------|
| Configuration surface and validation | Done |
| `key.rs`: type allowlist, leaf paths, encoder | Done |
| `chunk.rs`: bounded sorted chunk | Done |
| `startup.rs`: capability, protocol, schema, key and partition checks; regime detection | Done |
| `flush.rs`: the cursor walk, key encoding, chunking, commit, retry | Done |
| `tombstone.rs`: vector read-modify-write, packing, log actions, whole-file drop | Done |
| `probe.rs`: key-column read, physical ordinals, concurrency | Done |
| Wiring into `output.rs` | Done |
| `prune.rs`: file and row group pruning, partition filter | Done |
| Key columns that are also partition columns | Done. Reconstructed from the log, and used as exact statistics |
| Summaries (blooms, location cache) | Not built. The location cache should not be built as designed; see below |
| Metrics export and threshold warnings | Done, in `metrics.rs` |
| Connector-driven compaction (`optimize_interval_secs`) | Done, 4 tests. Background, opt-in, one at a time, first run one interval after startup |
| Benchmarks | Not started |

### What the implementation established, and what it changed here

Four assumptions in this document were unverified when it was written. All four now have
tests. One of them was wrong.

| Assumption | Verdict |
|------------|---------|
| delta-rs will write to a deletion vector table | Holds. `DeletionVectors` is in its writer feature set, and a created table lands on protocol (3, 7) |
| A `remove`/`add` pair for one path installs a vector a reader honours | Holds, verified against both delta-rs and Delta Spark |
| Only `ConcurrentDeleteDelete` can conflict with our commits | Holds. `DeltaOperation::Write` reports no read predicate and does not read the whole table, so the append and delete-read checks do not apply |
| Vacuum retains vector files referenced by live `add` actions | **False for delta-rs.** Its `VacuumMode::Full` lists the table directory and deletes paths no live `add` names; a vector file is named only inside a descriptor, so at the table root it is deleted and every row it tombstoned comes back. Spark's VACUUM gets this right |

The vacuum finding was first worked around by writing vectors under
`_feldera_deletion_vectors/`, a name both engines skip, and having the connector reclaim its
own. That was the wrong call, and it is reverted. Two reasons:

- The workaround protected only *our* vectors. A `DELETE` run by Spark writes a vector at the
  table root, where delta-rs full-mode vacuum still deletes it. We dodged the bug for
  ourselves and left it in place for everyone, including our own users' other jobs.
- Hiding the directory from vacuum meant nothing could ever reclaim it, which forced a
  reimplementation of VACUUM's own logic -- its reference set, its retention rule, and its
  reasoning about concurrent writers -- inside the connector.

Vectors now go at the table root under the protocol's name, exactly as Delta Spark writes
them, and reclaiming them is VACUUM's job. That is already true of the data files the
connector removes when every row in one is superseded, so it adds no obligation a merge-mode
table did not already have.

The delta-rs bug is fixed in the fork instead of designed around: `create_vacuum_plan` adds
each live file's vector to `valid_files`, via a `DeletionVectorDescriptor::absolute_path` that
delegates to the kernel's decoder. Its test fails without the fix, naming the vector it would
have deleted. The patch belongs upstream too -- as it stands, `VacuumMode::Full` silently
resurrects deleted rows for any deletion vector table, whichever engine wrote it.

The patch has not reached `feldera/delta-rs` yet, so the pinned rev does not carry it and
`vacuum_full_preserves_live_deletion_vectors` stays `#[ignore]`d. Until the rev moves, full
mode through delta-rs is unsafe on a merge-mode table; `Lite` and Spark are not.



Six further changes to the design, each with a reason:

| Change | Reason |
|--------|--------|
| The location cache is dropped, not deferred | As specified it is unsound. A cache hit short-circuits the lookup, so a second copy of the key in another file is never tombstoned -- and a second copy is exactly what the replay-convergence story above creates. It would turn "converges with one extra tombstone" into "two live rows for one key, permanently". At about 24 bytes per key it was also the largest structure in the design |
| A file whose every row is tombstoned is removed outright, not re-added with a full vector | It holds no live rows either way, and dropping it stops the file count and the vector size growing without bound under a delete-heavy workload. Needs the file's row count, so a file without statistics keeps its vector |
| `threads > 1` is rejected | Each thread walks a disjoint key range and would need its own pass over the candidate files: the dominant cost, multiplied by the thread count. The lookup already reads files concurrently within one pass |
| A key column that is also a partition column is rejected at startup | Delta keeps partition values in the log rather than in the data files, so the probe cannot read those key values back out of parquet. Supporting it means reconstructing the column from each file's partition values, which is also what would make partition pruning sound -- so the two belong in one piece of work, not guessed at now |
| `add_timestamp_column` is dropped | A user who wants a timestamp can project one into the view, which lets them choose its name and its semantics. The setting bought nothing and cost a config field, a validation rule, and a schema branch |
| The schema check compares Delta types, not arrow types | The connector's arrow schema uses `LargeBinary` and `LargeList`; Delta has neither and normalizes them. Comparing arrow reports mismatches that do not exist, as the first run of the model test showed |

Two corrections to claims made above:

- The flush happens entirely inside `encode`, not split across `encode` and `batch_end`. It
  has to: a retry re-runs the lookup, and that needs the batch, which only exists for the
  duration of that call. The controller delivers one `encode` per `batch_end`, so this is
  still one commit per flush.
- Deletion vector peak memory is one bit per table row, not one file's bitmap. The bitmaps
  are built one file at a time as claimed, but the packed object accumulates all of them in
  a buffer before it is put.

### How pruning came out, versus this document

The design proposed a three-filter funnel: partition values, then a per-leaf box of key
statistics, then row group statistics. The implementation is simpler, because one observation
collapses the middle stage. The row encoding orders bytes the way it orders values, so a
unit's per-column `[min, max]` box is contained in the single lexicographic interval
`[min_tuple, max_tuple]`. Testing "does the chunk meet this interval" is one binary search
whatever the key's arity, and it needs no per-leaf addressing, no leading-leaf special case,
and no separate row group path: the same test runs against Delta log statistics for a file and
against Parquet footer statistics for a row group.

Partition pruning did need its own mechanism after all, and for a reason worth recording. A
partition value is an exact statistic, so the document's claim that it needs no special
handling is true of the *interval test* -- and useless in practice, because a wide range on a
leading key column swallows whatever a trailing partition column contributes. The fix is
exact rather than approximate: the partition columns are key columns, so the flush knows every
partition its keys belong to, and a file in any other partition holds nothing. `PartitionFilter`
is a set of encoded partition tuples, bounded by the number of distinct partitions a flush
touches. On a table partitioned by date, a flush touching one day skips every other day's
files without opening them.

Three further departures:

| Change | Reason |
|--------|--------|
| A key column that is also a partition column is supported | Delta keeps the value in the log, so the probe reconstructs it as a constant column per file. This is what makes partition pruning possible, so the two arrived together, as predicted. Only a key made *entirely* of partition columns is rejected: nothing would be left in the file to read, and the table would have one directory per row |
| Statistics pruning is switched off wholesale for a key containing a float, rather than per column and per chunk | The document proposed dropping the offending column from the test when the chunk holds a NaN, which needs per-batch inspection of the key arrays on every flush. A float is a poor key anyway -- exactly because NaN does not behave -- so the blunt rule costs nothing real and is obviously sound |
| A null key switches pruning off for the whole lookup pass, rather than for its column | Same reasoning, but the trigger is a value rather than a type, so it is decided per pass: the flush already holds the key columns as arrays when it encodes them, and a null is one `null_count` read away. Dropping one column from a lexicographic interval test is not possible in any case -- the interval is over the whole tuple |
| The lexicographic-interval test replaces the per-leaf box test | Simpler, and tighter in the common case. The box test's advantage appears only for multi-column keys whose leading column is wide, and where that matters the partition filter is the better tool |

### The remaining gap: the summaries, now with a measurement

The location cache should not be built as designed at all, for the soundness reason above.
The blooms were left to a measurement: build them only if `probe_files_scanned` tracks the
table size rather than the change rate. That measurement has now been made, and it says
build them.

Growing a table one flush at a time with scattered updates -- eight updates and eight
inserts per flush, the steady state the connector is for -- every flush read **every file in
the table** and pruned none, at every table size measured up to 2,000 files. The cost per
flush is linear in the file count for that reason: 16 ms at 250 files, 134 ms at 1,000,
289 ms at 2,000.

Range statistics cannot fix this, and the reason is structural rather than a property of the
fixture. A flush appends one file holding the new versions of whatever keys changed. If those
keys are spread over the key space, that file's `[min, max]` spans the key space, so no later
flush can prune it -- the connector's own writes destroy the clustering that pruning needs.
`ZORDER` helps only for the files that existed when it ran.

What that leaves is the per-file summary the document proposed: something that answers "is
this file worth opening" without a range. Two shapes:

| Shape | Cost | Catch |
|-------|------|-------|
| Parquet bloom filters on the key columns | Written by the writer, read from the footer, no state to keep across restarts | Still one request per file per flush, and a bloom for a large file is not small |
| An in-memory bloom per file, hung off `FileIndex` | No request at all for a file it excludes | Scales with the table, and has to be rebuilt for files the connector did not write |

The floor on any of this is `min(changed keys, files)` file reads: locating scattered keys
without an index cannot beat that, which is what makes the summary the only real lever.

### The parquet filters, built and then deferred

The first row of that table was implemented in full -- writer properties, a row-group filter
test on the read path, metrics, and tests -- and then set aside without being committed. The
implementation is kept as `feldera-merge-bloom-deferred.patch` outside the repo. What the
measurements said is worth recording, because it is the reason to defer and it will be the
starting point whenever this is picked up again.

Filters help in one arrangement and one only: **large files whose key ranges overlap, updated
on a subset of the keys**, which is a table compacted without `ZORDER`. Twenty files of
100,000 rows, keys scattered so no range test excludes any file, eight keys per flush living
in one file:

| | flush 100 | flush 200 |
|---|-----------|-----------|
| Filters consulted | 10.1 ms | 18.7 ms |
| Filters ignored | 46.2 ms | 54.2 ms |

They are worth nothing in the two arrangements closer to the headline problem. With 600 files
and 500 keys per flush: 137.1 ms against 137.5 ms, because every file genuinely holds some
sought key and there is nothing for any test to skip. With 1,200 files of 16 rows and 8 keys
per flush, consulting filters measured 8 to 10 percent *slower* (169 ms against 154 ms) -- a
filter costs a request and skipping sixteen rows saves nothing. Gating the read on a row-count
threshold brings that back to 158.5 ms against 155.8 ms, an intended no-op.

Three costs decided the deferral:

- **A filter does not get relatively cheaper as the file grows.** It is about 2.6 bytes per
  row at a 0.0001 false positive rate, whatever the row count, so it stays near half the size
  of a raw `BIGINT` column at every scale: 32 KiB against 64 KiB at 8,192 rows, 4 MiB against
  8 MiB at a million. Against a *compressed* narrow column the filter is frequently the larger
  of the two, and then it should not be read at all. The 4.6x above is CPU -- the decode and
  the re-encode of every value through the row converter -- not bytes. Filters win on bytes
  only for wide keys.
- **Storage.** About 3.7 GiB of filters on a billion-row table, per key column.
- **Write memory.** `ndv` defaults to the row group ceiling, so each flush sizes a filter for
  1,048,576 distinct values, about 2.6 MiB per key column, and folds it down afterwards. A
  16-row flush pays that allocation too. Unmeasured.

And the evidence for the one favourable arrangement is weaker than the number suggests: that
benchmark shape was written *after* the first two arrangements showed no gain, so it was
constructed to suit the feature. Whether a real workload sits in it is unknown, and it turns
on the same question as everything else here -- whether updates are localized or uniform over
the table.

The accuracy setting is the other thing worth carrying forward. A row group is skipped only if
the filter rejects *every* key in the flush, so the useful rate is `(1 - fpp)^n` in the flush's
key count. At a thrifty 0.05 a 100-key flush skips a skippable row group 0.6 percent of the
time. Anything built here needs a tight rate, and past a few thousand keys per flush no rate
helps. The v1 design said this a priori -- bloom testing "pays while the chunk is small
relative to a row group and should be skipped above that" -- and it was right.

Note also that the v1 design specified *in-memory* per-row-group blooms with a
`bloom_cache_bytes` ceiling, not parquet ones. That shape was not built: it scales with the
table, has to be rebuilt after every restart, and covers files the connector did not write
only once it has probed them.

### Reproducing the numbers

`crates/adapters/examples/merge_scale.rs` drives the public `DeltaTableWriter` with the
`bench_common.rs` fixtures. `MERGE_MODE` picks the arrangement: unset grows a table one file
per flush, `size` and `hot` hold the file count fixed and vary file size, `overlap` builds the
range-overlapping shape the filters were for.
