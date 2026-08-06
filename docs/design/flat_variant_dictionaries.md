# FlatVariant: batch-scoped dictionaries

## Summary

`FlatVariant` stores every string of every document inline. On the customer records that
motivated the type, 73% of the encoded bytes are string bytes and 83% of those bytes repeat
a string already present elsewhere in the same table. A dictionary shared by all documents
of a batch removes them.

Measured on the 35 real records in `crates/sqllib/benches/data/user_props.jsonl.gz`
(280 VARIANT documents, 547 maps, 15.3 KiB of encoded VARIANT per record):

| Encoding | Total bytes | vs today | Large-batch limit |
|---|---:|---:|---:|
| today, strings inline | 546,970 | 1.00x | 1.00x |
| intern map-key strings | 351,939 | 1.55x | 1.60x |
| intern map keys and string values | 327,003 | 1.67x | 2.23x |
| intern map *shapes* | 228,943 | 2.39x | 2.51x |
| intern shapes and string values | 204,007 | 2.68x | **4.54x** |

"Large-batch limit" amortizes the dictionary over a batch much larger than 35 records,
which is the normal case. The proposal reaches it in three parts:

1. A **chunk**: a refcounted document buffer that carries a pointer to the dictionary it
   references, so a value stays self-contained and clone stays one refcount bump.
2. A **rewire** step that re-points a document at another dictionary. Rewiring is
   length-preserving and order-preserving, so it is a `memcpy` plus one `u32` write per
   string reference, and it drops unreferenced dictionary entries for free. It runs as
   *compaction*, triggered by a sparseness test, not on every push: pushing a value into a
   batch stays a refcount bump, as it is today (section 5.1).
3. A **shape table** that interns the whole sorted key vector of a map, not just the
   individual key strings. It is the single largest win and it rides on the same machinery.

The in-memory work lands first. Storage reuses the same dictionary as a per-file side
table, which is straightforward because the layer-file reader already deserializes every
key into an owned value before comparing it
(`crates/dbsp/src/storage/file/reader.rs:673-679`, `:735-760`).

## 0. Measured, M1

M1 is in the tree: `Chunk`, `Dict`, `TAG_STRING_REF`, the `Val` view, and `DocSet`,
which moves a set of documents onto one dictionary the way a batch builder will. The
benchmark below is `cargo bench -p feldera-sqllib --bench variant_memory` over the same
35 customer records, replayed 20 times.

Per-row heap, the sample's 8 VARIANT columns:

| representation | heap | jemalloc | live blocks | rkyv | SizeOf |
|---|---:|---:|---:|---:|---:|
| legacy `Variant` | 86.5 KiB | 103.9 | 780 | 48.1 | 75.4 |
| `FlatVariant` | 15.7 KiB | 17.2 | 20 | 15.6 | 15.8 |
| + dictionary, 700 rows | **6.9 KiB** | 8.4 | 0 | 15.6 | 7.2 |

**2.29x against `FlatVariant`, 12.5x against the enum.** The model in section 1 predicted
2.23x for strings alone, so it can be trusted for the shape table in M3 as well.

Rows sharing one dictionary, which is what a batch scope buys:

| rows per dictionary | 1 | 8 | 64 | 512 | 700 |
|---|---:|---:|---:|---:|---:|
| heap per row | 14.8 KiB | 10.1 | 8.2 | 7.0 | 6.9 |

Comparison cost, documents per second on the dominant column:

| keys | cmp | sort | hash | total vs inline |
|---|---:|---:|---:|---:|
| inline | 9,864,991 | 50,150 | 142,544 | - |
| one dictionary | 2,119,606 | 72,650 | 136,491 | 1.25x |
| two dictionaries | 3,555,556 | 52,222 | 136,070 | 1.01x |

**Sorting inside a batch is 45% faster**, which is the `ranks` table doing its job, and
hashing is 4% slower. The `cmp` column walks adjacent unsorted documents in sequence,
which suits the inline layout: equal map keys compare as a `memcmp` over adjacent bytes
rather than two random reads into `ranks`. Sorting wins anyway because it is bound by the
memory it touches, and there is half as much of it.

Two things M1 measured that the design had not accounted for:

- **A string shorter than five bytes costs more as a reference than inline**, and a string
  that never repeats costs 12 bytes more. Interning everything grew a corpus of long
  distinct strings by 35%. `DocSet` now counts occurrences first and interns only strings
  of at least `MIN_INTERN_LEN` bytes that appear at least twice. That also beats
  interning everything on the real corpus, which section 1 already showed (1.77x against
  1.67x).
- **`SizeOf` must be taken over a whole batch.** Summing it per row charges the shared
  dictionary once per row; a 700-row batch reported 54000% of its own heap. Pinned by
  `size_of_counts_a_shared_dictionary_once`.

M1 changes nothing for a running pipeline, because no batch builder calls `DocSet` yet, so
the `zeta_cnn` SF100 run measures only what the refactor costs. Against the pre-change
baseline (247,680 profiles, 3.75 GB of JSON, 8 workers):

| | baseline | M1 |
|---|---:|---:|
| throughput | 2,396 rec/s | 2,390 rec/s |
| peak RSS | 20.37 GiB | 22.01 GiB |
| peak storage | 70.15 GiB | 47.02 GiB |
| end to end | 151.8 s | 105.7 s |

Throughput matches within 0.2%. The storage and wall-clock columns must not be read as a
win: nothing in this build writes a dictionary, and
`interning_leaves_the_archived_form_alone` proves the serialized bytes are unchanged, so
the difference is how much the two runs happened to spill, not how large a record is. One
run per arm is not enough to see anything smaller than that spread.

The RSS column does have a plausible mechanism behind it. A `Chunk` is two allocations,
`Arc<Chunk>` and the boxed bytes, where an `Arc<[u8]>` was one, and the benchmark shows
live blocks per row going from 12 to 20 for inline documents. Folding the bytes into the
`Arc` allocation as a sized-header DST would recover it, and packing many documents per
chunk (M6) would more than recover it. Neither is worth doing before M2 puts real
dictionaries in the pipeline.

## 1. Where the bytes go

`crates/sqllib/src/flat_variant.rs:29-33` defines the layout. Modelling it byte for byte
over the sample corpus yields 15.26 KiB of VARIANT payload per record against the 15.6 KiB
per row the benchmark measures, and the benchmark's figure additionally covers the row's
`SqlString` columns and per-allocation overhead. The model tracks the real encoding, so the
component split below is trustworthy:

| Component | Bytes | Share | Removed by |
|---|---:|---:|---|
| map key strings | 259,840 | 47.5% | string dictionary, then the shape table |
| string values | 138,955 | 25.4% | string dictionary |
| offset tables | 111,636 | 20.4% | shape table (the key-end table disappears) |
| tags | 28,189 | 5.2% | shape table (one tag per key disappears) |
| counts and scalars | 8,350 | 1.5% | nothing |

Duplication, and how fast each kind of string saturates:

| Records in scope | 1 | 2 | 4 | 8 | 16 | 35 |
|---|---:|---:|---:|---:|---:|---:|
| distinct map keys | 378 | 379 | 381 | 382 | 383 | 397 |
| key occurrences | 397 | 785 | 1,576 | 3,152 | 6,307 | 13,856 |
| distinct string values | 196 | 373 | 712 | 877 | 1,200 | 3,387 |
| value occurrences | 296 | 589 | 1,182 | 2,362 | 4,727 | 10,375 |
| distinct map shapes | 9 | 10 | 12 | 13 | 14 | 28 |
| maps | 16 | 29 | 60 | 120 | 241 | 547 |

Three facts drive every decision below.

**The dictionary must span many documents.** Scoped to a single document it is a net loss
(0.90x). Two records break even, eight reach 2.01x, thirty-five reach 2.68x.

**Keys and shapes saturate; string values do not.** The key set is complete after one
record and the shape set after a handful. Distinct string values keep growing roughly
linearly with the corpus. A dictionary whose lifetime exceeds a batch would therefore
accumulate string values forever, which is the argument for scoping it to a batch. It is
also the argument for splitting the on-disk dictionary in two (section 10).

**Shapes beat key strings by 2x.** Interning a key string replaces `[tag][bytes]` with
`[tag][u32]` and leaves the key-end offset behind. Interning the shape removes the key
encoding, the key-end offset table, and the per-key tag together: a 3-key map costs
`1 + 4 + 24 + keys` today and `1 + 4 + 12` with a shape.

## 2. Value shape: chunks

Today (`crates/sqllib/src/flat_variant.rs:126-131`):

```rust
pub struct FlatVariant {
    buf: Arc<[u8]>,   // one allocation per document
    start: u32,
    len: u32,
}
```

Proposed:

```rust
pub struct FlatVariant {
    chunk: Arc<Chunk>,   // thin pointer: 8 bytes
    start: u32,
    len: u32,
}

/// A shared arena of encoded documents and the dictionary they reference.
pub struct Chunk {
    dict: Option<Arc<Dict>>,
    bytes: Box<[u8]>,
}
```

`FlatVariant` shrinks from 24 to 16 bytes. Reaching the dictionary costs one pointer chase
into a cache line the document start already occupies.

Putting the dictionary inside the chunk rather than beside it in `FlatVariant` matters for
two reasons. Clone stays a single atomic increment, and, more importantly, **a value stays
self-contained**. DBSP cursors hand out `&K` with no ambient context
(`crates/dbsp/src/trace/ord/file/wset_batch.rs:666`), and a merge compares a key from one
batch against a key from another, so no thread-local or per-cursor dictionary can serve
both. The dictionary pointer has to be reachable from the value.

**A chunk holds exactly one document to start with.** Retention, allocation count, and
clone cost then match today's `Arc<[u8]>` exactly, and the only change is the dictionary
pointer the chunk carries.

Packing many documents into one arena is tempting: it would remove the per-document `Arc`
header and the jemalloc size-class rounding that costs 1.4 KiB per row today. It also
couples their lifetimes. A filter that keeps 1% of a 10,000-row batch would pin every arena
those rows landed in, holding 150 MB to keep 1.5 MB, where today the other 99% of the
documents free independently. Arena packing is therefore deferred to M6 and gated on the
liveness signal in section 5, which is the machinery that detects and repairs exactly that
situation. Staging is the one place where packing is safe early, because staging documents
are uniformly short-lived (section 6).

## 3. Encoding

Two new tags, and the existing forms stay legal so that a document without a dictionary
still encodes:

```text
String ref:  [TAG_STRING_REF][id: u32]
Shaped map:  [TAG_SHAPED_MAP][shape: u32][val_end_i: u32 x count][values, concatenated]
```

`count` comes from the shape table, so the shaped map drops the count field, the key-end
table, and the key area.

The tag byte currently doubles as the type rank for `Ord`
(`crates/sqllib/src/flat_variant.rs:76-79`). The new tags cannot extend that order, so
comparison gains `fn rank(tag: u8) -> u8`, which folds `TAG_STRING_REF` onto `TAG_STRING`
and `TAG_SHAPED_MAP` onto `TAG_MAP`. Everything else keeps comparing tags directly.

**Ids are fixed-width `u32`.** A narrower or variable-width id would save another 25% (a
`u16` measures 2.78x against 2.23x) but it would forfeit the length-preserving rewire that
section 5 depends on, and once shapes are in place the id width is worth only 4.54x against
5.37x. Fixed `u32` is the right trade.

## 4. The dictionary

```rust
pub struct Dict {
    strings: StringTable,   // id -> &str
    shapes: ShapeTable,     // id -> &[u32], string ids sorted by content
    ranks: Box<[u32]>,      // id -> sort position, filled at freeze
}
```

`StringTable` is an offset array over one byte arena. `ShapeTable` is the same over a `u32`
arena. Both are append-only during a build and immutable afterwards.

The dictionary is **not** ordered by id. Ids are assigned in first-reference order, which
is what lets a merge append one dictionary onto another without renumbering (section 5).
Content order is recovered by `ranks`, a permutation computed once when the dictionary
freezes: comparing two references from the same dictionary is then `ranks[a].cmp(&ranks[b])`,
one indirection and no `memcmp`. Comparing across dictionaries resolves to bytes. Sorting
and merging inside one batch, the hottest path in DBSP, always takes the cheap route.

`ranks` is correct only if the dictionary never holds the same string twice, so interning
deduplicates strictly. The build-time index is a `hashbrown::HashTable<u32>` resolved
through the arena, which avoids a self-referential map, and it is dropped at freeze.

A shape's key ids are stored sorted by key *content*, matching the canonical order that map
keys already use, so a shaped map's value area stays parallel to its shape and binary search
over a shape works unchanged.

## 5. Rewiring, and when not to

### 5.1 Rewiring is compaction, not a precondition

Nothing about the design requires a batch to own a single dictionary. A value reaches its
dictionary through its own chunk, so a batch holding values from four dictionaries compares,
hashes, and serializes correctly with no extra work. Rewiring exists to reclaim space, not
to make a batch valid.

That distinction is what preserves today's most valuable property: **cloning a document into
another batch costs a pointer and a refcount bump, never a copy of the bytes.** A spine merge
today moves 24-byte handles; a document that appears in a base batch, its index, and three
downstream views exists once in memory. Rewiring on every push would turn each of those into
a full copy of the document, so a 1M-row merge over 15 KiB documents would copy 15 GB instead
of 400 MB, once per merge level. That regression would dwarf the dictionary win.

So the default on every push is the plain clone, exactly as today. Rewiring runs only when
the source dictionary has gone sparse:

```rust
// Documents per chunk is 1, so a dictionary's Arc strong count is the number of
// live documents still using it. `origin_docs` is recorded when it freezes.
fn is_sparse(dict: &Arc<Dict>) -> bool {
    Arc::strong_count(dict) * SPARSE_FACTOR < dict.origin_docs
}
```

`Arc::strong_count` is a relaxed atomic load, so the test costs a few nanoseconds per pushed
value and needs no walk, no refcount per entry, and no bookkeeping in the batch. A dictionary
that is still densely used is kept and shared; one whose documents have mostly been retracted
or filtered away is left behind, and the surviving documents move into the destination
builder's dictionary.

One unconditional case: documents still sitting in a **provisional** staging chunk are always
rewired (section 6). That is the transition from short-lived parse output to long-lived batch
storage, it is the point at which a staging dictionary would otherwise be pinned, and the
copy is cheap next to the parse that just produced the bytes.

### 5.2 Dictionary fragmentation costs almost nothing

A batch that references many dictionaries duplicates whatever those dictionaries have in
common. Section 1 says what that is: the key and shape tables, which saturate at about 8 KiB
per dictionary. The expensive part, the string values, does not overlap between dictionaries
built from different records in the first place, so a batch spanning 50 dense dictionaries
holds roughly the same bytes as one consolidated dictionary plus 400 KiB.

There is consequently no cap on dictionaries per batch and no consolidation pass. Sparseness
is the only trigger, and it is the only one that corresponds to wasted memory.

### 5.3 The rewire itself

Copying a document from a source chunk into a destination builder:

```text
remap[source dict] : Vec<u32>, sized to the source dictionary, filled with NONE

memcpy the document bytes into the destination arena
walk the document structure; at each id site holding old id `i`:
    new = remap[i]
    if new == NONE {
        new = dest.intern(source.string(i))   // or source.shape(i)
        remap[i] = new
    }
    write new
```

Four properties make this close to `memcpy` speed:

- **Length-preserving.** `u32` in, `u32` out. Offset tables never change, so the copy is
  bulk and the patch is in place. No re-layout, no re-encoding.
- **Order-preserving.** Map keys are ordered by content and content does not change, so the
  canonical sort order survives. This is precisely why the dictionary must not be ordered
  by id.
- **One hash per distinct source entry, not per reference.** The `remap` array turns a
  record's 13,856 key references into 397 hash inserts for the whole batch. Every other
  reference is an array read and a `u32` write.
- **Garbage collection is free.** An entry that no surviving document references is never
  interned into the destination. A merge that drops rows drops their strings with them,
  with no liveness pass and no refcounts.

The builder keeps one `remap` per source dictionary it actually rewires from, keyed by
`Arc::as_ptr`, and allocates the destination dictionary lazily on the first rewire. A builder
that never meets a sparse or provisional source never allocates one.

### 5.4 How often a document gets copied

Each rewire is one copy of the document. A document is rewired when it is first written into
a batch, and again each time the dictionary it currently sits in goes sparse under it. A
dictionary goes sparse when most of its documents die, so the sequence of dictionaries a
surviving document passes through shrinks geometrically by `SPARSE_FACTOR`: the copies are
O(log) in the number of retractions the document outlives, not O(number of batches it enters).

The pathological case is a batch whose every value comes from a different sparse dictionary.
That degenerates to a copy per value, which is the correct behaviour, because those
dictionaries are exactly the memory the compaction is there to release.

## 6. Lifecycle and staging

```text
parse / cast / index ──► staging chunk + staging dict, marked PROVISIONAL
                         (thread-local; rotates at a byte cap)
                              │
                              │  Builder::push_key / push_val
                              ▼
                         provisional?  ──yes──► rewire into the builder's dictionary
                         source sparse? ─yes──► rewire into the builder's dictionary
                         otherwise      ──────► clone: one refcount bump, no copy
                              │
                              ▼
batch lifetime           values reference one or more frozen dictionaries;
                         each dies when its last document does
```

Document construction funnels through `build_document`
(`crates/sqllib/src/flat_variant.rs:836`), `build_document_infallible` (`:863`), and
`from_bytes` (`:138`), so the staging dictionary attaches in one place. Staging exists so
that the rewire at batch construction takes the array-remap path rather than hashing every
string, and so that the input buffer, which holds a whole step's records and is itself large
in the case that motivated this work, is already deduplicated.

Thread-local staging state matches what `crates/sqllib/src/string_interner.rs:95-113`
already does for `CURRENT_STEP`, `PINNED_STRINGS`, and the interned-string cache.

### 6.1 What bounds staging memory

Staging memory is what no batch references yet. Three pieces, each capped:

| Piece | Cap | Note |
|---|---|---|
| current staging dictionary and its arena | `STAGING_DICT_BYTES`, 1 MiB | rotates when full |
| its build-time hash index | about 1x the arena | dropped the moment the dictionary freezes |
| the existing `build_document` scratch | `SCRATCH_RETAIN_BYTES`, 2 MiB | already enforced at `flat_variant.rs:845` |

The bound is `live_threads x (2 x STAGING_DICT_BYTES + SCRATCH_RETAIN_BYTES)`, so 4 MiB per
thread and 32 MiB across eight workers. Rotation, not refusal, enforces the cap: a full
dictionary freezes and a fresh one starts. Frozen staging dictionaries are no longer staging
memory; they are the dictionaries of whatever documents survived, accounted to those
documents' batches by `SizeOf`, and released by the sparseness rule in section 5.

Four details this depends on:

- **Rotation happens only at document boundaries**, checked in `build_document`, so no
  document ever spans two dictionaries.
- **Reentrant construction keeps the current dictionary.** `build_document` already takes a
  fresh scratch buffer on reentry (`flat_variant.rs:846-853`); the staging dictionary must
  not rotate under a nested build.
- **`live_threads` is not just the workers.** Input parsing runs on connector threads, and
  ad-hoc queries on HTTP handler threads. Thread-locals are dropped at thread exit, so the
  bound holds, but the multiplier is the process thread count. Export a gauge for total
  staging bytes and the number of live staging dictionaries so this stays visible.
- **Staging bytes are invisible to spill accounting** until a batch references them, because
  `SizeOf` runs over batches. At 32 MiB that is acceptable; the gauge is how we would notice
  if it stopped being.

## 7. Plumbing the hook into DBSP

The builder receives an erased `&K` (`crates/dbsp/src/trace.rs:1133`, `:1141`) whose
concrete type is a generated row struct with `FlatVariant` fields buried in it. Something
has to walk it.

Follow the `SupportsRoaring` precedent (`crates/dbsp/src/utils/supports_roaring.rs:17`),
which solved the same problem: a per-type capability reachable through `DBData`.

```rust
// dbsp::dynamic::intern
pub trait Interned {
    /// Whether this type can transitively hold interned references. Builders
    /// branch on it once per batch rather than once per row.
    const MAY_INTERN: bool = false;

    /// Create the session this type needs, if any.
    fn new_intern_session() -> Option<Box<dyn InternSession>> { None }

    /// Re-point every interned reference in `self` at `session`.
    fn reintern(&mut self, session: &mut dyn InternSession) { let _ = session; }
}
```

Wiring, in the same places `SupportsRoaring` touches:

| Where | Change |
|---|---|
| `crates/dbsp/src/trace.rs:98-113` | add `Interned` to `DBData`'s bounds |
| `crates/dbsp/src/dynamic/data.rs:80` | blanket `impl<T: DBData> Data for T` forwards `reintern_dyn` to `T::reintern` |
| `crates/dbsp/src/utils/supports_roaring.rs` sibling | `never_interned!` macro for leaf types |
| `crates/feldera-macros/src/lib.rs:55` | the `IsNone` derive also emits a field-forwarding `Interned` impl |
| `crates/feldera-macros/src/tuples.rs:262` | `TupN` forwards to its elements |
| `crates/dbsp/src/trace/layers/leaf.rs:26` | `LeafFactories` gains `may_intern: bool` from `KType::MAY_INTERN` |
| batch builders and mergers | own a session, call `reintern_dyn` after each push, freeze at `done()` |

No specialization is needed, because `Interned` becomes a supertrait of `DBData` and the
blanket `Data` impl can name it. `InternSession` is an opaque `Any` so that `dbsp` never
learns what a dictionary is; `FlatVariant::reintern` downcasts it to sqllib's `DictBuilder`.
The downcast is a type-id compare per field. If it shows up in a profile, move the downcast
to the row level.

`reintern` is where the section 5.1 test lives: it inspects the value's own dictionary and
returns without touching anything unless that dictionary is provisional or sparse. The common
call therefore costs a relaxed atomic load and a compare, and the value keeps sharing its
existing chunk.

**Cheaper alternative for a prototype.** A thread-local "current intern target" set around
the builder's pushes, read by `FlatVariant::clone`, needs none of the table above and can
produce an end-to-end measurement in a day. It is the wrong shipping answer: it puts a
thread-local read on every clone in the program, it silently interns clones that were never
destined for the batch, and it hides the control flow. Use it to validate the numbers, not
to ship.

## 8. Order, equality, hashing

Every helper that takes `&[u8]` today (`cmp_values` at `:459`, `eq_values` at `:529`,
`hash_value` at `:540`, `Container` at `:370`, `find_key_by` at `:270`) takes a view:

```rust
#[derive(Clone, Copy)]
pub(crate) struct Val<'a> {
    bytes: &'a [u8],
    dict: Option<&'a Dict>,
}
```

That is roughly 50 call sites across `flat_variant.rs`, `casts.rs`, and `functions.rs`. It
is the bulk of the mechanical work.

Semantics that must not move:

- **Equality and order are over content, never over ids.** The `memcmp` fast path in
  `eq_values` survives only when both values carry the same dictionary pointer, which covers
  every intra-batch comparison. Across dictionaries the comparison is structural, with
  string comparisons resolving through both dictionaries.
- **`hash_value` must produce the byte sequence it produces today**: `TAG_STRING` followed by
  the string bytes, whether the string is inline or a reference. Hashes are persisted inside
  Bloom and roaring batch filters and are asserted stable by
  `crates/dbsp/src/dynamic/data.rs:108`. A hash that depended on which batch a value came
  from would also break `shard()`, which routes by hash.
- **Canonicity is now per dictionary.** Byte equality still implies value equality within one
  dictionary. It no longer implies it across dictionaries, and inequality of bytes never
  implied inequality of values anyway (float payloads already forced a structural fallback).

## 9. Memory accounting

`SizeOf` decides when a fallback batch spills (`crates/dbsp/src/trace/ord/fallback/`), so a
dictionary shared by many values has to be counted once, not once per value. The `Arc` impl
deduplicates by pointer within a `size_of::Context`, which the current impl already relies
on (`crates/sqllib/src/flat_variant.rs:357-363`), and it deduplicates the nested `Arc<Dict>`
the same way. Add a test that a batch of N documents sharing one dictionary reports it once.

Two consequences of section 5.1 worth stating, because both differ from today:

- A dictionary shared by two batches is charged to whichever batch `SizeOf` visits, and to
  both if they are measured separately. Sharing already behaves this way for the document
  bytes today, so spill decisions are no more optimistic than they already are.
- A sparse dictionary that has not yet been compacted is charged in full to the batch that
  still references it. That is the honest number: the memory is genuinely resident.

## 10. On disk

Facts that constrain the storage design, all verified in the tree:

- The layer-file reader **always deserializes into an owned `K`** before comparing it
  (`reader.rs:673-679`, `:735-760`). Archived values are never compared semantically, so
  `ArchivedFlatVariant` needs no dictionary access. Only the deserializer does.
- `Deserializer` already carries per-file context (`crates/dbsp/src/storage/file.rs:579`).
  Adding a dictionary field gives every value deserialized from a file the same
  `Arc<Dict>`, so **the whole file's rows share one dictionary in memory**, preserving the
  in-memory win on read-back.
- The writer serializes each item with a freshly cleared shared-resolver map
  (`writer.rs:785-810`), so rkyv's own `Arc` deduplication cannot be relied on to write the
  dictionary once. It has to be an explicit side structure.
- Blocks hold at least `min_branch` = 32 items and are at least 8 KiB
  (`writer.rs:171-175`), so for 15 KiB rows a block covers about 32 records, which the
  scope table in section 1 puts at roughly 2x.

Design, matching the saturation behaviour measured in section 1:

- A **file-level dictionary** for shapes and map-key strings. Small, bounded, saturates
  after a few records, resident whenever the file is open.
- A **per-block dictionary** for string values. High cardinality, paged in and out with the
  block it belongs to, so a large on-disk batch never pins a large dictionary in memory.
- One id bit, or a split id space, distinguishes the two.
- New block magic (`LFSD`), referenced from `FileTrailer` for the file-level part and from
  `DataBlockHeader` for the block-level part. `VERSION_NUMBER`
  (`crates/dbsp/src/storage/file/format.rs:100`) goes to 6 and the `storage-test-compat`
  crate gets new golden files.
- A file must be self-contained, so **writing a batch to storage always rewires**, whatever
  section 5.1 decided in memory. That is free: the writer serializes every document byte
  anyway, so the rewire rides along with a copy already being paid for. Writing is therefore
  the one unconditional compaction point in the system, and a batch that spills comes back
  from disk with a single dense dictionary.
- `dbsp` must not depend on `sqllib`, so the file layer treats the dictionary as an opaque
  blob supplied by the batch, with a decoder handed to `Deserializer`.

First cut: file-level dictionary only, accept the residency, measure. Split out per-block
value dictionaries only if residency proves to be a problem.

## 11. Alternatives considered

**A process-wide content-addressed interner**, as
`crates/sqllib/src/string_interner.rs` already implements for `SqlString`: ids are the first
16 bytes of a BLAKE3 hash, resolved through a 1 GiB cache backed by a persistent spine. Ids
are then universally valid and nothing ever needs rewiring, which is a real attraction.
It does not work at `FlatVariant` granularity. A 16-byte id against a 13-byte average key
string saves almost nothing, and resolution is a cache probe (or a spine point lookup that
can perform I/O) on every comparison, every hash, and every `to_json`. A `FlatVariant`
dictionary has to be dense, local, and resolvable by array index.

**A worker-local generational dictionary with no rewiring at all**: documents attach to the
current generation, which rotates on a byte cap. This is much simpler, needs no DBSP changes,
and gets the same ratios while a generation is young. It fails on retention. Distinct string
values grow linearly with the corpus (section 1), so a generation that outlives its documents
pins strings for rows retracted long ago, with no mechanism to ever release them. The design
here is that proposal plus one thing: a sparseness test that moves the survivors out and lets
the generation die. Everything else, including the cheap clone, is shared between the two.

**Per-entry refcounts with a per-document use list**, which would let a global dictionary
collect entries precisely without rewiring. It costs a trailer of about 1 to 4 bytes per
distinct string per document, atomic traffic on every document creation and drop, and a
shared structure on the hot path. Rewiring buys the same collection for less.

**Variable-width or narrower ids.** Worth 25% before shapes and 18% after, and it costs the
length-preserving rewire. Rejected.

**Interning `SqlString` columns too.** The motivating row carries seven of them. Out of
scope here, but the `Interned` trait from section 7 is the mechanism it would use, and the
chunk from section 2 is where it would live.

## 12. Risks

| Risk | Mitigation |
|---|---|
| Rewiring on every push would replace today's shared-`Arc` clone with a full document copy, costing more than the dictionary saves. | Section 5.1: the default push is the plain clone; rewiring is conditional. Benchmark B5 fails the build if a dense-dictionary merge allocates document-sized bytes. |
| `SPARSE_FACTOR` tuned too aggressively copies too much; tuned too loosely leaks. | Both directions are measurable: B5 prices the copies, B8 catches the leak. Ship with a conservative factor and a metric. |
| `ArchivedDBData::Repr: Ord` (`crates/dbsp/src/dynamic/rkyv.rs`) requires `Ord` on the archived form, which cannot resolve a dictionary. No semantic use of it exists in the storage layer today. | Put a `panic!` in the impl, run the full suite, and confirm it is dead before relying on it. |
| Hash instability would invalidate persisted Bloom and roaring filters and break `shard()`. | Hash content, not ids, byte for byte as today. Add a test that a value hashes identically inline and interned. |
| Losing the `memcmp` fast path slows merges. | `ranks` restores an integer compare for the intra-dictionary case, which is every comparison inside a batch. Benchmark B3. |
| A staging dictionary pinned by a value that never reaches a batch. | Audit operators that hold values outside batches; rotate on the byte cap rather than on step boundaries. |
| Shape explosion on genuinely schemaless data. | Degrades gracefully: a shape entry costs 4 bytes per key, the same as the key reference it replaces, minus the tag and the offset. Never much worse than key interning. |

## 13. Benchmarking plan

Each benchmark has a budget. A milestone does not land until its benchmarks meet theirs.
B1, B2, B5, and B8 are the ones that would catch a design error rather than a tuning error.

### Sizing

**B1. Encoded size against dictionary scope.** Extend
`crates/sqllib/benches/variant_memory.rs`, which already reports live requested heap,
jemalloc-rounded bytes, live allocations, `SizeOf::total_bytes`, rkyv bytes, and parse rate.
Add a `FlatVariant + dict` column and a scope sweep over documents per dictionary
(1, 8, 64, 512, all). Budget: at least 2.2x against `FlatVariant` at large scope with
strings alone, at least 4.5x with shapes, and the curve reproducing the model in section 1.
This is the benchmark that says whether the whole exercise was worth it.

Reuse the bench's existing discipline: parse unmodified records, never through
`serde_json::Value`, because that sorts keys and inflates the legacy `Variant` baseline by
16%.

**B2. Adversarial corpora, must not regress.** Same harness, synthetic inputs:

| Corpus | Budget |
|---|---|
| every string distinct (UUID values) | at most 5% larger than today |
| one distinct shape per document | no worse than key interning |
| scalars and empty maps (measured 1.00x today) | no regression |
| one document larger than the staging cap | correct, and no pathological allocation |
| deeply nested documents | rewire recursion bounded |

### CPU

**B3. Compare, hash, sort.** A criterion bench over 100K documents: `cmp` within one
dictionary, `cmp` across two, `cmp` against the inline encoding; `hash`; sorting a `Vec`;
`index_string` probe. Budget: intra-dictionary compare no slower than today (the `ranks`
table should make it faster), cross-dictionary compare within 2x.

**B4. Rewire throughput.** Bytes per second of rewire against plain `memcpy` of the same
documents. Budget: within 2x of `memcpy`. This number sets how aggressive `SPARSE_FACTOR`
can be.

**B5. Batch operations, and the Q3 guard.** A bench over `OrdZSet<Row>` with VARIANT columns:
build from N tuples, and merge two batches. Instrument allocated bytes, not just time.
Budget, in order of importance:

1. Merging two batches whose dictionaries are dense allocates O(rows x row struct), **not**
   O(rows x document). Assert on the allocator counter the bench already has. This is the
   regression that the naive design would have introduced, so it is worth a hard assertion
   rather than a budget.
2. Dense-dictionary merge time within 5% of today.
3. Sparse-dictionary merge prices the compaction: report it, do not gate on it.

**B6. Parse throughput.** Already reported by `variant_memory.rs` (8.8K rows/s legacy,
15.6K flat). Interning adds a hash and a lookup per string. Budget: at most 15% below
`FlatVariant` today.

### End to end

**B7. The `user_props` load-test rig** (`~/projects/feldera/loadtest`, 10M records in
redpanda, `use_platform_compiler: true`). Three arms: legacy `Variant`, `FlatVariant` today,
`FlatVariant` with dictionaries. Report steady-state RSS, records per second, bytes spilled,
checkpoint size, and checkpoint time. Budget: RSS at least 2x below `FlatVariant` today at
throughput within 10%.

**B8. Retention guards.** The tests most likely to catch a design error, each written so
that it fails when the sparseness threshold is disabled:

- A filter keeping 1% of a 10,000-row batch: resident bytes fall to roughly 1%, not stay
  at 100%.
- A spine fed inserts and retractions for many steps: resident dictionary bytes track live
  distinct strings, not cumulative distinct strings.
- A single surviving sub-value does not pin a staging dictionary past one batch generation.
- Total staging bytes stay under the section 6.1 bound across a long run.

**B9. A second corpus.** `python/tests/workloads/test_jsonbench.py` covers differently shaped
documents and guards against overfitting to `user_props`. Report the same figures as B7.

### Continuous

Export per-batch dictionary bytes against payload bytes, and total staging bytes, as
pipeline metrics, so the QA workloads surface a regression without anyone rerunning a bench.

## 14. Milestones

**M1: encoding and dictionary, no DBSP integration.** `Chunk`, `Dict`, `TAG_STRING_REF`,
`Val`-based comparison, hashing, and cursors, plus a thread-local staging dictionary.
Benchmarks B1, B2, B3, B6. The bench holds rows in a `Vec` against one staging dictionary,
which models a batch-scoped dictionary exactly, so M1 alone validates the ratios end to end
on real data before any DBSP change.

**M2: DBSP integration.** In the tree. A batch of VARIANT rows now comes out holding one
dictionary with nothing in the SQL runtime asking for it, pinned by
`a_dbsp_batch_shares_one_dictionary`.

| piece | where |
|---|---|
| `Interned`, `InternSession`, `reintern_values` | `crates/dbsp/src/dynamic/intern.rs` |
| `may_intern` / `reintern_dyn` in the vtable | `crates/dbsp/src/dynamic/data.rs` |
| `Interned` in `DBData`'s bounds | `crates/dbsp/src/trace.rs` |
| field-forwarding derive, `#[interned(opaque \| manual)]` | `crates/feldera-macros/src/lib.rs` |
| `TupN` forwarding | `crates/feldera-macros/src/tuples.rs` |
| `FlatVariantSession`, the sparseness rule | `crates/sqllib/src/flat_variant.rs` |
| the call site | `Leaf::from_parts`, `crates/dbsp/src/trace/layers/leaf.rs:135` |

Two things worth knowing for anyone extending this:

- **The derive bounds the field types, not the type parameters.** Bounding parameters gives
  a type whose parameter only reaches a `PhantomData` field a bound its callers cannot
  satisfy, which is the usual trap with derived bounds.
- **Widening `DBData` cost almost nothing.** Six types needed `#[interned(opaque)]` because
  their fields come from other crates, and two needed a hand-written implementation. The
  `never_interned!` macro was barely used.

`Leaf::from_parts` is the single call site because every `Vec`-backed batch is assembled
there, builders and mergers alike. File batches are M4.

Still open: the mergers take the same path but a merge sees two dictionaries, so B4 and B5
have not been written yet, and `SPARSE_FACTOR` is a guess until B8 exists.

**M3: shape table.** `TAG_SHAPED_MAP`. The largest single win and strictly additive.
Benchmarks B1, B2, B3.

**M4: storage.** File-level dictionary block, `Deserializer` context, version bump, compat
goldens. Benchmarks B7, B9.

**M5: policy.** `SPARSE_FACTOR` and staging-cap tuning against B4, B5, and B8, plus the
continuous metrics.

**M6: arena packing, optional.** Many documents per chunk, gated on M2's liveness machinery.
Worth roughly 1.4 KiB per row of allocator rounding plus the per-document `Arc` header.
Do it only if B7 says the remaining allocation overhead still matters.

### Correctness tests

- Extend the existing `variant()` proptest strategy (`flat_variant.rs:1532`) so that
  generated documents round-trip through intern, rewire, and freeze, and assert that
  `Ord`, `Eq`, `Hash`, `to_json_string`, and rkyv agree with the inline encoding. This is
  the property that everything else rests on.
- Differential fuzzing of the same property: build each document both ways and compare every
  observable, including cross-dictionary comparison of two documents interned separately.
- Rewire-specific properties: rewiring is idempotent under a second rewire into the same
  dictionary; rewiring preserves document length; rewiring a set of documents produces a
  dictionary containing exactly the entries those documents reference, and no others.
- Hash stability: a value built inline and the same value interned hash identically, and the
  fixed hash in `crates/dbsp/src/dynamic/data.rs:108` still holds.
- Every retention guard in B8 must be shown to fail with the sparseness threshold disabled,
  so we know the test measures the mechanism and not the workload.
