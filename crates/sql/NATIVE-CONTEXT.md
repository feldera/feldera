# Native row context and archived-view feasibility

The isolated [native context probe](native-dbsp-probe/tests/native_context.rs)
implements DBSP's real `Data`, `Clonable`, `SerializeDyn`, `DeserializableDyn` and
`DeserializeDyn<DynData>` traits for one runtime-described Rust row type. Its
archived views retain both the storage and the descriptor that owns the loaded
Cranelift encoder. The initial five tests pass in the Linux ARM64 devcontainer.
The probe supplies a candidate ownership boundary for runtime factories.
The DBSP layer-file accessors used by this probe are available under the
`testing` feature.

## Interface finding

`Factory::archived_value<'a>(&self, bytes: &'a [u8], pos)` returns
`&'a Trait::Archived` (`crates/dbsp/src/dynamic/factory.rs:31`). The existing
implementation constructs that view from the concrete Rust type's static
metadata. It cannot return a temporary object containing a runtime descriptor:
the returned reference is tied only to the bytes, and no object containing both
bytes and context exists there. A pointer embedded in persisted bytes or a forged
Rust trait vtable is not an acceptable repair.

The probe changes the experimental boundary explicitly:

```
NativeFactory::archived(Arc<[u8]>, position) -> Result<ArchivedView, error>
ArchivedView::as_archived(&self) -> &<DynData as ArchiveTrait>::Archived
```

The view owns an `Arc` to its storage and layout. Its trait reference borrows the
view. `AsAny` and `Comparable` require a `'static` concrete implementor; the owned
view satisfies that bound without erasing a borrowed byte-slice lifetime or
using a raw pointer. The archive bytes contain neither ownership handles nor
process addresses. Dropping the factory and all live rows leaves the view valid;
dropping the last view releases the descriptor and native library. Reopening a
file obtains a fresh descriptor from the external schema contract.

The [layer-file follow-up](evidence/native-context-layer-items.json) writes
three rows with the regular DBSP writer and reopens the file with a fresh
buffer cache. A test-only cursor method supplies the buffer and item offset
to an owned `ArchivedItem`. Its key and auxiliary descriptors remain live
along with the original file block after the reader, cache and factories are
dropped. The test compares the exact values and ordering and verifies that
the last view releases the block and descriptors. Six focused tests pass.

The [index-bound follow-up](evidence/native-context-index-bounds.json) reopens a
9,216-byte layer file with 513 rows. At five cursor positions it compares the
ten stored lower and upper keys against the actual row keys. All bounds and
rows remain valid after dropping the reader, cache and original factories.
The last view releases the underlying blocks and descriptors.

The [layer-file roundtrip](evidence/native-context-layer-roundtrip.json)
writes all 513 rows through runtime-owned factories and DBSP's ordinary block
writer. The resulting 9,216 bytes match the classic file. The writer reads
eight archived item keys while building the index. The test releases the
writer descriptors, reloads the native-written file and creates fresh
descriptors for the declared layout.

The same file search compares native archived keys at 38 data probes and 24
index probes. It also locates and materializes selected rows through native
descriptors. The typed cursor reaches the same positions. A directed
comparator error propagates through the cursor. All 63 DBSP file tests, all
ten frozen native cases, the 50 default native tests and Clippy pass.

The two-column roundtrip uses separate runtime factories for the outer and
inner columns. It writes 153 groups and 3,796 child rows, exactly matching the
classic file. Fresh descriptors locate both ends of every group and confirm
that a seek past the group's final key never lands in its neighbor.
[Nested layer evidence](evidence/native-context-nested-layer.json)

The follow-up opens the same layer format using only a column count. It shares
the typed reader's file validation, parsed block cache and row search. A
descriptor supplies key comparison at each probe. The reader returns the
archived item location and, for nonterminal columns, the associated row group.
The regression verifies all 153 groups and rejects an incorrect column count.
[Factory-free reader evidence](evidence/native-context-raw-layer.json)

The layer writers and the factory-free reader are testing entry points.
The standard DBSP batch factory bundles still return static references,
including the vector factories in `trace/ord/vec/wset_batch.rs`. General
descriptor-backed batch construction still needs descriptor-aware vectors,
tuples, input handles and merge scratch values.

The ownership chain starts at `dyn_add_input_zset_mono`. Its batch factory
must supply both the row layout and the vector that carries rows to the
workers. The same descriptor must be available to the batch builder and to
merge cursors that allocate key scratch space. File reload receives the layout
from the caller. The storage tests cover serialization and search at that
boundary. The merge regression uses the DBSP list merger with runtime-owned key
scratch and reopens its persisted output with new descriptors. The ten case
adapters now send descriptor-owned input batches through DBSP input handles.
The five join and rolling fixtures decode their retained fields before
entering the existing typed batch operators.

The merge regression feeds 1,024 archived signed updates to DBSP's list
merger. The merged layer has the expected 448 nonzero rows after reopen.
A separate run writes 513 nullable DECIMAL(38, 2) keys through the same
factory, including mantissas wider than BIGINT. The classic and descriptor
files are byte-identical, all live key hashes match, and fresh descriptors
find the expected archive rows through the shared index.
[Merge and nullable decimal evidence](evidence/native-context-decimal.json)

Sequential descriptor reads reuse the current data block and index path.
The merge regression uses that scan across block boundaries, and the nested
regression checks that it stops at the end of each of the 153 groups.
[Sequential read evidence](evidence/native-context-advance.json)

The full DBSP library suite passes on a temporary XFS volume in the offline
devcontainer, with 1,029 passing and three ignored tests. The host-bind run
failed the page-cache test because the mount reported zero resident pages
after the initial reads. The same test passes on XFS. The suite uses the
cached workspace lockfile recorded in the archive.
[Full DBSP suite](evidence/native-context-dbsp-suite.json)

All ten frozen programs pass the 21 saved transactions with descriptor-owned
inputs. The VARCHAR fixtures pass through the checked inline and out-of-line
archive reader. The fixed-width fixtures read nullable INTEGER, BOOLEAN and
full-width DECIMAL fields before the first typed operator. Both global
aggregates use descriptor-owned input and spill state through the whole
prototype. They publish real DBSP checkpoints, unload the original
descriptors, and continue signed transactions in fresh runtimes.
[Frozen input and checkpoint evidence](evidence/native-context-frozen-inputs.json)

The aggregate checkpoint adapter copies its small layer into the standard
file-checkpoint writer. Its replay verifies the state layout and lifecycle
for the two selected operators. The approved C13 measurements use the
separate optimized circuit adapters.

## What the probe executes

The runtime layout owns the real `CompiledArchive` shared library and checked
field offsets. Live rows carry that layout and bounded integer cells. Factory
creation supplies defaults explicitly. Custom clone/move operations preserve the
source and destination layout; a moved-from row becomes that layout's default,
not a context-free `Default` value. Cross-layout operations reject mismatches,
including layouts with the same Rust `TypeId`. Descriptor identity is checked
in-process; persisted data still requires a trusted external schema identity.

Three reference representations are exercised: `Tup1<i64>`, `Tup2<i32, i64>` and
`Tup1<Option<i64>>`. The tests cover:

- Real `Box<DynData>` clone/move, default values, and DBSP's `Xxh3Default` hash
  against the reference tuples, with two simultaneously active layouts.
- Native byte equality, archived/archived and archived/live comparison through
  the actual associated archived trait object, and deserialization into a
  factory-created dynamic row. Integer boundaries, duplicates and NULLs appear.
- Descriptor/library retention after the original factory and live rows are
  dropped. The retained context is used to deserialize and call the native
  encoder again; weak references prove eventual release without a registry leak.
- DBSP's actual `SerializerInner`, including nonzero aligned row offsets, followed
  by its mutable dynamic deserialization entry point.
- Reference Rust tuple archives written to a real file, reopened under a newly
  compiled native descriptor, decoded and re-encoded with identical bytes and
  hashes. Invalid offsets, truncated rows, invalid option tags and cross-layout
  requests are refused.

The file test contains raw row archives. It is **not a DBSP spill batch, circuit
checkpoint, or the required rustc-to-native checkpoint restore**. The archived
view's comparator currently decodes bounded integer cells into a vector. That
is an explicit correctness fallback, not a zero-copy speed claim. The live row
still uses a generic Rust vector, not the final packed native allocation. Its
`SizeOf` reports visible row/layout allocations, not executable mappings or
private encoder/linker allocations; no memory benchmark relies on it. The helper
uses assertion failures for invalid row construction and cross-layout operations;
a production API needs explicit error propagation and schema verification.

## Validation and retained evidence

The first compilation failed because `DynDeserialize` is not publicly re-exported
by DBSP. The repair names the public `ArchiveTrait::Archived` associated type,
which is that actual dynamic trait object. All five focused tests pass, and
all-target Clippy passes with warnings denied. The initial failed log and every
later test log are retained in `$SCRATCH/p0-native-context-v1`. The certificate and
archive retain the final source/configuration snapshot, dependency graph, test
binary and toolchain identity. No prior failed source snapshot is claimed.

## Dynamic operator coverage

The [operator audit](evidence/native-operator-audit.json) links all 109
occurrences in the ten frozen circuits to runtime entry points for 18
operator classes. Thirteen classes have dynamic entry points. The generic
sum and negation operators accept erased batches directly. A circuit test
checks signed outputs from those operators through the ordinary output
mailbox and through the same accumulated-output facade used by generated
circuits.

The original constant generators required a type-level `Default`, and the
original accumulated sink required a typed batch facade. The [generator and
sink follow-up](evidence/native-final-with-outputs.json) adds explicit
empty-batch constructors and an erased accumulated-output entry point. The
typed and erased sinks share cohort publication and bootstrap state.

The two-worker dynamic test verifies signed transactions across both output
mailboxes. The incremental constant and accumulated sink exchange persistent
checkpoints in both directions. The restored constant remains quiet after
each switch. Classic fingerprints match the original constant, incremental
constant and accumulated-output circuits. The default native suite, all ten
frozen programs and six DBSP generator, balance and bootstrap regressions pass.

## N1/N2 results and remaining work

All ten frozen programs and 21 signed steps match their classic references
through case-specific native adapters. The operator audit and the [program
ledger](evidence/native-coverage-ledger.json) identify each construction path.
A general builder still needs to keep runtime descriptors live through batch
factories, seek, spill and reload. The current experiments cover different
parts of that path with native expressions, state archives and owned row views.

N2 verifies all 29 explicit stored archive shapes in the selection. It also
measures sort and merge for the five explicit index-key types. Classic and
native state interchange succeeds for selected pipelines. The [state
disposition](NATIVE-STATE.md#migration-disposition-under-decision-15) records
when those results support a compatibility check and when a pipeline needs
bootstrap or owner-approved reset.

An evidenced N1 failure triggers decision 16. An evidenced incompatible
checkpoint uses decision 15. A negative finding needs the attempted contract,
the observed failure and the scope of the explored repair.
