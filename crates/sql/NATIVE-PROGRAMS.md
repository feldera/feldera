# Frozen native candidate programs

The [selection](native-programs.json) freezes ten existing exported corpus cases
before implementing their native execution. Their 21 steps contain typed inputs,
signed multiplicities, and expected outputs. Strict Rust corpus validation passes
for all ten cases in nine complete JUnit test exports. The execution results
and remaining N1/N2 requirements appear in the implementation ledger below.

The producer is `c6ede756d-p0-ordered-asof-v4`, Calcite is
`4bf922bbbc1cbd570a2f201339a46a2d8f7a763b`, and the runtime label is
`c6ede756d`. Every case has its own SHA-256 in the selection. The evidence archive
retains unchanged case files and completion manifests, generated inventory,
connector bytes, validation output, and relevant source snapshots. Its certificate
is [native-program-inventory.json](evidence/native-program-inventory.json).

All test names below have prefix `org.dbsp.sqlCompiler.compiler.sql.`. The case
number after `/` is part of the exact identity, not an execution count.

| Test and case | Steps | Input semantics | Required behavior |
| --- | ---: | --- | --- |
| `simple.OptimizedIncrementalTests#leftOuterJoinTest/0` | 3 | delta | Left join on nullable/non-null keys, retractions |
| `simple.OptimizedIncrementalTests#fullOuterJoinTest/0` | 3 | delta | Full outer join, retractions |
| `simple.OptimizedIncrementalTests#optionAggregateTest/0` | 3 | delta | Nullable SUM and aggregate group presence |
| `simple.OptimizedIncrementalTests#emptyAggregate/0` | 3 | delta | Empty COUNT and COUNT DISTINCT |
| `simple.OptimizedIncrementalTests#overDecimalTest/0` | 3 | delta | COUNT window ordered by nullable decimal |
| `postgres.PostgresNumericTests#testAdd/0` | 1 | whole relation | Decimal addition and joins |
| `postgres.PostgresNumericTests#testMultiply/0` | 1 | whole relation | Decimal multiplication and joins |
| `simple.SerdeTest#jsonStructTest2/0` | 2 | whole relation | JSON-to-nested-row conversion and field case sensitivity |
| `simple.StringTests#testNonBmpCastsAndRanges/0` | 1 | whole relation | Unicode LEFT, TRIM, length, CHAR/VARCHAR casts; connector fixture |
| `simple.StringTests#testNonBmpCastsAndRanges/1` | 1 | whole relation | Unicode string ordering |

`inventory.json` preserves each command's position, port, relation identifier,
primary keys, row type, original weights, and canonical Z-set digest. The raw
cases preserve all values and expected output relations. Input/output order and
circuit port metadata remain separate: some expected relation names are formatted
result tables, not view names. Whole-relation inputs must not be fed as cumulative
deltas. The second JSON step, in particular, replaces the first relation.

## Complete explicit type requirements

The inventory resolves each circuit and step Z-set in its own inner-node
namespace, rejects missing or cyclic type references, and hashes structural types
after removing node IDs and source positions. It preserves nullability, widths,
precision, scale, field indices/names, struct identity, and type arguments.
It retains types of unused input columns, declarations, expressions, and outputs.

There are **208 distinct explicit type shapes in 20 classes**. These include 35
tuple shapes, 20 raw-tuple shapes, 25 named struct shapes, 18 Z-set shapes, and 13
indexed-Z-set shapes. There are five distinct explicit index-key shapes, eleven
index-value shapes, four input-row shapes, and nine output-row shapes. These
categories overlap; they are not counts of native layouts already implemented.
Every shape's case/path/role occurrences are recorded in the machine inventory.

The required scalar representations are:

- Signed 32-bit integers, nullable and non-null; non-null signed 64-bit integers;
  nullable and non-null signed 128-bit integers; nullable unsigned 32-bit integers;
  non-null unsigned 64-bit integers.
- Nullable and non-null BOOL; non-null DOUBLE. Floating comparison and hashing
  must follow the existing runtime's total-order conventions.
- Nullable DECIMAL(6,2), nullable and non-null DECIMAL(38,10), and nullable
  DECIMAL(38,20). The intermediate multiplication scale must not be narrowed to
  the input scale.
- Nullable and non-null unbounded VARCHAR and VARCHAR(1), non-null CHAR(1), and
  nullable and non-null CHAR(2). Length, slicing, padding, truncation, and ordering
  must preserve SQL Unicode behavior.
- Nullable VARIANT and nested rows, including the address row with nullable
  `city`, `street`, and `number` fields. Row nullability is separate from field
  nullability.

Other explicit classes are `Function` (39), `Ref` (21), `SqlResult` (9), `Any`,
`Lazy`, `Semigroup`, `User`, `Void`, and `Weight` (one each). These are compiler or
runtime control representations as well as values; a layout bridge must classify
them rather than treat every class as a connector-visible SQL scalar.

This is complete for **explicit exported types**, not implicit operator state.
Accumulator state, radix trees, trace timestamp/value pairs, optional aggregate
outputs, and checkpoint metadata still require a factory-by-factory inventory.
Nor do these ten cases replace PLAN's full SQL type requirements: types absent
from this candidate set remain obligations of the eventual native backend.

## Runtime capabilities and remaining implementation

The ten selected programs execute through specialized IR-driven probe
builders. The table maps all 18 observed operator classes to their DBSP
entry points and identifies the work to support general runtime layouts.

| Exported operator classes (prefix `DBSP`, suffix `Operator`) | Runtime starting point | Missing native work |
| --- | --- | --- |
| `SourceMultiset`, `Sink` | [dynamic/input.rs](../dbsp/src/operator/dynamic/input.rs), [output.rs](../dbsp/src/operator/output.rs) | Descriptor-backed row factories, handles, port identity, exact signed inputs/outputs |
| `Map`, `MapIndex`, `FlatMap`, `FlatMapIndex`, `Filter` | [dynamic/filter_map.rs](../dbsp/src/operator/dynamic/filter_map.rs), `dyn_map`, `dyn_map_index`, `dyn_flat_map`, `dyn_flat_map_index`, `dyn_filter` | Typed closure ABI, variable-size allocation/ownership, nullable results and errors |
| `Join`, `StreamJoin`, `AntiJoin` | [dynamic/join.rs](../dbsp/src/operator/dynamic/join.rs), incremental/stream join and antijoin APIs | Correct stream semantics, complete key/value factories, native join closures and persistent traces |
| `LeftJoin` | [dynamic/outer_join.rs](../dbsp/src/operator/dynamic/outer_join.rs), `dyn_left_join` | Null-extension closures and matching key/order/hash semantics |
| `AggregateLinearPostprocess` | [dynamic/aggregate.rs](../dbsp/src/operator/dynamic/aggregate.rs), `dyn_aggregate_linear_generic`; [typed adapter](../dbsp/src/operator/aggregate.rs) | Accumulator `WeightTrait`, count/group-presence rules, postprocessing, all internal factories |
| `PartitionedRollingAggregate` | [dynamic/time_series/rolling_aggregate.rs](../dbsp/src/operator/dynamic/time_series/rolling_aggregate.rs) | Concrete unsigned `TS` specialization, range ordering, partition/accumulator/output factories and radix-tree archives |
| `Constant`, `IncrementalConstant` | [generator.rs](../dbsp/src/operator/generator.rs), `ConstantGenerator` and `IncrementalConstantGenerator` | Decode exact typed constants; retain first-step and restored-clock behavior |
| `Sum`, `Negate` | [sum.rs](../dbsp/src/operator/sum.rs), [neg.rs](../dbsp/src/operator/neg.rs) | Correct dynamic batch algebra and multiplicities |
| `AtomicSum` | [Java lowering](../../sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/backend/rust/ToRustVisitor.java), [dynamic concat](../dbsp/src/operator/dynamic/concat.rs), then [shard](../dbsp/src/operator/dynamic/communication/shard.rs) | Preserve transaction sub-step atomicity; ordinary sum is insufficient |

All 18 selected operator classes have a runtime entry point in this inventory.
The port can use dynamic input, filter, map, indexing, join and aggregate APIs.
Constants, sinks, negation and ordinary sum use generic batch operators.
Atomic sum composes dynamic concatenation and sharding. The rolling API
erases the partition, value and accumulator arguments, but keeps
`TS: DBData + UnsignedPrimInt` concrete. Its factory constructors build typed
pairs and radix-tree state. Aggregate factory constructors also rely on
concrete generic types and static factories.
The [factory contract](../dbsp/src/dynamic/factory.rs) has no existing runtime
layout parameter in `WithFactory<T>::FACTORY`. Passing a row pointer alone cannot
supply the layout needed by default allocation or archived interpretation.

The next implementable bridge is a schema-independent row plus an immutable,
owned layout descriptor and factories that retain that layout through default
values, cloning, hashing, ordering, spill, and restore. Start with the actual four
input-row and five explicit index-key shapes. Extend through every output and
implicit state factory before claiming general descriptor-backed construction
for the ten programs. Check
[OrdRepr](../dbsp/src/dynamic/ord_repr.rs) against live `Ord` and
[HashRepr](../dbsp/src/dynamic/hash_repr.rs) against decoded hashing. A descriptor
and its generated code must outlive rows, factories, and archived views.

Expression work includes decimal operations/casts, string helpers, variant
conversion, generated `jsonstring_as_address2`, and fallible/lazy evaluation.
The function path inventory includes `char_lengthN`, `char_length_`,
`left_sN_i32`, `trim_leading_s_sN`, `parse_json_fv_sN`, `from_json_string`,
`jsonstring_as_address2`, `Tup3::from_results`, `map`, `into`, and `str`.
These are path segments, not a complete list of runtime calls: arithmetic and
casts also appear as IR expressions. `jsonstring_as_address2` is a compiler-
generated JSON conversion, not an unprovided user function; see
[ExternalFunction](../../sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/frontend/calciteCompiler/functions/ExternalFunction.java).

## Connector fixture and validation

The Unicode-functions case supplies the existing typed row `s = '🐍x'`, weight
`+1`. Its frozen JSON Lines input is `{"insert":{"s":"🐍x"}}` with configuration
`{"name":"json","config":{"update_format":"insert_delete","array":false}}`.
The decoder checks the exact unbounded VARCHAR schema and nullability; it allows
only a value-preserving nullability cast. Negative weights become repeated delete
records and positive weights repeated insert records, with bounded expansion.
It rejects later whole-relation steps without state differencing. This fixture
requires an initially empty relation.

The format follows [JSON configuration](../feldera-types/src/format/json.rs) and
[JSON input parsing](../connectors/src/format/json/input.rs). The production
parser ingested the exact fixture into the classic generated and native circuits. The input buffers agree on record count and decoded hash.
Both circuits produce the same weighted output and empty error sets. The
production encoder emits byte-identical JSON using the exported output schema.
The local harness supplies an in-memory output consumer for the format test.
[Connector execution certificate](evidence/native-connector-execution.json)

Rebuild the inventory with:

```sh
python3 crates/sql/scripts/native_inventory.py \
  crates/sql/native-programs.json CORPUS OUTPUT \
  --comparator /path/to/sql-difftest
```

`OUTPUT` must be new. The tool copies whole selected JUnit directories, verifies
the case pins, runs strict corpus validation, and writes the complete inventory
and connector bytes. Six focused tests passed in the devcontainer, covering type
identity/reference failures, Unicode/NULL/signed multiplicities, schema and
identifier mismatches, casts, and whole-relation semantics. No timings or native
execution result are inferred from inventory validation.

## Implementation ledger

The [generated coverage ledger](evidence/native-coverage-ledger.json) retains
every selected program, its exact steps and row types, operator IDs and runtime
entry points, expression-function paths, explicit stored types and index keys.
All 29 explicit stored rows and keys now have archive component evidence, and
all five explicit index-key types have paired sort and merge measurements.
The implicit-state checks verify four fixed-width payloads from the selected
linear aggregate and rolling-count construction, including an optional
accumulator with a shared discriminant. The factory inventory follows
their input, trace and output paths. The rolling COUNT radix-tree row
matches five typed rkyv archives. Trace containers, descriptor-backed
factory construction and checkpoint metadata still need coverage.
[Selected rows](evidence/native-selected-rows.json) · [Index keys](evidence/native-corpus-index-keys.json)
[Implicit layouts](NATIVE-VARIABLE-ARCHIVES.md#implicit-aggregate-and-rolling-layouts) · [Radix-tree evidence](evidence/native-rolling-tree-archives.json)

The [decimal and string increment](NATIVE-VARIABLE-ARCHIVES.md) verifies five
additional actual row shapes and restores a nullable-decimal checkpoint into a
native serializer. Its [certificate](evidence/native-variable-archives.json)
pins the source, successful and failed logs, actual checkpoint bytes and new
test executables. The ledger records those component results separately from
the six prior integer shapes and from normalized-key performance requirements.
Both `simple.StringTests#testNonBmpCastsAndRanges` cases pass whole-case
execution. The importer checks each graph's five nodes and links, reads the
literals and types from the exported expressions, and builds the scan,
transformation, empty error constant and both outputs on DBSP. Cranelift
emits the strict UTF-8 range predicate for case 1 and slice computation for
truncation and leading trim in case 0. Both exact frozen inputs produce the
expected weighted output and an empty error output. Both original
Java-generated Rust tests pass. [Unicode execution certificate](evidence/native-unicode-execution.json)

The left and full self-joins run all six frozen signed steps on DBSP. The
witness validates the exported graph, key projection, NULL filtering and padding
before building the circuit. A Cranelift kernel reads the nullable key, and DBSP
maintains matched and unmatched rows across transactions. Supplemental steps
exercise matching and retractions. Both original Java-generated Rust tests
pass independently. [Outer-join execution certificate](evidence/native-outer-join-execution.json)

The nullable SUM and COUNT DISTINCT cases replay six additional signed steps.
The importer reads the SUM input field and the COUNT filter from the exported
closures. Cranelift emits the nullable-value and predicate kernels. DBSP keeps
the per-key and global accumulators and supplies the default empty-group row
in the same transaction. Supplemental updates distinguish NULL SUM from
numerical zero and verify duplicate-key retractions and return to an empty
group. Both original classic tests pass independently.
[Aggregate execution certificate](evidence/native-aggregate-execution.json)

The decimal RANGE COUNT case replays all three frozen steps. Its Cranelift
kernel converts nullable decimal mantissas to the original unsigned timestamp
ordering. DBSP maintains the rolling counts and joins them back to the input
rows. Four additional steps cover out-of-order arrivals, negative decimal
ordering, NULLs first and NULL retraction. The original classic test passes.
[Rolling-window execution certificate](evidence/native-rolling-execution.json)

The Postgres addition and multiplication programs replay two whole-relation
steps through DBSP stream joins. The tests compare all 200 arithmetic results
with the original reference before applying the mismatch filter. The native
inequality kernel compares both words of a nullable DECIMAL(38, 10) mantissa.
Supplemental inputs exercise each word and NULL operands.
[Decimal-join execution certificate](evidence/native-decimal-join-execution.json)

The nested JSON program replays both whole-relation steps and both output
views. Cranelift dispatches present strings to sqllib callbacks for the
case-sensitive variant cast and case-insensitive contextual record decoder.
A supplemental NULL input preserves its weight in both views, and the
callback tests verify that Rust panics resume outside the native boundary.
The original classic tests pass for all three programs.
[Complete C7 execution certificate](evidence/native-json-execution.json)

The selected decimal rolling-window graph also restores a checkpoint written by
the original generated Rust circuit. Replacing two join-key types with the
native archive adapter preserves the runtime fingerprint and matches four
post-restore transactions against an uninterrupted classic circuit.
[Rolling checkpoint certificate](evidence/native-rolling-checkpoint.json)

The selected nullable SUM and COUNT DISTINCT circuits also exchange checkpoints
with their original generated classic circuits by swapping only the archived
accumulator weight. Each test covers retractions, reinsertion, fresh native
linking, and restore into the original classic graph.
[Aggregate checkpoint certificate](evidence/native-aggregate-checkpoints.json)

The matched Unicode benchmark measures throughput, mean step latency and
memory in 124 fresh processes, plus 15 classic shared-library rebuilds with
warm dependencies. [Paired benchmark results](BENCHMARKS.md#paired-native-unicode-projection)

General descriptor-backed construction, the implicit factory inventory and the
full performance budget remain open.
