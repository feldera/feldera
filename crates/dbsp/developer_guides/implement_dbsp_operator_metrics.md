# Implementing DBSP Operator Metrics

How to add profiler-visible metrics to a DBSP operator so that the dataflow profile graph shows batch sizes, memory usage, and operator-specific stats.

## Key Files

| File | Role |
|------|------|
| `src/circuit/metadata.rs` | `BatchSizeStats`, `MetaItem`, `OperatorMeta`, standard label constants |
| `src/circuit/operator_traits.rs` | `Operator` trait with `fn metadata(&self, meta: &mut OperatorMeta)` |

## Standard Label Constants

Defined in `metadata.rs`. Use these for metrics that all operators share:

| Constant | Display name | Description |
|----------|-------------|-------------|
| `INPUT_BATCHES_LABEL` | `"input batches"` | Input batch size distribution |
| `OUTPUT_BATCHES_LABEL` | `"output batches"` | Output batch size distribution |
| `NUM_ENTRIES_LABEL` | `"total size"` | Number of entries in operator state |
| `USED_BYTES_LABEL` | `"used bytes"` | Memory used (total minus excess capacity) |
| `NUM_ALLOCATIONS_LABEL` | `"allocations"` | Distinct heap allocations |
| `SHARED_BYTES_LABEL` | `"shared bytes"` | Bytes behind `Arc`/`Rc` |
| `COMPUTED_OUTPUTS_LABEL` | `"computed outputs"` | Output tuples before consolidation |
| `OUTPUT_REDUNDANCY_LABEL` | `"output redundancy"` | Fraction eliminated by consolidation |

## Step-by-Step

### 1. Add `BatchSizeStats` fields

```rust
use crate::circuit::metadata::{
    BatchSizeStats, MetaItem, OperatorMeta,
    INPUT_BATCHES_LABEL, OUTPUT_BATCHES_LABEL,
    NUM_ENTRIES_LABEL, USED_BYTES_LABEL,
    NUM_ALLOCATIONS_LABEL, SHARED_BYTES_LABEL,
};

struct MyOperator {
    // ... operator state ...
    input_batch_stats: BatchSizeStats,
    output_batch_stats: BatchSizeStats,
}
```

Initialize with `BatchSizeStats::new()` in the constructor.

### 2. Record batch sizes in `eval()`

Call `add_batch(len)` at the start and end of each evaluation:

```rust
async fn eval(&mut self, input: &InputBatch) -> OutputBatch {
    self.input_batch_stats.add_batch(input.inner().len());

    // ... operator logic ...

    let result: OutputBatch = TypedBatch::from_tuples((), tuples);
    self.output_batch_stats.add_batch(result.inner().len());
    result
}
```

For typed batches, use `.inner().len()` to get the record count from the underlying dynamic batch.

### 3. Implement `SizeOf` for memory reporting

Stateful operators should implement `SizeOf` so `self.size_of()` returns a `TotalSize` with used/shared/allocation counts.

**If your struct can derive it** (all fields implement `SizeOf`):
```rust
#[derive(SizeOf)]
struct MyOperator {
    state: BTreeMap<K, V>,
    #[size_of(skip)]          // skip fields that don't impl SizeOf
    config: SomeConfig,
    #[size_of(skip)]
    input_batch_stats: BatchSizeStats,
    #[size_of(skip)]
    output_batch_stats: BatchSizeStats,
}
```

**If you need a manual impl** (e.g., closure fields prevent derive):
```rust
use size_of::SizeOf;

impl<K: DBData, V: DBData, F> SizeOf for MyOperator<K, V, F> {
    fn size_of_children(&self, context: &mut size_of::Context) {
        self.state.size_of_children(context);
        // skip closure, PhantomData, config, BatchSizeStats
    }
}
```

`DBData` already requires `SizeOf`, so generic data types always satisfy the bound.

### 4. Report metrics in `metadata()`

Use the `metadata!` macro for concise construction:

```rust
fn metadata(&self, meta: &mut OperatorMeta) {
    let bytes = self.size_of();

    meta.extend(metadata! {
        // Standard batch stats (expands to batches/min/max/avg/total)
        INPUT_BATCHES_LABEL  => self.input_batch_stats.metadata(),
        OUTPUT_BATCHES_LABEL => self.output_batch_stats.metadata(),
        // Memory (stateful operators only)
        NUM_ENTRIES_LABEL    => MetaItem::Count(self.state.len()),
        USED_BYTES_LABEL     => MetaItem::bytes(bytes.used_bytes()),
        NUM_ALLOCATIONS_LABEL => MetaItem::Count(bytes.distinct_allocations()),
        SHARED_BYTES_LABEL   => MetaItem::bytes(bytes.shared_bytes()),
        // Operator-specific (use string literals, not constants)
        "my custom stat"     => MetaItem::Int(42),
    });
}
```

## MetaItem Variants

| Variant | Use for | Mergeable across workers? |
|---------|---------|--------------------------|
| `MetaItem::Count(usize)` | Summable counters (records, allocations) | Yes (added) |
| `MetaItem::Int(usize)` | Non-summable integers (num keys) | No |
| `MetaItem::bytes(usize)` | Memory sizes (renders human-readable) | Yes |
| `MetaItem::Percent { numerator, denominator }` | Ratios (redundancy) | Yes (fractions added) |
| `MetaItem::Duration(Duration)` | Time measurements | Yes |
| `MetaItem::Bool(bool)` | Flags | No |
| `MetaItem::String(String)` | Free-form text | No |
| `MetaItem::Map(OperatorMeta)` | Nested metric groups | Per-entry |

Use `Count` (not `Int`) for anything that should be summed when merging metrics across workers in multi-threaded execution.

## BatchSizeStats Detail

`BatchSizeStats::metadata()` produces a nested map:

```
"output batches": {
    "batches": 15,
    "min size": 0,
    "max size": 128,
    "avg size": 42,
    "total records": 630
}
```

If zero batches have been recorded, only `"batches": 0` is emitted.

## Stateless vs Stateful Operators

**Stateless** (filter, map): report `INPUT_BATCHES_LABEL` + `OUTPUT_BATCHES_LABEL` only.

**Stateful** (join, distinct, percentile): additionally report `NUM_ENTRIES_LABEL`, `USED_BYTES_LABEL`, `NUM_ALLOCATIONS_LABEL`, `SHARED_BYTES_LABEL`, plus any operator-specific state metrics.

## Reference Implementations

| Pattern | Example file |
|---------|-------------|
| Stateless with batch stats | `operator/dynamic/filter_map.rs` |
| Stateful with memory + batch stats | `operator/dynamic/distinct.rs` |
| Stateful with custom metrics + manual SizeOf | `operator/dynamic/percentile_op.rs` |
| Complex with redundancy tracking | `operator/dynamic/join.rs` |
| Disk-backed with storage size + cache stats | `trace/spine_async.rs` |
