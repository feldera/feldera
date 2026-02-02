# DBSP Operator Architecture

This directory implements DBSP operators—the computational primitives that process incremental data streams. Operators transform streams of changes (insertions, deletions) rather than full datasets, enabling efficient incremental computation.

## Directory Structure

- **Top-level modules**: Individual operator implementations (filter, map, join, aggregate, etc.)
- **`dynamic/`**: Runtime-typed operator variants for SQL compiler integration
- **`group/`**: Specialized grouping operators (top-k, lag) for window functions and ranking
- **`communication/`**: Multi-worker operators (exchange, gather, shard) for distributed execution
- **`time_series/`**: Windowing and temporal operators

## Core Trait Hierarchy

Operators implement a layered trait system defined in `circuit/operator_traits.rs`:

### Base Operator Trait

All operators implement the `Operator` trait providing lifecycle management:

```rust
pub trait Operator: 'static {
    fn name(&self) -> Cow<'static, str>;
    fn fixedpoint(&self, scope: Scope) -> bool;  // Convergence detection for nested circuits
    fn clock_start(&mut self, scope: Scope);     // Invoked at clock cycle start
    fn clock_end(&mut self, scope: Scope);       // Invoked at clock cycle end
    fn checkpoint(...) -> Result<(), Error>;     // State persistence
    fn restore(...) -> Result<(), Error>;        // State recovery
}
```

### Arity-Specific Traits

Built atop the base trait, these define the operator's input/output signature:

- **`SourceOperator<O>`**: Produces output without input (e.g., input streams)
- **`SinkOperator<I>`**: Consumes input without output (e.g., output handles)
- **`UnaryOperator<I, O>`**: Single input → single output
- **`BinaryOperator<I1, I2, O>`**: Two inputs → single output (e.g., joins)
- **`TernaryOperator`, `QuaternaryOperator`, `NaryOperator`**: Higher-arity variants

All operator traits are async-first and support both borrowed and owned input handling via `eval` and `eval_owned` methods.

## Stream Integration Pattern

Operators connect to circuits through the `Stream<C, D>` type. New operators are exposed as extension methods on `Stream`:

```rust
impl<C: Circuit, T> Stream<C, T> {
    pub fn my_operator<F, O>(&self, func: F) -> Stream<C, O>
    where
        F: Fn(&T) -> O + 'static,
    {
        self.circuit().add_unary_operator(
            MyOperator::new(func),
            self,
        )
    }
}
```

Circuit builder methods (`add_unary_operator`, `add_binary_operator`, etc.) handle wiring operators into the dataflow graph.

## Type System: Static and Dynamic APIs

The operator system provides dual APIs:

### Static API (Compile-Time Safety)
Type-safe operators with generic parameters. Used for Rust-native development:

```rust
stream.filter(|k, v| predicate(k, v))
      .map(|k, v| transform(k, v))
```

### Dynamic API (Runtime Flexibility)
Type-erased operators using trait objects and factory patterns. Used by the SQL compiler for runtime code generation:

```rust
// Dynamic operators use factories for type construction
stream.inner()
      .dyn_filter(&factories, &filter_func)
      .typed()
```

Conversion between APIs uses `inner()` (static → dynamic) and `typed()` (dynamic → static).

## Data Abstractions

### Z-Sets and Batches

Operators process **Z-sets**—collections where each element has an integer weight (multiplicity):
- Positive weights: insertions / presence count
- Negative weights: deletions
- Zero weight: element absent

The `Batch` trait hierarchy provides efficient storage:
- `BatchReader`: Read-only access to batch contents
- `Batch`: Mutable batch with construction capabilities
- `IndexedZSet`: Key-value Z-set (grouped by key)
- `ZSet`: Non-indexed Z-set (key only)

### Delta Streams

Most operators work with **delta streams**—streams of changes rather than full state. Key operators convert between representations:
- `integrate()`: Delta stream → cumulative state
- `differentiate()`: Cumulative state → delta stream

## Implementing New Operators

### Stateless Operator Pattern

```rust
pub struct MyOperator<F> {
    func: F,
    location: &'static Location<'static>,
}

impl<F: 'static> Operator for MyOperator<F> {
    fn name(&self) -> Cow<'static, str> { Cow::Borrowed("MyOperator") }
    fn fixedpoint(&self, _scope: Scope) -> bool { true }  // Stateless → always at fixedpoint
}

impl<I, O, F: FnMut(&I) -> O + 'static> UnaryOperator<I, O> for MyOperator<F> {
    async fn eval(&mut self, input: &I) -> O {
        (self.func)(input)
    }
}
```

### Stateful Operator Pattern

Stateful operators (joins, aggregates) maintain state across invocations:

```rust
impl<S> Operator for StatefulOperator<S> {
    fn fixedpoint(&self, scope: Scope) -> bool {
        self.state.is_stable()  // False until state converges
    }

    fn checkpoint(&self, ...) -> Result<(), Error> {
        // Persist state for fault tolerance
    }
}
```

### Ownership-Aware Scheduling

Operators signal input ownership preference to the scheduler:

```rust
fn input_preference(&self) -> OwnershipPreference {
    OwnershipPreference::PREFER_OWNED  // Avoid cloning for last consumer
}
```

## Group Subdirectory

The `group/` subdirectory contains operators for per-key grouping operations:
- **Top-K selection**: Retains k smallest/largest values per key
- **Lag operations**: Window lookback within groups
- **Custom ordering**: User-defined comparison functions

These operators use `OrderStatisticsMultiset` for O(log n) rank operations, enabling efficient SQL window functions like `ROW_NUMBER()`, `RANK()`, and `PERCENTILE_CONT()`.

## Key Design Principles

1. **Incremental Correctness**: All operators preserve mathematical correctness for delta processing
2. **Composability**: Operators compose via Stream method chaining
3. **Zero-Cost Abstraction**: Static API compiles to efficient code; dynamic API adds minimal overhead
4. **Fault Tolerance**: Stateful operators support checkpointing and recovery
5. **Parallelism**: Operators integrate with multi-threaded scheduler via `communication/` operators