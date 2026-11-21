# Source attribution for Python runtime diagnostics

The [attribution catalog](evidence/python-runtime-error-attribution.json) maps
all 61 observed runtime-negative cases to **10 repository-owned error-body roots
and one delegated Rust panic formatter**. It records the exact generated helper,
observed panic text and location, source anchor, source hash, dependencies and
shared wrappers. This is a bounded attribution of the executed cases, not a census
of every sqllib, DBSP, standard-library or platform diagnostic.

| Error-body root | Cases | Message owner and dependencies |
| --- | ---: | --- |
| `plus<T>` | 8 | [operators.rs](../sqllib/src/operators.rs): checked-add panic; shared SQL type-name mapping |
| `minus<T>` | 8 | [operators.rs](../sqllib/src/operators.rs): checked-subtract panic; shared SQL type-name mapping |
| `times<T>` | 8 | [operators.rs](../sqllib/src/operators.rs): checked-multiply panic; shared SQL type-name mapping |
| `div<T>` | 8 | [operators.rs](../sqllib/src/operators.rs): checked-divide panic; shared SQL type-name mapping |
| `cast_to_SqlDecimal_s` | 1 | [casts.rs](../sqllib/src/casts.rs) plus [ParseDecimalError display](../fxp/src/lib.rs) |
| `cast_to_SqlDecimal_f` | 1 | [casts.rs](../sqllib/src/casts.rs), [Fixed conversion](../fxp/src/fixed.rs), and [OutOfRange display](../fxp/src/lib.rs) |
| `cast_to_Uuid_bytes` | 1 | [casts.rs](../sqllib/src/casts.rs): fixed binary-width message |
| `cast_to_Time_s` | 1 | [casts.rs](../sqllib/src/casts.rs) plus Chrono 0.4.45 `ParseError::fmt`, `Invalid` variant |
| `Uuid::try_from_ref` | 1 | [uuid.rs](../sqllib/src/uuid.rs), reached through the string-to-UUID cast |
| `cast_to_i_i!` | 8 | [casts.rs](../sqllib/src/casts.rs): one macro body, SQL type-name mapping, standard `TryFromIntError` display |
| Rust remainder-by-zero panic | 16 | [operators.rs](../sqllib/src/operators.rs) evaluates `%`; observed stack identifies `core::panicking::panic_const::panic_const_rem_by_zero` |

The two modulo suites repeat the same eight type instances; they do not create
sixteen formatter implementations. The arithmetic helpers likewise specialize
one generic body over signed/unsigned widths and nullable wrappers. Aggregate
conversions narrow from `i64` or `i128`; the catalog records each generated helper
instead of assuming every aggregate uses the same accumulator width.

The 13 cast/conversion failures all panic at `handle_error_with_position` in
`casts.rs`. That shared panic site is not their error-body owner. The cast or UUID
formatter first constructs the message, `SqlRuntimeError` displays it unchanged,
and the wrapper adds a `SourcePosition`. The frozen Java
`DBSPHandleErrorExpression.getFunction` selects that wrapper. DBSP then adds its
worker/runtime envelope. The catalog records six shared repository wrappers
separately from the ten error-body roots.

Dependency formatters are also separate. In particular, `ParseDecimalError`,
`OutOfRange`, Chrono's parse error, Rust's integer-conversion error and primitive
numeric display contribute text to these messages. The ten-root count must not
be presented as the total number of formatter bodies in the dependency graph.

The exact runtime execution certificate and all referenced generated modules are
hash-checked before attribution. Repository sources must match the execution
archive. Chrono's cached formatter source is separately pinned and archived.
Rust source is not installed in the devcontainer: the remainder formatter,
`TryFromIntError` text and primitive display are pinned to the recorded Rust 1.99.0
toolchain and observed behavior, with unavailable source bodies explicitly marked.
No additional runtime tests were run for this attribution.

These are the same registered but dormant Python negative views described in
[the execution review](PYTHON-RUNTIME-DIAGNOSTICS.md). The catalog neither enables
the normal suite nor adds platform/HTTP coverage. Java runtime-panic assertions
and the remaining diagnostic populations require their own attribution; matching
text alone does not prove they exercise these exact roots.
