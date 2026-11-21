# Fold semantics and runtime extraction

The prototype now demonstrates a compatibility problem that a runtime-version
check alone cannot solve. On the measured ARM64 devcontainer, Java folding and
the actual Rust runtime return different DOUBLE bits for these expressions:

| Expression | Java folded bits | sqllib/Rust bits |
|---|---|---|
| `EXP(1)` | `4005bf0a8b14576a` | `4005bf0a8b145769` |
| `LOG(3, 4)` | `3fe95c01a39fbd68` | `3fe95c01a39fbd69` |

The full journal contains six occurrences of those differences. They are not
compiler crashes, approved deviations, or approximate passes. Java uses
`Math.exp` and a quotient of `Math.log` calls. The runtime uses Rust's math
functions. Their mathematical intent is insufficient to establish identical
results. The prototype's default `runtime-v1` profile follows the runtime and
retains the exact differences. The full compilation audit records their effect
separately from value parity.

The [origin experiment](../evidence/fold-math-origin.json) reproduces both Java
results directly through Math, StrictMath and Calcite, before and after warm-up,
with the interpreter and with EXP/LOG intrinsics disabled. This rules out the
fold journal's encoding as their cause on the measured ARM64 JDK. Pinned Rust
`libm` 0.2.15 reproduces both Java results. A separate two-kernel candidate
matches all 16,429 comparable values in the full Java journal.

The explicit experimental profile `legacy-exp-log-libm-0.2.15-v1` uses those
kernels for EXP and two-argument LOG. It does not change other operators or the
default runtime profile. Worker responses identify their profile, and the
adapter and offline comparison refuse a missing or mismatched identity.
The audit records Java comparison and runtime-edge profiles separately: passing
both does not assert that the two math profiles are equal. This provides a
candidate for preserving legacy folds, not approval for production use or proof
of Java equality across other JVMs and targets.

Runtime F32/F64 also normalize negative zero to positive zero and every NaN to
one NaN representation at construction. Omitting that boundary changed both
values and error behavior: for example, a raw negative zero triggered the
runtime logarithm's negative-sign check in the prototype, whereas the runtime
had already normalized it. The corrected prototype normalizes numeric operation
inputs and outputs, while retaining original bits in literal wire records for
comparison. It passes 370 actual-runtime vectors; the original 13 normalization
failures remain in [the evidence](../evidence/fold-float-normalization-failures.json).

The stronger warning-policy audit found differences hidden by an unrelated
unused-table warning. The explicit `legacy-janino-v1` profile extends the legacy
math profile with measured parser and domain rules: Java rejects lowercase
`nan`, the `inf` alias, unpadded timestamp fields, and nonpositive arguments to
two-argument LOG. Its SPLIT returns an empty array for empty input. The runtime
profile remains unchanged. Parser checks execute only on evaluated casts, so
unreachable CASE branches remain lazy. Direct JDK/Calcite parser probes and
compiler comparisons establish this bounded envelope; they do not establish
complete Java parsing equivalence.

Literal conversion is also part of Janino's atomic reduction boundary. In the
pinned Calcite, converting an empty list to a Rex array fails because element
type inference requires an operand. The Rust adapter retains that batch failure
when converting a computed value. Protocol decoding stays outside this boundary
so a malformed worker response still fails the test.

Mixed-type maps expose another ordering constraint. Janino concatenates maps
using Java key identity before coercing keys to the result SQL type. Integer
`1` and decimal `1.0` can therefore produce two equal decimal keys in a Rex map
constructor. The legacy profile retains source key types during concatenation
and converts the result afterward. The adapter preserves the entry sequence;
rebuilding a map after coercion would discard an entry. Runtime-profile maps
retain their separate, uniformly typed behavior.

The finite-point prototype identifies Calcite's spatial built-ins by their
backing methods. A user function named ST_POINT is refused. Runtime distance
uses `hypot`; the legacy profile uses JTS's squared-distance square root. The
625 runtime vectors cover NULLs and very small/large finite coordinates. They
do not establish support for nonfinite coordinates or other geometry types.

RexOptimize eligibility also matters independently of evaluation. It admits
only canonical temporal cast strings and refuses timestamp results with more
fractional digits than the declared precision. These checks run before literal
conversion could round away the difference. OFFSET and ORDINAL subscript forms
remain explicit refusals; sharing SqlKind.ITEM does not give them ITEM's index
base or out-of-bounds behavior.

## Folding stage is observable

The interval-cast extension adds 3,532 exact Java value matches and passes 483
runtime vectors, but its first full compilation run introduced four additional
constant-view warnings. The value kernel was correct. The test adapter had
called it from `RexOptimize`, whose Java implementation does not fold interval
casts. That changed the graph before the normal executor could see it.

The repair limits that pass to its declared interval operation kinds. Casts
still run through the Rust executor at the normal boundary; no Java evaluation
fallback is used. The four recorded regressions, 584 focused compilations and
483 runtime vectors pass after this repair. The first interval-focused cases
were all rejected by the public interval-output restriction; the expanded set
also exercises unrestricted interval outputs under every warning policy.
The full rerun preserves all verdicts and schemas, returning diagnostic
differences to 268; the six math-bit differences remain unresolved. This is why matching individual
fold values cannot establish warning, verdict, graph or persistent-ID parity.

## Compatibility identity

The eventual handshake must identify the chosen **fold behavior**, including
legacy Java folds where they differ from runtime evaluation. It must also bind
the registry, value representation, overflow and cast rules, temporal precision,
collation, and supported runtime functions. A compiler commit or runtime release
string is provenance, not proof that these semantics match.

Platform-dependent math needs either shared deterministic implementations or an
explicitly supported target envelope with measured equality. Merely including a
target triple in a hash detects a difference; it does not make two platforms
compatible. Windows/wasm rlib builds establish compilation portability only.

Negotiation must finish before compilation. A missing or incompatible identity
must not select a default registry or silently disable folds. A refusal can alter
constant-view diagnostics, warning promotion, circuit shape and persistent IDs
even when the value could be evaluated at runtime. Constant-required sites need
the stronger compatibility rule specified by the plan. Ordinary dynamic
functions and user functions remain outside the pure registry.

The production selector still uses Java. No compatibility identity, supported
version matrix, platform equivalence, or change to legacy behavior is approved
by these experiments. The owner must choose how to preserve or migrate the
observed Java/runtime differences before a Rust folding path becomes production.

## Shared implementation boundary

The adapter that walks FRex and manages evaluation budgets belongs in the
compiler. The pure value operations belong in a library shared with sqllib.
Neither layer needs DBSP's execution engine. Sqllib retains engine integration
and calls the same value kernels used by the folder.

| Area | Extraction work and required proof |
|---|---|
| Decimal | `feldera-fxp` already builds without engine dependencies. Preserve precision/scale checks and conversion mode; verify all supported scales and overflow boundaries. |
| Floating point | Move or share F32/F64 canonicalization with the kernels. Keep DBSP-specific trait implementations in the engine. Check arithmetic, casts, NaNs, zeros, math functions, and target-dependent results. |
| Strings and binary | Separate value operations from engine adapters, serialization and storage ownership. Preserve Unicode units, collation, allocation limits and error behavior. Resolve the measured Java UTF-16/runtime scalar-indexing differences. |
| Temporal and intervals | Share parsing, ranges, leap-second handling, precision, calendar arithmetic and epoch conventions. Preserve DATE/TIME/TIMESTAMP/TIMESTAMP_TZ distinctions and interval qualifiers. |
| Collections and VARIANT | Share element equality/order and nested NULL behavior. Avoid converting a NULL array into an empty array or confusing NULL elements with missing elements. Preserve the selected VARIANT representation. |
| Errors | Pure operations return typed errors. Engine wrappers can retain the current panic policy where required. The folder must distinguish an error, an unsupported operation and a budget refusal, and preserve lazy branches. |

Do not move engine traits into a kernel crate or introduce a dependency cycle to
preserve existing paths. Re-exports can preserve callers, but moving a type does
not by itself prove ABI, archive, checkpoint, or persistent-ID compatibility.
Those remain separate measurements. Existing Java annotations and diagnostic
origins also remain compiler concerns, not kernel state.

The extraction is incomplete: the prototype still adapts string, temporal and
floating operations and depends on platform math. [SPLIT.md](SPLIT.md) now gives
a pinned source inventory, the measured decimal slice, boundary risks and a
provisional 25–44 engineer-day estimate. It is not measured human port time or
an approved rebaseline. Named review capacity and independent acceptance remain
open. Passing the focused runtime sample does not substitute for that work.
