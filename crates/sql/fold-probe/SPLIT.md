# Price of the shared-runtime extraction

The provisional remaining estimate is **25–44 engineer-days**, or **1.25–2.2
engineer-months at 20 working days per month**. This is engineering judgment,
not measured human port time or an approved replacement for PLAN.md row 13's
1–2 months. It includes implementation, tests, review and repair within each
work package. Independent architectural acceptance and owner availability are
not established by this estimate.

The [source inventory](../evidence/fold-split-inventory.json) pins 25 sqllib Rust
files containing 32,346 physical lines, including tests. Textual engine/type
references identify inspection points; their absence does not prove that a
module is independent of DBSP. Macros, re-exports and trait bounds also couple
the modules. Line counts do not determine the estimate.

## Measured extraction slice

The folder consumes `feldera-fxp` with default features disabled. Decimal
arithmetic and parsing build without DBSP; engine traits, archive support,
serialization and random generation have separate features. Sqllib explicitly
enables the `dbsp` feature. This preserves the runtime dependency direction
while letting the folder use arithmetic without linking the engine.

The folder's Windows MSVC and wasm rlibs build, and the selected runtime
operations pass 5,240 differential vectors. These results establish a usable
boundary for the decimal slice and a compiler prototype. They do not establish
Windows/wasm runtime equivalence, final linking, or extraction of the remaining
value types. The 3,038-line folder still contains adapters and compatibility
implementations rather than a complete set of shared runtime kernels.

## Remaining work packages

| Package | Concrete change and completion evidence | Engineer-days |
|---|---|---|
| Value and error contracts | Define the pure value API and typed failures; keep compiler expression traversal and evaluation budgets outside it. Audit Decimal feature combinations and caller behavior. No engine dependency in the pure crate's resolved graph. | 3–5 |
| Strings and binary | Separate `SqlString`/`ByteArray` value operations from DBSP ordering/archive/context adapters. Preserve UTF-8 operations, bounds, allocation policy and serialization behavior. Test existing runtime functions against shared kernels. | 4–7 |
| Temporal values and intervals | Separate calendar arithmetic, parsing, precision and epoch operations from engine traits and context serialization. Cover all temporal flavors, leap handling and qualifier boundaries. | 4–7 |
| Floating point and spatial values | Separate F32/F64 representation and canonicalization from DBSP traits. Preserve signed zero, NaNs and finite-point behavior; retain explicit runtime versus legacy Java math profiles. | 3–5 |
| Collections and VARIANT | Separate element operations, nested NULLs, ordering and conversion from semigroup/weight integration. Preserve both supported VARIANT representations and archive adapters. | 4–7 |
| Casts and registry integration | Route conversions through shared operations, preserve lazy evaluation and runtime error policy, and bind resolved overloads rather than names. | 2–4 |
| Portability and differential checks | Build and link the intended target envelope; run target-specific vectors where execution is available. Extend coverage beyond today's unsupported/refused observations and keep refusals separate from successful equality. | 3–5 |
| Runtime integration and compatibility checks | Re-export stable caller paths, test feature unification, run runtime regressions, and compare serialization/hash evidence before accepting the move. | 2–4 |
| **Total** | | **25–44** |

Moving a foreign trait implementation can violate Rust's orphan rules. Place
engine traits in the crate that owns those traits where possible, or retain an
engine wrapper around the pure value. Do not solve that problem by making the
pure crate depend on DBSP. `feldera-types` context serialization and generated
`OrdRepr` implementations need the same boundary audit.

The estimate excludes a new SQL parser/binder, general S1 replay, a new native
row/archive format, production version negotiation, and a change in SQL
semantics. Those belong to separate plan rows or phase gates. Sharing kernels
does not authorize changing Java's historical fold behavior. The legacy Janino
profile remains necessary wherever its folds affect existing verdicts and IDs.

## Decision still needed

Review the packages against the pinned sources and choose the supported target
envelope. A reviewer should also decide whether preserving both VARIANT
representations belongs in the first extraction. The current Spike 7 records
the build result and dependency boundary.
