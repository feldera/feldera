# Type-derivation experiment

This disposable P0 crate compares two implementations of the 40 scalar
functions in `sql-type-probes`. `translated` composes operand-family coercion
and return-type rules; `rewritten` uses direct Rust control flow. They share
only the scalar representation and the list of function names. Neither reads
Java answers, SQL text, or the other implementation. `extras` separately
prototypes arithmetic, comparisons, global aggregates, CASE and set operations.

Inputs retain literal/column origin and boundary text in the JSONL protocol.
These inference strategies consume declared types only. The scalar output
operands now match the validated SQL boundary, including COALESCE's selective
coercion. `binding` is a shared encoder for each strategy's selected operator,
full type descriptors, casts around original arguments, and TRIM defaults.
It is not a second independent lowering implementation. Additional-form operands
remain proposals; they are not complete CASE/comparison cast trees. Runtime
values, diagnostic parity, and complete C9 remain separate.
The result comparison retains nullability, integer width, decimal precision
and scale, fixed/varying character type and length, timestamp precision, and
array element types. Unsupported operations are distinct from SQL rejections.
An arity outside the manifest's sampled signature is unsupported, even when
SQL accepts that signature (for example, one-argument COALESCE). Binary types
are generated coercion targets; binary inputs are outside this experiment.

## Source provenance

The reference is Apache Calcite commit
`4bf922bbbc1cbd570a2f201339a46a2d8f7a763b`, under Apache-2.0, with Feldera's
`SqlToRelCompiler.TYPE_SYSTEM` (decimal precision/scale 38, timestamp precision
6, ragged CHAR union types become VARCHAR). `LICENSE` and `NOTICE` retain
the upstream notices. The translated and decimal-formula files carry change
notices; the reference Java files and their hashes accompany the evidence.
Feldera's `CustomFunctions.CalciteFunctionClone` retains the upstream checker
and return inference while changing operator identity and optimization policy.
For example, ABS resolves to `CustomFunctions.AbsFunction`. Formula agreement
does not establish agreement on the selected operator or its folding behavior.

| Rust rule | Calcite source under `core/src/main/java/org/apache/calcite` |
|---|---|
| Function signatures and return-rule selection | `sql/fun/SqlStdOperatorTable.java`, `SqlLibraryOperators.java`; `sql/type/OperandTypes.java`, `ReturnTypes.java`, `SqlTypeTransforms.java` |
| `translated::implicit_cast` | `sql/validate/implicit/AbstractTypeCoercion.implicitCast`; `sql/type/SqlTypeFamily.getDefaultConcreteType`; `rel/type/RelDataTypeFactoryImpl.decimalOf` |
| Decimal FLOOR/CEIL | `sql/type/ReturnTypes.DECIMAL_SCALE0`; `sql/fun/SqlFloorFunction` |
| MOD precision and scale | `rel/type/RelDataTypeSystem.deriveDecimalModType`; `ReturnTypes.NULLABLE_MOD` |
| Common result types | `sql/type/SqlTypeFactoryImpl.leastRestrictiveSqlType`; `AbstractTypeCoercion.getWiderTypeForTwo` |
| NULLIF/COALESCE | `sql/fun/SqlNullifFunction`, `SqlCoalesceFunction`; `SqlTypeUtil.isComparable` and their operand checkers |
| Additional decimal arithmetic | `RelDataTypeSystem.deriveDecimalPlusType`, `deriveDecimalMultiplyType`, `deriveDecimalDivideType`; `TypeCoercionImpl.binaryArithmeticWithStrings` |
| Global aggregate results | `RelDataTypeSystemImpl.deriveSumType`, `deriveAvgAggType`; aggregate return rules in `ReturnTypes` |
| Feldera function clones | Repository `compiler/frontend/calciteCompiler/functions/CustomFunctions.java`, `CalciteFunctionClone` and `AbsFunction` |
| Validated cast placement | `SqlCoalesceFunction.inferReturnType`; `AbstractTypeCoercion.needToCast`/`castTo`; `TypeCoercionImpl.coalesceCoercion` |
| Binding descriptor and operator wire identity | `RelDataTypeSystemImpl.getDefaultPrecision`; repository `S1OperatorRegistry`/`S1BuiltinOperatorRegistry` |

The bounded scalar representation omits intervals, collations, arbitrary
timestamp precisions, structured inputs, overload scopes, and validator state.
Those omissions are outside this declared probe population, not permissions
for a production binder to discard them. Array results preserve their scalar
element descriptor; grouped aggregates need a different nullability context.

## Reproduction

Run the original generator and Java golden collector as documented in the
parent P0 evidence. Then, in the devcontainer:

```sh
cargo run --release --offline --manifest-path crates/sql/type-derivation-probe/Cargo.toml -- INPUT.jsonl OUTPUT.jsonl
python3 crates/sql/scripts/type_derivation_compare.py JAVA-GOLDEN.jsonl OUTPUT.jsonl COMPARISON-DIRECTORY
python3 crates/sql/scripts/compare_type_bindings.py JAVA-BINDINGS.jsonl OUTPUT.jsonl BINDING-COMPARISON --require-equal
```

Outputs must not exist. The adapter limits record size, argument count, IDs,
and total population. The comparator requires the full 84,224-record population
by default, rejects duplicate JSON keys and IDs, checks input order, and fails
on unknown result-type metadata or Java internal failures. Its report states
which projections were compared and keeps every mismatch.

The binding comparison uses the separate source-bound Java observer capture.
It checks full type metadata, including charset/collation and exact type text,
registry identities, added cast chains and argument positions. It retains the
original argument trees in the Java capture but does not claim to reimplement
literal parsing or source positions. Later SQL-to-Rel simplification and S2
runtime casts/function suffixes are outside this boundary.
