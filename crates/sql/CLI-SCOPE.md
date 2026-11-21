# Compiler CLI scope

Decision 8 retains product callers and documented standalone use. The current
Java compiler declares 47 named options with 54 spellings. The count includes
seven hidden S2–S4 options added during P0. The input SQL file is a required
positional argument. [Compiler declarations](../../sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/CompilerOptions.java)

| Scope | Options | Evidence |
|---|---|---|
| Pipeline-manager | `-i`, `-o`, `-je` / `--je`, `-js` / `--js`, `--alltables`, `--ignoreOrder`, `--dataflow`, `--gen2`, `--noRust`, `--enterprise`, `--runtime`, `--crates` | The compiler command has distinct Gen-2, classic-shadow and classic multi-crate branches. Felderize also uses `-i`, `--ignoreOrder`, `--alltables` and `--noRust`. [Pipeline-manager](../pipeline-manager/src/compiler/sql_compiler.rs#L868) · [Felderize](../../python/felderize/felderize/feldera_client.py#L26) |
| Standalone compilation and output | `--anonymize`, `--format`, `--errors`, `--handles`, `--jit`, `--plan`, `--correlatedColumns`, `--jpg` / `-jpg`, `--png` / `-png`, `--svg` / `-svg`, `-h` / `--help` / `-?`, `--version` | The user guide lists these commands. It explains typed handles and the column-correlation report, and shows how to compile a single Rust source file. [Standalone compiler guide](../../sql-to-dbsp-compiler/using.md#command-line-options) |
| Standalone language and logging controls | `-O`, `-T`, `-q`, `-v`, `--outputsAreSets`, `--streaming`, `--lenient`, `--no-restrict-io`, `--unaryPlusNoop`, `--trimInputs` | The public option reference documents their effects. The output and parsing contracts stay in scope. [Standalone compiler guide](../../sql-to-dbsp-compiler/using.md#command-line-options) |
| Compiler test and replay | `--cteViews`, `--sqlnames`, `--raw`, `--input_circuit`, `--skip_calcite_optimization`, `--testing`, `--dump-circuits`, `--dump-producer-commit`, `--dump-runtime-abi`, `--read-s2`, `--write-s2`, `--read-s3`, `--write-s3` | These declarations are hidden. Preserve them for the active differential harness and migration seams. [Seam contracts](S2.md) · [Declarations](../../sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/CompilerOptions.java#L224) |

The standalone path keeps documented classic artifact forms available during
the migration. The implementation also keeps the backend-specific resolution
in decisions 2 and 5. The three image flags use the DOT backend through
`CompilerMain`. DOT has no separate named compiler option.
[Artifact selection](../../PLAN.md#751-compiler-identity-and-selection) ·
[Compiler output dispatch](../../sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/CompilerMain.java#L260)

`FELDERA_FLAT_VARIANT=OFF` is a documented checkpoint compatibility path.
`GEOMETRY` is a documented SQL type, and the spatial operator table supplies
functions used in compiler regression programs. Keep the current tested
spatial surface and its rejection behavior.
[VARIANT migration](../../docs.feldera.com/docs/sql/grammar.md#experimental-options) ·
[SQL types](../../docs.feldera.com/docs/sql/types.md) ·
[Spatial registry](../../sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/frontend/calciteCompiler/functions/CalciteFunctions.java#L774)
