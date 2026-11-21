# Python runtime-negative diagnostic review

All **61 runtime-negative cases match their original expected error text** in
the devcontainer with eight workers. Every matched failure occurs during a
table transaction after real JSON record decoding; none is a constructor or
decoder error. All runtimes clean up successfully. The
[evidence certificate](evidence/python-runtime-diagnostics.json) retains the
exact population, results and archive hashes. These are local generated-circuit
results, not executions of the platform Python suite.

The compile-only diagnostic experiment leaves **61 successfully compiled cases**
whose expected errors require execution. They remain part of the P0 diagnostic
contract. Fifty-six exercise integer arithmetic or aggregate conversion errors;
five exercise string-to-decimal conversion, REAL-to-decimal range checking,
BINARY-to-UUID conversion, array element conversion, or UNION coercion to UUID.
None of their expected messages names a platform transport or deployment error.

There is a separate execution-status distinction: these negative views are
registered by their suite imports and class prefixes, but
[`TstAccumulator.run_tests`](../../python/tests/runtime_aggtest/aggtst_base.py)
currently comments out `run_expected_failures`. Thus the existing extractor's
`active` flag means registered, not executed by the normal Python suite. This
review does not enable that path or treat dormancy as acceptance. Any local run
is an explicitly independent experiment.

## Exact fixture extraction

[`python_runtime_inputs.py`](scripts/python_runtime_inputs.py) joins the pinned
diagnostic inputs and compile results by exact identity, verifies source hashes,
and extracts the literal table data without importing Python test modules. It
checks that the SQL is exactly the registered table definitions followed by the
negative view. Constructors containing calls, conditional assignments, mutations,
extra behavior, or non-JSON values are rejected.

The 61 cases share eight table fixtures:

| Fixture class | Records | Purpose |
| --- | ---: | --- |
| `illarg_str_tbl` | 5 | Registered Unicode strings, including unused data |
| `illarg_connector_metadata_legal_tbl` | 0 | Registered metadata-default table |
| `illarg_default_legal` | 0 | Registered default-value table |
| `illarg_tbl` | 3 | Mixed scalar, temporal, binary, UUID, array, map and nested-row values |
| `neg_numeric_tbl` | 2 | Signed arithmetic operands |
| `neg_numeric_tbl1` | 2 | Signed aggregate operands |
| `neg_numeric_unsigned_tbl` | 1 | Unsigned arithmetic operands |
| `neg_numeric_unsigned_tbl1` | 2 | Unsigned aggregate operands |

Per-case table sequences retain registration order. Integer values remain exact,
including unsigned values above signed BIGINT's maximum. Floating JSON numbers,
NULLs, arrays, maps, field names and Unicode strings retain their Python values.
The derived JSON insert envelopes implement `Table.add_insert`; their bytes are
an auditable fixture, not a capture of a live HTTP request.

The platform harness submits each table's JSON inserts and waits for completion
before proceeding to the next table, including empty-table completion checks.
It compares a trimmed, case-insensitive expected substring against the exception.
These behaviors matter: replacing all registered data with a hand-picked row,
dropping unused tables, using SQL INSERT coercion instead of JSON decoding, or
combining all tables into one transaction changes the experiment.

Four extraction tests pass in the devcontainer, covering exact large integers,
Unicode and NULLs, rejected dynamic constructors/non-JSON data, pinned source
drift, table sequencing, and the dormant normal-suite state.

## Local execution route

The [standalone Rust driver](scripts/python_runtime_runner.rs) is copied beside
an ordinary generated catalog circuit in a disposable crate. The compiler uses
`-i -O 2 --alltables --ignoreOrder`, matching the material pipeline compiler flags
in [`sql_compiler.rs`](../pipeline-manager/src/compiler/sql_compiler.rs).
`--alltables` is essential: the original compile-only diagnostic options did not
include it, and the first proof generation dropped three registered input tables.
That generation is retained separately; the corrected proof includes every input.

The driver uses the catalog's `configure_deserializer(Json(Default))` and
`insert` APIs from [the real record decoder](../static-connectors/src/deinput.rs),
flushes one table at a time, and calls `DBSPHandle::transaction()` for completion.
It preserves raw JSON number text while reading fixture records. It also runs an
initial empty transaction and collects output deltas, so early failures or errors
written to `error_view` are observable. Worker errors, record-decoding errors,
flush errors, successful completion and cleanup failures have separate stages.
The driver never reads or prints the expected error string; comparison belongs
to the supervising experiment.

This route executes generated Rust and real record decoding without requiring a
remote platform. It does not execute HTTP ingress, the insert/delete envelope
parser, pipeline provisioning, restart detection or deployment-error enrichment.
An error from decoding or construction must not be counted as a matching circuit
runtime error merely because its text contains the expected fragment. Likewise,
an `error_view` row without an exception does not meet this Python exception
contract. These distinctions remain in the recorded results.

`SqlIoTest.qf` is useful as a reference for expected-panic tests, but is not a
drop-in execution path: its defaults are nonincremental, it prepares typed inputs
through SQL, and it wraps a query in a new view. The catalog route preserves the
actual registered SQL and JSON decoder semantics.

The one-case proof reproduced `neg_add_tiny_int`'s expected worker error with
the original registered records. An empty-input control completed all table
transactions without that error. Both runs cleaned up their runtime. That proof
used one worker; the Python default is eight workers on one host, so the full
population driver defaults to eight and records the count explicitly.

[`run_python_runtime_diagnostics.py`](scripts/run_python_runtime_diagnostics.py)
generates one independent circuit per case and combines their modules into a
single executable to share dependency compilation. Each case then runs in its
own process. The generated modules originally each declared a global allocator;
the combined build rejected those duplicate declarations. The packaging repair
retains each raw generated module and removes only duplicate `global_allocator`
and `malloc_conf` export attributes after the first module. Two failed packaging
builds remain retained. The repair does not change circuit functions; the final
build reuses the unchanged raw generated modules after verifying their hashes.

Four supervisor tests check that matching text from construction, decoding,
flushing, malformed output, another case, a failed process, or failed cleanup
cannot satisfy a circuit runtime expectation. They also check the allocator
packaging adjustment. Together with the four extraction tests, these pass in the
devcontainer.

The standalone experiment can be reproduced without platform access:

```sh
python3 crates/sql/scripts/python_runtime_inputs.py \
  PINNED-DIAGNOSTIC-INPUTS.json PINNED-COMPILE-RESULTS.jsonl RUNTIME-INPUTS.json
CARGO_HOME=$CARGO_HOME python3 crates/sql/scripts/run_python_runtime_diagnostics.py \
  RUNTIME-INPUTS.json NEW-OUTPUT-DIRECTORY \
  --classes FROZEN-JAVA-CLASSES --dependencies FROZEN-DEPENDENCIES.jar \
  --target CACHED-CARGO-TARGET --workers 8
```

Compilation is offline with two jobs and no debug information or incremental
cache for this disposable crate. Its isolated lockfile is copied from the
workspace and resolved offline for the new local driver package. Tracked
dependencies are not changed. Compiler commands, source modules, fixture records,
schemas, build logs, process outputs, and per-case assessments are retained.
