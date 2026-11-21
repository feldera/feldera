# Connector validation prototype

This disposable P0 crate tests whether a Rust compiler can preserve Java's
connector diagnostics without depending on the runtime's deserialization rules.
It covers Postgres input, Kafka input, Iceberg input, both Avro formats, and HTTP
output. It never opens a connection or executes a pipeline.

`decode.rs` handles field names, defaults, scalar coercions, enums, collections,
and first-error ordering. `validate.rs` handles cross-field constraints in Java's
warning order. `duration.rs` checks durations without DBSP dependencies.
`locations.rs` finds tokens in the original JSON and counts UTF-16 columns.
`source.rs` applies SQL source origins and warning policy, including the SQL
driver's terminal diagnostic after promoted warnings. These modules contain pure
functions; `main.rs` owns bounded JSONL input and file output.

The embedded Java field inventory supplies the six schemas, not expected
diagnostics. The Rust validator receives no oracle outputs. The Java probe
reflects all public fields of all 29 configuration classes and invokes production
validation. A separate runtime executable exports utoipa schemas. The driver
checks both inventories before comparing independently generated results. Changes
to either field set fail the check and require an explicit inventory update.

## Reproduction

Build Java test classes and its dependency jar with the repository's usual Maven
commands. Then, from the repository root:

```sh
cargo test --locked --manifest-path crates/sql/connector-probe/Cargo.toml
cargo clippy --locked --manifest-path crates/sql/connector-probe/Cargo.toml --all-targets -- -D warnings
cargo build --locked --manifest-path crates/sql/connector-probe/Cargo.toml
cargo build --locked -p feldera-types --example connector_schema
python3 crates/sql/scripts/run_connector_probe.py $SCRATCH/connector-results \
  --probe crates/sql/connector-probe/target/debug/feldera-connector-probe \
  --runtime-schema target/debug/examples/connector_schema \
  --classpath sql-to-dbsp-compiler/SQL-compiler/target/test-classes:sql-to-dbsp-compiler/SQL-compiler/target/classes:sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar
```

Adjust executable paths when using `CARGO_TARGET_DIR`. The output directory must
be new. Raw inputs, both outputs, logs, source/executable digests, and comparison
results remain available on failure. The Java heap, process duration, free disk,
and output size are bounded. The driver rejects missing or reordered cases,
duplicate identities, malformed diagnostics, and internal compiler failures.

Every generated configuration also runs through actual Java SQL compilation with
warnings enabled, suppressed, promoted, and suppressed plus promoted. The hard
comparison includes acceptance, warning flag, category, count, complete point
range, and SQL snippet. Message text differences are counted separately and still
need independent review; they are not an approved compatibility deviation.

## Existing differences from runtime types

These are recorded compatibility facts, not permission to broaden or narrow
validation silently:

| Configuration | Difference |
|---|---|
| Kafka | Java accepts arbitrary values in unknown flattened keys; runtime Kafka options are strings. Java lacks explicit `header_filter` and `oauth_provider` fields, so they take that flattened path. Java also accepts legacy `topics` and `group_join_timeout_secs`, which utoipa does not list. |
| Iceberg | Java accepts arbitrary flattened values. Runtime has `end_snapshot_id`, `max_retries`, `num_parsers`, and `transaction_mode`; Java treats them as flattened options. |
| Avro input | Runtime accepts `cdc_field`; Java rejects it as unknown. Both Java Avro classes recognize the legacy `registry_timeout_secs` alias, absent from utoipa. |
| HTTP output | Java warns on unknown fields; runtime serde ignores them. |
| Scalar fields | Jackson accepts some numbers and booleans as strings, numeric enum ordinals, truncated floats as integers, and null primitives. Runtime serde differs. |

The prototype retains those Java decisions. It also retains current location
quirks: missing required Postgres fields have an invalid range, and Jackson error
paths with unescaped slash/tilde map keys fall back to the config value. SQL
locations follow Java's offset into the decoded literal; correcting locations
after doubled SQL quotes is a separate contract change.

Three Java null-pointer defects were repaired before recording passing evidence:
required Postgres strings now warn on null, null Kafka topics follow the existing
legacy-topic choice, and null Avro input registry URLs produce a located warning.
The last repair also warns without an explicit schema, consistent with the runtime
rejecting a null list. The original failing inputs and outputs are retained.

This is a six-class experiment, not the production `sql-catalog` implementation.
It does not prove the other validators, selected-runtime contracts, connector
execution, rollout compatibility, or P0's other technical and review gates.
