# DataFusion binder experiments

This isolated crate uses the workspace's DataFusion SQL planner release 55.2.0.
It does not add dependencies to the workspace's production compiler. Build it
with `cargo build --locked --manifest-path crates/sql/datafusion-binder-probe/Cargo.toml`.

## Generated scalar calls

`--signature-manifest INPUT.jsonl OUTPUT.jsonl` parses each generated table/view
program, builds its own table schema and plans the view through DataFusion
`SqlToRel`. The catalog registers the forty sampled function signatures. The
functions call the shared Feldera type rules and refuse execution. The
`TypePlanner` retains fixed or varying character width on Arrow fields.
`--signature-functions SQL` is the single-query form for exploring the planner.

The generated population contains 42,880 probes. H agrees on all 28,960 accepted
names and result types and all 13,920 rejection verdicts. The projection span is
retained in an AST record. The comparison does not check diagnostic text,
nested positions, argument coercion, or downstream FRel and S2.

## Set operations

The default command prints the stock DataFusion logical plan. On
`INTERSECT ALL` or `EXCEPT ALL`, it shows a membership join. With
`--preserve-set-operations SQL`, the prototype selects the AST operator and
quantifier before calling DataFusion on each leaf. It constructs a typed
INTEGER relation tree with explicit Intersect and Minus nodes and evaluates
its bags. It handles nulls, duplicates, nesting and empty constant leaves.

`set_fixtures.py BINARY OUTPUT.json` compares the prototype with 16 independently
specified bags and writes the inputs for `DataFusionSetOperationTests`.
The Java test constructs relations from those Rust outputs, writes the S1
relation payload, imports it, and generates actual Rust runtime tests.
The Java parser also runs on the original SQL to supply the view metadata and
an independent baseline. The imported candidate's entire relation tree
comes from the Rust output. Both paths assert the same expected bags.
See [the D1 report](../D1.md) for scope and remaining work.
