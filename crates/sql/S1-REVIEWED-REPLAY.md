# Reviewed S1 replay after the source audit

The reviewed compiler snapshot passes detached S1 replay across the complete
classic and Gen-2 exported populations after exactly one source-driven S1 input
replacement per mode. The [certificate](evidence/s1-reviewed-prefix.json)
contains the final counts; the [archive index](evidence/s1-reviewed-prefix-archive.json)
pins every input, source snapshot, executable, command, original failure,
corrected replay, comparison, and independent adjudication result.

This result uses the existing `full-prefix-v1`, `seam-v1`, and
`all-engines-v1` comparison contracts. The comparator is byte-identical to the
one in the prior S1 evidence. Schema, diagnostics, canonical S2/S3/S4
observations, hash inputs, generated Rust, and canonical Gen-2 outputs keep
their established comparisons. Raw Gen-2 output bytes remain a separately
reported metric under the original `seam-v1` contract. No tolerance was added
for this review.

## Two retained failures and two provenance-bound replacements

The original reviewed-build run against the old S1 inputs finishes all ten
shards and retains two mismatch results. Both concern
`RegressionTests#issue3326/0`: classic shard 5 and Gen-2 shard 1. The new
character-index ROW selector branch changes the intermediate expression for
`scopeSpan.scope['name']`. An old typed S1 program and a fresh SQL compilation
therefore no longer start from the same expression graph.

The controlled rollback removes only that branch from the reviewed
`ConvertletTable.java`, compiles it into an overlay, and keeps the remaining
reviewed classes. Its canonical classic S2 returns to the old result and differs
from the reviewed result. The archive preserves both sources, the exact removed
text, bytecode, commands, and canonical outputs; the adjudicator independently
checks the source transformation and equality evidence. This is a classic
single-case causal check, not a claim of a separate Gen-2 rollback run.

The corrected case is exported by the frozen reviewed Java compiler, transported
through Rust, and replayed independently in each mode. Java and Rust wire files
have equal JSON values and array order; their root object key order differs.
Both original byte streams are retained. The single-case comparisons cover 440 classic and 446 Gen-2
observations. Each affected full shard then substitutes exactly that one program
record and preserves every other input byte and the original order. The
independent adjudicator verifies the replacement file hashes, the original
parent-certificate hashes, the changed identity, case counts, and the resulting
full-shard comparisons. Only those two import results are selected for the final
replay result; the old failed results remain in the archive.

## Old production is a separate comparison

A passing replay means reviewed SQL compilation and reviewed detached-S1
compilation agree. It does not erase differences between the reviewed compiler
and the older production snapshot.

For the ROW-selector case, classic final S3, hash-input streams, and generated
Rust remain equal; the S2/S4 observation fingerprints change. In Gen-2, final
S3, hash-input streams, and canonical `circuit.json` remain equal, but S2/S4 and
canonical `dataflow.json` change. Raw Gen-2 byte differences are listed
individually, including cases whose canonical output stays equal. The full
per-field production difference report preserves those fingerprints without
calling raw-byte drift canonical equality or calling dataflow changes absent.

## Source cutoff and limits

The certificate is tied to the frozen reviewed sample's Java sources and class
trees, its dependency jar, and its Rust comparator executable. It does not
certify the later working tree. In particular, the calendar RANGE diagnostic
repair, optimizer/correlation observer hooks, and quoted-parameter decoder repair
are outside this snapshot and need their own evidence.

The replay traverses the complete Java optimizer prefix from detached S1 and
compares its observations and backend artifacts. It does not execute generated
runtime programs, certify a Rust binder/decorrelator, or establish resumability
from an arbitrary isolated S4 checkpoint. Independent review and licensing or
estimate acceptance remain separate phase obligations.

Measured populations:

| Mode | Cases | Statements | Prefix observations |
|---|---:|---:|---:|
| classic | 8,571 | 127,564 | 3,503,220 |
| gen2 | 8,527 | 127,296 | 3,527,714 |
