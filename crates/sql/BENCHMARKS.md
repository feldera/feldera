# Compiler boundary measurements

These experiments compare direct Java compilation with compiler stages split
across Java and Rust. P3 requires hybrid p95 latency of at most 1.2× direct
Java p95 on representative workloads.

## Compact snapshot strings, 2026-11-22

On `29f71e8ea`, all three workloads met the 1.2× ceiling. The ARM64
devcontainer ran two warmup and 21 measured pairs per workload in fresh
JVMs on CPU 15 with no concurrent verification, alternating which
compiler ran first. All 69 pairs matched through classic artifact emission.
Timing ends before Cargo builds the generated pipeline.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 1.408 | 1.102 | 0.783× |
| Nexmark suite | 3.171 | 3.586 | 1.131× |
| Outer join | 1.321 | 1.043 | 0.790× |

Three Nexmark JFR captures identified string escaping in the optimizer
snapshot writer. In ten alternating pairs, median optimizer CPU time
fell from 1,650.597 ms to 1,552.712 ms when the writer copied ordinary
strings directly. Jackson still escapes quotes, backslashes and control
characters.

[Paired timings and build identities](evidence/p3-compact-snapshot-strings-cold.json) · [Optimizer CPU measurements](evidence/p3-compact-snapshot-strings.json)

## Unchanged simplifications, 2026-11-22

On `a98a0ce96`, the ARM64 devcontainer ran two warmup and 21 measured
pairs per workload in fresh JVMs on CPU 15, alternating which compiler
ran first. All 69 pairs matched through classic artifact emission. Timing
ends before Cargo builds the pipeline. Nexmark exceeded the 1.2× ceiling.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 1.430 | 1.121 | 0.784× |
| Nexmark suite | 3.164 | 3.866 | 1.222× |
| Outer join | 1.367 | 1.072 | 0.784× |

[Paired timings and build identities](evidence/p3-simplify-unchanged-roots-cold.json)

## Deep optimizer expressions, 2026-11-22

On `955f165ae`, Nexmark exceeded the 1.2× ceiling. The ARM64 devcontainer
ran two warmup and 21 measured pairs per workload on CPU 15, alternating
which compiler ran first. All 69 pairs matched through classic artifact
emission. Timing ends before Cargo builds the generated pipeline.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 1.421 | 1.115 | 0.785× |
| Nexmark suite | 3.162 | 3.843 | 1.215× |
| Outer join | 1.327 | 1.037 | 0.781× |

[Paired timings and build identities](evidence/p3-lowering-expression-batches-cold.json)

## Declared column waterlines, 2026-11-22

On `432124575`, all three workloads met the 1.2× ceiling. The devcontainer
ran 69 fresh-process pairs on CPU 15 with the same isolation and sample
counts as the aggregate field-use build. All artifacts matched through
classic source emission.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 1.425 | 1.110 | 0.779× |
| Nexmark suite | 3.160 | 3.728 | 1.180× |
| Outer join | 1.308 | 1.029 | 0.786× |

[Paired timings and build identities](evidence/p3-declared-column-waterlines-cold.json)

## Aggregate field uses, 2026-11-22

On `5c6c0cd5b`, the hybrid compiler met the 1.2× direct Java p95 ceiling
on all three workloads. The ARM64 devcontainer ran each pair on CPU 15,
with two warmups and 21 measured pairs per workload. All 69 pairs produced
matching compiler artifacts. Timing ends when the compiler emits classic
source, before the pipeline's Cargo build.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 1.401 | 1.104 | 0.788× |
| Nexmark suite | 3.174 | 3.761 | 1.185× |
| Outer join | 1.331 | 1.026 | 0.770× |

[Paired timings and build identities](evidence/p3-aggregate-field-uses-cold.json)

## Projected candidate keys, 2026-11-22

On `63dbe7a9f`, all 69 cold SQL-to-classic-artifact pairs matched. Each
workload ran two warmup pairs and 21 measured pairs on CPU 15 of the ARM64 VM.
Key analysis comparisons, corpus replay and runtime tests shared the VM.
Nexmark exceeded the 1.2× p95 ceiling on this run.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 2.841 | 3.136 | 1.104× |
| Nexmark suite | 5.967 | 8.205 | 1.375× |
| Outer join | 2.777 | 2.930 | 1.055× |

[Paired timings and build identities](evidence/p3-projected-candidate-keys-cold.json)

## Compound window expressions, 2026-11-22

On `9c0cd1ed9`, all 69 cold SQL-to-classic-artifact pairs produced matching
output. Each workload ran two warmup pairs and 21 measured pairs on CPU 15
of the ARM64 VM while corpus verification ran on the same machine. Timing
ends when the compiler emits the artifacts, before Cargo builds the pipeline.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 2.439 | 1.962 | 0.804× |
| Nexmark suite | 5.516 | 6.551 | 1.187× |
| Outer join | 2.311 | 1.915 | 0.829× |

[Paired timings and build identities](evidence/p3-interval-window-projections-cold.json)

## Shared window indexes, 2026-11-22

On `59f09c271`, each workload ran two warmup pairs and 21 measured cold
pairs on CPU 15 of the shared ARM64 verification VM. All 69 pairs produced
matching classic artifacts. Timing ends at source emission, before Cargo
builds the pipeline. Corpus replay and decoder verification ran concurrently.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 2.611 | 2.262 | 0.866× |
| Nexmark suite | 5.523 | 6.135 | 1.111× |
| Outer join | 2.650 | 2.228 | 0.841× |

[Paired timings and build identities](evidence/p3-indexed-row-window-keys-cold.json)

## Nested empty rows, 2026-11-22

On `bf609aab1`, all 69 paired SQL compilations produced matching classic
artifacts. Each workload ran two warmup pairs and 21 measured pairs on CPU 15.
Corpus replay, runtime tests and decoder verification shared the ARM64 VM.
The timings end at artifact emission, before Cargo compilation.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 5.184 | 4.271 | 0.824× |
| Nexmark suite | 11.418 | 13.412 | 1.175× |
| Outer join | 5.557 | 4.211 | 0.758× |

[Paired timings and build identities](evidence/p3-empty-row-clock-inputs-cold.json)

## Fixed-point scheduling, 2026-11-22

On `a291991c6`, the hybrid compiler ran repeated optimizer passes under Rust
control. Both paths used the same packaged jar and CPU 15 on the shared Linux
ARM64 verification VM. Each workload ran two warmup pairs and 21 measured
pairs. All 69 pairs produced matching classic artifacts. Timing ends before Cargo.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 2.485 | 2.117 | 0.852× |
| Nexmark suite | 5.929 | 7.008 | 1.182× |
| Outer join | 2.315 | 1.856 | 0.802× |

[Paired timings and build identities](evidence/p3-repeat-scheduler-cold.json)

## Positive column lineage, 2026-11-22

On `61105fd2c`, all 69 cold pairs matched through classic artifact emission.
Both paths used the packaged compiler jar. CPU 15 ran the benchmark while
other verification ran on the same Linux ARM64 VM. Each workload had two
warmup pairs and 21 measured pairs. The timings end before Cargo compilation.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 2.305 | 1.929 | 0.837× |
| Nexmark suite | 5.160 | 6.065 | 1.175× |
| Outer join | 2.171 | 1.793 | 0.826× |

[Compilation timings and build identities](evidence/p3-positive-lineage-cold.json)

## Shared row types, 2026-11-22

On `19f82bffc`, CPU 15 ran the cold compiler benchmark in its own container.
Concurrent verification used CPUs 0–14 on the same VM. Each workload had two
warmup pairs and 21 measured pairs. All 69 pairs matched through artifact publication.

| Workload | Direct Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 4.370 | 3.538 | 0.810× |
| Nexmark suite | 9.974 | 11.529 | 1.156× |
| Outer join | 4.214 | 3.278 | 0.778× |

[Compilation timings and build identities](evidence/p3-shared-row-cold.json)

## Expression value numbering, 2026-11-22

Each cell is hybrid p95 divided by direct Java p95 for 21 cold pairs on CPU 15.
The two implementations ran separately while corpus verification shared the VM.
All 69 pairs per build matched on diagnostics and classic artifacts, including
the two warmups per workload.

| Implementation | Array subquery | Nexmark | Outer join |
|---|---:|---:|---:|
| Intermediate build returning all value numbers | 0.823× | 1.155× | 0.710× |
| Shared uses only (`7d54ab029`) | 0.831× | 1.231× | 0.744× |

On the shared-use build, Nexmark took 9.736 s at p95 against 7.910 s for
direct Java, above the 1.2× ceiling. [Paired measurements](evidence/p3-value-numbering-cold.json)

## Comparator declarations, 2026-11-22

Batching the selected index functions reduced median calling-thread CPU time
on classic Nexmark from 82.18 ms to 5.72 ms for this pass. Nine alternating
pairs ran in fresh JVMs on CPU 13 during full corpus and runtime replay.
The medians use the final eight pairs.
[Measurements and replay receipts](evidence/p3-ordered-index-functions.json)

## Scalar row waterlines, 2026-11-22

On `b81436777`, all 69 cold pairs matched through classic source emission.
Each workload ran two warmup pairs and 21 measured pairs on CPU 15 with
the devcontainer replay idle. Direct Java used its deployed JVM flags and
the hybrid worker used tier 1. The timings end before Cargo compilation.

| Workload | Java p95 (s) | Hybrid p95 (s) | Hybrid / Java |
|---|---:|---:|---:|
| Correlated array subquery | 1.436 | 1.301 | 0.906× |
| Nexmark suite | 3.224 | 3.781 | 1.173× |
| Outer join | 1.352 | 1.028 | 0.760× |

[Cold timings and compiler identities](evidence/p3-scalar-row-cold.json)

## Shared trace edits, 2026-11-22

On classic Nexmark, sending the operator topology to Rust reduced median
calling-thread CPU time for `MergeNoops` from 57.98 ms to 7.32 ms. Nine
alternating pairs ran in fresh JVMs on CPU 13 during corpus replay. The
medians use the final eight pairs.
[Pass timings and verification](evidence/p3-shared-trace-edits.json)

## Single-request JVM, 2026-11-22

The classic hybrid worker now limits its one-request JVM to tier 1 of the
HotSpot compiler. Direct Java used the deployed JVM settings. On CPU 15, each
workload ran two warmup pairs and 21 measured pairs in alternating order.
All 69 pairs produced matching diagnostics, schema, dataflow and classic source.

| Workload | Java p95 (s) | Hybrid p95 (s) | p95 ratio |
|---|---:|---:|---:|
| Correlated array subquery | 1.437 | 1.097 | 0.764× |
| Nexmark suite | 3.177 | 3.893 | 1.225× |
| Outer join | 1.354 | 1.036 | 0.765× |

In a separate sensitivity check, both JVMs used tier 1. Median direct and
hybrid times over five measured pairs were 0.914 and 1.092 s for the array
subquery, 1.878 and 3.804 s for Nexmark, and 0.872 and 1.005 s for the outer
join. The shorter JVM startup accounts for much of the gain over the deployed
baseline. These measurements end at source emission and exclude Cargo.
[Paired results and executable identities](evidence/p3-single-request-jvm.json)

## Flat integral edits, 2026-11-22

On the integral-planner build, median main-thread CPU time for
Nexmark's two integral-pushing passes fell from 182.73 to 4.43 ms. Each
build ran in nine alternating fresh-JVM pairs on CPU 13 during corpus
verification. The medians exclude the first pair.
[Integral pass profiles](evidence/p3-flat-integral-edits.json)

## Fixed filter keys, 2026-11-22

On `4e8cc5856`, all 69 cold pairs matched through classic source emission.
Each workload ran two warmup pairs and 21 measured pairs on CPU 15. Twenty
cancellations of workers waiting for SQL measured 5.73 ms p95. Peak combined
driver and waiting JVM RSS was 72.4 MiB.

| Workload | Java p95 (s) | Hybrid p95 (s) | p95 ratio |
|---|---:|---:|---:|
| Correlated array subquery | 1.527 | 1.843 | 1.207× |
| Nexmark suite | 3.376 | 6.736 | 1.995× |
| Outer join | 1.427 | 1.728 | 1.211× |

[Cold compilation and worker measurements](evidence/p3-filter-key-latency.json)

## Join key build, 2026-11-22

On `c15e725ac`, all 69 cold pairs matched through classic source emission.
Each workload ran two warmup pairs and 21 measured pairs on CPU 15.
Twenty worker cancellations measured 5.08 ms p95, with a peak combined driver
and waiting JVM RSS of 72.3 MiB.

| Workload | Java p95 (s) | Hybrid p95 (s) | p95 ratio |
|---|---:|---:|---:|
| Correlated array subquery | 1.415 | 1.761 | 1.245× |
| Nexmark suite | 3.192 | 6.501 | 2.037× |
| Outer join | 1.344 | 1.600 | 1.190× |

[Cold compilation and cancellation measurements](evidence/p3-join-key-latency.json)

## Selected expression roots, 2026-11-22

Four optimizer passes now send Rust only the expression roots they may change.
On classic Nexmark, median main-thread CPU time fell from 40.46 to 11.62 ms for
`SimplifyConditionals`, 45.60 to 3.99 ms for `AdjustSqlIndex`, and 43.52 to
11.81 ms for `ExpandUnsafeCasts`. `ExpandSafeCasts` rose from 1.34 to 1.63 ms.
Each build ran in nine alternating fresh-JVM pairs on CPU 14 with concurrent
corpus replay. The medians use the last eight pairs.
[Pass profiles and replay digests](evidence/p3-selected-expression-roots.json)

## HOP expansion build, 2026-11-22

On `e177acab4`, each workload ran two warmup pairs and 21 measured pairs
in fresh processes on CPU 15. All 69 pairs produced equal diagnostics and
classic artifacts. Nexmark exceeded the 1.2× ceiling.

| Workload | Java p95 (s) | Hybrid p95 (s) | p95 ratio |
|---|---:|---:|---:|
| Correlated array subquery | 1.459 | 1.729 | 1.185× |
| Nexmark suite | 3.357 | 7.137 | 2.126× |
| Outer join | 1.352 | 1.596 | 1.180× |

[Paired timing results](evidence/p3-hop-window-fragments.json)

## Relation waterline build, 2026-11-22

After the frozen-corpus replay finished, all 69 cold pairs matched through
classic Rust source emission, schema, dataflow and diagnostics. Each workload
ran two warmup pairs and 21 measured pairs in alternating order on CPU 15.

| Workload | Java p95 (s) | Hybrid p95 (s) | p95 ratio |
|---|---:|---:|---:|
| Correlated array subquery | 1.443 | 1.759 | 1.219× |
| Nexmark suite | 3.176 | 7.605 | 2.394× |
| Outer join | 1.351 | 1.608 | 1.190× |

Four concurrent waiting workers took 5.77 ms at p95 to cancel in isolation,
below the approved 25 ms. The largest combined driver and JVM RSS was
72.3 MiB. [Timing and worker measurements](evidence/p3-relation-waterlines.json)

## Column initializer build, 2026-11-22

On `4f84a9cf6`, each workload ran two warmup pairs and 21 measured pairs
in alternating order on CPU 15. Each process compiled from SQL through
classic Rust source emission. All 69 direct/hybrid pairs produced equal
diagnostics and artifacts.

| Workload | Java p50 (s) | Hybrid p50 (s) | Java p95 (s) | Hybrid p95 (s) | p95 ratio |
|---|---:|---:|---:|---:|---:|
| Correlated array subquery | 1.399 | 1.715 | 1.437 | 1.743 | 1.213× |
| Nexmark suite | 3.128 | 6.975 | 3.146 | 7.062 | 2.245× |
| Outer join | 1.322 | 1.589 | 1.336 | 1.608 | 1.204× |

All three ratios exceeded 1.2×. A separate run on the same build measured
4.89 ms cancellation p95 across five rounds of four concurrent workers.
The largest combined driver and waiting JVM high-water RSS was 72.4 MiB.
[Timing and resource evidence](evidence/p3-metadata-latency.json)

The later clock-source build at `4bfcbf28b` ran the same 69 pairs while
frozen-corpus and runtime verification shared the VM. All diagnostics and
artifacts matched. Java/hybrid p95 was 3.793/4.516 s for the array subquery
(1.191×), 7.429/18.016 s for Nexmark (2.425×), and 3.072/3.733 s for the
outer join (1.215×). [Paired measurements](evidence/p3-clock-source-cold-latency.json)

On the window and clock build (`2c8dd594e`), all 69 cold pairs matched again.
Frozen-corpus verification shared the VM. Java/hybrid p95 was 3.520/3.757 s
for the array subquery (1.067×), 6.578/14.999 s for Nexmark (2.280×), and
2.970/3.997 s for the outer join (1.346×).
[Window and clock build timings](evidence/p3-window-clock-cold-latency.json)

## Packaged mixed optimizer, 2026-11-21

The opt-in compiler ran 106 Rust pass kinds. Each timing starts a fresh
process and ends when it publishes the classic Rust source, UDF stubs,
schema and dataflow. The hybrid starts one JVM and an isolated Rust worker.
The baseline runs direct Java from the same Maven JAR. Cargo compilation of
the generated Rust and pipeline startup fall outside these times.

| Workload | Java p50 (s) | Hybrid p50 (s) | Java p95 (s) | Hybrid p95 (s) | p95 ratio |
|---|---:|---:|---:|---:|---:|
| Correlated array subquery | 1.413 | 1.705 | 1.438 | 1.740 | 1.211× |
| Nexmark suite | 3.161 | 7.055 | 3.296 | 7.256 | 2.202× |
| Outer join | 1.327 | 1.561 | 1.353 | 1.595 | 1.179× |

The table measures the limiter projection and stream antijoin build at
`45ffbdba0` with no concurrent compiler verification. Each workload ran two
warmup pairs and 21 measured pairs on CPU 15. All 69 pairs matched on
diagnostics and emitted artifacts. Only the outer join met the 1.2× ceiling.
The preceding expression-reader build at `11fefc9af` measured 1.215×, 2.191×
and 1.325× in workload order. [Raw pair digests and results](evidence/p3-hybrid-latency.json)

A separate decoder benchmark
replayed the 554 projection requests captured from one Nexmark compile. Across
21 alternating pairs, median decode time for the full sequence was 53.035 ms
with the tree reader and 25.604 ms with the streaming reader. Both readers
produced identical root identities and expression graphs. [Projection benchmark](evidence/p3-expression-batch-reader.json)

A Nexmark compile sent 34 full circuit snapshots totaling 62.6 MB. Four
consecutive repeats accounted for 7.0 MB. Reusing the validated arena on
byte-identical input reduced median sequence decode time from 432.346 to
390.186 ms over 21 paired runs. All 34 graphs re-encoded identically. The
median paired reduction was 47.560 ms (95% bootstrap interval 35.363–54.279
ms). Peak RSS including buffered input rose from 105,548 to 107,572 KiB.
A compiler build and verification overlapped part of this microbenchmark.
[Decoder measurements and sample digests](evidence/p3-live-decoder-reuse.json)

On the preceding deindex build (`3baee479d`), the p95 ratios were 1.418×,
2.262× and 1.376× in workload order.
On the preceding typed NULL waterline build at `c03c4fcee`, the p95 ratios
were 1.281×, 2.326× and 1.129× in workload order.
Under the same profile, the earlier chain-topology build at `72183b58e`
measured 1.094×, 2.346× and 1.221× in workload order.
The earlier filter-trimming build measured 1.239×, 2.119× and 1.183× in
workload order, with concurrent corpus verification.
The EMIT FINAL window build at `88ee907de` measured 1.223×, 2.306× and 1.118×
in workload order. [Pair digests and summaries](evidence/p3-hybrid-latency.json)

Skipping indentation work for compact JSON and writing fixed punctuation directly
reduced mean thread CPU time from 17.08 to 14.42 ms for two Nexmark optimizer
snapshots. Both writers produced the original bytes. Across 31 alternating
pairs, the 95% bootstrap interval for the candidate-to-baseline CPU ratio was
0.822–0.866. This timing covers serialization through UTF-8 encoding.
[Writer trials and detached replay](evidence/p3-compact-json-writer.json)

Sending only the expression roots visited by `SimplifyWaterline` reduced the
pass's mean main-thread CPU time from 51.60 ms to 7.48 ms on Nexmark. Each
of the 12 pairs ran the full classic hybrid optimizer in fresh JVMs on CPU
13 with alternating build order. The 95% paired bootstrap interval for the
CPU ratio was 0.133–0.159. Corpus verification shared the VM during the run.
[Waterline pass measurements](evidence/p3-waterline-root-transport.json)

Sending the aggregate list and its live bindings reduced mean main-thread
CPU in `ExpandStreamAggregates` from 271.54 ms to 27.88 ms on classic Nexmark.
The 12 cold JVM pairs alternated build order on CPU 13 while corpus verification
shared the VM. [Aggregate pass measurements](evidence/p3-stream-aggregate-lists.json)

A chain-topology run at `72183b58e` overlapped ASOF corpus and runtime verification.
All 69 pairs agreed on artifacts and diagnostics. Direct/hybrid p95 was
6.752/12.263 s on the array subquery, 22.529/46.318 s on Nexmark and
8.039/16.439 s on the outer join. These timings include contention.
[Chain-topology benchmark](evidence/p3-hybrid-latency.json)

On the earlier ASOF build (`54c227440`), the ratios were 0.961×, 2.551× and 1.369× in workload order. Compiler verification overlapped part of that run. The circuit lowering build at `99f23c273` measured 1.301×, 2.387× and 1.187×, respectively. The preceding checked waterline build at `5a92a1eb3` measured 1.247×, 2.521× and 1.117×. The earlier compact consumer graph build at `f08e3eb5d` measured 1.072×, 2.394× and 1.131× in workload order. The circuit-writer build at `5472764d0` measured 1.246×, 2.112× and 1.214×. [Pair digests and summaries](evidence/p3-hybrid-latency.json)

The static-declaration build at `a039e2fda` measured 1.237×, 2.435× and 1.198× in workload order. The directed limiter comparisons overlapped the beginning of that run. The full corpus and restore runs started after that benchmark.

The isolated snapshot join build at `65fbccc7f` measured 1.239×, 2.843× and 1.189× in workload order.

Reusing lexical scopes during common subexpression elimination reduced the time to
compute alias maps for all 24 rounds of one Nexmark compilation. Across 14 paired
processes the median fell from 32.86 ms to 27.60 ms. The 95% paired bootstrap
interval for the median reduction was 5.09–5.47 ms. Each timed repetition checked
all 24 alias maps. The timer started after decoding the graphs. [CSE measurements](evidence/p3-cse-scopes.json)

On a captured Nexmark fanout rewrite, tracking emitted nodes in a preallocated
hash set reduced the median from 92.26 ms to 85.54 ms. The 95% paired bootstrap
interval for the reduction was 6.49–6.85 ms. Sixteen pairs used identical Cargo
features and produced byte-identical graph replies. This timer covers the
Rust rewrite inside one optimizer pass. [Paired measurements](evidence/p3-fanout-membership.json)

The expression-cache build at `632a0670f` measured 1.277×, 2.235× and 1.236× in workload order.
The shared-expression build at `eb580978e` measured 1.058×, 2.187× and 1.120× in workload order.
The map grouping build at `6db65a1a9` measured 1.350×, 2.334× and 1.074× in workload order.
The preceding decoder build at `b3a0afd8c` measured 1.208×, 2.428× and
1.228× in workload order. On the earlier ASOF retainer build at `fbff97a54`, the ratios were
1.419×, 2.275× and 1.176× in workload order.

On the join-filter build at `77879893f`, all 69 pairs matched. The array subquery took 1.92 s hybrid and 1.57 s direct at p95 (1.223×), Nexmark took 6.40 s and 2.62 s (2.442×), and the outer join took 1.58 s and 1.37 s (1.152×). This run allowed CPUs 0–15. The earlier pinned runs and this run have different scheduling conditions.

The UDF and aggregate eligibility build exceeded the 1.2× ceiling on all three workloads. The earlier build with relational snapshots retained in the JVM measured 1.177×, 2.061× and 1.180× in workload order. Before that change, a separate run measured 1.312×, 1.994× and 1.248× in workload order. Host load varied between runs. The 97-pass build measured 1.175×, 1.980× and 1.241× in workload order. The 96-pass build measured 1.290×, 2.367× and 1.171× in workload order.
The 94-pass build measured 1.303×, 2.693× and 1.230× in workload order. The 91-pass build with live graph encoding reuse measured 1.273×, 2.449× and 1.259×. Before the live codec change, the
91-pass build measured 1.270×, 2.774× and 1.158×. The 89-pass build measured 1.211×,
2.606× and 1.271× in workload order. The corrected 88-pass build measured
1.161×, 3.221× and 1.403×. The 86-pass build measured
1.293×, 2.781× and 1.289× in workload order. The 84-pass build measured
1.344×, 2.995× and 1.017× in workload order. The 83-pass build measured 1.315×,
2.560× and 1.222× in workload order. The 81-pass build measured
1.057×, 3.139× and 1.008× in workload order. The 79-pass build measured
1.079×, 2.470× and 1.218× in workload order. The 78-pass build measured
1.269×, 2.626× and 1.135× in workload order.

Each workload has two warmup pairs and 21 measured pairs. All 69 pairs
matched on diagnostics and output artifacts. The harness alternated compiler
order and rotated workload order. The map grouping, decoder and ASOF retainer runs each used one pinned Linux ARM64 CPU with a warm
filesystem cache on a shared host. The p95 is the twentieth ordered observation out of 21.

The earlier twelve-pass run used Java class directories on a bind mount. The
packaged JAR changed direct Java latency substantially. The gate uses the
packaged comparison. The results retain both classpath profiles, all measured
ratios, artifact and binary identities and the archive paths for raw pairs.
[Paired measurements](evidence/p3-hybrid-latency.json)

On the captured Nexmark input to `CloneOperatorsWithFanout`, reusing unchanged
inner-node field maps reduced median Rust rewrite time from 441.19 ms to 322.27 ms.
Six alternating pairs on one pinned Linux CPU gave a 95% bootstrap interval of
0.690–0.755 for the mean paired ratio. Both versions produced identical serialized
output. The timer covered only the Rust fixed-point rewrite.
[Isolated fanout rewrite](evidence/p3-fanout-rewrite-latency.json)

On the same fanout input, checking shared field maps by identity and moving the
encoded graph into its reply reduced the median time to construct the live
graph from 19.20 ms to 9.33 ms (median of eight process medians). Both binaries
produced the same 621,760 bytes. These timings exclude the JNI envelope and
SQL compilation. [Live graph construction](evidence/p3-live-codec-latency.json)

On the captured Nexmark fanout input, parsing directly into the Rust circuit arena
reduced median decode and validation time from 42.61 ms to 30.42 ms. Both paths
produced identical serialized circuits. Ten alternating pairs on one pinned
Linux ARM64 CPU gave a 95% bootstrap interval of 0.703–0.735 for the ratio of
mean process medians. This measurement excludes JNI and the optimizer passes.
[Decoder timings](evidence/p3-stream-decode-latency.json)

On the same fanout input, borrowing the JSON header strings and reserving space
for each node’s fields reduced the median streaming decode from 22.48 ms to
14.99 ms across 12 alternating pairs. The 95% paired bootstrap interval for the
ratio of mean process medians was 0.654–0.672. Both decoders produced the same
serialized circuit. [Decoder header measurements](evidence/p3-decoder-headers.json)

Writing the Nexmark S3 checkpoint with compact JSON reduced its size from
3,491,925 to 2,624,668 bytes. The median of seven JVM medians was 186.2 ms
with formatted JSON and 156.7 ms with compact JSON on CPU 12. The paired
95% bootstrap interval for the ratio of mean write times was 0.759–0.892.
Each pair produced equal decoded checkpoints. In a separate Nexmark run on CPU 15,
the hybrid compiler with compact checkpoints took 15.657 s at p95 from SQL
to artifacts. Direct Java took 7.567 s (2.069×). The schema, dataflow, Rust
source and stubs matched in both warmups and all 21 measured pairs.
[Checkpoint write measurements](evidence/p3-checkpoint-write.json)

On Nexmark, the five read-only expression analyses can omit replay metadata,
and the two retention analyses can compare unrelated policies from the operator
graph alone. In a paired capture on the 97-pass build, request traffic at the
pass adapter fell from 143,617,231 to 128,736,618 bytes. All five emitted files
matched byte for byte. [JNI byte counts](evidence/p3-analysis-payloads.json)

On the 99-pass compiler, keeping relation snapshots in the calling JVM reduced
request bytes from 135,781,212 to 120,454,899 and reply bytes from 22,830,197
to 11,459,152 across 325 calls for Nexmark. The four compiler artifacts and
the result manifest matched byte for byte. [Paired capture](evidence/p3-retained-relations.json)

On Nexmark, sending the filter predicates reduced the input to
`DecomposeExpensiveFilters` from 2,597,371 bytes across two requests to 89,873
bytes in one request. Median filter-pass time fell from 739 ms to 15 ms over six
alternating pairs. The next pass requiring a full snapshot, `PropagateEmptySources`,
rose from 262 ms to 798 ms. The sum of timed pass calls had medians of 7.16 s
and 7.01 s respectively. All twelve compiles produced byte-identical artifacts and
result manifests. [Paired filter profiles](evidence/p3-filter-predicate-profile.json)

On the same Nexmark input, the UDF and array-aggregate eligibility checks reduced
request bytes from 120,454,899 to 114,217,010. The candidate made 323 calls
against 325 for the baseline. All five files matched. [JNI byte counts](evidence/p3-aggregate-udf-probes.json)

In a separate Nexmark run on the 18-pass compiler, Java Flight Recorder captured 1,054 main-thread
stack samples. Of those, 340 were inside a Rust JNI call, 170 were encoding
or applying pass results in Java and 386 were in the Java Rust printer.
The samples include waits for file writes.

To reproduce, build the release driver and JNI library together, package the
Java compiler with Maven, then run the following on Linux with the three
frozen SQL inputs.

```sh
taskset -c "$BENCH_CPU" python3 crates/sql/scripts/benchmark_live_optimizer.py \
  --compiler target/release/sql2dbsp --classpath "$COMPILER_JAR" \
  --producer "$(git rev-parse HEAD)" --output "$NEW_RESULT_DIR" \
  "$INPUTS"/array-subquery.sql "$INPUTS"/nexmark-suite.sql "$INPUTS"/outer-join.sql
```

## Initial cold-process measurement, 2026-11-16

The baseline uses one fresh Java process. The hybrid uses two sequential fresh
Java processes: SQL to S2, then S2 to the generated Rust workspace. Both paths
include process startup and file I/O. No generated SQL runtime is executed.
Every pair must produce byte-identical workspace files, schema and diagnostics.

| Workload | Direct mean (s) | Hybrid mean (s) | Hybrid/direct mean | Paired bootstrap 95% interval |
|---|---:|---:|---:|---:|
| Outer join | 0.725 | 1.224 | 1.688 | 1.677–1.699 |
| Correlated array subquery | 0.758 | 1.264 | 1.668 | 1.657–1.680 |
| Nexmark suite | 1.421 | 2.378 | 1.673 | 1.664–1.682 |

Each workload has two warmup pairs and eleven measured pairs, alternating which
path runs first. All 39 pairs match. Ratios divide summed costs; the paired
bootstrap uses 10,000 resamples with seed zero. The empirical nearest-rank p95
equals the maximum with eleven samples: direct/hybrid values are 0.737/1.232 s,
0.769/1.274 s and 1.438/2.419 s, respectively. These small samples do not establish
the population's tail latency.

The outer-join, array and Nexmark S2 files contain 81,246, 121,258 and 4,080,189
bytes. Their maximum direct/hybrid child RSS is 209,184/197,784 KiB,
212,764/201,392 KiB and 332,276/435,960 KiB. Sequential child maxima do not measure
the memory of a concurrent service.

The experiment used the Linux ARM64 devcontainer, JDK 21, four allowed CPUs via
`taskset`, and `-Xms128m -Xmx1g`. Every JVM is fresh, but filesystem caches are
warm. VM host load and CPU frequency are uncontrolled. The three selected
workloads use default CLI language options, not their JUnit option matrix.
They do not represent the full corpus.

The measured compiler is frozen at `$SCRATCH/p0-seam-tools-v45`. Raw commands,
resource measurements, outputs, classpath/input hashes and environment details
are in `$SCRATCH/p0-seam-benchmark-v1`. The complete host archive is
`.bench-results/seam-cli/2026-16-11-14-12-c6ede/`, including the base commit,
working-tree diff, untracked sources, inputs and raw results. This archive is
gitignored. `results/summary.json` is the original summary;
`results/summary-quantiles.json` adds quantiles from the same raw observations.

## Reproduction

Prepare a fresh archive under `.bench-results/seam-cli/`, recording the commit,
working-tree diff, untracked source files, environment and description before
running. Keep failed runs. Prepare an input directory containing three `.sql`
files from `$SCRATCH/p0-determinism-v3-input.jsonl`:

- `org.dbsp.sqlCompiler.compiler.sql.simple.OuterJoinTests#testMix/0`
- `org.dbsp.sqlCompiler.compiler.sql.simple.AggregateTests#testArrayConstructor/0`
- `org.dbsp.sqlCompiler.compiler.sql.suites.nexmark.NexmarkTest#testCompile/0`

Require every submission's batch to be zero and join its original SQL with
`\n;\n`. Preserve the case identities and original options alongside the SQL.
In the devcontainer, run:

```sh
python3 $REPO/crates/sql/scripts/benchmark_seams.py \
  $SCRATCH/p0-seam-benchmark-input-v1 $SCRATCH/NEW-RESULT-DIRECTORY \
  --classpath $SCRATCH/p0-seam-tools-v45/classes:$SCRATCH/p0-seam-tools-v6/dependencies.jar \
  --runtime $REPO --samples 11 --warmups 2
```

Use a new output directory and a frozen classpath for each run. Copy the inputs
and complete output directory into the archive. The runner rejects changed
inputs, changed classpaths, failed compilation and differing outputs before
writing its completion record.

## Replay context without SQL initialization

A diagnostic run attributed 329 ms of the small workload's replay to compiler
setup. The replay factory now omits the SQL front end. SQL compilation retains
its original initialization order. Replay rejects accidental front-end access;
removing an internal table updates metadata without touching absent INSERT-test
contents. The first test attempt exposed that cleanup dependency and remains
recorded in `$SCRATCH/p0-replay-context-java-v1.log`. The repair passes 53 Java tests,
including five generated Rust runtime tests.

The same experiment with frozen compiler `$SCRATCH/p0-seam-tools-v46` gives:

| Workload | Direct mean (s) | Hybrid mean (s) | Hybrid/direct mean | Paired bootstrap 95% interval |
|---|---:|---:|---:|---:|
| Outer join | 0.733 | 1.071 | 1.460 | 1.442–1.476 |
| Correlated array subquery | 0.768 | 1.132 | 1.474 | 1.459–1.489 |
| Nexmark suite | 1.427 | 2.249 | 1.575 | 1.565–1.586 |

All 39 pairs match workspace bytes, schema and diagnostics. The empirical
p95 direct/hybrid times are 0.792/1.104 s, 0.787/1.162 s and 1.451/2.281 s.
Maximum direct/hybrid child RSS is 201,436/204,584 KiB, 202,520/199,204 KiB and
331,932/445,980 KiB. S2 file sizes are unchanged. The direct and hybrid paths
are paired within each run; the two compiler versions were measured in
successive runs, not randomized across versions.

The second archive is `.bench-results/seam-cli/2026-16-11-14-26-c6ede/`; raw
container output is `$SCRATCH/p0-seam-benchmark-v2`. The inputs, affinity, JVM limits,
warmups and sample counts match the first run. Both runs finished before the
full-corpus correctness jobs started. The same workload and environment
limitations apply. The remaining measured overhead still warrants a process
lifetime/transport design decision before committing to the production target.

## Native BIGINT keys, 2026-11-16

The [key experiment](native-probe/README.md#key-sortmerge-measurement) completed
594 paired observations: 31 measured pairs and two warmups for each of eighteen
configurations. Every stable sort and merge output matches. The release binary
ran on one allowed CPU; replay workers were suspended for eight seconds and
resumed automatically. Their resident memory remained allocated. VM host load
and frequency were uncontrolled.

The table reports mean **component sums**: direct sort plus merge, versus key
encoding plus byte-key sort plus merge. Input/oracle preparation, initial index
allocation and key destruction are excluded. It is not an end-to-end pipeline
latency. Ratios below one favor normalized keys. The paired bootstrap uses
10,000 resamples with seed zero. Do not aggregate these distributions into one
speedup.

| Rows | Fields | Distribution | Direct total (ms) | Encoded total (ms) | Encoded/direct | Paired 95% interval |
|---:|---:|---|---:|---:|---:|---:|
| 4,096 | 1 | CommonPrefix | 0.179 | 0.175 | 0.982 | 0.963–1.001 |
| 4,096 | 1 | Duplicates | 0.129 | 0.106 | 0.824 | 0.800–0.849 |
| 4,096 | 1 | Uniform | 0.178 | 0.177 | 0.993 | 0.977–1.013 |
| 4,096 | 4 | CommonPrefix | 0.498 | 0.201 | 0.404 | 0.399–0.409 |
| 4,096 | 4 | Duplicates | 0.198 | 0.138 | 0.697 | 0.691–0.703 |
| 4,096 | 4 | Uniform | 0.206 | 0.207 | 1.004 | 0.993–1.019 |
| 4,096 | 12 | CommonPrefix | 1.303 | 0.294 | 0.226 | 0.223–0.228 |
| 4,096 | 12 | Duplicates | 0.365 | 0.189 | 0.517 | 0.512–0.522 |
| 4,096 | 12 | Uniform | 0.209 | 0.268 | 1.282 | 1.264–1.301 |
| 65,536 | 1 | CommonPrefix | 3.609 | 3.426 | 0.949 | 0.936–0.961 |
| 65,536 | 1 | Duplicates | 2.029 | 1.496 | 0.737 | 0.727–0.751 |
| 65,536 | 1 | Uniform | 3.569 | 3.403 | 0.953 | 0.947–0.960 |
| 65,536 | 4 | CommonPrefix | 9.946 | 4.068 | 0.409 | 0.407–0.411 |
| 65,536 | 4 | Duplicates | 3.128 | 2.356 | 0.753 | 0.748–0.758 |
| 65,536 | 4 | Uniform | 4.229 | 4.137 | 0.978 | 0.974–0.983 |
| 65,536 | 12 | CommonPrefix | 25.977 | 6.154 | 0.237 | 0.236–0.238 |
| 65,536 | 12 | Duplicates | 5.747 | 3.338 | 0.581 | 0.576–0.586 |
| 65,536 | 12 | Uniform | 4.357 | 5.149 | 1.182 | 1.172–1.193 |

The twelve-field uniform workloads lose 18–28% after paying for encoding,
despite faster sort comparisons with reused keys. Common prefixes make the
field comparator examine more fields and favor normalized keys. Several small
workloads have intervals spanning one, so no gain is resolved there. Single-field
CommonPrefix and Uniform use identical input generation. The baseline is a
dynamic field comparator over the same batch storage, not generated typed Rust
tuples. These measurements cannot select a universal key representation.

The twelve-field, 65,536-row key buffer adds **6.75 MiB** to **6.5 MiB** of batch
payload/NULL storage; vector metadata, sort scratch and outputs are additional.
No total RSS or archive/checkpoint measurement is claimed. Per-phase means,
reused-key ratios, source/binary hashes and limitations are in the
[certificate](evidence/native-key-sort-merge.json).

Raw samples, build provenance, the replay pause record and the source snapshot
are archived at `.bench-results/native-keys/2026-16-11-14-56-c6ede/`. The earlier
14:45 archive records a cancelled deferred wait; it contains no benchmark run.
The experiment does not close N1/N2: ten-program C7, strings/decimals and other
key types, DBSP integration, archive comparison and checkpoint restore remain.

## Warm-process file seam, 2026-11-18

The [warm-process certificate](evidence/seam-warm.json) executes direct and
split compilation in one JVM, constructing a fresh compiler for each command.
It removes JVM startup from command latency but retains S2 and output file I/O.
Five warmup pairs precede 31 measured pairs per workload; the order alternates.
All **108 pairs** match workspace files, schema, and diagnostics exactly.

| Workload | Direct mean (ms) | Split mean (ms) | Split/direct | Paired 95% interval |
|---|---:|---:|---:|---:|
| Outer join | 9.315 | 19.226 | 2.064 | 1.939–2.211 |
| Correlated array subquery | 12.234 | 27.671 | 2.262 | 2.062–2.485 |
| Nexmark suite | 191.296 | 1,318.328 | 6.892 | 6.112–7.696 |

Empirical p95 direct/split values are 14.185/30.372 ms, 15.359/44.276 ms,
and 246.765/1,945.506 ms. The shared JVM's maximum RSS is 800,308 KiB;
it cannot be attributed separately to either path. These samples do not
establish production tail latency. The measured split path exceeds the plan's
1.2× target on all three workloads, but this file experiment is not the actual
production Rust/Java architecture or an approved G7/G10 evaluation.

The Nexmark head averages 1,000.245 ms and its tail 318.083 ms. GC collection
time totals 693 ms across the 31 measured heads, so GC alone does not explain
the roughly 31 seconds spent there. The follow-up JFR profile identifies the
shared SQL-renderer's alias allocation as a source of growing work; it is a
separate diagnostic run, not a benchmark observation.

Timing ran after the full S1 replay and archive jobs finished, while other
container work was paused. The host is an Apple M5 Pro (18 cores, 64 GiB RAM);
the Linux ARM64 guest used JDK 21, four allowed CPUs and `-Xms128m -Xmx1g`.
VM host scheduling and frequency remain uncontrolled. Inputs use CLI defaults,
and the experiment measures no Rust mid-end or IPC. The fixed warmup count
cannot establish JIT/GC stationarity.

The complete archive is `.bench-results/seam-warm/2026-18-11-00-54-c6ede/`,
including raw per-command times, GC and heap counters, inputs, source/classes,
dependency JAR, environment, working-tree snapshots, and exact equality
artifacts. The frozen compiler predates the later diagnostic-capture and ROW
selector repairs. Historical cold and warm runs remain separate evidence.

## Renderer lifetime repair

The follow-up [certificate](evidence/seam-warm-renderer.json) records a fresh
`RelToSqlConverter` for each complete SQL conversion. The old JVM-wide converter
retained aliases between unrelated renderings. Two regression tests fail on the
old build with changing alias names and pass after the repair; 35 current-source
Maven tests also pass, including S2/S3 replay and metadata preservation.
Independent review confirmed that alias uniqueness remains scoped to the whole
query and each entry point preserves its formatting and fallback behavior.

A diagnostic JFR recording of the old build contains 433 execution samples,
320 with `SqlValidatorUtil.uniquify` on the stack. This profile ran separately
from timing. It supported the lifetime repair; its latency is not used in the
benchmark results.

Repeating the same schedule on a frozen build with only the two production
renderer classes changed gives:

| Workload | Direct mean (ms) | Split mean (ms) | Split/direct | Paired 95% interval |
|---|---:|---:|---:|---:|
| Outer join | 8.052 | 14.077 | 1.748 | 1.622–1.894 |
| Correlated array subquery | 10.862 | 19.623 | 1.807 | 1.764–1.853 |
| Nexmark suite | 182.455 | 385.329 | 2.112 | 2.085–2.141 |

All 108 pairs remain byte-equal. Nexmark's head falls from 1,000.245 ms to
87.972 ms; its tail averages 297.357 ms. Empirical direct/split p95 values are
10.351/17.472 ms, 13.201/24.058 ms, and 210.774/449.988 ms. Shared-process maximum
RSS is 678,912 KiB. The two versions ran in successive idle windows, not a
randomized version comparison; the other workload/environment limitations
still apply. Ratios remain above 1.2×, so this experiment does not pass the
production target or approve a budget.

The follow-up archive is
`.bench-results/seam-warm/2026-18-11-01-55-c6ede/`. Its `raw.tar.gz` contains
50,884 files, including all results, frozen sources/classes, dependency JAR,
profile, failed old-control tests, repaired tests, and setup-attempt evidence.
A single-pass verifier checks the archive hash, all classpath hashes, and
rechecks all 108 output/schema/diagnostic pairs. `verify.py` reproduces that
verification without extracting the archive.

## Eight-workload standalone Java baseline

The standalone CLI compiled eight fixed programs in classic and Gen-2 modes.
The suite adds ASOF, decimal and string aggregation, a CSV connector, a keyed
table, and EXCEPT ALL to the original outer-join, correlated-array and Nexmark
programs. Each mode ran 31 measured compilations and five warmups per
workload, both in fresh JVMs and in a warmed JVM. All 1,152 compilations
matched their mode and workload's cold reference on schema and diagnostics,
and on exact classic Rust bytes or canonical Gen-2 graphs.

| Program | Classic cold p50/p95 (ms) | Classic warm p50/p95 (ms) | Gen-2 cold p50/p95 (ms) | Gen-2 warm p50/p95 (ms) |
|---|---:|---:|---:|---:|
| Decimal and string aggregate | 737.2 / 745.3 | 7.2 / 9.2 | 735.9 / 743.8 | 6.7 / 8.4 |
| Correlated array | 753.3 / 765.5 | 8.4 / 11.0 | 735.7 / 749.8 | 7.1 / 10.7 |
| ASOF | 754.0 / 768.9 | 9.1 / 11.4 | 748.4 / 757.7 | 7.4 / 10.0 |
| CSV connector | 686.9 / 701.7 | 4.0 / 5.2 | 680.0 / 711.6 | 3.5 / 4.7 |
| Keyed table | 694.3 / 700.6 | 4.5 / 5.6 | 690.0 / 701.2 | 3.4 / 4.5 |
| Nexmark suite | 1,331.1 / 1,345.2 | 124.7 / 157.0 | 1,333.4 / 1,350.5 | 111.9 / 164.8 |
| Outer join | 715.6 / 736.1 | 6.3 / 9.1 | 711.5 / 725.9 | 6.1 / 9.2 |
| EXCEPT ALL | 674.5 / 685.2 | 3.7 / 5.5 | 675.4 / 683.0 | 3.4 / 6.3 |

Peak cold child RSS was 361,300 KiB for classic and 440,592 KiB for Gen-2.
The shared warm JVMs peaked at 556,488 and 656,768 KiB. The Linux ARM64
devcontainer used four CPUs per JVM and a 1 GiB heap limit. Filesystem caches
were warm and VM scheduling and CPU frequency were uncontrolled. The
standalone profile uses CLI defaults and stops at artifact emission. It leaves
pipeline resource usage, concurrency, cancellation, runtime throughput and
state size to separate experiments.

Reproduce via `scripts/benchmark_java_baseline.py` using the archived inputs,
classpath and comparator. The [archive index](evidence/java-baseline-archive.json)
pins the raw schedule, observations, outputs and the initial smoke-test failure
caused by malformed quoting in the connector input. The remaining resource
budgets still require approval.

## Eight-workload incremental Java baseline

The same suite ran with the pipeline manager's incremental flags and
dataflow output. Classic emitted the multi-crate Rust workspace and Gen-2
emitted circuit IR. All 1,152 compilations matched the mode and workload's
cold reference on schema, diagnostics, dataflow and output artifacts. The
run used 31 measured trials and five warmups per mode, lifetime and workload.

| Program | Classic cold p50/p95 (ms) | Classic warm p50/p95 (ms) | Gen-2 cold p50/p95 (ms) | Gen-2 warm p50/p95 (ms) |
|---|---:|---:|---:|---:|
| Decimal and string aggregate | 783.4 / 864.1 | 11.3 / 15.8 | 781.2 / 798.7 | 7.3 / 9.6 |
| Correlated array | 798.1 / 854.2 | 12.9 / 18.0 | 791.5 / 809.6 | 8.7 / 12.6 |
| ASOF | 823.0 / 957.6 | 16.8 / 21.8 | 808.3 / 824.7 | 9.9 / 12.6 |
| CSV connector | 721.0 / 787.0 | 6.5 / 10.8 | 723.0 / 747.2 | 4.2 / 6.1 |
| Keyed table | 729.3 / 911.9 | 7.0 / 9.1 | 732.1 / 748.6 | 4.2 / 5.7 |
| Nexmark suite | 1,781.6 / 1,916.7 | 364.7 / 444.4 | 1,595.0 / 1,628.1 | 217.0 / 255.5 |
| Outer join | 764.0 / 848.5 | 10.5 / 14.6 | 754.8 / 779.1 | 7.3 / 9.4 |
| EXCEPT ALL | 704.4 / 810.8 | 6.4 / 9.1 | 707.8 / 722.2 | 3.8 / 5.4 |

Peak cold child RSS was 432,328 KiB for classic and 447,432 KiB for Gen-2.
The separate warm JVMs peaked at 747,688 and 668,576 KiB. The run retained
all samples, including the interval during which a verified archived
capture was removed from the devcontainer to free disk space. The maintenance
log gives the time and free space. These timings end at artifact emission,
before `rustc` or circuit execution.

Use `--pipeline-runtime $REPO` with the baseline runner to
reproduce this profile. The [completion record](evidence/java-pipeline-baseline.json)
links the suite, output and classpath.

### Concurrent workers and cancellation

A cold-worker pilot ran 96 incremental compilations across four workloads
and both output modes. The groups used one, two or four process slots on
the same four CPUs. Every output matched its cold baseline. Peak RSS is a
sampled sum of per-process RSS and can count shared pages more than once.

| Mode | Slots | Batches | Median batch (s) | Peak summed RSS (KiB) |
|---|---:|---:|---:|---:|
| Classic | 1 | 16 | 0.817 | 423,332 |
| Classic | 2 | 8 | 1.547 | 438,432 |
| Classic | 4 | 4 | 2.846 | 831,508 |
| Gen-2 | 1 | 16 | 0.799 | 440,748 |
| Gen-2 | 2 | 8 | 1.448 | 456,520 |
| Gen-2 | 4 | 4 | 2.605 | 833,988 |

Twelve Nexmark compilations were cancelled by process group, at 50 or 300 ms
after Java started. Kill-to-reap time ranged from 3.68 to 9.56 ms and each
process group was gone after reaping. These are CLI worker measurements.
G7 still needs the deployment and generated-circuit measurements and
approved budgets.

## G10 resource calibration

With a 1 GiB virtual-address-space limit and a 128 MiB per-file limit,
the parser processed 27,560 corpus-derived inputs in 0.90 CPU seconds
and peaked at 17,676 KiB child RSS. The A binder processed 20,480 input
programs in 0.17 CPU seconds and peaked at 15,516 KiB. The circuit decoder
accepted 50 original graphs and rejected all 50 duplicate-identity variants.
These runs use a finite deterministic mutation set and include worker launch
overhead in the resource observations.

The first combined parser run hit its 120-second deadline on the depth
fixtures. Isolated runs with a two-second deadline narrowed the cause to
stock sqlparser 0.62. It timed out on nested arrays at depths 128, 512,
1,024 and 4,096. Stock 0.64 and Feldera's 0.64 parser returned on all 15
depth fixtures. The Feldera parser was exercised through the A binder entry
point. It returned an explicit recursion-limit error on the deeper inputs.
The comparative failure remains part of the result.

The structural calibration accepted all 100 original circuits and rejected
600 mutations covering operator identities, output ports and malformed JSON.
The earlier comparator accepted the nonexistent output-port fixture. The
strict decoder also accepted 13,776 archived optimizer observations.
The 700 local debug-build checks used 1.02 child CPU seconds and 46,180 KiB
peak child RSS under a 1 GiB address-space limit.
[Decoder calibration](evidence/decoder-port-validation.json)

The release-mode CPU-minute pilots processed 5,930,944 parser inputs,
5,350,592 binder inputs and 84,640 circuit inputs. All workers exited
normally and completed their batches within the two-second cap.
Peak worker RSS ranged from 17,884 to 26,508 KiB. Each request in
this run contains 32 mutations, and the maximum observed request latency
was 32.8 ms.
[CPU-minute run](evidence/resource-calibration-extended.json)

The CPU-hour run used one pinned CPU per release worker and the same
two-second batch deadline. All targets reached 3,600 worker CPU seconds
and exited normally. Attempts include repeated controls and mutations.

| Target | Attempts | Peak worker RSS (KiB) | Longest batch (ms) |
|---|---:|---:|---:|
| Feldera parser | 351,746,464 | 22,656 | 13.5 |
| Scoped A binder | 310,597,696 | 17,676 | 13.1 |
| Canonical circuit decoder | 4,993,344 | 26,312 | 50.2 |

The run covers the archived probe worker, including its then-current binder
scope. Budget approval, deployment worker validation, the engine-build matrix
and artifact consumer tests remain open.
[CPU-hour run](evidence/resource-calibration-cpu-hour.json)

The calibration record lists the inputs, outputs, measured limits and
executables. [Calibration record](evidence/resource-calibration.json)

## Mixed text, decimal and BIGINT keys

The mixed-key prototype encodes nullable UTF-8, fixed-scale decimal mantissas
and BIGINT in one ordered key. A separate devcontainer test compares
1,742,400 pairs against the runtime's typed order, including Unicode,
embedded NULs, text-prefix boundaries, signed extrema and all 64 direction
and NULL-placement policies.

The release benchmark ran on one allowed CPU and verified each stable sort
and merge against field-by-field comparison. The six workloads use 31 measured
pairs and two warmups apiece. Costs include encoding for the normalized path.
They exclude input preparation and key destruction.

| Rows | Distribution | Direct sort + merge (ms) | Encode + sort + merge (ms) | Normalized/direct | Paired 95% interval |
|---:|---|---:|---:|---:|---:|
| 4,096 | Common prefix | 0.336 | 0.422 | 1.255 | 1.242–1.270 |
| 4,096 | Duplicates | 0.173 | 0.197 | 1.135 | 1.120–1.150 |
| 4,096 | Uniform | 0.212 | 0.286 | 1.353 | 1.326–1.381 |
| 65,536 | Common prefix | 7.684 | 8.965 | 1.167 | 1.141–1.191 |
| 65,536 | Duplicates | 2.958 | 3.492 | 1.181 | 1.136–1.210 |
| 65,536 | Uniform | 4.556 | 5.918 | 1.299 | 1.289–1.311 |

Total component cost rose 13–35% in these synthetic workloads. The report
separately records reused-key sort and merge, encoding time and payload bytes.
The [certificate](evidence/native-mixed-keys.json) links the raw measurements,
runtime ordering test and executable archive.
Run `scripts/benchmark_keys.py --mixed` against the release `key_benchmark`
example to reproduce the measurements.

### Index-key types from the ten frozen cases

All 858 paired sorts and merges agree with typed field ordering for the
five explicit index-key types. The 26 synthetic workloads each ran 31
measured pairs and two warmups at 4,096 or 65,536 rows. The normalized path
includes encoding.

| Selected key type | Workloads | Normalized/direct total range |
|---|---:|---:|
| Empty tuple | 2 | 4.028–5.030 |
| Single INTEGER | 6 | 3.284–4.873 |
| Two INTEGERs | 6 | 1.822–2.702 |
| Nullable DECIMAL(6,2) | 6 | 2.462–3.498 |
| Unsigned time | 6 | 3.420–4.926 |

The first run's validator rejected a zero-duration encode sample for the
empty tuple. The repaired validator accepts sub-tick timing for that
zero-byte operation and preserves the positive timing checks elsewhere.
End-to-end DBSP performance remains open. Reproduce with
`scripts/benchmark_keys.py --corpus` and the release example.
[Index-key evidence](evidence/native-corpus-index-keys.json)

## Paired native Unicode projection

The [measurement archive](evidence/native-runtime-benchmark-archive.json)
holds 31 paired runs per batch size and 15 generated-crate rebuilds. Each
runtime sample starts a fresh process pinned to the same Linux ARM64 CPU with
one worker. Both engines use full steps and accumulate through the same DBSP
output reader. The harness verifies every weighted output row before five
warmup steps. It times the remaining 200 or 50 steps, including input append
and output consolidation.

| Rows per step | Engine | Throughput p50, rows/s | Mean step latency per run, p50 / p95 (ms) | Peak RSS p50 (MiB) |
| ---: | --- | ---: | ---: | ---: |
| 64 | Classic | 245,075 | 0.261 / 0.264 | 36.4 |
| 64 | Native | 276,631 | 0.231 / 0.233 | 41.0 |
| 4,096 | Classic | 521,492 | 7.854 / 7.926 | 38.5 |
| 4,096 | Native | 697,532 | 5.872 / 5.928 | 43.1 |

Native circuit initialization takes 8.39–8.68 ms median, including Cranelift
code generation, system linking, loading and circuit startup. Classic starts
an already-loaded circuit in 1.79–1.87 ms. Building the classic shared object
with warm dependencies takes 1.599 s median and 1.619 s empirical p95, including
Cargo, rustc and linking. All results use the unoptimized Rust dev profile
with debug info disabled. These results cover a stateless Unicode projection.
The optimized deployment suite and its state-size measurements remain open
for C13. [Measurement certificate](evidence/native-runtime-benchmark.json)

## Paired incremental COUNT DISTINCT

The frozen `emptyAggregate` program seeds 16,384 distinct keys. Half of each
update changes an existing weight and the rest adds or removes distinct keys.
Two steps return the circuit to its starting state. Both engines verify every
signed output delta and the empty error output. After six warmup steps, each
sample times 40 or 20 full transactions, including input append and output
consolidation. Setup, seed loading, and checkpoint creation have separate costs.

The run uses 31 pairs per batch size, alternating engine order on one Linux
ARM64 CPU. Both engines use one worker and force spill with zero thresholds.
The test uses the Rust dev profile with debug info and incremental compilation
disabled. The native probe compiles the predicate at initialization and reuses
DBSP's aggregate operators.

| Updates per step | Engine | Throughput p50, rows/s | Mean step latency per run, p50 / p95 (ms) | Peak RSS p50 (MiB) |
| ---: | --- | ---: | ---: | ---: |
| 256 | Classic | 49,066 | 5.217 / 5.249 | 40.9 |
| 256 | Native | 43,604 | 5.871 / 5.936 | 46.8 |
| 2,048 | Classic | 64,678 | 31.665 / 33.037 | 46.7 |
| 2,048 | Native | 58,185 | 35.198 / 35.538 | 54.9 |

Median paired latency is 12.8% higher at 256 updates and 10.8% higher at
2,048. Native initialization takes 8.43–8.89 ms median, compared with
2.79–2.81 ms in the already-compiled classic circuit. Both engines commit
a checkpoint after every sample. The sorted storage file sizes agree in
all 62 pairs, totaling 255,639 and 282,775 bytes for the two batch sizes.
This measures the forced-spill storage footprint for the selected circuit.
Optimized production performance and byte-level checkpoint parity remain
separate requirements.
[Measurement certificate](evidence/native-count-benchmark.json)

## Optimized native calibration

The release-profile run pins both engines to one Linux ARM64 CPU and uses
one DBSP worker. Each row summarizes 31 paired fresh processes with
alternating engine order. Latency is the mean full-step duration within a
process, then summarized across processes. Both workloads check their
expected output. COUNT DISTINCT forces spill and checks every signed update.

| Workload | Rows per step | Classic / native throughput p50 (rows/s) | Classic / native step p95 (ms) | Classic / native peak RSS p50 (MiB) | Paired native/classic median step time (95% bootstrap interval) |
|---|---:|---:|---:|---:|---:|
| Unicode projection | 64 | 1,570,980 / 1,665,828 | 0.042 / 0.040 | 11.22 / 13.16 | 0.942 (0.934–0.947) |
| Unicode projection | 4,096 | 5,614,311 / 6,625,436 | 0.737 / 0.623 | 13.35 / 15.16 | 0.845 (0.844–0.848) |
| COUNT DISTINCT | 256 | 275,676 / 242,824 | 0.998 / 1.104 | 27.78 / 32.29 | 1.132 (1.126–1.138) |
| COUNT DISTINCT | 2,048 | 671,139 / 592,886 | 3.120 / 3.547 | 33.28 / 40.29 | 1.135 (1.132–1.138) |

Native Unicode initialization, including code generation, linking, loading
and circuit startup, takes 6.84–6.91 ms at p50. Classic initialization
takes 1.29–1.36 ms with the circuit code already loaded. Fifteen warm-dependency
rebuilds of the optimized classic shared library take 14.28 s at p50 and
15.46 s at empirical p95. These timings exclude the common SQL front end.

At the two COUNT DISTINCT sizes, the classic checkpoint directories are
259,493 and 285,481 bytes. The corresponding native directories are each
20 bytes smaller in all pairs. A follow-up file inventory attributes the
difference to `dependencies.json` and shows identical spill file lengths.
Peak RSS comes from a small native parent process. Earlier samples with
an inherited Python process RSS floor are retained in the archive.

[Measurement certificate](evidence/native-c13-optimized.json)

### Nullable SUM accumulator

The forced-spill SUM benchmark uses the original generated SQL circuit and
changes only the accumulator's archive adapter on the native path. It alternates
insertion and retraction of distinct input rows, half of which have a NULL
aggregate argument. Both engines check exact signed output and the empty error
stream on every transaction. All 124 fresh-process samples use an optimized
build, one pinned Linux ARM64 CPU and one DBSP worker.

| Rows per step | Classic / native throughput p50 (rows/s) | Classic / native step p95 (ms) | Classic / native peak RSS p50 (MiB) | Paired native/classic median step time (95% bootstrap interval) |
|---|---:|---:|---:|---:|
| 256 | 112,315 / 111,975 | 2.333 / 2.348 | 23.59 / 24.85 | 1.002 (0.996–1.007) |
| 2,048 | 807,421 / 807,133 | 2.628 / 2.576 | 27.29 / 28.60 | 1.002 (0.997–1.008) |

Each checkpoint directory is 10,721 bytes, with identical spill file lengths.
Startup through the first output takes 3.43–3.55 ms at p50 for classic and
9.19–9.20 ms for native, including generation of the native archive writer.
The owner approved C13 limits for the measured Linux ARM64 profile and kept Track N open. The deployed workload mix and target hardware remain open. [Approved limits](P0-REBASELINE.md#approved-devcontainer-budgets)
[Nullable SUM measurement certificate](evidence/native-c13-nullable-sum.json)

The four-worker follow-up uses four pinned ARM64 CPUs and the same force-spill
configuration. The 124 optimized fresh-process measurements use the same
input batches and per-transaction assertions. Step p95 is the 95th
percentile of each process's average step latency.

| Rows per step | Classic / native throughput p50 (rows/s) | Classic / native step p95 (ms) | Classic / native peak RSS p50 (MiB) |
|---|---:|---:|---:|
| 256 | 19,252 / 19,263 | 15.014 / 13.680 | 54.27 / 55.59 |
| 2,048 | 150,228 / 149,753 | 18.621 / 17.836 | 66.88 / 68.22 |

Checkpoint directories are 1,514 bytes for each sample, and every pair
has identical spill-file lengths. Startup through first output is
6.41–6.52 ms at p50 for classic and 12.62–12.69 ms for native.
[Four-worker SUM measurement](evidence/native-c13-sum-four-workers.json)

### Four-worker COUNT DISTINCT

The four-worker COUNT DISTINCT follow-up uses the same four pinned CPUs and
force-spill configuration. All 124 optimized fresh-process samples pass the
signed-bag and error-stream assertions for every transaction.

| Rows per step | Classic / native throughput p50 (rows/s) | Classic / native step p95 (ms) | Classic / native peak RSS p50 (MiB) |
|---|---:|---:|---:|
| 256 | 10,041 / 8,834 | 44.082 / 46.584 | 98.48 / 106.49 |
| 2,048 | 88,015 / 79,183 | 35.069 / 42.384 | 106.72 / 118.29 |

At 2,048 rows per step, the p95 ratio is 1.209 against the approved
1.20 limit. These percentiles describe process means. Native checkpoint
directories are consistently 89 bytes smaller. The sorted spill lengths
in each pair differ only by one byte in the smallest file.
[Four-worker COUNT DISTINCT measurement](evidence/native-c13-count-four-workers.json)

The same binary, CPU set and 124-sample protocol produced a replication.
Native median throughput was 88.5% and 90.2% of classic at 256 and 2,048 rows,
respectively. The p95 ratios were 1.095 and 1.091. At 2,048 rows the two runs
straddle the approved p95 limit, so the current data do not establish a
consistent pass. Both sets of raw measurements are retained.
[Four-worker COUNT DISTINCT replication](evidence/native-c13-count-four-workers-replication.json)

The median of the paired process latency ratios is 1.108–1.111 across both
batch sizes and runs. A four-pair block resample puts the original
2,048-row p95 ratio at 0.935–1.209 and the replication at 1.073–1.120
over the central 95% of draws. These intervals describe the observed
devcontainer runs. The original tail leaves the approved 1.20 ceiling
unresolved.
[Paired latency analysis](evidence/native-count-paired-uncertainty.json)

### COUNT DISTINCT transaction latency

The earlier samples summarized each process's mean step time. In this
follow-up each process records all measured transactions after warmup.
Both profiles use paired release binaries and the same four CPUs, input
batches, forced-spill settings and signed-bag assertions.

| Profiling | Rows per step | Classic / native transaction p95 (ms) | Native p95 / classic |
|---|---:|---:|---:|
| Generated default | 256 | 44.008 / 53.180 | 1.208 |
| Generated default | 2,048 | 35.236 / 43.118 | 1.224 |
| Disabled in both engines | 256 | 31.969 / 34.211 | 1.070 |
| Disabled in both engines | 2,048 | 25.107 / 28.146 | 1.121 |

The generated default exceeds 1.20 at both batch sizes. The unprofiled
repeat meets the limit on this workload. The Java generator enables heap profiling in its Rust preamble. The
profiled result remains relevant to that default configuration.

A sampled allocator profile identified per-row allocation while packaging
native inputs. A fixed-width batch candidate reduced transaction p95 by
1.6% at 256 rows and 1.1% at 2,048 rows in 21 paired triples per size.
The paired bootstrap intervals include equal latency at both sizes, so
the candidate was reverted. All timed processes preserve signed outputs
and the native checkpoint sizes. [Measurement and experiment record](evidence/native-count-transaction-latency.json)

The DBSP runtime counters attribute approximately 64–67% of summed worker
runtime to storage metadata waits in the profiled host-bind cases. The
native input adapter records about 0.48–0.49 seconds of inclusive operator
time across four workers over 87 transactions, including warmup. This
attribution run enables DBSP CPU profiling and serves to locate costs.

Changing where the same executable writes forced-spill state gives these
transaction p95 ratios. Each cell uses 21 fresh-process pairs.

| State location | Profiling | 256 rows | 2,048 rows |
|---|---|---:|---:|
| Linux overlay storage | Generated default | 1.041× | 1.052× |
| Linux overlay storage | Disabled | 1.069× | 1.037× |
| Linux tmpfs | Generated default | 1.020× | 1.056× |
| Linux tmpfs | Disabled | 1.041× | 1.046× |

All four storage and profiling combinations meet the approved p95 ratio
for this forced-spill workload. Both engine outputs match on every
transaction. The original host-bind results remain in the record.
[Storage sensitivity measurements](evidence/native-count-storage-sensitivity.json)

The host-bind repeat with the current layer reader and writer meets the
limits for both batch sizes. Native transaction p95 was 1.097× classic
at 256 rows and 1.083× at 2,048 rows. The upper bounds of the paired
95% bootstrap intervals were 1.112× and 1.108×, respectively. Both engines
ran faster on the repeat. The two runs therefore establish variation in
the host-bind profile. The earlier threshold excursions remain part of
the calibration record. [Host-bind repeat](evidence/native-count-storage-recheck.json)

## Warm 24-placement optimizer replay

Three set-output programs run through S3 in one warm JVM per engine on a
single pinned Linux ARM64 CPU. Each workload and engine has six warmup and 31
measured triples. Each triple compiles with direct Java, Java with the replay
codec, and Rust replacing the 24 selected placements with Java between them.
The schedule rotates the order. The measurement starts before SQL submission
and ends at S3. All 444 graph and diagnostic comparisons pass outside the
measurement.

| Engine | Workload | Direct p50 / p95 (ms) | Java codec p50 / p95 (ms) | Rust replay p50 / p95 (ms) |
|---|---|---:|---:|---:|
| Classic | Integer union | 7.56 / 14.32 | 60.13 / 86.87 | 563.15 / 598.43 |
| Classic | Lateness | 8.59 / 16.97 | 68.56 / 104.41 | 593.37 / 646.63 |
| Classic | Two tables with weight validation | 5.95 / 11.81 | 49.70 / 84.58 | 527.64 / 618.30 |
| Gen-2 | Integer union | 6.87 / 13.62 | 53.58 / 89.22 | 570.28 / 620.21 |
| Gen-2 | Lateness | 9.59 / 13.99 | 70.90 / 119.88 | 595.89 / 662.19 |
| Gen-2 | Two tables with weight validation | 5.22 / 11.03 | 48.55 / 80.87 | 537.10 / 583.87 |

Rust replay p95 is 38.1–52.9 times direct. Both replay variants include
intermediate graph writes and Java decoding. The Rust variant launches a
dev-profile process for each replaced invocation. The benchmark measures this
traced replay path up to S3. Generated artifact emission and the production hybrid
1.2× gate remain separate. Raw timings, paired median ratios with bootstrap
intervals and every graph are archived.
[Scheduled optimizer measurements](evidence/scheduled-optimizer-benchmark.json)

### Reused Rust process

The four-way follow-up adds a reused Rust process and uses the same release
binary for both Rust paths. Each warm JVM runs eight warmup and 32 measured
groups for each workload. The schedule rotates the four modes. Per-pass
graph files and Java decoding remain in the timed region. The run verifies
all 720 graph and diagnostic pairs outside the timed region.

| Engine | Workload | Direct p50 / p95 (ms) | Java codec p50 / p95 (ms) | One-shot Rust p50 / p95 (ms) | Reused Rust p50 / p95 (ms) |
|---|---|---:|---:|---:|---:|
| Classic | Integer union | 6.53 / 12.67 | 53.12 / 85.97 | 142.30 / 157.97 | 128.76 / 173.55 |
| Classic | Lateness | 6.35 / 13.33 | 60.35 / 87.36 | 162.15 / 217.18 | 143.85 / 159.90 |
| Classic | Two tables with weight validation | 3.95 / 8.44 | 41.26 / 80.99 | 133.19 / 172.35 | 119.95 / 158.43 |
| Gen-2 | Integer union | 6.38 / 10.05 | 49.30 / 79.16 | 143.61 / 168.69 | 126.90 / 164.65 |
| Gen-2 | Lateness | 6.70 / 13.05 | 60.22 / 85.47 | 160.67 / 185.43 | 140.11 / 163.95 |
| Gen-2 | Two tables with weight validation | 3.72 / 10.70 | 43.11 / 79.80 | 134.04 / 193.87 | 118.23 / 196.58 |

Median paired latency falls 11–12% across all six workload and engine
combinations. The paired bootstrap intervals exclude parity with one-shot Rust
in each combination. Empirical p95 improves in four and increases in two.
The Java codec control also spends several times the direct compile latency.
The measurements cover the traced test harness. The production hybrid 1.2×
gate remains open.
[Reused worker evidence](evidence/reused-optimizer-worker.json)

## Warm 44-placement optimizer replay

The same three workloads run in both engine modes with 44 replaced placements.
Each storage profile contains 768 measured and 192 warmup compilations across
direct Java, Java codec replay, one-shot Rust and a reused Rust worker.
All 1,440 graph and diagnostic comparisons pass. The devcontainer pins each
warm JVM and its child processes to one CPU.

| Per-pass file storage | Reused Rust p95 divided by direct Java p95 | Java codec p95 divided by direct Java p95 |
|---|---:|---:|
| Host-backed files | 53.7–73.6× | 39.4–51.4× |
| Devcontainer memory-backed files | 42.7–57.4× | 32.4–45.6× |

The replay pays for a wire crossing, graph validation and Java reconstruction
at every replaced placement. It exceeds 1.2× in both storage profiles. These
measurements cover warm S3 replay. The production hybrid still needs a
measurement with its complete typed prefix and Java tail.
[44-placement measurements](evidence/optimizer-44-warm-benchmark.json)

### Four passes in one Rust handoff

The early set-output group executes four consecutive passes at their original
position. It tracks generated names across the handoff and preserves the
exact resulting graphs and final outputs for 45 programs per engine. Five
directed fixtures from the 44-placement run inject an extra pass inside
the grouped region and remain covered by the individual-pass results.

The memory-backed benchmark ran 768 measured and 192 warmup compilations,
with 720 matching final graph and diagnostic comparisons. Grouping brought
median latency to 45.2–49.7% of separate Rust handoffs for the same outputs.
Grouped p95 still ranged from 4.57× to 8.32× direct Java. This timing
predates Rust validation between the grouped passes.
[Grouped early optimizer replay](evidence/optimizer-early-set-segment.json) ·
[Stream-kind validation replay](evidence/optimizer-stream-kinds.json)

### Stream and early-set groups together

Grouping the stream passes as well reduces the 45 individual handoffs to
34. The benchmark covers 768 measured and 192 warmup compilations across
both engines. All 720 graph and diagnostic comparisons match. Across the
three workloads and two engine modes, median latency is 69.7–79.1% of the
same passes with separate handoffs. Grouped p95 ranges from 36.5× to 48.9×
direct Java. The grouped Java codec control is 22.7× to 36.8× direct Java
at p95.
[Combined warm optimizer benchmark](evidence/optimizer-stream-groups-warm.json)

### Persistent four-worker SUM

The optimized nullable SUM adapter clears the C13 runtime ratios at 256
and 2,048 rows per transaction on both Linux overlay and host-bind
storage. Each configuration uses 21 paired processes on CPUs 0, 1, 6
and 7. This rerun enables persistent mode, whereas the earlier
COUNT DISTINCT storage comparison used ephemeral mode. Transaction p95
ratios range from 0.992× to 1.011×, and every checkpoint is 3,794 bytes.
The run leaves end-to-end artifact build and the remaining persistent
workloads open. [Persistent SUM evidence](evidence/native-persistent-sum-latency.json)

### Persistent four-worker COUNT DISTINCT

The direct native builder and the archive adapter both pass the four
C13 runtime ratios with forced spill on Linux overlay and host-bind
storage at 256 and 2,048 rows per transaction. The direct builder reaches
1.175× classic transaction p95 at 2,048 rows on overlay storage, with a
minimum throughput ratio of 0.896× across the four configurations. The
archive adapter peaks at 1.0114× classic transaction p95. The final runs
use 21 paired processes per configuration and run sequentially on the
same reserved CPUs. The archive retains the initial samples that overlapped.
[COUNT DISTINCT evidence](evidence/native-persistent-count-latency.json)

Fifteen paired startup measurements on the same four-worker persistent
circuit also meet the artifact-build budget. Native emits, links and loads
its predicate and starts the runtime in 11.63 ms at p50 and 13.34 ms at p95.
The classic warm-dependency shared-library build, load and startup take
20.84 s at p50 and 24.83 s at p95. The native measurement starts with its
prebuilt runtime executable. The classic measurement includes the generated
circuit crate build, and its loader verifies a transaction after the timed
entry point. Both measurements exclude the common SQL front end.

The classic library uses private jemalloc with dynamic TLS. The archive
retains the preceding loader failures and the replacement for one sample
that waited on a competing Cargo build. These are local measurements for
the measured COUNT DISTINCT program.
[Artifact-build evidence](evidence/native-count-artifact.json)

### Persistent four-worker rolling decimal

The generated decimal rolling COUNT runs on classic DBSP and with the
native nullable DECIMAL join-key archive adapter. Each configuration
uses 21 paired processes on CPUs 0, 1, 6 and 7. Every timed transaction
checks its signed output and empty error stream. Both engines start with
2,048 rows and write a checkpoint after each run.

| Storage | Rows per step | Native/classic median throughput | Transaction p95 | Peak RSS p50 | Checkpoint bytes |
|---|---:|---:|---:|---:|---:|
| Linux overlay | 256 | 0.992× | 1.022× | 1.012× | 1.000× |
| Linux overlay | 2,048 | 0.936× | 1.030× | 1.006× | 1.000× |
| Host bind | 256 | 1.006× | 1.075× | 1.013× | 1.000× |
| Host bind | 2,048 | 0.989× | 0.984× | 1.004× | 1.000× |

The initial adapter reached 0.827× throughput at 2,048 rows on overlay.
The revised adapter encodes each archive into a stack buffer and caches
weak encoder references per thread. Every final configuration meets the
four C13 runtime ratios, with identical checkpoint sizes in all 84 pairs.
The archives retain the initial failure and two builds of the optimized
adapter. [Rolling runtime evidence](evidence/native-rolling-runtime.json)

The build with the descriptor-aware reader and one-column writer repeated
the same 84 pairs. Every profile met the approved limits. Total checkpoint
bytes and the sorted spill-file sizes matched in each pair.
[Storage recheck](evidence/native-rolling-storage-recheck.json)

| Storage | Rows per step | Native/classic median throughput | Transaction p95 | Peak RSS p50 | Checkpoint bytes |
|---|---:|---:|---:|---:|---:|
| Linux overlay | 256 | 0.985× | 1.024× | 1.012× | 1.000× |
| Linux overlay | 2,048 | 0.923× | 1.047× | 1.006× | 1.000× |
| Host bind | 256 | 1.001× | 0.996× | 1.013× | 1.000× |
| Host bind | 2,048 | 0.992× | 1.011× | 1.004× | 1.000× |

The build that shares parsed DBSP layer blocks and the list merger repeated
the 84 pairs within all four limits. The lowest native throughput ratio
was 0.937×, the highest transaction p95 ratio was 1.041×, and the highest
median peak RSS ratio was 1.012×, rounded to three decimals. Checkpoint
and spill-file sizes matched in every pair.
[Shared layer and merger repeat](evidence/native-rolling-raw-layer.json)

The next repeat, after extracting the rolling operator for the owned-input
replay, met all runtime ratios. One of the 84 pairs exceeded the checkpoint
ratio. At 2,048 rows on overlay storage, the native checkpoint had two extra
spill files and occupied 262,774 bytes against 242,990 classic bytes. The
other 83 checkpoint sizes matched.
[Uncompacted checkpoint measurements](evidence/native-rolling-owned-input-update.json)

The follow-up waited for DBSP to compact each worker's state after the
timed transactions. All 84 pairs matched in checkpoint bytes and spill-file
sizes. Transaction p95 was 1.057× classic at most, and the lowest median
throughput ratio was 0.924×. The uncompacted excursion remains in the
calibration record.
[Compacted checkpoint measurements](evidence/native-rolling-checkpoint-compacted.json)


### Persistent four-worker VARCHAR join

The reference VARCHAR row and native archive adapter use the same DBSP join.
Every process checks signed output batches with NULL, inline and long UTF-8
keys, including embedded NUL. Each configuration has 21 paired processes.
DBSP compacts the checkpoint outside the timed transactions. The measurement
records manifest-referenced bytes and total allocated bytes.

| Storage | Rows per step | Native/classic median throughput | Transaction p95 | Peak RSS p50 | Checkpoint bytes |
|---|---:|---:|---:|---:|---:|
| Linux overlay | 256 | 0.984× | 1.032× | 1.030× | 1.000× |
| Linux overlay | 2,048 | 0.980× | 0.971× | 1.021× | 1.000× |
| Host bind | 256 | 0.998× | 0.964× | 1.029× | 1.000× |
| Host bind | 2,048 | 0.995× | 1.010× | 1.021× | 1.000× |

All 84 pairs have equal manifest-referenced and allocated checkpoint bytes.
The initial adapter measured 0.570× throughput at 2,048 rows on overlay.
The final adapter writes the root directly after the string serializer has
written its payload and shares encoder ownership across the circuit.
[Paired results and attempt history](evidence/native-persistent-varchar-runtime.json)


### Decimal rolling artifact build and startup

Fifteen warm pairs compare a freshly rebuilt classic shared library with the
native join-key archive adapter for the same generated rolling COUNT program.
The classic measurement includes the warm-dependency Cargo build, `dlopen`
and circuit startup. The native measurement includes Cranelift emission,
linking, loading and circuit startup. Both paths verify signed transactions
outside the startup clocks. The Linux ARM64 overlay profile uses four workers
with forced spill.

| Path | p50 | p95 |
|---|---:|---:|
| Classic | 36.49 s | 38.71 s |
| Native | 10.13 ms | 11.25 ms |

The native p95 meets the approved C13 ceiling for this workload. The retained
warm pairs use an isolated Cargo cache. The toolchain emitted jemalloc
profiling-configuration warnings during classic compilation.
[Artifact timings and raw attempts](evidence/native-rolling-artifact.json)
