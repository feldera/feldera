# P0 independent reviews

The plan owner authorized review assignments on 2026-11-17. GitHub access
resumed on 2026-11-18. The plan owner then directed the work to continue
locally. The review requests remain open.

Independent Codex agents have now completed the
[architecture review](ARCHITECTURE-REVIEW.md),
[runtime review](RUNTIME-REVIEW.md),
[attribution source review](LICENSING-REVIEW.md), and
[diagnostic evidence review](DIAGNOSTIC-REVIEW.md). Their concrete decoder,
process-supervision, and inventory findings have led to repairs and follow-up
tests. These are automated technical reviews. Maintainer and licensing acceptance remain open.

## Compiler maintainer

Review [PORTING.md](PORTING.md), [FREL.md](FREL.md), [S4-STATE.md](S4-STATE.md), and
[P0.md](P0.md), with particular attention to state ownership, the neutral IR,
source and object identity, and the completeness of the S1/S2/S3 boundaries.

Please identify missing observable fields, unsuitable Rust abstractions, and
unjustified compatibility exclusions. Record accepted scope, required repairs,
and the evidence reviewed. For the phase-exit record, include a dated feature
trace through each delivered crate and explain the invariants of three modules
or passes of your choice. Detached S1 lowering now matches S2, schemas and
diagnostics across all 17,098 exported cases in the recorded snapshot. The subsequent
[version-5 certificate](evidence/s1-prefix.json) transports options and source
context and matches the complete optimizer prefix across the same population:
254,860 statements and 7,030,934 observations, with zero artifact-byte differences. The S4 lifetime audit now links the source contracts to each configured
placement and verifies 32 paired activation programs. See the bounded
referent scope in [S4-STATE.md](S4-STATE.md).

## Runtime maintainer

Review the runtime-facing native ABI and state
migration requirements in PLAN.md, the N1/N2 evidence linked from P0.md, and the
proposed compiler-selection and rollback contracts. Check equality, ordering,
layout, checkpoint compatibility, persistent identity, and the limitations of
the currently measured type and runtime subsets.

Please record scope accepted, remaining blockers, and the measurements needed
for a go/no-go decision. A small passing prototype does not establish coverage
of all required SQL types or workloads.

## Forge IR contract owner

On 2026-11-19 the plan owner confirmed the Forge IR contract owner for the compiler IR interface consumed by `circuit-ir` and `circuit-loader`.
The consumer review covers the pinned compiler and Forge revisions and the
stored-IR compatibility matrix. The plan
owner retained both engine targets under decision 2 on 2026-11-18. The
below-80% Gen-2 result sets its canonical-equal S3 scope under §11.1.
[Forge inspection](P0.md#forge-inspection)

## Licensing reviewer

Review [LICENSING.md](LICENSING.md), its pinned-source and dependency evidence,
and PLAN.md section 9.2. Check translated code beyond explicit copy comments,
distributed notices, parser dependencies, and the plan for future translations.
Record the exact sources and distribution artifacts reviewed, required repairs,
and acceptance or refusal. No licensing exception has been granted.

## Phase decisions

The plan owner recorded go for Tracks M, N and F on 2026-11-19. General
batch factory work moves to PN-a. Binder A proceeds under the approved
structured diagnostics policy and zero-exception cut-over gates. Independent
maintainer and licensing acceptance remain pending. [Gate register](P0-REBASELINE.md)

- **Mid-end.** The 99-placement selection matches all 100 direct outputs,
  detached S2 replays and Java replays of Rust-written S3 artifacts. All
  2,200 observed stage graphs and 2,100 name counters also match. The warm
  grouped probe exceeds the approved P3 ceiling of 1.2× direct Java p95.
  The rule port ledger records the proposed strategy for each of 71 classes.
  [Complete prefix](evidence/optimizer-prefix-99.json) · [Timing results](evidence/optimizer-stream-groups-warm.json) · [Rule strategies](evidence/rule-port-approaches.json)
- **Native.** Archive tests cover all 29 explicit stored shapes, all five
  explicit index keys have measurements, and all ten frozen cases pass C7.
  The production JSON format replay passes, and the selected rolling circuit
  restores classic state with two native-serialized keys. Decision 15 allows
  a preflighted bootstrap or an owner-approved reset for incompatibilities.
  The selected SUM and COUNT DISTINCT accumulator traces exchange checkpoints
  in both directions with their original generated circuits. Runtime-owned
  factories write and reopen byte-identical one- and two-column layer files
  with fresh descriptors. The rolling workload meets the approved runtime
  limits across both storage profiles and both batch sizes in 84 process
  pairs. The first default host-bind COUNT DISTINCT run exceeded the p95
  limit at both batch sizes. The same binaries met it on Linux overlay and
  tmpfs, and the current host-bind repeat met all four limits. The runtime
  descriptors also support nested row groups, signed compaction through
  DBSP's merger, and byte-identical nullable DECIMAL(38, 2) archive files. General
  descriptor-backed batch construction and implicit trace and radix-tree
  layouts remain open. The new input replay verifies all 21 transactions with
  descriptor-owned batches. The global aggregate prototypes also checkpoint
  and restore descriptor-owned state through the DBSP runtime. The join and
  rolling prototypes convert inputs at the existing typed operator boundary.
  [Native coverage ledger](evidence/native-coverage-ledger.json) · [Owned input replay](evidence/native-context-frozen-inputs.json) · [Aggregate checkpoint interchange](evidence/native-aggregate-checkpoints.json) · [Factory-free reader](evidence/native-context-raw-layer.json) · [Compaction and decimal archives](evidence/native-context-decimal.json) · [Rolling runtime](evidence/native-rolling-raw-layer.json) · [COUNT DISTINCT storage profiles](evidence/native-count-storage-sensitivity.json) · [Host-bind repeat](evidence/native-count-storage-recheck.json)
- **Front end.** The plan owner stopped D1 on 2026-11-18 and kept binder A
  as the planning assumption. The generated type and diagnostic matrices
  match. The current held-out run compares typed S1 and detached lowering for
  29 accepted table programs in both modes and matches one structured C0
  rejection. The other 1,533 inputs have explicit scope or parser refusals.
  The selected decorrelation stage passes 149 of 155 weighted C7 occurrences.
  [Binder evidence](D1.md) · [Held-out coverage](evidence/a-binder-held-out-current.json) · [Decorrelation](evidence/correlation-spike6-weighted.json) · [Diagnostics](SPIKE9-ADJUDICATION.md)
