# Handoff — left-join non-incremental oracle

Branch: `outer_join_new_batch_path_oracle` (up to date with `main`, 0 commits behind)
File under work: `crates/dbsp/src/operator/dynamic/outer_join.rs` (test module only)
Date: 2026-08-14

## State

```
15a8f54cb [dbsp] Exercise the left-join oracle with inserts and multiplicities   <- mine, this session
efe216af6 Add a non-incremental batch oracle to the indexed left-join property tests...
```

Working tree is clean apart from untracked `CLAUDE.md` context files (from
`scripts/claude.sh`), which must NOT be committed outside the `claude-context` branch.
Nothing is pushed yet beyond what was already on the PR.

## What the change does

`proptest_left_join_index` now asserts the incremental `left_join_index` against **two**
oracles instead of one:

1. `reference_left_join_index` — pre-existing, built from incremental operators.
2. A new non-incremental batch oracle inside a `non_incremental` subcircuit.

The oracle integrates both inputs, then computes:

```
matched + unmatched - suppressed
```

- `matched` — inner join rows, via `stream_join` then flat-map through `f`.
- `unmatched` — every left row treated optimistically as having no match, `f(k, v, &None)`.
- `suppressed` — withdraws that optimism for left rows whose key occurs on the right.

Then `.differentiate()` back to a delta stream.

## Verification already done — do not redo

**The oracle's algebra is correct and weight-general.** `right_presence` emits exactly one
entry of weight 1 per present key, so `suppressed` weighs `w_left × 1` and cancels
`unmatched`'s `w_left` exactly — for any integer weight, any multiplicity.

**Its presence definition matches production.** `Fold::aggregate`
(`operator/dynamic/aggregate/fold.rs:95`) gates on `!weight.is_zero()` and returns `None`
when every value cancels. `saturate.rs` retracts the ghost `(k, None)` on the first value
and re-adds it when the last is removed. Same notion of "key present".

**Sharding is safe at 4 workers.** `dyn_stream_join` shards both inputs
(`operator/dynamic/join.rs:373-374`); `dyn_stream_aggregate_generic` shards its input
(`operator/dynamic/aggregate.rs:388`). No key's aggregate splits across workers, so
`right_presence` cannot emit duplicate per-key entries.

### Dead end — do NOT retry

Replacing the three `dyn_flat_map_generic` + `unsafe { downcast() }` blocks with the typed
`flat_map_index` **does not compile**. I tried it twice — all three call sites, then the
single simplest one:

```
error[E0599]: no method named `flat_map_index` found for
  struct `Stream<ChildCircuit<ChildCircuit<(), ()>, ()>, ...>`
```

`flat_map_index` resolves only for `RootCircuit` and `NestedCircuit` stream types
(`mono.rs:729/894/1218/1339`); the `non_incremental` subcircuit is a third circuit type.
The `unsafe` there is forced, not a style choice.

A background review agent recommended this rewrite as a safe drop-in that would delete ~35
lines. It is wrong — the agent did not compile it. If that suggestion resurfaces, this is
the answer.

The real underlying gap, if anyone ever wants it: add a `mono.rs` impl covering the
non-incremental child circuit type, so future oracles can stay in the safe API. Out of
scope for this PR.

## What my commit (15a8f54cb) changed

### The substantive fix — weight domain

`generate_test_indexed_zset` (`operator/dynamic/join.rs:2389`) draws weights from
`-max_weight..max_weight`, a **half-open** Rust range. All four indexed proptests passed
`max_weight = 1`, giving `{-1, 0}`. Zero-weight tuples are no-ops, so **every effective
tuple was a deletion** — the oracle never saw an insertion and multiplicity above 1 never
arose.

Raised to `3` → `{-3..=2}`, covering insertions, deletions, multiplicity, mixed
cancellation. Four call sites, `outer_join.rs:592, 597, 612, 617`.

This was pre-existing, dating to `8dc614332 [dbsp] Optimized left join.` — not introduced
by `efe216af6`. Sibling call sites verified to use wider ranges: `join.rs` (antijoin and
join proptests) uses 3, the non-indexed left-join tests use 3, `balance/test.rs` uses 10.
I did **not** check what `concat.rs` passes into its helper — don't claim it publicly.

**Rejected alternative:** making the range inclusive inside `generate_test_indexed_zset`.
It is shared with `join.rs`, `concat.rs`, and `balance/test.rs`, so it would silently
shift the domain of unrelated proptests.

### Cleanups

- Comment above the `non_incremental` block explaining the `matched + unmatched −
  suppressed` algebra and why `right_presence`'s unit weight makes the cancellation exact
  under signed weights.
- `.plus(&suppressed.neg())` → `.minus(&suppressed)` (`Stream::minus` is at `plus.rs:82`,
  generic over circuit type).
- Bare `f` → `suppressed_f`, matching `matched_f` / `unmatched_f`.
- Both `assert_eq!` pairs now carry messages naming which oracle disagreed. Replaced the
  stale commented-out `println!` that printed only two of the three outputs.

## Test results and timings

`cargo test -p dbsp --lib outer_join` — 9 tests (8 proptests + `test_concurrent_outer_join`).
Default proptest cases: 256 (the block has no `proptest_config`). All measured on the
current, weaker machine, debug profile:

| Tree | Wall | Result |
|---|---|---|
| `main` (no oracle) | 193.8s | 9 passed |
| `efe216af6` (oracle, `max_weight = 1`) | 240.6s | 9 passed |
| `15a8f54cb` (oracle, `max_weight = 3`) | 205.1s | 9 passed |

Widening the weight domain made it *faster*, which I did not predict. Plausible reason:
richer weights cancel and consolidate more, leaving smaller accumulated collections than
an all-negative domain. Not chased down. It is the opposite of a regression, but the
number did move — worth a glance if anyone re-measures.

Also confirmed: zero compiler warnings, `cargo fmt -p dbsp -- --check` clean, and no
`crates/dbsp/proptest-regressions/operator/dynamic/outer_join.txt` was created (i.e. no
failure was ever recorded and shrunk).

## Open items

1. **Non-indexed path has no batch oracle.** `proptest_left_join` (`outer_join.rs:306`,
   covering `left_join` / `left_join_balanced_inner`) is still checked only by
   `reference_left_join`, which is built from the same incremental machinery it tests.
   This was deliberately left out of scope. It is the remaining half of the gap.

2. **Squash / reword pending.** Mihai asked for a shorter commit message. Proposed:
   squash `efe216af6` + `15a8f54cb` into one commit, subject
   `[dbsp] Add a non-incremental oracle to the left-join property tests`, three-or-four
   line body covering independence and the weight range. **Not done** — the branch is
   under review, so history rewrite needs a explicit go-ahead.

3. **Reply to review comments drafted, not posted.** See next section.

## PR review thread

Two comments from **mihaibudiu** (Contributor):

> I think this could use a shorter commit message. You are just adding a new way to
> compute the reference results for a left-join if I understand right.

> What in particular caused this PR? What do you find insufficient in the existing tests?

Drafted reply (not yet posted):

---

Thanks — both fair, and yes, I'll shorten the messages.

**On "just a new way to compute the reference results":** that's right as far as it goes,
but the *kind* of reference is the point I was after. `reference_left_join_index` is itself
built out of incremental operators — `join_index`, `aggregate`, `plus`/`neg` — so it and
`left_join_index` sit on the same substrate: accumulate traces, delayed integrals, and the
transaction splitter. These tests run with
`CircuitConfig::from(4).with_splitter_chunk_size_records(2)`, so a single logical input is
chopped into 2-record chunks spread over several steps. If that shared path mishandles
something, both sides can be wrong in the same direction and the assertion still passes.

The new oracle runs inside `non_incremental`, which (per its own doc comment) accumulates
and consolidates its inputs into a single batch before the subcircuit fires. It integrates
both sides, computes the join over the full relations with `stream_join`/`stream_aggregate`,
and differentiates the result back. So it never touches the incremental machinery under
test. What we had before cross-checks the saturate/ghost-tuple strategy against an explicit
presence-and-subtract strategy — genuinely useful, but not independent of incrementalization.
I'd rather have one oracle that can't fail the same way the implementation does.

**On what prompted it:** the above was the motivation going in. The more concrete thing I
found while writing it is that the indexed left-join proptests could not generate an
insertion.

`generate_test_indexed_zset` draws weights from `-max_weight..max_weight`, a half-open
range, and all four indexed tests passed `max_weight = 1` — so weights came from `{-1, 0}`.
Zero-weight tuples are no-ops, which left every effective tuple a deletion, and multiplicity
above 1 never arose. The sibling call sites all use wider ranges (`join.rs` and the
non-indexed left-join tests use 3, the balancer tests use 10), and this looks like it has
been the case since 8dc614332 rather than a deliberate choice for the indexed path.

So part of the honest answer to "what's insufficient" is that this path wasn't exercising
much to begin with. I've raised it to 3 (`{-3..=2}`), which brings in insertions,
multiplicity, and mixed cancellation. All nine tests still pass, so this is a coverage
improvement and not a bug report — but it does mean the oracle now has something to check.

Happy to squash the two commits into one with a short message if you'd prefer that.

---

## Worth running on the bigger machine

The current box made 4-minute test runs the bottleneck. With headroom, in rough priority
order:

1. **Deeper proptest sweep.** The block uses the 256-case default; proptest honours the
   `PROPTEST_CASES` env var (see the note at `trace/test.rs:1694`).
   ```bash
   PROPTEST_CASES=5000 cargo test -p dbsp --lib outer_join
   ```
   This is the single highest-value thing — the oracle has only ever seen 256 cases per
   test on a workload that until today could not insert a row.

2. **Release profile.** Proptests run far faster optimised:
   ```bash
   cargo test --release -p dbsp --lib outer_join
   ```
   Note: `DowncastTrait::downcast` only `debug_assert!`s its `TypeId`
   (`dynamic/downcast.rs`), so release mode does *not* type-check the three `unsafe`
   downcasts in the oracle. Run both profiles; don't treat release-green as equivalent.

3. **Push `max_weight` higher** (10, matching `balance/test.rs`) and see whether anything
   falls out. Cheap experiment now that 3 is known green.

4. **Close open item 1** — add the same oracle to `proptest_left_join`. Needs the
   `OrdZSet`-output shape rather than `OrdIndexedZSet`; same `dyn_flat_map_generic`
   constraint applies, so expect similar boilerplate.

5. **Whole-crate and workspace runs**, which were impractical here:
   ```bash
   cargo test -p dbsp           # ~583 lib tests
   cargo test --workspace
   cargo clippy -p dbsp
   ```

## Commands

```bash
# the loop used all session
cargo test -p dbsp --lib outer_join

# quick compile check without running (~20s incremental)
cargo test -p dbsp --lib outer_join --no-run

# formatting (rustfmt reflows the matched/unmatched/suppressed chain — run after editing)
cargo fmt -p dbsp && cargo fmt -p dbsp -- --check

# check whether a proptest failure got recorded
ls crates/dbsp/proptest-regressions/operator/dynamic/
```

## Conventions

- Untracked `CLAUDE.md` files come from `scripts/claude.sh`; keep them out of commits on
  this branch.
- Commit trailer in use: `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.
- This handoff file is untracked scratch — delete it or keep it out of the PR.
