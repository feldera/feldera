//! Shuttle concurrency test for `OutputEndpointControl`'s state machine.
//!
//! The struct below is a standalone copy of `OutputEndpointControl` from
//! `src/controller.rs`. Keep it in sync whenever the real one changes;
//! shuttle needs the code under test to use `shuttle::sync::atomic` in
//! place of `std::sync::atomic`, which makes it awkward to share the
//! type with the production code. Each `#[test]` runs its scenario
//! under three shuttle strategies (DFS, random, PCT) via `shuttle_run`.
//!
//! Run with:
//!
//! ```text
//! cargo test --release -p dbsp_adapters --test output_endpoint_control_shuttle
//! ```

#![allow(dead_code)]

use crossbeam::utils::CachePadded;
use shuttle::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use shuttle::sync::{Arc, Mutex};
use shuttle::thread;
use std::collections::VecDeque;

/// Copy of `OutputEndpointControl` from `src/controller.rs`.
struct OutputEndpointControl {
    inner: CachePadded<OutputEndpointControlInner>,
}

struct OutputEndpointControlInner {
    state: AtomicU64,
    last_delivered_generation: AtomicU64,
    initial_snapshot_sent: AtomicBool,
}

impl OutputEndpointControl {
    const PENDING_BIT: u64 = 1;

    fn pack(generation: u64, pending: bool) -> u64 {
        (generation << 1) | (pending as u64)
    }

    fn unpack(value: u64) -> (u64, bool) {
        (value >> 1, (value & Self::PENDING_BIT) != 0)
    }

    fn new(send_snapshot: bool, snapshot_already_sent: bool) -> Self {
        let pending = send_snapshot && !snapshot_already_sent;
        let delivered = !send_snapshot || snapshot_already_sent;
        Self {
            inner: CachePadded::new(OutputEndpointControlInner {
                state: AtomicU64::new(Self::pack(0, pending)),
                last_delivered_generation: AtomicU64::new(0),
                initial_snapshot_sent: AtomicBool::new(delivered),
            }),
        }
    }

    fn generation(&self) -> u64 {
        Self::unpack(self.inner.state.load(Ordering::Acquire)).0
    }

    fn is_snapshot_pending(&self) -> bool {
        Self::unpack(self.inner.state.load(Ordering::Acquire)).1
    }

    fn last_delivered_generation(&self) -> u64 {
        self.inner.last_delivered_generation.load(Ordering::Acquire)
    }

    fn mark_snapshot_delivered(&self, captured_generation: u64) {
        let expected = Self::pack(captured_generation, true);
        let new = Self::pack(captured_generation, false);
        if self
            .inner
            .state
            .compare_exchange(expected, new, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.inner
                .last_delivered_generation
                .fetch_max(captured_generation, Ordering::AcqRel);
            self.inner
                .initial_snapshot_sent
                .store(true, Ordering::Release);
        }
    }

    fn request_reset(&self) -> u64 {
        loop {
            let current = self.inner.state.load(Ordering::Acquire);
            let (current_gen, _) = Self::unpack(current);
            let new_gen = current_gen + 1;
            let new = Self::pack(new_gen, true);
            if self
                .inner
                .state
                .compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return new_gen;
            }
        }
    }

    fn initial_snapshot_sent(&self) -> bool {
        self.inner.initial_snapshot_sent.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug)]
struct BatchEntry {
    generation: u64,
    is_snapshot: bool,
}

type Queue = Mutex<VecDeque<BatchEntry>>;

const ITERATIONS: usize = 5_000;
const PCT_DEPTH: usize = 3;
const DFS_CAP: Option<usize> = Some(100_000);

fn shuttle_run<F: Fn() + Send + Sync + 'static + Copy>(scenario: F) {
    shuttle::check_dfs(scenario, DFS_CAP);
    shuttle::check_random(scenario, ITERATIONS);
    shuttle::check_pct(scenario, ITERATIONS, PCT_DEPTH);
}

/// An API thread calls `request_reset` while a circuit thread runs
/// push_output. Checks that the state never ends up `Sent` without a
/// matching snapshot at the final generation.
#[test]
fn reset_during_initial_snapshot() {
    shuttle_run(|| {
        let control = Arc::new(OutputEndpointControl::new(true, false));
        let queue: Arc<Queue> = Arc::new(Mutex::new(VecDeque::new()));

        let a = {
            let control = control.clone();
            thread::spawn(move || {
                control.request_reset();
            })
        };

        let b = {
            let control = control.clone();
            let queue = queue.clone();
            thread::spawn(move || {
                for _ in 0..2 {
                    if control.is_snapshot_pending() {
                        let captured = control.generation();
                        queue.lock().unwrap().push_back(BatchEntry {
                            generation: captured,
                            is_snapshot: true,
                        });
                        control.mark_snapshot_delivered(captured);
                    } else {
                        let captured = control.generation();
                        queue.lock().unwrap().push_back(BatchEntry {
                            generation: captured,
                            is_snapshot: false,
                        });
                    }
                }
            })
        };

        a.join().unwrap();
        b.join().unwrap();

        let final_gen = control.generation();
        // If the in-memory pending bit is cleared, `mark_snapshot_delivered`
        // must have CAS'd from `(final_gen, Pending)` to `(final_gen, Sent)`,
        // which implies the matching snapshot was enqueued just before the
        // CAS. So a snapshot tagged with `final_gen` must be in the queue.
        if !control.is_snapshot_pending() {
            let queued: Vec<_> = queue.lock().unwrap().iter().copied().collect();
            let snapshots_at_final: Vec<_> = queued
                .iter()
                .filter(|b| b.is_snapshot && b.generation == final_gen)
                .collect();
            assert!(
                !snapshots_at_final.is_empty(),
                "pending=false but no snapshot at gen {final_gen}; queued={queued:?}",
            );
        }
    });
}

/// Three concurrent threads: reset, circuit, output. The output thread's
/// accepted batches must have monotonically non-decreasing generations
/// and must not exceed the output thread's final local generation.
#[test]
fn output_never_accepts_stale_generation() {
    shuttle_run(|| {
        let control = Arc::new(OutputEndpointControl::new(
            true, /*already_sent=*/ true,
        ));
        let queue: Arc<Queue> = Arc::new(Mutex::new(VecDeque::new()));

        let a = {
            let control = control.clone();
            thread::spawn(move || {
                control.request_reset();
            })
        };

        let b = {
            let control = control.clone();
            let queue = queue.clone();
            thread::spawn(move || {
                for _ in 0..2 {
                    if control.is_snapshot_pending() {
                        let captured = control.generation();
                        queue.lock().unwrap().push_back(BatchEntry {
                            generation: captured,
                            is_snapshot: true,
                        });
                        control.mark_snapshot_delivered(captured);
                    } else {
                        let captured = control.generation();
                        queue.lock().unwrap().push_back(BatchEntry {
                            generation: captured,
                            is_snapshot: false,
                        });
                    }
                }
            })
        };

        let c = {
            let control = control.clone();
            let queue = queue.clone();
            thread::spawn(move || {
                let mut local_gen = 0u64;
                let mut accepted: Vec<BatchEntry> = Vec::new();
                for _ in 0..3 {
                    let cur = control.generation();
                    if cur != local_gen {
                        local_gen = cur;
                    }
                    let mut q = queue.lock().unwrap();
                    match q.front() {
                        Some(batch) if batch.generation == local_gen => {
                            accepted.push(q.pop_front().unwrap());
                        }
                        Some(batch) if batch.generation < local_gen => {
                            // Stale: drop so the simulator doesn't revisit it.
                            q.pop_front();
                        }
                        _ => {
                            // Either empty, or a future-gen batch waiting
                            // for `local_gen` to catch up on the next
                            // iteration. Leave it in place.
                        }
                    }
                }
                (accepted, local_gen)
            })
        };

        a.join().unwrap();
        b.join().unwrap();
        let (accepted, final_local_gen) = c.join().unwrap();

        let mut prev_gen = 0u64;
        for batch in &accepted {
            assert!(
                batch.generation >= prev_gen,
                "accepted batch at gen {} after gen {}; accepted={accepted:?}",
                batch.generation,
                prev_gen,
            );
            assert!(
                batch.generation <= final_local_gen,
                "accepted batch at gen {} exceeds final local gen {final_local_gen}; accepted={accepted:?}",
                batch.generation,
            );
            prev_gen = batch.generation;
        }
    });
}

/// Two concurrent resets plus one circuit thread. Asserts that
/// `request_reset` hands out distinct generations, the final generation
/// is the max, and if the state ends up `Sent`, a snapshot tagged with
/// the final generation is in the queue.
#[test]
fn two_concurrent_resets() {
    shuttle_run(|| {
        let control = Arc::new(OutputEndpointControl::new(
            true, /*already_sent=*/ true,
        ));
        let queue: Arc<Queue> = Arc::new(Mutex::new(VecDeque::new()));

        let a1 = {
            let control = control.clone();
            thread::spawn(move || control.request_reset())
        };
        let a2 = {
            let control = control.clone();
            thread::spawn(move || control.request_reset())
        };
        let b = {
            let control = control.clone();
            let queue = queue.clone();
            thread::spawn(move || {
                for _ in 0..2 {
                    if control.is_snapshot_pending() {
                        let captured = control.generation();
                        queue.lock().unwrap().push_back(BatchEntry {
                            generation: captured,
                            is_snapshot: true,
                        });
                        control.mark_snapshot_delivered(captured);
                    } else {
                        let captured = control.generation();
                        queue.lock().unwrap().push_back(BatchEntry {
                            generation: captured,
                            is_snapshot: false,
                        });
                    }
                }
            })
        };

        let g1 = a1.join().unwrap();
        let g2 = a2.join().unwrap();
        b.join().unwrap();

        let mut gens = [g1, g2];
        gens.sort();
        assert_eq!(gens, [1, 2]);
        let final_gen = control.generation();
        assert_eq!(final_gen, 2);

        if !control.is_snapshot_pending() {
            let queued: Vec<_> = queue.lock().unwrap().iter().copied().collect();
            let at_final: Vec<_> = queued
                .iter()
                .filter(|b| b.is_snapshot && b.generation == final_gen)
                .collect();
            assert!(
                !at_final.is_empty(),
                "pending=false but no snapshot at gen {final_gen}; queued={queued:?}",
            );
        }
    });
}

/// One reset thread racing one circuit thread. Invariants for
/// `last_delivered_generation`:
///
/// * Every observed `last_delivered_generation` value must correspond to a
///   snapshot actually queued at that generation. If a reset at gen N is
///   overwritten by a later reset at gen N+1 before its snapshot lands, a
///   `mark_snapshot_delivered(N)` call must not advance the counter: its
///   CAS loses to the newer generation and the counter stays at the
///   previous value.
/// * The counter is monotonically non-decreasing.
/// * It never exceeds the current generation.
#[test]
fn last_delivered_matches_queue() {
    shuttle_run(|| {
        let control = Arc::new(OutputEndpointControl::new(
            true, /*already_sent=*/ false,
        ));
        let queue: Arc<Queue> = Arc::new(Mutex::new(VecDeque::new()));

        let reset = {
            let control = control.clone();
            thread::spawn(move || control.request_reset())
        };

        let circuit = {
            let control = control.clone();
            let queue = queue.clone();
            thread::spawn(move || {
                let mut observed_deliveries: Vec<u64> = Vec::new();
                for _ in 0..3 {
                    if control.is_snapshot_pending() {
                        let captured = control.generation();
                        queue.lock().unwrap().push_back(BatchEntry {
                            generation: captured,
                            is_snapshot: true,
                        });
                        control.mark_snapshot_delivered(captured);
                    }
                    observed_deliveries.push(control.last_delivered_generation());
                }
                observed_deliveries
            })
        };

        reset.join().unwrap();
        let observed = circuit.join().unwrap();

        // Monotonic non-decreasing.
        let mut prev = 0;
        for v in &observed {
            assert!(
                *v >= prev,
                "last_delivered_generation went backwards: {prev} -> {v}",
            );
            prev = *v;
        }

        // Bounded by the current generation.
        let final_gen = control.generation();
        let final_delivered = control.last_delivered_generation();
        assert!(
            final_delivered <= final_gen,
            "last_delivered_generation {final_delivered} exceeds current {final_gen}",
        );

        // Every delivered generation must match a queued snapshot at that
        // exact generation (not merely at a higher one). A reset that was
        // overwritten before its snapshot landed must not leave behind a
        // `last_delivered` value pointing to a gen that was never enqueued.
        let queued: Vec<_> = queue.lock().unwrap().iter().copied().collect();
        if final_delivered > 0 {
            let matches: Vec<_> = queued
                .iter()
                .filter(|b| b.is_snapshot && b.generation == final_delivered)
                .collect();
            assert!(
                !matches.is_empty(),
                "last_delivered={final_delivered} but no snapshot at that gen; queued={queued:?}",
            );
        }
    });
}
