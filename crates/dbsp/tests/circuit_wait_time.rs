//! The circuit's wait time must reflect a worker's async runtime having nothing
//! to run.
//!
//! This was silently zero for two months: the scheduler's own wait accounting
//! only fires for DBSP-asynchronous operators, and once operators became
//! Rust-async, a blocked operator became a pending task rather than an idle
//! scheduler. The tests below pin the observable behaviour, both directions: a
//! skewed multi-worker circuit must report waiting, and a circuit with nothing
//! to wait for must not.

use dbsp::circuit::Circuit;
use dbsp::circuit::metadata::{
    CIRCUIT_CPU_TIME_SECONDS, CIRCUIT_NONBLOCKING_PERCENT, CIRCUIT_RUNTIME_SECONDS,
    CIRCUIT_WAIT_TIME_SECONDS, MetaItem, MetricId,
};
use dbsp::typed_batch::OrdZSet;
use dbsp::utils::Tup2;
use dbsp::{DBSPHandle, Runtime, operator::Generator};
use std::thread::sleep;
use std::time::Duration;

/// Sums a duration metric over all workers, in seconds.
fn duration_metric(handle: &mut DBSPHandle, metric: &MetricId) -> f64 {
    let profile = handle.retrieve_profile().unwrap();
    profile
        .worker_profiles
        .iter()
        .flat_map(|w| w.attribute_profile(metric).into_values())
        .map(|item| match item {
            MetaItem::Duration(d) => d.as_secs_f64(),
            other => panic!("expected a duration, got {other:?}"),
        })
        .sum()
}

/// The worst worker's percentage for `metric`, as (numerator, denominator).
fn percent_metric(handle: &mut DBSPHandle, metric: &MetricId) -> Vec<(u64, u64)> {
    let profile = handle.retrieve_profile().unwrap();
    profile
        .worker_profiles
        .iter()
        .flat_map(|w| w.attribute_profile(metric).into_values())
        .map(|item| match item {
            MetaItem::Percent {
                numerator,
                denominator,
            } => (numerator, denominator),
            other => panic!("expected a percent, got {other:?}"),
        })
        .collect()
}

/// Builds a circuit whose source optionally stalls worker 0, so its peers have
/// to wait for it at the exchange that `shard()` inserts.
fn skewed_circuit(workers: usize, stall: Duration, steps: usize) -> DBSPHandle {
    let (mut handle, _) = Runtime::init_circuit(workers, move |circuit| {
        let source = circuit.add_source(Generator::new(move || {
            if Runtime::worker_index() == 0 && !stall.is_zero() {
                sleep(stall);
            }
            let keys: Vec<Tup2<u64, i64>> = (0..64)
                .map(|k| Tup2(k * 7 + Runtime::worker_index() as u64, 1i64))
                .collect();
            OrdZSet::from_keys((), keys)
        }));
        source.shard().integrate().apply(|_| ());
        Ok(())
    })
    .unwrap();

    handle.enable_cpu_profiler().unwrap();
    handle.start_transaction().unwrap();
    for _ in 0..steps {
        handle.step().unwrap();
    }
    handle.commit_transaction().unwrap();
    handle
}

/// A worker stalled every step makes its peers wait, and that wait is reported.
#[test]
fn stalled_worker_is_reported_as_circuit_wait_time() {
    const WORKERS: usize = 4;
    const STEPS: usize = 20;
    const STALL: Duration = Duration::from_millis(5);

    let mut handle = skewed_circuit(WORKERS, STALL, STEPS);
    let wait = duration_metric(&mut handle, &CIRCUIT_WAIT_TIME_SECONDS);

    // Three of four workers wait out most of each stall. Half of the total
    // stall time is a deliberately loose floor: the point is that the metric
    // tracks the stall rather than reading zero.
    let stalled = STALL.as_secs_f64() * STEPS as f64;
    let floor = stalled * (WORKERS - 1) as f64 * 0.5;
    assert!(
        wait > floor,
        "circuit wait time {wait:.3}s should exceed {floor:.3}s with a \
         {STALL:?} stall on 1 of {WORKERS} workers over {STEPS} steps"
    );
    handle.kill().unwrap();
}

/// Nothing to wait for means no wait time: a single worker whose operators never
/// block must not accumulate any. Without this, a hook that counted parks
/// outside a step, or counted the runtime's own startup, would pass the test
/// above while reporting nonsense here.
#[test]
fn a_circuit_with_nothing_to_wait_for_reports_no_wait_time() {
    let mut handle = skewed_circuit(1, Duration::ZERO, 20);
    let wait = duration_metric(&mut handle, &CIRCUIT_WAIT_TIME_SECONDS);
    let runtime = duration_metric(&mut handle, &CIRCUIT_RUNTIME_SECONDS);
    assert!(
        wait < runtime * 0.5,
        "a single worker with no exchange waited {wait:.3}s of {runtime:.3}s"
    );
    handle.kill().unwrap();
}

/// The step budget has to close: CPU time and wait time are both parts of the
/// step's wall time, so neither may exceed it, and a busy circuit must show CPU
/// time rather than leaving it at the zero it used to report.
#[test]
fn step_time_decomposes_into_cpu_and_wait() {
    let mut handle = skewed_circuit(4, Duration::from_millis(2), 20);

    let runtime = duration_metric(&mut handle, &CIRCUIT_RUNTIME_SECONDS);
    let cpu = duration_metric(&mut handle, &CIRCUIT_CPU_TIME_SECONDS);
    let wait = duration_metric(&mut handle, &CIRCUIT_WAIT_TIME_SECONDS);

    assert!(cpu > 0.0, "circuit cpu time should not be zero");
    // A 10% tolerance covers the two clocks: the step is timed with `Instant`,
    // the CPU with `CLOCK_THREAD_CPUTIME_ID`, and the parks with a third set of
    // reads against a monotonic base.
    assert!(
        cpu + wait <= runtime * 1.1,
        "cpu {cpu:.3}s + wait {wait:.3}s should fit within runtime {runtime:.3}s"
    );

    for (numerator, denominator) in percent_metric(&mut handle, &CIRCUIT_NONBLOCKING_PERCENT) {
        assert!(denominator > 0, "nonblocking percent has no denominator");
        assert!(
            numerator <= denominator,
            "nonblocking percent {numerator}/{denominator} exceeds 100%"
        );
    }
    handle.kill().unwrap();
}
