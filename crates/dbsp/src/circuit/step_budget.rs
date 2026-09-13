//! A time budget for one step of an operator that opts into being bounded by
//! one.

use crate::circuit::operator_step_budget;
use std::time::{Duration, Instant};

/// Ends a step once the operator has held it long enough.
///
/// An operator that walks a long input has to yield periodically, or it holds
/// the step for as long as the whole input takes.  Bounding the walk by a count
/// of items bounds the wrong quantity: the workers meet at a barrier at the end
/// of every step, so what they wait through is time, and how much time an item
/// costs depends on whether it had to be read from storage.  A count that suits
/// a cached run is far too large for one that reads.
///
/// Reading the clock once per item would cost a few percent of a step whose
/// items take well under a microsecond each, so the budget is looked at once
/// every `stride` items.  The stride is re-estimated from the rate just
/// observed, aimed at the halfway point of what is left, which converges on the
/// deadline and keeps an overshoot to at most half of what remained rather than
/// a whole budget.  It also climbs by no more than [`Self::GROWTH`] at a time,
/// because the first stride measures a single item and an item that happened to
/// hit cache says nothing about the one that has to read: extrapolating from it
/// alone put a step 120 times past its deadline.  A step costs a few dozen
/// clock reads however many items it walks.
pub struct StepBudget {
    budget: Duration,
    deadline: Instant,
    /// When the stride now running began.
    started: Instant,
    /// Items left before the next look at the clock.
    countdown: u64,
    stride: u64,
    /// How many times the clock has been read.
    ///
    /// The whole design rests on this staying small next to the items walked,
    /// which is a claim worth being able to check rather than assert.  One
    /// increment on a path that runs a few dozen times a step costs nothing.
    reads: u64,
}

impl StepBudget {
    /// The stride every step starts from.
    ///
    /// One, so that an operator given no budget at all yields immediately
    /// rather than after a stride's worth of work, and so that the first
    /// estimate rests on a measurement rather than a guess.  Climbing back from
    /// here each step costs a couple of dozen clock reads, which is nothing
    /// beside the millions of items a step walks.
    const FIRST_STRIDE: u64 = 1;

    /// The most the stride may climb at one look at the clock.
    ///
    /// The estimate extrapolates from the stride just walked, so a stride that
    /// happened to be cheap projects a wildly optimistic rate.  Capping the
    /// climb bounds a step to this much past the stride that misled it, and
    /// costs only a logarithmic ramp to reach the right stride.
    const GROWTH: u64 = 4;

    /// A budget for a step starting now, sized by the circuit's configuration.
    pub fn new() -> Self {
        Self::with_budget(operator_step_budget())
    }

    pub fn with_budget(budget: Duration) -> Self {
        let now = Instant::now();
        Self {
            budget,
            deadline: now + budget,
            started: now,
            countdown: Self::FIRST_STRIDE,
            stride: Self::FIRST_STRIDE,
            reads: 0,
        }
    }

    /// Accounts for one item of work and says whether the step should end.
    ///
    /// Call once per item.  All but one call in `stride` is a decrement and a
    /// comparison.
    #[inline]
    pub fn spent(&mut self) -> bool {
        self.countdown = self.countdown.saturating_sub(1);
        self.countdown == 0 && self.out_of_time()
    }

    /// The one call in `stride` that looks at the clock.
    fn out_of_time(&mut self) -> bool {
        self.reads += 1;
        let now = Instant::now();
        // Zero left is spent, not "still inside": with a budget of nothing,
        // and under a clock coarse enough that both reads land in the same
        // tick, the difference decides whether the step ever ends.
        let remaining = self.deadline.saturating_duration_since(now);
        if remaining.is_zero() {
            return true;
        }

        // Aim the next look at the halfway point of what is left.  While the
        // stride is too short to time, climb instead of dividing by zero.
        let elapsed = now.saturating_duration_since(self.started);
        let reach = if elapsed.is_zero() {
            u64::MAX
        } else {
            (self.stride as u128 * remaining.as_nanos() / elapsed.as_nanos() / 2)
                .clamp(1, u64::MAX as u128) as u64
        };
        self.stride = reach.min(self.stride.saturating_mul(Self::GROWTH)).max(1);
        self.countdown = self.stride;
        self.started = now;
        false
    }

    /// How many times the budget has looked at the clock.
    pub fn clock_reads(&self) -> u64 {
        self.reads
    }

    /// Opens the budget for the next step.
    ///
    /// The stride starts over rather than carrying across, because by the end
    /// of a step it has decayed to whatever fits in the last sliver of the
    /// budget, which says nothing about the step to come.
    pub fn restart(&mut self) {
        let now = Instant::now();
        self.deadline = now + self.budget;
        self.started = now;
        self.countdown = Self::FIRST_STRIDE;
        self.stride = Self::FIRST_STRIDE;
    }
}

impl Default for StepBudget {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::StepBudget;
    use std::{
        hint::black_box,
        time::{Duration, Instant},
    };

    /// Burns roughly `nanos` of CPU, so a test can set a rate the budget has to
    /// measure rather than assume.
    fn work(nanos: u64) {
        let until = Instant::now() + Duration::from_nanos(nanos);
        while Instant::now() < until {
            black_box(0);
        }
    }

    /// A budget of nothing ends a step on the first item, which is what lets a
    /// test drive an operator one item at a time however fast the machine is.
    #[test]
    fn a_budget_of_nothing_ends_the_step_at_once() {
        let mut budget = StepBudget::with_budget(Duration::ZERO);
        for _ in 0..5 {
            assert!(budget.spent());
            budget.restart();
        }
    }

    /// The point of the budget: a step ends near its deadline, not far past it.
    #[test]
    fn a_step_ends_near_its_deadline() {
        const BUDGET: Duration = Duration::from_millis(20);

        for item_nanos in [0, 100, 10_000] {
            let mut budget = StepBudget::with_budget(BUDGET);
            let start = Instant::now();
            let mut items = 0u64;
            while !budget.spent() {
                work(item_nanos);
                items += 1;
                assert!(items < 100_000_000, "the budget never ended the step");
            }
            let held = start.elapsed();
            assert!(
                held >= BUDGET,
                "at {item_nanos} ns an item the step ended early, after {held:?}"
            );
            assert!(
                held < BUDGET * 2,
                "at {item_nanos} ns an item the step ran {held:?} against a {BUDGET:?} budget"
            );
        }
    }

    /// The budget has to cost far less than the work it bounds, which is what
    /// the stride buys: an operator whose items take well under a microsecond
    /// cannot afford a clock read on each one.
    #[test]
    fn the_clock_is_read_a_handful_of_times_a_step() {
        const BUDGET: Duration = Duration::from_millis(20);

        let mut budget = StepBudget::with_budget(BUDGET);
        let mut items = 0u64;
        while !budget.spent() {
            items += 1;
        }

        assert!(items > 100_000, "only {items} items fit in {BUDGET:?}");
        assert!(
            budget.clock_reads() < 64,
            "{} clock reads to walk {items} items, which is not amortized",
            budget.clock_reads()
        );
    }

    /// Restarting opens a fresh budget rather than continuing the old one.
    #[test]
    fn restarting_opens_a_fresh_budget() {
        const BUDGET: Duration = Duration::from_millis(20);

        let mut budget = StepBudget::with_budget(BUDGET);
        while !budget.spent() {}

        budget.restart();
        let start = Instant::now();
        let mut items = 0u64;
        while !budget.spent() {
            items += 1;
        }
        let held = start.elapsed();
        assert!(items > 100_000, "the second step walked only {items} items");
        assert!(
            (BUDGET..BUDGET * 2).contains(&held),
            "the second step ran {held:?} against a {BUDGET:?} budget"
        );
    }

    /// An operator whose items each take longer than the whole budget still
    /// ends its step on the first one rather than running a stride's worth.
    #[test]
    fn an_item_slower_than_the_budget_ends_the_step() {
        let mut budget = StepBudget::with_budget(Duration::from_micros(100));
        work(500_000);
        assert!(budget.spent());
    }
}
