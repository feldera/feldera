//! Lower bound on the number of distinct programs compared by this manager in one UTC day.
//! Only counts leave the process. Restarts and the sampling cap can lower the reported total.
use super::Outcome;
use chrono::{NaiveDate, Utc};
use std::collections::HashSet;
use std::hash::{BuildHasher, RandomState};
use std::sync::{LazyLock, Mutex};
use tracing::info;

const REQUIRED_PROGRAMS: usize = 200;

#[derive(Default)]
struct Counts {
    matched: HashSet<u64>,
    different: u64,
    incomplete: u64,
}

impl Counts {
    fn record(&mut self, outcome: &Outcome, accepted: bool, fingerprint: impl FnOnce() -> u64) {
        match outcome {
            Outcome::Equal if accepted && self.matched.len() < REQUIRED_PROGRAMS => {
                self.matched.insert(fingerprint());
            }
            Outcome::Equal => {}
            Outcome::Different => self.different += 1,
            Outcome::Failed(_) | Outcome::TimedOut => self.incomplete += 1,
        }
    }
}

struct Day {
    date: NaiveDate,
    counts: Counts,
    // The key and individual fingerprints are never logged or persisted.
    hasher: RandomState,
}

impl Day {
    fn new(date: NaiveDate) -> Self {
        Self {
            date,
            counts: Counts::default(),
            hasher: RandomState::new(),
        }
    }

    fn record(&mut self, date: NaiveDate, sql: &str, accepted: bool, outcome: &Outcome) {
        if self.date != date {
            *self = Self::new(date);
        }
        self.counts
            .record(outcome, accepted, || self.hasher.hash_one(sql));
    }
}

pub(super) fn record(sql: &str, accepted: bool, outcome: &Outcome) {
    static DAY: LazyLock<Mutex<Day>> =
        LazyLock::new(|| Mutex::new(Day::new(Utc::now().date_naive())));
    let mut day = DAY.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let date = Utc::now().date_naive();
    day.record(date, sql, accepted, outcome);
    info!(%date, compiler_build = env!("VERGEN_GIT_SHA"), matched_distinct = day.counts.matched.len(), differences = day.counts.different,
          incomplete = day.counts.incomplete, "Rust optimizer daily shadow sample");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_programs_failures_and_day_boundaries_do_not_inflate_the_sample() {
        let today = NaiveDate::from_ymd_opt(2026, 11, 20).unwrap();
        let mut day = Day::new(today);
        day.record(today, "select 1", true, &Outcome::Equal);
        day.record(today, "select 1", true, &Outcome::Equal);
        day.record(today, "broken SQL", false, &Outcome::Equal);
        day.record(today, "select 2", true, &Outcome::Different);
        day.record(today, "select 3", true, &Outcome::TimedOut);
        assert_eq!(
            (
                day.counts.matched.len(),
                day.counts.different,
                day.counts.incomplete
            ),
            (1, 1, 1)
        );
        for n in 0..=REQUIRED_PROGRAMS {
            day.record(today, &format!("select {n}"), true, &Outcome::Equal);
        }
        assert_eq!(day.counts.matched.len(), REQUIRED_PROGRAMS);
        let tomorrow = today.succ_opt().unwrap();
        day.record(tomorrow, "select 1", true, &Outcome::Equal);
        assert_eq!(
            (
                day.counts.matched.len(),
                day.counts.different,
                day.counts.incomplete
            ),
            (1, 0, 0)
        );
    }
}
