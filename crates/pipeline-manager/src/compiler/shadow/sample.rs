//! Lower bound on the number of distinct programs compared by this manager in one UTC day.
//! Only counts leave the process. Restarts and the sampling cap can lower the reported total.
use super::Outcome;
use chrono::{NaiveDate, Utc};
use std::collections::{HashSet, VecDeque};
use std::hash::{BuildHasher, RandomState};
use std::sync::{LazyLock, Mutex};
use tracing::info;

const REQUIRED_PROGRAMS: usize = 200;
const MAX_RECENT_PACKAGES: usize = 32;

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
    packages: VecDeque<(Option<String>, Counts)>,
    // The key and individual fingerprints are never logged or persisted.
    hasher: RandomState,
}

impl Day {
    fn new(date: NaiveDate) -> Self {
        Self {
            date,
            packages: VecDeque::new(),
            hasher: RandomState::new(),
        }
    }

    fn record(
        &mut self,
        date: NaiveDate,
        package: Option<&str>,
        sql: &str,
        accepted: bool,
        outcome: &Outcome,
    ) -> &Counts {
        if self.date != date {
            *self = Self::new(date);
        }
        let entry = self
            .packages
            .iter()
            .position(|(id, _)| id.as_deref() == package)
            .and_then(|index| self.packages.remove(index))
            .unwrap_or_else(|| (package.map(str::to_owned), Counts::default()));
        if self.packages.len() == MAX_RECENT_PACKAGES {
            self.packages.pop_front();
        }
        let (_, counts) = self.packages.push_back_mut(entry);
        // A worker without a verifiable package may veto a day but cannot qualify one.
        counts.record(outcome, accepted && package.is_some(), || {
            self.hasher.hash_one(sql)
        });
        counts
    }
}

pub(super) fn record(package: Option<&str>, sql: &str, accepted: bool, outcome: &Outcome) {
    static DAY: LazyLock<Mutex<Day>> =
        LazyLock::new(|| Mutex::new(Day::new(Utc::now().date_naive())));
    let mut day = DAY.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let date = Utc::now().date_naive();
    let counts = day.record(date, package, sql, accepted, outcome);
    info!(%date, compiler_build = env!("VERGEN_GIT_SHA"), package_sha256 = package.unwrap_or("unidentified"),
          matched_distinct = counts.matched.len(), differences = counts.different,
          incomplete = counts.incomplete, "Rust optimizer daily shadow sample");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interleaved_packages_keep_separate_counts_and_unknown_matches_never_qualify() {
        let date = NaiveDate::from_ymd_opt(2026, 11, 20).unwrap();
        let mut day = Day::new(date);
        day.record(date, Some("a"), "sql 1", true, &Outcome::Equal);
        let b = day.record(date, Some("b"), "sql 2", true, &Outcome::Equal);
        assert_eq!(b.matched.len(), 1);
        let a = day.record(date, Some("a"), "sql 3", true, &Outcome::Equal);
        assert_eq!(a.matched.len(), 2);
        day.record(date, None, "sql 1", true, &Outcome::Equal);
        let unknown = day.record(date, None, "sql 2", true, &Outcome::Different);
        assert_eq!((unknown.matched.len(), unknown.different), (0, 1));
    }

    #[test]
    fn repeated_programs_failures_and_day_boundaries_do_not_inflate_the_sample() {
        let today = NaiveDate::from_ymd_opt(2026, 11, 20).unwrap();
        let mut day = Day::new(today);
        day.record(today, Some("a"), "select 1", true, &Outcome::Equal);
        day.record(today, Some("a"), "select 1", true, &Outcome::Equal);
        day.record(today, Some("a"), "broken SQL", false, &Outcome::Equal);
        day.record(today, Some("a"), "select 2", true, &Outcome::Different);
        let counts = day.record(today, Some("a"), "select 3", true, &Outcome::TimedOut);
        assert_eq!(
            (counts.matched.len(), counts.different, counts.incomplete),
            (1, 1, 1)
        );
        for n in 0..=REQUIRED_PROGRAMS {
            day.record(
                today,
                Some("a"),
                &format!("select {n}"),
                true,
                &Outcome::Equal,
            );
        }
        assert_eq!(
            day.record(today, Some("a"), "select 1", true, &Outcome::Equal)
                .matched
                .len(),
            REQUIRED_PROGRAMS
        );
        let tomorrow = today.succ_opt().unwrap();
        let counts = day.record(tomorrow, Some("a"), "select 1", true, &Outcome::Equal);
        assert_eq!(
            (counts.matched.len(), counts.different, counts.incomplete),
            (1, 0, 0)
        );
    }
}
