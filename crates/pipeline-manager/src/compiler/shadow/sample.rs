//! Lower bound on the number of distinct programs compared by this manager in one UTC day.
//! Only counts leave the process. Restarts and the sampling cap can lower the reported total.
use super::{Outcome, PipelineId};
use chrono::{NaiveDate, Utc};
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::hash::{BuildHasher, RandomState};
use std::sync::{LazyLock, Mutex};
use tracing::info;

const MAX_PROGRAMS: usize = 500;
const MAX_RECENT_PACKAGES: usize = 32;

#[derive(Default)]
struct Counts {
    pipelines: BTreeSet<PipelineId>,
    matched: HashSet<u64>,
    different: u64,
    incomplete: u64,
}

impl Counts {
    fn record(
        &mut self,
        outcome: &Outcome,
        accepted: bool,
        pipeline: PipelineId,
        fingerprint: impl FnOnce() -> u64,
    ) {
        match outcome {
            Outcome::Equal if accepted && self.matched.len() < MAX_PROGRAMS => {
                if !self.pipelines.contains(&pipeline) && self.matched.insert(fingerprint()) {
                    self.pipelines.insert(pipeline);
                }
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
        pipeline: PipelineId,
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
        counts.record(outcome, accepted && package.is_some(), pipeline, || {
            self.hasher.hash_one(sql)
        });
        counts
    }
}

pub(super) fn record(
    package: Option<&str>,
    pipeline: PipelineId,
    sql: &str,
    accepted: bool,
    outcome: &Outcome,
) {
    static DAY: LazyLock<Mutex<Day>> =
        LazyLock::new(|| Mutex::new(Day::new(Utc::now().date_naive())));
    let mut day = DAY.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let date = Utc::now().date_naive();
    let counts = day.record(date, package, pipeline, sql, accepted, outcome);
    info!(%date, compiler_build = env!("VERGEN_GIT_SHA"), package_sha256 = package.unwrap_or("unidentified"),
          matched_distinct = counts.matched.len(), differences = counts.different,
          difference_this_attempt = u8::from(*outcome == Outcome::Different),
          incomplete = counts.incomplete, "Rust optimizer daily shadow sample");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipeline(n: u128) -> PipelineId {
        PipelineId(uuid::Uuid::from_u128(n))
    }

    #[test]
    fn interleaved_packages_keep_separate_counts_and_unknown_matches_never_qualify() {
        let date = NaiveDate::from_ymd_opt(2026, 11, 20).unwrap();
        let mut day = Day::new(date);
        day.record(date, Some("a"), pipeline(1), "sql 1", true, &Outcome::Equal);
        let b = day.record(date, Some("b"), pipeline(2), "sql 2", true, &Outcome::Equal);
        assert_eq!(b.matched.len(), 1);
        let a = day.record(date, Some("a"), pipeline(3), "sql 3", true, &Outcome::Equal);
        assert_eq!(a.matched.len(), 2);
        day.record(date, None, pipeline(1), "sql 1", true, &Outcome::Equal);
        let unknown = day.record(date, None, pipeline(2), "sql 2", true, &Outcome::Different);
        assert_eq!((unknown.matched.len(), unknown.different), (0, 1));
    }

    #[test]
    fn repeated_programs_failures_and_day_boundaries_do_not_inflate_the_sample() {
        let (a, b, c, d) = (pipeline(1), pipeline(2), pipeline(3), pipeline(4));
        let today = NaiveDate::from_ymd_opt(2026, 11, 20).unwrap();
        let mut day = Day::new(today);
        day.record(today, Some("a"), a, "select 1", true, &Outcome::Equal);
        day.record(today, Some("a"), a, "select 1", true, &Outcome::Equal);
        day.record(today, Some("a"), a, "edited SQL", true, &Outcome::Equal);
        day.record(today, Some("a"), d, "select 1", true, &Outcome::Equal);
        day.record(today, Some("a"), b, "broken SQL", false, &Outcome::Equal);
        day.record(today, Some("a"), b, "select 2", true, &Outcome::Different);
        let counts = day.record(today, Some("a"), c, "select 3", true, &Outcome::TimedOut);
        assert_eq!(
            (counts.matched.len(), counts.different, counts.incomplete),
            (1, 1, 1)
        );
        for n in 0..=MAX_PROGRAMS {
            day.record(
                today,
                Some("a"),
                pipeline(n as u128),
                &format!("select {n}"),
                true,
                &Outcome::Equal,
            );
        }
        assert_eq!(
            day.record(today, Some("a"), a, "select 1", true, &Outcome::Equal)
                .matched
                .len(),
            500
        );
        let tomorrow = today.succ_opt().unwrap();
        let counts = day.record(tomorrow, Some("a"), a, "select 1", true, &Outcome::Equal);
        assert_eq!(
            (counts.matched.len(), counts.different, counts.incomplete),
            (1, 0, 0)
        );
    }
}
