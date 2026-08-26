//! Metric history from `GET /v0/pipelines/{name}/time_series`.
//!
//! The server keeps 60 one-second samples; the console additionally accumulates
//! its own longer history so charts survive beyond the server window.

use serde_json::Value;
use std::collections::VecDeque;

use super::json;

/// One sample: processed records, memory, and storage at an instant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimePoint {
    pub at_millis: i64,
    pub processed_records: i64,
    pub memory_bytes: i64,
    pub storage_bytes: i64,
}

/// The server-side sample window, newest last.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimeSeries {
    pub points: Vec<TimePoint>,
}

impl TimeSeries {
    pub fn from_json(series: &Value) -> Self {
        let mut points: Vec<TimePoint> = json::array(series, "samples")
            .iter()
            .filter_map(|sample| {
                Some(TimePoint {
                    at_millis: json::timestamp_millis(sample, "t")?,
                    processed_records: json::integer_or_zero(sample, "r"),
                    memory_bytes: json::integer_or_zero(sample, "m"),
                    storage_bytes: json::integer_or_zero(sample, "s"),
                })
            })
            .collect();
        points.sort_by_key(|point| point.at_millis);
        Self { points }
    }
}

/// A bounded, deduplicated accumulation of [`TimePoint`]s across refreshes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricHistory {
    points: VecDeque<TimePoint>,
    capacity: usize,
}

impl Default for MetricHistory {
    fn default() -> Self {
        // 10 minutes of one-second samples.
        Self::with_capacity(600)
    }
}

impl MetricHistory {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "metric history needs a positive capacity");
        Self {
            points: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Merge a fresh server window, skipping samples already recorded.
    /// A sample older than the newest recorded one means the pipeline was
    /// restarted; the history resets so rates stay truthful.
    pub fn merge(&mut self, series: &TimeSeries) {
        let Some(first_new) = series.points.first() else {
            return;
        };
        if self
            .points
            .back()
            .is_some_and(|last| first_new.at_millis < last.at_millis && series.points.len() > 1)
        {
            let known_tail = self
                .points
                .back()
                .map(|last| last.at_millis)
                .unwrap_or(i64::MIN);
            let all_older = series
                .points
                .iter()
                .all(|point| point.at_millis <= known_tail);
            if all_older {
                self.points.clear();
            }
        }
        let newest_known = self
            .points
            .back()
            .map(|point| point.at_millis)
            .unwrap_or(i64::MIN);
        for point in &series.points {
            if point.at_millis > newest_known {
                if self.points.len() == self.capacity {
                    self.points.pop_front();
                }
                self.points.push_back(*point);
            }
        }
    }

    pub fn clear(&mut self) {
        self.points.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    pub fn latest(&self) -> Option<&TimePoint> {
        self.points.back()
    }

    /// Records-per-second between consecutive samples, one value per gap.
    pub fn throughput_series(&self) -> Vec<(f64, f64)> {
        self.points
            .iter()
            .zip(self.points.iter().skip(1))
            .filter_map(|(earlier, later)| {
                let gap_millis = later.at_millis.saturating_sub(earlier.at_millis);
                if gap_millis <= 0 {
                    return None;
                }
                let records = later
                    .processed_records
                    .saturating_sub(earlier.processed_records);
                Some((
                    later.at_millis as f64 / 1_000.0,
                    records as f64 * 1_000.0 / gap_millis as f64,
                ))
            })
            .collect()
    }

    /// Memory samples as chart points.
    pub fn memory_series(&self) -> Vec<(f64, f64)> {
        self.points
            .iter()
            .map(|point| (point.at_millis as f64 / 1_000.0, point.memory_bytes as f64))
            .collect()
    }

    /// Storage samples as chart points.
    pub fn storage_series(&self) -> Vec<(f64, f64)> {
        self.points
            .iter()
            .map(|point| (point.at_millis as f64 / 1_000.0, point.storage_bytes as f64))
            .collect()
    }

    /// Throughput over the most recent gap, in records per second.
    pub fn current_throughput(&self) -> Option<f64> {
        self.throughput_series().last().map(|(_, rate)| *rate)
    }

    /// Mean throughput over the retained window.
    pub fn average_throughput(&self) -> Option<f64> {
        let first = self.points.front()?;
        let last = self.points.back()?;
        let window_millis = last.at_millis.saturating_sub(first.at_millis);
        if window_millis <= 0 {
            return None;
        }
        let records = last
            .processed_records
            .saturating_sub(first.processed_records);
        Some(records as f64 * 1_000.0 / window_millis as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::{MetricHistory, TimePoint, TimeSeries};
    use serde_json::json;

    fn series(samples: &[(i64, i64)]) -> TimeSeries {
        TimeSeries {
            points: samples
                .iter()
                .map(|(at_millis, processed_records)| TimePoint {
                    at_millis: *at_millis,
                    processed_records: *processed_records,
                    memory_bytes: 7,
                    storage_bytes: 8,
                })
                .collect(),
        }
    }

    #[test]
    fn parses_and_sorts_the_server_window() {
        let parsed = TimeSeries::from_json(&json!({
            "now": "2026-01-02T03:05:00Z",
            "samples": [
                {"t": "2026-01-02T03:04:06Z", "r": 20, "m": 1, "s": 2},
                {"t": "2026-01-02T03:04:05Z", "r": 10, "m": 1, "s": 2},
                {"t": "not a time", "r": 1, "m": 1, "s": 2}
            ]
        }));
        assert_eq!(parsed.points.len(), 2);
        assert!(parsed.points[0].at_millis < parsed.points[1].at_millis);
        assert_eq!(parsed.points[0].processed_records, 10);
    }

    #[test]
    fn parses_epoch_milli_timestamps_as_the_server_sends_them() {
        let parsed = TimeSeries::from_json(&json!({
            "now": 1_787_564_700_000i64,
            "samples": [
                {"t": 1_787_564_660_666i64, "r": 0, "m": 90_243_072, "s": 119},
                {"t": 1_787_564_661_666i64, "r": 5_000, "m": 90_243_072, "s": 119}
            ]
        }));
        assert_eq!(parsed.points.len(), 2);
        assert_eq!(parsed.points[1].processed_records, 5_000);
    }

    #[test]
    fn merge_appends_only_unseen_samples() {
        let mut history = MetricHistory::with_capacity(10);
        history.merge(&series(&[(1_000, 10), (2_000, 30)]));
        history.merge(&series(&[(2_000, 30), (3_000, 60)]));
        assert_eq!(history.throughput_series().len(), 2);
        assert_eq!(history.latest().unwrap().processed_records, 60);
    }

    #[test]
    fn merge_resets_after_a_pipeline_restart() {
        let mut history = MetricHistory::with_capacity(10);
        history.merge(&series(&[(10_000, 500), (11_000, 600)]));
        history.merge(&series(&[(1_000, 5), (2_000, 10)]));
        assert_eq!(history.points.len(), 2);
        assert_eq!(history.latest().unwrap().processed_records, 10);
    }

    #[test]
    fn capacity_evicts_the_oldest_point() {
        let mut history = MetricHistory::with_capacity(2);
        history.merge(&series(&[(1_000, 1), (2_000, 2), (3_000, 3)]));
        assert_eq!(history.points.len(), 2);
        assert_eq!(history.points.front().unwrap().at_millis, 2_000);
    }

    #[test]
    fn throughput_uses_per_gap_rates() {
        let mut history = MetricHistory::with_capacity(10);
        history.merge(&series(&[(1_000, 0), (2_000, 500), (4_000, 1_500)]));
        let rates: Vec<f64> = history
            .throughput_series()
            .into_iter()
            .map(|(_, rate)| rate)
            .collect();
        assert_eq!(rates, vec![500.0, 500.0]);
        assert_eq!(history.current_throughput(), Some(500.0));
        assert_eq!(history.average_throughput(), Some(500.0));
    }

    #[test]
    fn empty_history_yields_no_rates() {
        let history = MetricHistory::default();
        assert!(history.is_empty());
        assert_eq!(history.current_throughput(), None);
        assert_eq!(history.average_throughput(), None);
        assert!(history.memory_series().is_empty());
        assert!(history.storage_series().is_empty());
    }

    #[test]
    #[should_panic(expected = "positive capacity")]
    fn zero_capacity_is_rejected() {
        MetricHistory::with_capacity(0);
    }
}
