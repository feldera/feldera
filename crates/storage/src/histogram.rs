use std::{
    ops::RangeInclusive,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use itertools::Itertools;

const N_BUCKETS: usize = 92;

/// A histogram with exponential buckets.
///
/// This histogram maintains 92 buckets, one for each value or range below,
/// listing ranges by their lower endpoints:
///
/// - 0
/// - 1, 2, 3, ... 9
/// - 10, 20, 30, ... 90
/// - 100, 200, 300, ... 900
/// - 1000, 2000, 3000, ... 9000
/// - 10_000, 20_000, 30_000, ... 90_000
/// - 100_000, 200_000, 300_000, ... 900_000
/// - 1_000_000, 2_000_000, 3_000_000, ... 9_000_000
/// - 10_000_000, 20_000_000, 30_000_000, ... 90_000_000
/// - 100_000_000, 200_000_000, 300_000_000, ... 900_000_000
/// - 1_000_000_000, 2_000_000_000, 3_000_000_000, ... 9_000_000_000
/// - 10_000_000_000 through [u64::MAX].
#[derive(Debug)]
pub struct ExponentialHistogram {
    buckets: [AtomicU64; N_BUCKETS],
    sum: AtomicU64,
}

impl ExponentialHistogram {
    /// Constructs a new exponential histogram.
    pub const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; N_BUCKETS],
            sum: AtomicU64::new(0),
        }
    }

    /// Records `value` in the histogram.
    pub fn record(&self, value: impl TryInto<u64>) {
        if let Ok(value) = value.try_into() {
            self.buckets[number_to_bucket(value)].fetch_add(1, Ordering::Relaxed);
            self.sum.fetch_add(value, Ordering::Relaxed);
        }
    }

    /// Records the time elapsed since `start` in the histogram, as a count of
    /// microseconds.
    pub fn record_elapsed(&self, start: Instant) {
        self.record(start.elapsed().as_micros());
    }

    /// Returns a snapshot of the histogram.
    pub fn snapshot(&self) -> ExponentialHistogramSnapshot {
        ExponentialHistogramSnapshot {
            buckets: self
                .buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect_array()
                .unwrap(),
            sum: self.sum.load(Ordering::Relaxed),
        }
    }

    pub fn record_callback<F, T>(&self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        let start = Instant::now();
        let retval = f();
        self.record_elapsed(start);
        retval
    }
}

impl Default for ExponentialHistogram {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ExponentialHistogramSnapshot {
    buckets: [u64; N_BUCKETS],
    sum: u64,
}

impl ExponentialHistogramSnapshot {
    pub fn iter_buckets(&self) -> impl Iterator<Item = Bucket> + use<'_> {
        self.buckets
            .iter()
            .enumerate()
            .map(|(index, count)| Bucket {
                range: bucket_to_range(index),
                count: *count,
            })
    }
    pub fn sum(&self) -> u64 {
        self.sum
    }
}

pub struct Bucket {
    pub range: RangeInclusive<u64>,
    pub count: u64,
}

fn number_to_bucket(number: u64) -> usize {
    let bucket = match number {
        // buckets 0..=9
        0..10 => number,
        // buckets 10..=18
        10..100 => (number - 10) / 10 + 10,
        // buckets 19..=27
        100..1000 => (number - 100) / 100 + 19,
        // buckets 28..=36
        1000..10_000 => (number - 1000) / 1000 + 28,
        // buckets 37..=45
        10_000..100_000 => (number - 10_000) / 10_000 + 37,
        // buckets 46..=54
        100_000..1_000_000 => (number - 100_000) / 100_000 + 46,
        // buckets 55..=63
        1_000_000..10_000_000 => (number - 1_000_000) / 1_000_000 + 55,
        // buckets 64..=72
        10_000_000..100_000_000 => (number - 10_000_000) / 10_000_000 + 64,
        // buckets 73..=81
        100_000_000..1_000_000_000 => (number - 100_000_000) / 100_000_000 + 73,
        // buckets 82..=90
        1_000_000_000..10_000_000_000 => (number - 1_000_000_000) / 1_000_000_000 + 82,
        // bucket 91
        _ => 91,
    };
    bucket as usize
}

fn bucket_to_range(bucket: usize) -> RangeInclusive<u64> {
    let bucket = bucket as u64;
    fn bucket_range(index: u64, width: u64) -> RangeInclusive<u64> {
        let start = (index + 1) * width;
        let end = start + (width - 1);
        start..=end
    }
    match bucket {
        0..=9 => bucket..=bucket,
        10..=18 => bucket_range(bucket - 10, 10),
        19..=27 => bucket_range(bucket - 19, 100),
        28..=36 => bucket_range(bucket - 28, 1000),
        37..=45 => bucket_range(bucket - 37, 10_000),
        46..=54 => bucket_range(bucket - 46, 100_000),
        55..=63 => bucket_range(bucket - 55, 1_000_000),
        64..=72 => bucket_range(bucket - 64, 10_000_000),
        73..=81 => bucket_range(bucket - 73, 100_000_000),
        82..=90 => bucket_range(bucket - 82, 1_000_000_000),
        91 => 1_000_000_001..=u64::MAX,
        _ => unreachable!(),
    }
}
