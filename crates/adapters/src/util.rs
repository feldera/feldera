use std::borrow::Cow;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};

use anyhow::{Result as AnyResult, bail};
use dashmap::DashMap;
use feldera_adapterlib::catalog::SerCursor;
use feldera_types::program_schema::SqlIdentifier;
use itertools::Itertools;
use tracing::warn;

/// Operations over an indexed view.
#[derive(Debug)]
pub enum IndexedOperationType {
    Insert,
    Delete,
    Upsert,
}

/// Report unique key constraint violation in an output connector.
pub fn non_unique_key_error(
    view_name: &SqlIdentifier,
    index_name: &SqlIdentifier,
    error: &str,
    cursor: &mut dyn SerCursor,
) -> String {
    cursor.rewind_vals();
    let key_json = cursor.key_to_json().unwrap_or_else(|e| {
        serde_json::Value::String(format!(
            "(unable to display key: error converting record key to JSON format: {e})"
        ))
    });

    let mut updates = Vec::new();
    let mut counter = 0;

    // Print up to 10 updates. 3 is sufficient to show non-uniqueness.
    while cursor.val_valid() && counter < 10 {
        let w = cursor.weight();

        if w == 0 {
            cursor.step_val();
            continue;
        }

        updates.push(format!(
            "    {}: {:+}",
            cursor.val_to_json().unwrap_or_else(|e| {
                serde_json::Value::String(format!(
                    "(unable to display record: error converting record to JSON format: {e})"
                ))
            }),
            w
        ));

        cursor.step_val();
        counter += 1;
    }

    if cursor.val_valid() {
        updates.push("    ...".to_string());
    }

    let updates = updates.join("\n");

    format!(
        r#"Output connector configured with 'index={}' encountered multiple values with the same key. When configured with SQL index, the connector expects keys to be unique. To resolve this, either remove the 'index' attribute from the connector configuration or fix the '{}' view definition to ensure that '{}' is a unique index.
The offending key is: {}.
Error description: {error}.
List of updates associated with this key:
{updates}
        "#,
        index_name, view_name, index_name, key_json,
    )
}

/// Determine whether the key under an indexed cursor is an insert, delete or upsert.
pub fn indexed_operation_type(
    view_name: &SqlIdentifier,
    index_name: &SqlIdentifier,
    cursor: &mut dyn SerCursor,
) -> AnyResult<Option<IndexedOperationType>> {
    let mut found_insert = false;
    let mut found_delete = false;

    // First pass: determine the operation type.
    while cursor.val_valid() {
        let w = cursor.weight();

        if w == 0 {
            cursor.step_val();
            continue;
        }

        if w > 1 {
            bail!(non_unique_key_error(
                view_name,
                index_name,
                &format!(
                    "Record {} is inserted {w} times",
                    cursor.val_to_json().unwrap_or_default()
                ),
                cursor
            ));
        }

        if w < -1 {
            bail!(non_unique_key_error(
                view_name,
                index_name,
                &format!(
                    "Record {} is deleted {} times",
                    cursor.val_to_json().unwrap_or_default(),
                    -w
                ),
                cursor
            ));
        }

        if w == 1 {
            if found_insert {
                bail!(non_unique_key_error(
                    view_name,
                    index_name,
                    "Multiple new values for the same key",
                    cursor
                ));
            }

            found_insert = true;
        }

        if w == -1 {
            if found_delete {
                bail!(non_unique_key_error(
                    view_name,
                    index_name,
                    "Multiple deleted values for the same key",
                    cursor
                ));
            }

            found_delete = true;
        }

        cursor.step_val();
    }

    Ok(match (found_insert, found_delete) {
        (true, false) => Some(IndexedOperationType::Insert),
        (false, true) => Some(IndexedOperationType::Delete),
        (true, true) => Some(IndexedOperationType::Upsert),
        (false, false) => return Ok(None),
    })
}

/// Largest offset `<= index` that falls on a char boundary of `s`.
fn floor_char_boundary(s: &str, index: usize) -> usize {
    let mut index = index.min(s.len());
    while index > 0 && !s.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// Smallest offset `>= index` that falls on a char boundary of `s`.
fn ceil_char_boundary(s: &str, index: usize) -> usize {
    let mut index = index.min(s.len());
    while index < s.len() && !s.is_char_boundary(index) {
        index += 1;
    }
    index
}

/// Truncates `s` to `len` bytes, appending `ellipse` if anything was dropped.
///
/// The cut lands on a char boundary, so the retained prefix never exceeds `len`
/// bytes.
pub(crate) fn truncate_ellipse<'a>(s: &'a str, len: usize, ellipse: &str) -> Cow<'a, str> {
    if s.len() <= len {
        return Cow::Borrowed(s);
    } else if len == 0 {
        return Cow::Borrowed("");
    }

    let mut result = String::with_capacity(len + ellipse.len());
    result.push_str(&s[..floor_char_boundary(s, len)]);
    result.push_str(ellipse);
    Cow::Owned(result)
}

/// Truncates `s` to roughly `len` bytes by cutting out the middle, reporting how
/// many bytes were dropped.
///
/// Use this instead of [`truncate_ellipse`] when the tail of the string carries
/// as much information as the head. An `anyhow` error chain formatted with
/// `{:?}` is the motivating case: it leads with the outermost context and ends
/// with the root cause, so dropping the tail throws away the actual failure.
pub(crate) fn truncate_ellipse_middle(s: &str, len: usize) -> Cow<'_, str> {
    if s.len() <= len {
        return Cow::Borrowed(s);
    }

    let head_len = len / 2;
    let head_end = floor_char_boundary(s, head_len);
    let tail_start = ceil_char_boundary(s, s.len() - (len - head_len));

    format!(
        "{}\n... [{} bytes elided] ...\n{}",
        &s[..head_end],
        tail_start - head_end,
        &s[tail_start..]
    )
    .into()
}

pub(crate) fn missing_pipeline_identity_message(operation: &str) -> String {
    format!(
        "{operation}: pipeline has no system-assigned name (config.name), which is necessary to verify ownership"
    )
}

/// For logging with a non-constant level.  From
/// <https://github.com/tokio-rs/tracing/issues/2730>
#[macro_export]
macro_rules! dyn_event {
    ($lvl:expr_2021, $($arg:tt)+) => {
        match $lvl {
            ::tracing::Level::TRACE => ::tracing::trace!($($arg)+),
            ::tracing::Level::DEBUG => ::tracing::debug!($($arg)+),
            ::tracing::Level::INFO => ::tracing::info!($($arg)+),
            ::tracing::Level::WARN => ::tracing::warn!($($arg)+),
            ::tracing::Level::ERROR => ::tracing::error!($($arg)+),
        }
    };
}

/// Execute a set of `tasks` on a thread pool with `num_threads`.
///
/// Each of the `tasks` is a `(name, closure)` pair.  The names allow the names
/// of tasks that run a long time to be logged.
///
/// Will execute up to `num_threads` tasks in parallel.
///
/// # Error handling
///
/// If one of the tasks returns an error, this error is returned to the caller.
/// At this point any tasks that are not yet running will be canceled; however
/// tasks that are a already running will be allowed to finish; however tasks that
/// are already running will continue running, since we don't have a way to cancel them.
/// The function doesn't wait for these tasks to finish; they will continue running
/// in the background after the function returns.
///
// TODO:
// * add a flag to wait for all tasks to finish on error.
// * if we go async, it may be possible to cancel tasks.
//
// # Panics
//
// This function doesn't implement any form of panic handling. If a task can panic,
// you can wrap it in `catch_unwind` and convert a panic into an error.
pub(crate) fn run_on_thread_pool<I, T, E>(name: &str, num_threads: usize, tasks: I) -> Result<(), E>
where
    I: IntoIterator<Item = (String, Box<dyn FnOnce() -> Result<T, E> + Send>)>,
    T: Send + 'static,
    E: Send + Display + 'static,
{
    let thread_pool = threadpool::Builder::new()
        .num_threads(num_threads)
        .thread_name(name.to_string())
        .build();
    let (tx, rx) = std::sync::mpsc::channel();
    let cancel = Arc::new(AtomicBool::new(false));

    let mut names = Vec::new();
    for (name, task) in tasks {
        names.push(name.clone());
        let cancel = cancel.clone();
        let tx = tx.clone();
        thread_pool.execute(move || {
            if cancel.load(Ordering::Acquire) {
                return;
            }

            let result = task();
            if result.is_err() {
                cancel.store(true, Ordering::Release);
            }
            let _ = tx.send((result, name));
        })
    }

    let mut long_operation = LongOperationWarning::new(Duration::from_secs(5));
    while !names.is_empty() {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok((Err(error), name)) => {
                warn!("{name}: task failed: {error}");
                return Err(error);
            }
            Ok((Ok(_), name)) => {
                // Remove name of completed task from list.  We know that the
                // task name has be in there, hence the `unwrap()`.  Even if
                // there are duplicate names, we only remove one of them, so it
                // is safe in that case too.
                names.swap_remove(names.iter().position(|s| s == &name).unwrap());
            }
            Err(RecvTimeoutError::Disconnected) => unreachable!(),
            Err(RecvTimeoutError::Timeout) => (),
        }
        long_operation.check(|duration| {
            warn!(
                "tasks still running after {} seconds: {}",
                duration.as_secs(),
                names.iter().join(", ")
            )
        });
    }

    thread_pool.join();

    Ok(())
}

struct TokenBucket {
    /// Number of tokens currently available in the bucket.
    available_tokens: AtomicU32,

    /// Timestamp when the bucket was last refilled.
    /// Used to calculate how many tokens to add based on elapsed time.
    last_refill_ms: AtomicU64,

    /// Number of consecutive suppressed attempts since last successful consumption
    /// in the current suppression window.
    suppressed_count: AtomicU64,

    /// Timestamp of the first suppressed attempt in the current suppression window.
    /// 0 indicates no suppressions recorded yet.
    first_suppression_ms: AtomicU64,

    /// Timestamp of the last suppressed attempt in the current suppression window.
    /// 0 indicates no suppressions recorded yet.
    last_suppression_ms: AtomicU64,

    /// Maximum tokens the bucket can hold.
    max_tokens: u32,

    /// Milliseconds per token refill (== ceil(window_ms / capacity)).
    token_refill_interval_ms: u64,
}

impl TokenBucket {
    fn new(max_tokens: u32, refill_window: Duration, now: u64) -> Self {
        let refill_window_ms = refill_window.as_millis() as u64;

        // token_refill_interval = ceil(window_duration / max_tokens)
        let token_refill_interval_ms = if max_tokens == 0 {
            // checked_div_ceil isn't available for u64
            // so we check if max_tokens is 0 to avoid
            // panic caused due to div by 0
            1
        } else {
            refill_window_ms.div_ceil(max_tokens as u64)
        };

        Self {
            available_tokens: AtomicU32::new(max_tokens),
            last_refill_ms: AtomicU64::new(now),
            suppressed_count: AtomicU64::new(0),
            first_suppression_ms: AtomicU64::new(0),
            last_suppression_ms: AtomicU64::new(0),
            max_tokens,
            token_refill_interval_ms,
        }
    }

    /// Attempt to consume a token, returning the rate limit result if successful.
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe and atomic: it uses atomic operations to ensure that
    /// token consumption and suppression counters are updated correctly even when called
    /// concurrently from multiple threads.
    fn try_consume_token(&self) -> Option<RateLimitCheckResult> {
        if self
            .available_tokens
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |old| {
                if old > 0 { Some(old - 1) } else { None }
            })
            .is_ok()
        {
            // If there were suppressed attempts while tokens were exhausted,
            // capture and reset suppression counters and report them upstream.
            let suppressed = self.suppressed_count.swap(0, Ordering::AcqRel);
            let check_result = if suppressed > 0 {
                let first = self.first_suppression_ms.swap(0, Ordering::AcqRel);
                let last = self.last_suppression_ms.swap(0, Ordering::AcqRel);
                RateLimitCheckResult::AllowedAfterSuppression {
                    suppressed,
                    first_suppression: first,
                    last_suppression: last,
                }
            } else {
                RateLimitCheckResult::Allowed
            };
            return Some(check_result);
        }
        None
    }

    /// Try refilling tokens based on elapsed time.
    ///
    /// This method atomically updates `last_refill` and `available` to ensure that
    /// multiple threads do not over-refill tokens. The use of `compare_exchange` on
    /// `last_refill` ensures only one thread performs the refill for a given time window,
    /// while others will observe the updated timestamp and skip redundant refills.
    /// There is a possible race where multiple threads may attempt to refill simultaneously,
    /// but only one will succeed in updating `last_refill`, preventing double-counting tokens.
    fn try_refill_tokens(&self, now_ms: u64) {
        // Load the last refill timestamp. If no time has passed, nothing to do.
        let last = self.last_refill_ms.load(Ordering::Acquire);
        if now_ms <= last {
            return;
        }

        let elapsed = now_ms.saturating_sub(last);

        // Calculate how many tokens to add based on elapsed time and refill interval.
        //
        // NOTE: self.token_refill_interval_ms is guaranteed to be > 0 in constructor.
        // Hence this division is safe.
        let tokens_to_add = elapsed / self.token_refill_interval_ms;

        if tokens_to_add == 0 {
            return;
        }

        // Only one thread should update last_refill_ms for this interval. Use
        // compare_exchange to ensure only one thread performs the refill and
        // the increment of available_tokens.
        if self
            .last_refill_ms
            .compare_exchange(last, now_ms, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            // Safely add tokens but cap to capacity.
            self.available_tokens
                .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |old| {
                    Some((old.saturating_add(tokens_to_add as u32)).min(self.max_tokens))
                })
                .ok();
        }
    }

    /// Checks if a token can be consumed, refilling if needed.
    ///
    /// Returns:
    /// - `RateLimitCheckResult::Allowed` if a token is available and consumed.
    /// - `RateLimitCheckResult::Suppressed` if no tokens are available; increments suppression counters.
    /// - `RateLimitCheckResult::AllowedAfterSuppression` if a token is consumed after previous suppressions,
    ///   including the count and timing of suppressed attempts.
    ///
    /// Suppression logic: When tokens are exhausted, each failed attempt increments a suppression counter.
    /// Upon the next successful token consumption, the suppression count and timing are reported and reset.
    pub fn check(&self, now_ms: u64) -> RateLimitCheckResult {
        // Fast path: try to consume immediately
        if let Some(result) = self.try_consume_token() {
            return result;
        }

        // Attempt refill if needed
        self.try_refill_tokens(now_ms);

        // Retry consume after potential refill
        if let Some(result) = self.try_consume_token() {
            return result;
        }

        // No tokens - record suppression and reject
        let prev_count = self.suppressed_count.fetch_add(1, Ordering::AcqRel);
        if prev_count == 0 {
            self.first_suppression_ms.store(now_ms, Ordering::Release);
        }
        self.last_suppression_ms.store(now_ms, Ordering::Release);

        RateLimitCheckResult::Suppressed
    }
}

#[derive(Debug, Clone)]
pub(crate) enum RateLimitCheckResult {
    Allowed,
    AllowedAfterSuppression {
        suppressed: u64,
        first_suppression: u64, // ms since start
        last_suppression: u64,  // ms since start
    },
    Suppressed,
}

#[derive(Clone)]
/// A rate limiter keyed by `K`.
///
/// Each key is associated with a `TokenBucket`. The limiter tracks a global
/// `start_instant` which is used to convert `Instant` durations into a
/// monotonic millisecond counter used by the buckets.
pub(crate) struct TokenBucketRateLimiter<K: Eq + Hash> {
    /// Map of keys to their corresponding token buckets.
    buckets: Arc<DashMap<K, TokenBucket>>,

    /// Maximum number of tokens per bucket.
    max_tokens: u32,

    /// Window within which a maximum of `max_tokens` events are allowed.
    /// In other words: up to `max_tokens` events are permitted per `refill_window` duration.
    refill_window: Duration,

    /// Start instant for measuring elapsed time.
    start_instant: Instant,
}

impl<K: Eq + Hash> TokenBucketRateLimiter<K> {
    pub fn new(max_tokens: u32, window: Duration) -> Self {
        Self {
            buckets: Arc::new(DashMap::new()),
            max_tokens,
            refill_window: window,
            start_instant: Instant::now(),
        }
    }

    #[inline]
    pub(crate) fn now_ms(&self) -> u64 {
        self.start_instant.elapsed().as_millis() as u64
    }

    pub fn check(&self, key: K) -> RateLimitCheckResult {
        let now = self.now_ms();

        // fast path: get read lock if key already exits
        if let Some(bucket) = self.buckets.get(&key) {
            return bucket.check(now);
        }

        // slower path: create entry of new bucket for key
        let bucket = self
            .buckets
            .entry(key)
            .or_insert_with(|| TokenBucket::new(self.max_tokens, self.refill_window, now));
        bucket.check(now)
    }
}

pub(crate) struct LongOperationWarning {
    start: Instant,
    warn_threshold: Duration,
}

impl LongOperationWarning {
    pub fn new(warn_threshold: Duration) -> Self {
        Self {
            start: Instant::now(),
            warn_threshold,
        }
    }

    pub fn check(&mut self, warn: impl FnOnce(Duration)) {
        let elapsed = self.start.elapsed();
        if elapsed >= self.warn_threshold {
            warn(elapsed);
            self.warn_threshold *= 2;
        }
    }

    pub fn next_warning(&self) -> Instant {
        self.start + self.warn_threshold
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

/// Run any function in a single-worker dbsp::Runtime configured with POSIX storage backend.
///
/// Used to test code that needs to write to storage without creating a circuit.
#[cfg(test)]
pub(crate) fn run_in_posix_runtime<F>(
    min_storage_bytes: Option<usize>,
    min_step_storage_bytes: Option<usize>,
    test_fn: F,
) where
    F: FnOnce() + Send + 'static,
{
    use dbsp::Runtime;
    use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
    use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
    use std::sync::{Arc, Mutex};

    let temp = tempfile::tempdir().expect("failed to create temp dir for storage");
    let cconf = CircuitConfig::with_workers(1).with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: temp.path().to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions {
                min_storage_bytes,
                min_step_storage_bytes,
                ..StorageOptions::default()
            },
        )
        .expect("failed to configure storage"),
    ));

    let test_fn: Arc<Mutex<Option<F>>> = Arc::new(Mutex::new(Some(test_fn)));
    let test_fn_cloned = Arc::clone(&test_fn);
    let handle = Runtime::run(cconf, move |_parker| {
        if let Some(test_fn) = test_fn_cloned.lock().unwrap().take() {
            test_fn();
        }
    })
    .unwrap();

    handle.kill().unwrap();
}

#[cfg(test)]
mod test {
    use std::{
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };

    use crate::util::{
        RateLimitCheckResult, TokenBucketRateLimiter, truncate_ellipse, truncate_ellipse_middle,
    };

    // -------- Truncation tests -------- //

    #[test]
    fn truncate_ellipse_leaves_short_strings_alone() {
        assert_eq!(truncate_ellipse("abc", 3, "..."), "abc");
        assert_eq!(truncate_ellipse("abc", 4, "..."), "abc");
        assert_eq!(truncate_ellipse("", 0, "..."), "");
    }

    #[test]
    fn truncate_ellipse_bounds_the_prefix_in_bytes() {
        assert_eq!(truncate_ellipse("abcdef", 3, "..."), "abc...");
        assert_eq!(truncate_ellipse("abcdef", 0, "..."), "");

        // A multi-byte string must be measured in bytes, not chars: 10 chars of
        // 3 bytes each may not be reported as fitting in a 10-byte budget.
        let s = "日".repeat(10);
        let truncated = truncate_ellipse(&s, 10, "...");
        assert_eq!(truncated, "日日日...");
        assert!(truncated.len() - "...".len() <= 10);
    }

    #[test]
    fn truncate_ellipse_middle_keeps_both_ends() {
        let s = format!("head{}tail", "x".repeat(1000));
        let truncated = truncate_ellipse_middle(&s, 100);

        assert!(truncated.starts_with("head"), "{truncated}");
        assert!(truncated.ends_with("tail"), "{truncated}");
        assert!(truncated.contains("bytes elided"), "{truncated}");
        // The retained text is bounded even though the marker adds to it.
        assert!(truncated.len() < 100 + 64, "{}", truncated.len());
    }

    #[test]
    fn truncate_ellipse_middle_leaves_short_strings_alone() {
        assert_eq!(truncate_ellipse_middle("abcdef", 6), "abcdef");
        assert_eq!(truncate_ellipse_middle("abcdef", 7), "abcdef");
        assert!(!truncate_ellipse_middle("abcdef", 6).contains("elided"));
    }

    // Cutting in the middle of a multi-byte char would panic on a slice.
    #[test]
    fn truncate_ellipse_middle_respects_char_boundaries() {
        let s = "日".repeat(100);
        for len in 0..32 {
            let truncated = truncate_ellipse_middle(&s, len);
            assert!(truncated.contains("bytes elided"), "len={len}");
        }
    }

    // The elided-byte count must add up: head + elided + tail == original.
    #[test]
    fn truncate_ellipse_middle_reports_the_elided_byte_count() {
        let s = "abcdefghijklmnopqrstuvwxyz".repeat(10);
        let truncated = truncate_ellipse_middle(&s, 40);

        let elided: usize = truncated
            .split("... [")
            .nth(1)
            .and_then(|s| s.split(" bytes elided] ...").next())
            .expect("elision marker")
            .parse()
            .expect("elided byte count");
        let retained =
            truncated.len() - "\n... [ bytes elided] ...\n".len() - elided.to_string().len();
        assert_eq!(retained + elided, s.len());
    }

    // -------- Basic single-thread tests -------- //

    #[test]
    fn test_basic_allowance_and_suppression() {
        let limiter = TokenBucketRateLimiter::new(3, Duration::from_millis(300));
        let key = "user1";

        // First 3 allowed
        for _ in 0..3 {
            assert!(matches!(limiter.check(key), RateLimitCheckResult::Allowed));
        }
        // 4th suppressed
        assert!(matches!(
            limiter.check(key),
            RateLimitCheckResult::Suppressed
        ));
    }

    #[test]
    fn test_suppression_and_refill_with_timing() {
        let limiter = TokenBucketRateLimiter::new(2, Duration::from_millis(200));
        let key = "user2";

        // Burn tokens
        assert!(matches!(limiter.check(key), RateLimitCheckResult::Allowed));
        assert!(matches!(limiter.check(key), RateLimitCheckResult::Allowed));

        // Now suppressed a few times
        let _ = limiter.check(key); // suppressed
        thread::sleep(Duration::from_millis(50));
        let _ = limiter.check(key); // suppressed again

        // window duration is of 200ms, and 2 tokens are allowed within it,
        // that means time taken to refill 1 token would be 100ms
        // ie. token_refill_interval = window_duration / max_tokens
        // = 200ms / 2 = 100ms
        //
        // we sleep for 120ms ( 20ms extra ) just to make sure we are past it.
        thread::sleep(Duration::from_millis(120));

        // This call should be allowed WITH suppression info
        match limiter.check(key) {
            RateLimitCheckResult::AllowedAfterSuppression {
                suppressed,
                first_suppression,
                last_suppression,
            } => {
                assert!(
                    suppressed >= 2,
                    "Expected >=2 suppressed, got {}",
                    suppressed
                );
                assert!(last_suppression >= first_suppression);
                let now = limiter.now_ms();
                let first_suppressed_elapsed = (now - first_suppression) as f64 / 1000.0;
                let last_suppressed_elapsed = (now - last_suppression) as f64 / 1000.0;
                println!(
                    "Suppressed {suppressed} errors in last {first_suppressed_elapsed:.2}s (most recently {last_suppressed_elapsed:.2}s ago, tag={key}) due to excessive rate"
                );
            }
            other => panic!("Expected AllowedWithSuppression, got {:?}", other),
        }
    }

    #[test]
    fn test_multiple_refills() {
        let limiter = TokenBucketRateLimiter::new(2, Duration::from_millis(200));
        let key = "user3";

        limiter.check(key);
        limiter.check(key);

        // refill both tokens
        thread::sleep(Duration::from_millis(250));

        let mut allowed_count = 0;
        for _ in 0..2 {
            match limiter.check(key) {
                RateLimitCheckResult::Allowed
                | RateLimitCheckResult::AllowedAfterSuppression { .. } => {
                    allowed_count += 1;
                }
                _ => {}
            }
        }
        assert_eq!(allowed_count, 2);
    }

    #[test]
    fn test_independent_keys() {
        let limiter = TokenBucketRateLimiter::new(1, Duration::from_millis(300));
        let key1 = "alice";
        let key2 = "bob";

        assert!(matches!(limiter.check(key1), RateLimitCheckResult::Allowed));
        assert!(matches!(limiter.check(key2), RateLimitCheckResult::Allowed));

        assert!(matches!(
            limiter.check(key1),
            RateLimitCheckResult::Suppressed
        ));
        assert!(matches!(
            limiter.check(key2),
            RateLimitCheckResult::Suppressed
        ));
    }

    // -------- Concurrency tests -------- //

    // These tests verify that the TokenBucketRateLimiter correctly tracks and reports
    // the number and timing of suppressed attempts across multiple threads.
    //
    // Steps:
    // 1. Burn all available tokens for a key.
    // 2. Spawn multiple threads, synchronizing their start so all attempt to consume a token
    //    while the bucket is empty, ensuring all are suppressed.
    // 3. After all threads have attempted and been suppressed, sleep to allow a token to refill.
    // 4. The next check should succeed and return AllowedAfterSuppression, reporting the total
    //    number of suppressed attempts and their timing.
    //
    // The use of a barrier ensures all threads hit the suppressed state before any refill occurs,
    // making the test robust against timing and scheduling variations.
    #[test]
    fn test_concurrent_token_consumption() {
        let limiter = Arc::new(TokenBucketRateLimiter::new(5, Duration::from_secs(60)));
        let key = "concurrent";

        let num_threads = 10;

        // create a barrier that can block a given number of threads.
        // A barrier will block n-1 threads which call wait()
        // and then wake up all threads at once when the nth thread calls wait().
        let barrier = Arc::new(Barrier::new(num_threads + 1));

        let mut handles = vec![];
        for _ in 0..num_threads {
            let limiter = limiter.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                // Synchronize thread start so all hit suppressed state before refill
                barrier.wait();
                limiter.check(key)
            }));
        }

        // Wait for all threads to be ready, then release them
        barrier.wait();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        let allowed = results
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    RateLimitCheckResult::Allowed
                        | RateLimitCheckResult::AllowedAfterSuppression { .. }
                )
            })
            .count();
        let suppressed = results
            .iter()
            .filter(|r| matches!(r, RateLimitCheckResult::Suppressed))
            .count();

        assert_eq!(allowed, 5);
        assert_eq!(suppressed, 5);
    }

    #[test]
    fn test_suppressed_count_and_timing_across_threads() {
        let limiter = Arc::new(TokenBucketRateLimiter::new(2, Duration::from_millis(600)));
        let key = "threaded_suppress";

        let num_threads = 5;
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let ready_barrier = Arc::new(Barrier::new(num_threads + 1));

        let mut handles = vec![];
        for _ in 0..num_threads {
            let limiter = limiter.clone();
            let barrier = barrier.clone();
            let ready_barrier = ready_barrier.clone();
            handles.push(thread::spawn(move || {
                // Signal ready
                ready_barrier.wait();
                // Wait for main thread to burn tokens
                barrier.wait();
                limiter.check(key)
            }));
        }

        // Wait for all threads to be ready
        ready_barrier.wait();
        // Burn 2 tokens immediately before releasing threads
        assert!(matches!(limiter.check(key), RateLimitCheckResult::Allowed));
        assert!(matches!(limiter.check(key), RateLimitCheckResult::Allowed));
        // Release all threads
        barrier.wait();

        for res in handles {
            assert!(matches!(
                res.join().unwrap(),
                RateLimitCheckResult::Suppressed
            ));
        }

        thread::sleep(Duration::from_millis(320)); // refill 1

        // This call should flush suppressed info
        match limiter.check(key) {
            RateLimitCheckResult::AllowedAfterSuppression {
                suppressed,
                first_suppression,
                last_suppression,
            } => {
                assert!(suppressed >= num_threads as u64);
                assert!(last_suppression >= first_suppression);
                let now = limiter.now_ms();
                let first_suppressed_elapsed = (now - first_suppression) as f64 / 1000.0;
                let last_suppressed_elapsed = (now - last_suppression) as f64 / 1000.0;
                println!(
                    "Suppressed {suppressed} errors in last {first_suppressed_elapsed:.2}s (most recently {last_suppressed_elapsed:.2}s ago, tag={key}) due to excessive rate"
                );
            }
            _ => panic!("Expected AllowedWithSuppression"),
        }
    }

    #[test]
    fn test_concurrent_independent_keys() {
        let limiter = Arc::new(TokenBucketRateLimiter::new(1, Duration::from_millis(300)));

        let mut handles = vec![];
        for user in &["alice", "bob", "carol", "dave"] {
            let limiter = limiter.clone();
            let key = user.to_string();
            handles.push(thread::spawn(move || limiter.check(key)));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        for r in results {
            assert!(matches!(r, RateLimitCheckResult::Allowed));
        }
    }
}
