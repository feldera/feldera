use std::borrow::Cow;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "with-deltalake")]
use std::{error::Error, future::Future, pin::Pin};

use anyhow::{bail, Result as AnyResult};
use dashmap::DashMap;
use feldera_adapterlib::catalog::SerCursor;
use feldera_types::program_schema::SqlIdentifier;
#[cfg(feature = "with-deltalake")]
use futures::channel::oneshot;
use itertools::Itertools;
#[cfg(feature = "with-deltalake")]
use tokio::{spawn, task::JoinHandle};
use tracing::warn;

#[cfg(feature = "with-deltalake")]
pub(crate) fn root_cause(mut err: &dyn Error) -> &dyn Error {
    while let Some(source) = err.source() {
        err = source;
    }
    err
}

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

pub(crate) fn truncate_ellipse<'a>(s: &'a str, len: usize, ellipse: &str) -> Cow<'a, str> {
    if s.len() <= len {
        return Cow::Borrowed(s);
    } else if len == 0 {
        return Cow::Borrowed("");
    }

    let result = s.chars().take(len).chain(ellipse.chars()).collect();
    Cow::Owned(result)
}

/// For logging with a non-constant level.  From
/// <https://github.com/tokio-rs/tracing/issues/2730>
#[macro_export]
macro_rules! dyn_event {
    ($lvl:expr, $($arg:tt)+) => {
        match $lvl {
            ::tracing::Level::TRACE => ::tracing::trace!($($arg)+),
            ::tracing::Level::DEBUG => ::tracing::debug!($($arg)+),
            ::tracing::Level::INFO => ::tracing::info!($($arg)+),
            ::tracing::Level::WARN => ::tracing::warn!($($arg)+),
            ::tracing::Level::ERROR => ::tracing::error!($($arg)+),
        }
    };
}

/// A job queue that dispatches work to a pool of tokio tasks.
///
/// While the jobs can complete out-of-order, their outputs are consumed in the same order
/// they were enqueued. This is useful for implementing parallel parsing, where parsed
/// records must be fed into the circuit in the order they were received.
/*
                          sync
   ┌─────────────────────────────────────────────────────────┐
   │                           ┌───────┐                     │
   │                        ┌─►│worker1├────────┐            │
   │              jobs      │  └───────┘        │            │
   ▼          ┌─┬─┬─┬─┬─┬─┐ │  ┌───────┐        │       ┌────┴───┐
producer  ─┬─►│ │ │ │ │ │ ├─┼─►│worker2├────┐   │       │consumer│
           │  └─┴─┴─┴─┴─┴─┘ │  └───────┘    │   │       └────────┘
           │                │  ┌───────┐    │   │            ▲
           │                └─►│worker3├──┐ │   │            │
           │                   └───────┘  ▼ ▼   ▼            │
           │                             ┌─┬─┬─┬─┬─┬─┐       │
           └────────────────────────────►│ │ │ │ │ │ ├───────┘
                                         └─┴─┴─┴─┴─┴─┘
                                          completions

* Producer adds jobs to the job queue. A job consists of an input value and
  a completion one-shot channel where the worker will send the output of the job.

* The receiving side of the channel is pushed to the completions queue.

* Worker tasks dequeue jobs from the jobs queue and send the result to the
  one-shot channel associated with each job. The consumer receives the next
  item from the completion queue and waits for the output of the job.

* When the producer needs to wait for all jobs in the queue to complete, it
  sends a special Sync message to the completions queue. Upon receiving
  this message, the consumer sends an acknowledgement to the sync channel.
*/
#[cfg(feature = "with-deltalake")]
pub(crate) struct JobQueue<I, O> {
    /// The producer side of the job queue.
    job_sender: async_channel::Sender<(I, oneshot::Sender<O>)>,

    /// The producer side of the completions queue.
    completion_sender: async_channel::Sender<Completion<O>>,

    /// The receiving side of the sync channel.
    sync_receiver: async_channel::Receiver<()>,

    /// Worker tasks.
    workers: Vec<JoinHandle<()>>,

    /// Consumer task.
    consumer: JoinHandle<()>,
}

// Every message in the completions channel contains the receiving side of the
// oneshot channel that will contain the result of the completed job, or a special
// Sync message.
#[cfg(feature = "with-deltalake")]
enum Completion<O> {
    Completion(oneshot::Receiver<O>),
    Sync,
}

#[cfg(feature = "with-deltalake")]
impl<I, O> JobQueue<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    /// Create a job queue.
    ///
    /// # Arguments
    ///
    /// * `num_workers` - the number of threads in the worker pool. Must be >0.
    /// * `worker_builder` - a closure that returns a closure that each worker will execute for each job.
    ///   The outer closure is needed to allocate any resources needed by the worker.
    /// * `consumer_func` - closure that the consumer will execute for each completed job.
    pub(crate) fn new(
        num_workers: usize,
        worker_builder: impl Fn() -> Box<dyn FnMut(I) -> Pin<Box<dyn Future<Output = O> + Send>> + Send>,
        mut consumer_func: impl FnMut(O) + Send + 'static,
    ) -> Self {
        assert_ne!(num_workers, 0);

        // The jobs queue length is equal to the number of workers. This way, workers don't get
        // starved, but we also don't queue more data than necessary to keep the workers busy.
        // TODO: does it make sense to make queue length separately configurable?
        let (job_sender, job_receiver) =
            async_channel::bounded::<(I, oneshot::Sender<O>)>(num_workers);

        // The completion queue can contain at most one message per worker + one message per
        // job in the jobs queue plus the Sync message.
        let (completion_sender, completion_receiver) = async_channel::bounded(2 * num_workers + 1);
        let (sync_sender, sync_receiver) = async_channel::bounded(1);

        let workers = (0..num_workers)
            .map(move |_| {
                let mut worker_fn = worker_builder();

                let job_receiver = job_receiver.clone();
                spawn(async move {
                    loop {
                        let Ok((input, completion_sender)) = job_receiver.recv().await else {
                            return;
                        };
                        let result = worker_fn(input).await;
                        if completion_sender.send(result).is_err() {
                            return;
                        };
                    }
                })
            })
            .collect();

        let consumer = spawn(async move {
            loop {
                match completion_receiver.recv().await {
                    Err(_) => {
                        return;
                    }
                    Ok(Completion::Completion(receiver)) => {
                        let Ok(v) = receiver.await else {
                            continue;
                        };
                        consumer_func(v);
                    }
                    Ok(Completion::Sync) => {
                        if sync_sender.send(()).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        Self {
            job_sender,
            completion_sender,
            sync_receiver,
            workers,
            consumer,
        }
    }

    /// Push a new job to the queue. Blocks until there is space in the queue.
    pub(crate) async fn push_job(&self, job: I) {
        let (completion_sender, completion_receiver) = oneshot::channel();
        let _ = self.job_sender.send((job, completion_sender)).await;
        let _ = self
            .completion_sender
            .send(Completion::Completion(completion_receiver))
            .await;
    }

    /// Wait for all previously queued jobs to complete.
    pub(crate) async fn flush(&self) {
        let _ = self.completion_sender.send(Completion::Sync).await;
        let _ = self.sync_receiver.recv().await;
    }
}

#[cfg(feature = "with-deltalake")]
impl<I, O> Drop for JobQueue<I, O> {
    fn drop(&mut self) {
        self.consumer.abort();
        for worker in self.workers.drain(..) {
            worker.abort();
        }
    }
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
                if old > 0 {
                    Some(old - 1)
                } else {
                    None
                }
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
}

#[cfg(test)]
mod test {
    #[cfg(feature = "with-deltalake")]
    use std::sync::Mutex;
    use std::{
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };

    use crate::util::{RateLimitCheckResult, TokenBucketRateLimiter};

    #[cfg(feature = "with-deltalake")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_job_queue() {
        let result = Arc::new(Mutex::new(Vec::new()));
        let result_clone = result.clone();

        let job_queue = super::JobQueue::new(
            6,
            || Box::new(|i: u32| Box::pin(async move { i })),
            move |i| result_clone.lock().unwrap().push(i),
        );

        for i in 0..1000000 {
            job_queue.push_job(i).await;
        }

        job_queue.flush().await;

        let expected: Vec<u32> = (0..1000000).collect();

        assert_eq!(&*result.lock().unwrap(), &*expected);
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
