//! Waiting in a test for a condition to hold.

use std::future::Future;
use std::sync::LazyLock;
use std::thread::sleep;
use std::time::{Duration, Instant};

/// The time limit of the waits that take the default when `FELDERA_TEST_WAIT_MS` is not set.
const DEFAULT_TIMEOUT_MS: u64 = 600_000;

/// Returns the time limit, in milliseconds, of the waits that take the default: 600 s, or the
/// value of the `FELDERA_TEST_WAIT_MS` environment variable.
///
/// A mutation run sets the variable lower, so that a mutant that keeps rows from arriving
/// fails in seconds rather than after the full wait.
///
/// Panics if the variable is set but is not a number of milliseconds.
pub fn default_timeout_ms() -> u64 {
    static TIMEOUT_MS: LazyLock<u64> =
        LazyLock::new(|| timeout_ms_from(std::env::var("FELDERA_TEST_WAIT_MS").ok()));
    *TIMEOUT_MS
}

/// Returns the time limit that `FELDERA_TEST_WAIT_MS`, with the value `variable`, sets.
fn timeout_ms_from(variable: Option<String>) -> u64 {
    match variable {
        None => DEFAULT_TIMEOUT_MS,
        Some(text) => text.trim().parse().unwrap_or_else(|_| {
            panic!("FELDERA_TEST_WAIT_MS is not a number of milliseconds: '{text}'")
        }),
    }
}

/// Wait for `predicate` to become `true`.
///
/// Returns the number of milliseconds elapsed or `Err(())` on timeout.
#[allow(clippy::result_unit_err)]
pub fn wait<P>(mut predicate: P, timeout_ms: u64) -> Result<u64, ()>
where
    P: FnMut() -> bool,
{
    let start = Instant::now();
    let timeout = Duration::from_millis(timeout_ms);

    while !predicate() {
        if start.elapsed() >= timeout {
            return Err(());
        }
        sleep(Duration::from_millis(10));
    }

    Ok(elapsed_ms(start))
}

/// Wait for `predicate` to become `true`.
///
/// Returns the number of milliseconds elapsed or `Err(())` on timeout.
#[allow(clippy::result_unit_err)]
pub async fn async_wait<P, Fut>(predicate: P, timeout_ms: u64) -> Result<u64, ()>
where
    P: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    let timeout = Duration::from_millis(timeout_ms);

    while !predicate().await {
        if start.elapsed() >= timeout {
            return Err(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    Ok(elapsed_ms(start))
}

/// Returns the milliseconds since `start`.
fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::timeout_ms_from;

    /// Without the variable the waits take 600 s; with it, its value.
    #[test]
    fn the_variable_sets_the_default_timeout() {
        assert_eq!(timeout_ms_from(None), 600_000);
        assert_eq!(timeout_ms_from(Some("2500".to_string())), 2_500);
    }

    #[test]
    #[should_panic(expected = "FELDERA_TEST_WAIT_MS is not a number of milliseconds")]
    fn a_variable_that_is_not_a_number_panics() {
        timeout_ms_from(Some("ten seconds".to_string()));
    }
}
