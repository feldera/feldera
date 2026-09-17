use crate::storage::init;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub(crate) fn init_test_logger() {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("debug"))
        .expect("valid default filter");

    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(env_filter)
        .try_init();
    init();
}

/// Number of proptest cases for a test that drives a whole circuit.
///
/// Such a test builds and tears down a multi-worker [`Runtime`] per case, which
/// costs milliseconds rather than microseconds, so proptest's own default of
/// 256 makes a handful of tests dominate the suite.  Each run still draws fresh
/// seeds, so coverage accumulates across runs, and a failure that does surface
/// lands in `proptest-regressions` and is replayed from then on.
///
/// `PROPTEST_CASES` overrides this, as it does any case count: `proptest!`
/// passes the configuration through `contextualize_config`, which re-reads the
/// environment after the expression is evaluated.
///
/// [`Runtime`]: crate::Runtime
pub(crate) const CIRCUIT_CASES: u32 = 32;
