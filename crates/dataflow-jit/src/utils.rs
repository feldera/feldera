#[cfg(test)]
pub(crate) fn test_logger() {
    use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

    let filter = EnvFilter::try_from_env("DBSP_JIT")
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .init();
}
