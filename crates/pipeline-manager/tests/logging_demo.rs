use colored::Colorize;
use pipeline_manager::logging::init_logging;
use tracing::info;

#[test]
fn emits_sample_log() {
    // Force INFO output for the demo regardless of upstream defaults.
    std::env::set_var("RUST_LOG", "info");

    init_logging("[logging-demo]".cyan());
    info!("logging demo event");
    info!(
        demo_int = 42i64,
        demo_u64 = u64::MAX,
        demo_float = 3.1415f64,
        demo_neg_float = -2.5f64,
        demo_text = "sample",
        "typed field coverage"
    );
}
