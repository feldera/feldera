use colored::ColoredString;
use env_logger::Env;
use std::io::Write;

pub fn init_logging(name: ColoredString) {
    // By default, logging is set to INFO level for the Feldera crates:
    // - "pipeline_manager" for the pipeline-manager crate
    // - "feldera_types" for the feldera-types crate
    // For all others, the WARN level is used.
    // Note that this can be overridden by setting the RUST_LOG environment variable.
    let _ = env_logger::Builder::from_env(
        Env::default().default_filter_or("warn,pipeline_manager=info,feldera_types=info"),
    )
    .format(move |buf, record| {
        let t = chrono::Utc::now();
        let t = format!("{}", t.format("%Y-%m-%d %H:%M:%S"));
        writeln!(
            buf,
            "{} {} {} {}",
            t,
            buf.default_level_style(record.level()),
            name,
            record.args()
        )
    })
    .try_init();
}
