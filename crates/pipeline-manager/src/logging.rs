use colored::ColoredString;
use env_logger::Env;
use log::warn;
use std::io::Write;

/// Initializes the logger by setting its filter and template.
/// By default, the logging level is set to `INFO`.
/// This can be overridden by setting the `RUST_LOG` environment variable.
pub fn init_logging(name: ColoredString) {
    if env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format(move |buf, record| {
            let t = chrono::Utc::now();
            let t = format!("{}", t.format("%Y-%m-%d %H:%M:%S"));
            let level_style = buf.default_level_style(record.level());
            writeln!(
                buf,
                "{} {level_style}{}{level_style:#} {} {}",
                t,
                record.level(),
                name,
                record.args()
            )
        })
        .try_init()
        .is_err()
    {
        warn!("Unable to initialize logging -- has it already been initialized?")
    }
}
