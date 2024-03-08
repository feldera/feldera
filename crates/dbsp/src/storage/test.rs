use env_logger::Env;
use std::io::Write;

use crate::storage::init;

pub(crate) fn init_test_logger() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("debug"))
        .is_test(true)
        .format(move |buf, record| {
            let t = chrono::Utc::now();
            let t = format!("{}", t.format("%Y-%m-%d %H:%M:%S%.6f"));
            writeln!(
                buf,
                "{t} {} {}",
                buf.default_level_style(record.level()),
                record.args()
            )
        })
        .try_init();
    init();
}
