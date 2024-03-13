use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Command, FromArgMatches};

use colored::Colorize;
use pipeline_manager::config::{DatabaseConfig, ProberConfig};
use pipeline_manager::db::ProjectDB;
use pipeline_manager::prober::run_prober;
use tokio::spawn;
use tokio::sync::Mutex;

// Entrypoint to bring up the standalone prober service.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let name = "[prober]".purple();
    pipeline_manager::logging::init_logging(name);
    let cli = Command::new("Feldera prober service");
    let cli = DatabaseConfig::augment_args(cli);
    let cli = ProberConfig::augment_args(cli);
    let matches = cli.get_matches();

    let database_config = DatabaseConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let prober_config = ProberConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let prober_config = prober_config.canonicalize().unwrap();

    let db: ProjectDB = pipeline_manager::retries::retry_async(
        || async {
            let db = ProjectDB::connect(
                &database_config,
                #[cfg(feature = "pg-embed")]
                None,
            )
            .await;
            if let Ok(d) = db {
                d.check_migrations().await
            } else {
                db
            }
        },
        30,
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let db = Arc::new(Mutex::new(db));
    let _prober = spawn(async move {
        run_prober(&prober_config.clone(), db).await.unwrap();
    });
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c event");
    Ok(())
}
