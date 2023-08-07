use std::sync::Arc;

use clap::{Args, Command, FromArgMatches};

use colored::Colorize;

use pipeline_manager::config::{DatabaseConfig, ManagerConfig};
use pipeline_manager::db::ProjectDB;
use tokio::sync::Mutex;

// Entrypoint to bring up a standalone api-server.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let name = "[local-runner]".cyan();
    pipeline_manager::logging::init_logging(name);
    let cli = Command::new("Feldera local runner service");
    let cli = DatabaseConfig::augment_args(cli);
    let cli = ManagerConfig::augment_args(cli);
    let matches = cli.get_matches();

    let database_config = DatabaseConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let manager_config = ManagerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let manager_config = manager_config.canonicalize().unwrap();
    let db = ProjectDB::connect(
        &database_config,
        #[cfg(feature = "pg-embed")]
        None,
    )
    .await
    .unwrap();
    let db = Arc::new(Mutex::new(db));

    let listener = pipeline_manager::api::create_listener(manager_config.clone())?;
    // The api-server blocks forever
    pipeline_manager::api::run(listener, db, manager_config)
        .await
        .unwrap();
    Ok(())
}
