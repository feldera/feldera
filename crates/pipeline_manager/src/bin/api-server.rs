use std::sync::Arc;

use clap::{Args, Command, FromArgMatches};

use colored::Colorize;

use pipeline_manager::config::{ApiServerConfig, DatabaseConfig};
use pipeline_manager::db::ProjectDB;
use tokio::sync::Mutex;

// Entrypoint to bring up a standalone api-server.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let name = "[local-runner]".cyan();
    pipeline_manager::logging::init_logging(name);
    let cli = Command::new("Feldera local runner service");
    let cli = DatabaseConfig::augment_args(cli);
    let cli = ApiServerConfig::augment_args(cli);
    let matches = cli.get_matches();

    let database_config = DatabaseConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let api_config = ApiServerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let api_config = api_config.canonicalize().unwrap();
    let db = ProjectDB::connect(
        &database_config,
        #[cfg(feature = "pg-embed")]
        None,
    )
    .await
    .unwrap();
    let db = Arc::new(Mutex::new(db));

    // The api-server blocks forever
    pipeline_manager::api::run(db, api_config).await.unwrap();
    Ok(())
}
