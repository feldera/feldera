use std::time::Duration;

use clap::{Args, Command, FromArgMatches};

use colored::Colorize;

use pipeline_manager::config::DatabaseConfig;
use pipeline_manager::db::{DBError, ProjectDB};

// A binary to run DB migrations
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let name = "[migrations]".magenta();
    pipeline_manager::logging::init_logging(name);
    let cli = Command::new("Feldera DB migrations");
    let cli = DatabaseConfig::augment_args(cli);
    let matches = cli.get_matches();
    let database_config = DatabaseConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let _: Result<ProjectDB, DBError> = pipeline_manager::retries::retry_async(
        || async {
            let db = ProjectDB::connect(
                &database_config,
                #[cfg(feature = "pg-embed")]
                None,
            )
            .await?;
            db.run_migrations().await?;
            Ok(db)
        },
        30,
        Duration::from_secs(1),
    )
    .await;
    Ok(())
}
