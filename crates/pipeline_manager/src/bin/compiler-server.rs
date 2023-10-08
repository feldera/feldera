use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Command, FromArgMatches};

use colored::Colorize;
use pipeline_manager::compiler::Compiler;
use pipeline_manager::config::{CompilerConfig, DatabaseConfig};
use pipeline_manager::db::ProjectDB;
use tokio::spawn;
use tokio::sync::Mutex;

// Entrypoint to bring up the standalone compiler service.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let name = "[compiler]".cyan();
    pipeline_manager::logging::init_logging(name);
    let cli = Command::new("Feldera compiler service");
    let cli = DatabaseConfig::augment_args(cli);
    let cli = CompilerConfig::augment_args(cli);
    let matches = cli.get_matches();

    let database_config = DatabaseConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let compiler_config = CompilerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let compiler_config = compiler_config.canonicalize().unwrap();

    if compiler_config.precompile {
        Compiler::precompile_dependencies(&compiler_config).await?;
        return Ok(());
    }
    let db: ProjectDB = pipeline_manager::retries::retry_async(
        || async {
            ProjectDB::connect(
                &database_config,
                #[cfg(feature = "pg-embed")]
                None,
            )
            .await
        },
        30,
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let db = Arc::new(Mutex::new(db));
    let _compiler = spawn(async move {
        Compiler::run(&compiler_config.clone(), db).await.unwrap();
    });
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c event");
    Ok(())
}
