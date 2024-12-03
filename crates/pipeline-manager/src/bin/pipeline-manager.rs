/// A binary that brings up all three of the api-server, compiler and local
/// runner services.
use clap::{Args, Command, FromArgMatches};

use colored::Colorize;
use pipeline_manager::api::ApiDoc;
use pipeline_manager::compiler::main::{compiler_main, compiler_precompile};
#[cfg(feature = "pg-embed")]
use pipeline_manager::config::PgEmbedConfig;
use pipeline_manager::config::{
    ApiServerConfig, CommonConfig, CompilerConfig, DatabaseConfig, LocalRunnerConfig,
};
use pipeline_manager::db::storage_postgres::StoragePostgres;
use pipeline_manager::runner::local_runner::LocalRunner;
use pipeline_manager::runner::main::runner_main;
use pipeline_manager::{ensure_default_crypto_provider, init_fd_limit};
use std::sync::Arc;
use tokio::sync::Mutex;
use utoipa::OpenApi;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    ensure_default_crypto_provider();
    init_fd_limit();

    // Create env logger.
    let name = "[manager]".cyan();
    pipeline_manager::logging::init_logging(name);

    let cli = Command::new("Pipeline manager CLI");
    let cli = CommonConfig::augment_args(cli);
    #[cfg(feature = "pg-embed")]
    let cli = PgEmbedConfig::augment_args(cli);
    let cli = DatabaseConfig::augment_args(cli);
    let cli = ApiServerConfig::augment_args(cli);
    let cli = CompilerConfig::augment_args(cli);
    let cli = LocalRunnerConfig::augment_args(cli);
    let matches = cli.get_matches();
    let common_config = CommonConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    #[cfg(feature = "pg-embed")]
    let pg_embed_config = PgEmbedConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let api_config = ApiServerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    if api_config.dump_openapi {
        let openapi_json = ApiDoc::openapi().to_pretty_json()?;
        tokio::fs::write("openapi.json", openapi_json.as_bytes()).await?;
        return Ok(());
    }
    let compiler_config = CompilerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let local_runner_config = LocalRunnerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();

    #[cfg(feature = "pg-embed")]
    let pg_embed_config = pg_embed_config.canonicalize()?;
    // `api_config` currently does not have any paths
    let compiler_config = compiler_config.canonicalize()?;
    let local_runner_config = local_runner_config.canonicalize()?;

    let metrics_handle = pipeline_manager::metrics::init();
    if compiler_config.precompile {
        compiler_precompile(common_config, compiler_config).await?;
        return Ok(());
    }
    let database_config = DatabaseConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let db: StoragePostgres = StoragePostgres::connect(
        &database_config,
        #[cfg(feature = "pg-embed")]
        pg_embed_config,
    )
    .await
    .expect("Could not open connection to database");

    // Run migrations before starting any service
    db.run_migrations().await?;
    let db = Arc::new(Mutex::new(db));
    let db_clone = db.clone();
    let common_config_clone = common_config.clone();
    let _compiler = tokio::spawn(async move {
        compiler_main(common_config_clone, compiler_config, db_clone)
            .await
            .expect("Compiler server main failed");
    });
    let db_clone = db.clone();
    let common_config_clone = common_config.clone();
    let _local_runner = tokio::spawn(async move {
        runner_main::<LocalRunner>(
            db_clone,
            common_config_clone,
            local_runner_config.clone(),
            local_runner_config.runner_main_port,
        )
        .await
        .expect("Local runner main failed");
    });
    pipeline_manager::metrics::create_endpoint(metrics_handle, db.clone()).await;
    // The api-server blocks forever
    pipeline_manager::api::run(db, common_config, api_config)
        .await
        .expect("API server main failed");
    Ok(())
}
