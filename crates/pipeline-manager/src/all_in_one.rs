//! Runs the api-server, compiler and runner services in one process.
use crate::api::main::ApiDoc;
use crate::cluster_monitor::{LocalResourcesPoller, cluster_monitor};
use crate::compiler::main::{compiler_main, compiler_precompile};
#[cfg(feature = "postgresql_embedded")]
use crate::config::PgEmbedConfig;
use crate::config::{
    ApiServerConfig, CommonConfig, CompilerConfig, DatabaseConfig, LocalRunnerConfig,
};
use crate::db::storage_postgres::StoragePostgres;
use crate::events_cleaner::events_cleaner;
use crate::license::LicenseCheck;
use crate::platform_enable_unstable;
use crate::runner::main::runner_main;
use crate::runner::pipeline_executor::PipelineExecutor;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::info;
use utoipa::OpenApi;

/// Adds the CLI arguments of all services run by [`run`].
pub fn augment_args(cli: Command) -> Command {
    let cli = CommonConfig::augment_args(cli);
    #[cfg(feature = "postgresql_embedded")]
    let cli = PgEmbedConfig::augment_args(cli);
    let cli = DatabaseConfig::augment_args(cli);
    let cli = ApiServerConfig::augment_args(cli);
    let cli = CompilerConfig::augment_args(cli);
    LocalRunnerConfig::augment_args(cli)
}

/// Runs the services until the api-server stops, or performs `--dump-openapi` or `--precompile`.
pub async fn run<E: PipelineExecutor + 'static>(
    matches: &ArgMatches,
    license_check: Arc<RwLock<Option<LicenseCheck>>>,
    runner_config: impl FnOnce(LocalRunnerConfig) -> E::Config,
) -> anyhow::Result<()>
where
    E::Config: Send + 'static,
{
    let common_config = CommonConfig::from_arg_matches(matches)
        .map_err(|err| err.exit())
        .unwrap();
    if let Some(features) = &common_config.unstable_features {
        platform_enable_unstable(features);
    }
    #[cfg(feature = "postgresql_embedded")]
    let pg_embed_config = PgEmbedConfig::from_arg_matches(matches)
        .map_err(|err| err.exit())
        .unwrap();
    let api_config = ApiServerConfig::from_arg_matches(matches)
        .map_err(|err| err.exit())
        .unwrap();
    if api_config.dump_openapi {
        let openapi_json = ApiDoc::openapi().to_pretty_json()?;
        tokio::fs::write("openapi.json", openapi_json.as_bytes()).await?;
        return Ok(());
    }
    let compiler_config = CompilerConfig::from_arg_matches(matches)
        .map_err(|err| err.exit())
        .unwrap();
    let local_runner_config = LocalRunnerConfig::from_arg_matches(matches)
        .map_err(|err| err.exit())
        .unwrap();

    let common_config = common_config.canonicalize()?;
    #[cfg(feature = "postgresql_embedded")]
    let pg_embed_config = pg_embed_config.canonicalize()?;
    // `api_config` currently does not have any paths
    let compiler_config = compiler_config.canonicalize()?;
    let executor_config = runner_config(local_runner_config.canonicalize()?);

    if compiler_config.precompile {
        compiler_precompile(common_config, compiler_config).await?;
        info!("Pre-compilation finished");
        return Ok(());
    }
    let database_config = DatabaseConfig::from_arg_matches(matches)
        .map_err(|err| err.exit())
        .unwrap();
    let db: StoragePostgres = StoragePostgres::connect(
        &database_config,
        #[cfg(feature = "postgresql_embedded")]
        pg_embed_config.clone(),
    )
    .await
    .expect("Could not open connection to database");

    // Run migrations before starting any service
    db.run_migrations().await?;
    let db = Arc::new(Mutex::new(db));

    let db_clone = db.clone();
    let common_config_clone = common_config.clone();
    let compiler_config_clone = compiler_config.clone();

    // Running multiple compiler workers on the same machine is not yet supported
    // due to the HTTP server ran by compiler_main & cleanup status files.
    // For now, we run a single compiler worker in the pipeline manager.
    let worker_id = 0;
    let total_workers = 1;
    let _compiler = tokio::spawn(async move {
        compiler_main(
            common_config_clone,
            compiler_config_clone,
            db_clone,
            worker_id,
            total_workers,
            // The pipeline-manager does not have automatic restart if it fails, as such
            // it cannot exit when target is cleared to have any precomputed precompilation
            // reapplied.
            false,
        )
        .await
        .expect("Compiler server main failed");
    });

    // Spawn local runner
    let db_clone = db.clone();
    let common_config_clone = common_config.clone();
    let _local_runner = tokio::spawn(async move {
        runner_main::<E>(db_clone, common_config_clone, executor_config).await;
    });

    // Spawn cluster monitor
    let common_config_clone = common_config.clone();
    let db_clone = db.clone();
    tokio::spawn(async move {
        cluster_monitor(db_clone, common_config_clone, LocalResourcesPoller {}).await;
    });

    // Spawn events cleaner
    let db_clone = db.clone();
    let common_config_clone = common_config.clone();
    tokio::spawn(async move {
        events_cleaner(db_clone, common_config_clone).await;
    });

    // The api-server blocks forever
    crate::api::main::run(db, common_config, api_config, license_check)
        .await
        .expect("API server main failed");
    Ok(())
}
