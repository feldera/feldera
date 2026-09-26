/// A binary that brings up all three of the api-server, compiler and local
/// runner services.
use clap::Command;

use colored::Colorize;
use feldera_observability as observability;
use pipeline_manager::runner::local_runner::LocalRunner;
use pipeline_manager::{all_in_one, ensure_default_crypto_provider, init_fd_limit};
use std::sync::Arc;
use tokio::sync::RwLock;

fn main() -> anyhow::Result<()> {
    ensure_default_crypto_provider();
    init_fd_limit();
    pipeline_manager::logging::init_service_logging(
        "[manager]".cyan(),
        feldera_observability::json_logging::ServiceName::Manager,
    );
    if let Some(provider) = rustls::crypto::CryptoProvider::get_default() {
        observability::fips::log_rustls_provider_fips_status(
            "startup default provider",
            provider.fips(),
        );
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let matches =
                all_in_one::augment_args(Command::new("Pipeline manager CLI")).get_matches();
            all_in_one::run::<LocalRunner>(&matches, Arc::new(RwLock::new(None)), |config| config)
                .await
        })
}
