//! This code downloads and install postgres.
//!
//! It caches the downloaded binaries as well. Check the `pg_embed` crate for
//! more details.

use pg_embed::pg_enums::PgAuthMethod;
use pg_embed::pg_errors::PgEmbedError;
use pg_embed::pg_fetch::{PgFetchSettings, PG_V15};
use pg_embed::postgres::{PgEmbed, PgSettings};
use std::path::PathBuf;

/// Install and start an embedded postgres DB instance. This only runs if
/// the manager is started with postgres-embedded.
///
/// # Arguments
/// - `database_dir` - Path to the directory where the database files will be
///   stored.
/// - `persistent` - If true, the database will be persistent. If false, the
///   database will be deleted after the process exits.
/// - `port` - The port on which the database will be available on.
pub(crate) async fn install(
    database_dir: PathBuf,
    persistent: bool,
    port: Option<u16>,
) -> Result<PgEmbed, PgEmbedError> {
    let pg_settings = PgSettings {
        database_dir,
        port: port.unwrap_or(5432),
        user: "postgres".to_string(),
        password: "postgres".to_string(),
        auth_method: PgAuthMethod::Plain,
        persistent,
        timeout: None,
        migration_dir: None,
    };

    let fetch_settings = PgFetchSettings {
        version: PG_V15,
        ..Default::default()
    };

    let mut pg = PgEmbed::new(pg_settings, fetch_settings).await?;

    // Download, unpack, create password file and database cluster
    pg.setup().await?;
    pg.start_db().await?;

    Ok(pg)
}
