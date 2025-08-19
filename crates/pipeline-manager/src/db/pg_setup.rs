//! This code downloads and install postgres.
//!
//! It caches the downloaded binaries as well. Check the `pg_embed` crate for
//! more details.

use std::env;
use std::path::PathBuf;
use std::str::FromStr;

use postgresql_embedded::{
    Error as PostgreSQLError, PostgreSQL, Settings as PostgreSQLSettings, VersionReq,
};

/// Install and start an embedded postgres DB instance. This only runs if
/// the manager is started with postgres-embedded.
///
/// # Arguments
/// - `database_dir` - Path to the directory where the database files will be
///   stored.
/// - `persistent` - If true, the database will be persistent. If false, the
///   database will be deleted after the process exits.
/// - `port` - The port on which the database will be available on.
#[allow(clippy::field_reassign_with_default)]
pub(crate) async fn install(
    database_dir: PathBuf,
    persistent: bool,
    port: Option<u16>,
) -> Result<PostgreSQL, PostgreSQLError> {
    let pg_settings = PostgreSQLSettings {
        data_dir: database_dir,
        username: "postgres".to_string(),
        password: "postgres".to_string(),
        version: VersionReq::from_str(env!("POSTGRESQL_VERSION"))?,
        temporary: !persistent,
        port: port.unwrap_or(5432),
        ..Default::default()
    };

    let mut postgresql = PostgreSQL::new(pg_settings);
    postgresql.setup().await?;
    postgresql.start().await?;

    Ok(postgresql)
}
