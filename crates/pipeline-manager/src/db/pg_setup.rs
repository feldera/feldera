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
pub(crate) async fn install(
    database_dir: PathBuf,
    persistent: bool,
    port: Option<u16>,
) -> Result<PostgreSQL, PostgreSQLError> {
    let mut pg_settings = PostgreSQLSettings::default();
    pg_settings.data_dir = database_dir;
    pg_settings.username = "postgres".to_string();
    pg_settings.password = "postgres".to_string();
    // Set to v15 in build.rs, note that we can't change/upgrade this without making
    // sure the database directories in ~/.feldera get upgraded too.
    pg_settings.version = VersionReq::from_str(env!("POSTGRESQL_VERSION"))?;
    pg_settings.temporary = !persistent;
    pg_settings.port = port.unwrap_or(5432);

    let mut postgresql = PostgreSQL::new(pg_settings);
    postgresql.setup().await?;
    postgresql.start().await?;

    Ok(postgresql)
}
