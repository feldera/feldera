use deadpool_postgres::Transaction;
use log::debug;
use pipeline_types::config::{ConnectorConfig, TransportConfig};
use uuid::Uuid;

use crate::auth::TenantId;

use super::{DBError, ProjectDB};

use std::fmt::{self, Display};

use utoipa::ToSchema;

use serde::{Deserialize, Serialize};
use tokio_postgres::types::ToSql;

/// Unique connector id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct ConnectorId(
    #[cfg_attr(test, proptest(strategy = "super::test::limited_uuid()"))] pub Uuid,
);
impl Display for ConnectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Connector descriptor.
#[derive(Deserialize, Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
pub(crate) struct ConnectorDescr {
    pub connector_id: ConnectorId,
    pub name: String,
    pub description: String,
    pub config: ConnectorConfig,
}

/// Validates the connector transport configuration.
/// In particular, checks that http_input and http_output
/// are not created directly in connectors via the API.
fn validate_connector_transport(config: &ConnectorConfig) -> Result<(), DBError> {
    match config.transport {
        TransportConfig::HttpInput => Err(DBError::InvalidConnectorTransport {
            reason: "Transport type http_input cannot be created directly".to_string(),
        }),
        TransportConfig::HttpOutput => Err(DBError::InvalidConnectorTransport {
            reason: "Transport type http_output cannot be created directly".to_string(),
        }),
        _ => Ok(()),
    }
}

pub(crate) async fn new_connector(
    db: &ProjectDB,
    tenant_id: TenantId,
    id: Uuid,
    name: &str,
    description: &str,
    config: &ConnectorConfig,
    txn: Option<&Transaction<'_>>,
) -> Result<ConnectorId, DBError> {
    debug!("new_connector {name} {description} {config:?}");
    validate_connector_transport(config)?;
    let query = "INSERT INTO connector (id, name, description, config, tenant_id) VALUES($1, $2, $3, $4, $5)";
    let row = if let Some(txn) = txn {
        let stmt = txn.prepare_cached(query).await?;
        txn.execute(
            &stmt,
            &[&id, &name, &description, &config.to_yaml(), &tenant_id.0],
        )
        .await
    } else {
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;
        manager
            .execute(
                &stmt,
                &[&id, &name, &description, &config.to_yaml(), &tenant_id.0],
            )
            .await
    };
    row.map_err(ProjectDB::maybe_unique_violation)
        .map_err(|e| ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None))?;
    Ok(ConnectorId(id))
}

pub(crate) async fn list_connectors(
    db: &ProjectDB,
    tenant_id: TenantId,
) -> Result<Vec<ConnectorDescr>, DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached("SELECT id, name, description, config FROM connector WHERE tenant_id = $1")
        .await?;
    let rows = manager.query(&stmt, &[&tenant_id.0]).await?;

    let mut result = Vec::with_capacity(rows.len());

    for row in rows {
        result.push(ConnectorDescr {
            connector_id: ConnectorId(row.get(0)),
            name: row.get(1),
            description: row.get(2),
            config: ConnectorConfig::from_yaml_str(row.get(3)),
        });
    }

    Ok(result)
}

pub(crate) async fn get_connector_by_name(
    db: &ProjectDB,
    tenant_id: TenantId,
    name: &str,
    txn: Option<&Transaction<'_>>,
) -> Result<ConnectorDescr, DBError> {
    let query = "SELECT id, description, config FROM connector WHERE name = $1 AND tenant_id = $2";
    let row = if let Some(txn) = txn {
        let stmt = txn.prepare_cached(query).await?;
        txn.query_opt(&stmt, &[&name, &tenant_id.0]).await?
    } else {
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;
        manager.query_opt(&stmt, &[&name, &tenant_id.0]).await?
    };

    if let Some(row) = row {
        let connector_id: ConnectorId = ConnectorId(row.get(0));
        let description: String = row.get(1);
        let config = ConnectorConfig::from_yaml_str(row.get(2));

        Ok(ConnectorDescr {
            connector_id,
            name: name.to_string(),
            description,
            config,
        })
    } else {
        Err(DBError::UnknownConnectorName {
            connector_name: name.to_string(),
        })
    }
}

pub(crate) async fn get_connector_by_id(
    db: &ProjectDB,
    tenant_id: TenantId,
    connector_id: ConnectorId,
) -> Result<ConnectorDescr, DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached(
            "SELECT name, description, config FROM connector WHERE id = $1 AND tenant_id = $2",
        )
        .await?;

    let row = manager
        .query_opt(&stmt, &[&connector_id.0, &tenant_id.0])
        .await?;

    if let Some(row) = row {
        let name: String = row.get(0);
        let description: String = row.get(1);
        let config: String = row.get(2);
        let config = ConnectorConfig::from_yaml_str(&config);

        Ok(ConnectorDescr {
            connector_id,
            name,
            description,
            config,
        })
    } else {
        Err(DBError::UnknownConnector { connector_id })
    }
}

pub(crate) async fn update_connector(
    db: &ProjectDB,
    tenant_id: TenantId,
    connector_id: ConnectorId,
    connector_name: &Option<&str>,
    description: &Option<&str>,
    config: &Option<ConnectorConfig>,
    txn: Option<&Transaction<'_>>,
) -> Result<(), DBError> {
    if let Some(internal_config) = config {
        validate_connector_transport(internal_config)?;
    }
    let query = r#"UPDATE connector
                         SET name = COALESCE($1, name),
                             description = COALESCE($2, description),
                             config = COALESCE($3, config)
                         WHERE id = $4 AND tenant_id = $5"#;
    let config_yaml = config.as_ref().map(|config| config.to_yaml());
    let params: &[&(dyn ToSql + Sync)] = &[
        &connector_name,
        &description,
        &config_yaml,
        &connector_id.0,
        &tenant_id.0,
    ];
    let modified_rows = if let Some(txn) = txn {
        let stmt = txn.prepare_cached(query).await?;
        txn.execute(&stmt, params).await
    } else {
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;
        manager.execute(&stmt, params).await
    }
    .map_err(ProjectDB::maybe_unique_violation)?;

    if modified_rows > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownConnector { connector_id })
    }
}

pub(crate) async fn delete_connector(
    db: &ProjectDB,
    tenant_id: TenantId,
    connector_name: &str,
) -> Result<(), DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached("DELETE FROM connector WHERE name = $1 AND tenant_id = $2")
        .await?;
    let res = manager
        .execute(&stmt, &[&connector_name, &tenant_id.0])
        .await?;

    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownConnectorName {
            connector_name: connector_name.to_string(),
        })
    }
}
