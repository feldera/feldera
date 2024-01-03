use log::debug;
use pipeline_types::config::ConnectorConfig;
use uuid::Uuid;

use crate::auth::TenantId;

use super::{storage::Storage, DBError, ProjectDB};

use std::fmt::{self, Display};

use utoipa::ToSchema;

use serde::{Deserialize, Serialize};

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

pub(crate) async fn new_connector(
    db: &ProjectDB,
    tenant_id: TenantId,
    id: Uuid,
    name: &str,
    description: &str,
    config: &ConnectorConfig,
) -> Result<ConnectorId, DBError> {
    debug!("new_connector {name} {description} {config:?}");
    let manager = db.pool.get().await?;
    let stmt = manager
            .prepare_cached("INSERT INTO connector (id, name, description, config, tenant_id) VALUES($1, $2, $3, $4, $5)")
            .await?;
    manager
        .execute(
            &stmt,
            &[&id, &name, &description, &config.to_yaml(), &tenant_id.0],
        )
        .await
        .map_err(ProjectDB::maybe_unique_violation)
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
) -> Result<ConnectorDescr, DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached(
            "SELECT id, description, config FROM connector WHERE name = $1 AND tenant_id = $2",
        )
        .await?;
    let row = manager.query_opt(&stmt, &[&name, &tenant_id.0]).await?;

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
    connector_name: &str,
    description: &str,
    config: &Option<ConnectorConfig>,
) -> Result<(), DBError> {
    let descr = db.get_connector_by_id(tenant_id, connector_id).await?;
    let config = config.clone().unwrap_or(descr.config);
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached(
            "UPDATE connector SET name = $1, description = $2, config = $3 WHERE id = $4",
        )
        .await?;

    manager
        .execute(
            &stmt,
            &[
                &connector_name,
                &description,
                &config.to_yaml(),
                &connector_id.0,
            ],
        )
        .await
        .map_err(ProjectDB::maybe_unique_violation)?;

    Ok(())
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
