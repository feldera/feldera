use deadpool_postgres::Transaction;
use log::debug;
use pipeline_types::config::{ConnectorConfig, TransportConfig, TransportConfigVariant};
use uuid::Uuid;

use crate::auth::TenantId;

use super::{DBError, ProjectDB};

use std::fmt::{self, Display};

use utoipa::ToSchema;

use crate::db::service::{get_service_by_id, get_service_by_name};
use crate::db::ServiceId;
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

/// Serializes a connector configuration to YAML, and returning the service
/// identifiers that are referred to by name in the transport configuration.
/// The result is padded with [None] until its length is two, for the two
/// columns.
async fn connector_config_to_yaml_with_service_ids(
    db: &ProjectDB,
    tenant_id: TenantId,
    config: ConnectorConfig,
    txn: &Transaction<'_>,
) -> Result<(String, Vec<Option<Uuid>>), DBError> {
    let yaml_str = serde_yaml::to_string(&config).unwrap();
    let service_names = config.transport.service_names();
    if service_names.len() > 2 {
        return Err(DBError::InvalidConnectorTransport {
            reason: format!(
                "Expected 2 service names but instead got {}",
                service_names.len()
            ),
        });
    }
    let mut service_ids = vec![];
    for service_name in config.transport.service_names() {
        service_ids.push(Some(
            get_service_by_name(db, tenant_id, &service_name, Some(txn))
                .await?
                .service_id
                .0,
        ));
    }
    while service_ids.len() < 2 {
        service_ids.push(None);
    }
    Ok((yaml_str, service_ids))
}

/// Deserializes the connector configuration from YAML, in which any transport
/// configuration service names are replaced with the latest associated to the
/// provided identifiers.
pub(crate) async fn connector_config_from_yaml_replace_services(
    db: &ProjectDB,
    tenant_id: TenantId,
    yaml_str: &str,
    service_ids: Vec<Option<Uuid>>,
    txn: &Transaction<'_>,
) -> Result<ConnectorConfig, DBError> {
    let mut deserialized_config: ConnectorConfig = serde_yaml::from_str(yaml_str).unwrap();
    let mut replacement_service_names = vec![];
    for service_id in service_ids.into_iter().flatten() {
        // flatten() removes None
        replacement_service_names.push(
            get_service_by_id(db, tenant_id, ServiceId(service_id), Some(txn))
                .await?
                .name,
        );
    }
    deserialized_config.transport = deserialized_config
        .transport
        .replace_any_service_names(&replacement_service_names)
        .map_err(|e| DBError::InvalidConnectorTransport {
            reason: format!("{:?}", e),
        })?;
    Ok(deserialized_config)
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
    match txn {
        None => {
            let mut manager = db.pool.get().await?;
            let txn = manager.transaction().await?;
            new_connector_with_txn(db, tenant_id, id, name, description, config, &txn).await?;
            txn.commit().await?;
        }
        Some(txn) => {
            new_connector_with_txn(db, tenant_id, id, name, description, config, txn).await?;
        }
    };
    Ok(ConnectorId(id))
}

/// Creates a new connector using the provided transaction without committing.
/// Only used internally to prevent code duplication.
async fn new_connector_with_txn(
    db: &ProjectDB,
    tenant_id: TenantId,
    id: Uuid,
    name: &str,
    description: &str,
    config: &ConnectorConfig,
    txn: &Transaction<'_>,
) -> Result<(), DBError> {
    let query = "INSERT INTO connector (id, name, description, config, tenant_id, service1, service2) VALUES($1, $2, $3, $4, $5, $6, $7)";
    let (config_yaml_str, service_ids) =
        connector_config_to_yaml_with_service_ids(db, tenant_id, config.clone(), txn).await?;
    let stmt = txn.prepare_cached(query).await?;
    txn.execute(
        &stmt,
        &[
            &id,
            &name,
            &description,
            &config_yaml_str,
            &tenant_id.0,
            &service_ids[0],
            &service_ids[1],
        ],
    )
    .await
    .map_err(ProjectDB::maybe_unique_violation)
    .map_err(|e| ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None))?;
    Ok(())
}

pub(crate) async fn list_connectors(
    db: &ProjectDB,
    tenant_id: TenantId,
) -> Result<Vec<ConnectorDescr>, DBError> {
    let mut manager = db.pool.get().await?;
    let txn = manager.transaction().await?;
    let stmt = txn
        .prepare_cached(
            "SELECT id, name, description, config, service1, service2 FROM connector WHERE tenant_id = $1",
        )
        .await?;
    let rows = txn.query(&stmt, &[&tenant_id.0]).await?;

    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let config = connector_config_from_yaml_replace_services(
            db,
            tenant_id,
            row.get(3),
            vec![row.get(4), row.get(5)],
            &txn,
        )
        .await?;
        result.push(ConnectorDescr {
            connector_id: ConnectorId(row.get(0)),
            name: row.get(1),
            description: row.get(2),
            config,
        });
    }

    txn.commit().await?;
    Ok(result)
}

pub(crate) async fn get_connector_by_name(
    db: &ProjectDB,
    tenant_id: TenantId,
    name: &str,
    txn: Option<&Transaction<'_>>,
) -> Result<ConnectorDescr, DBError> {
    match txn {
        None => {
            let mut manager = db.pool.get().await?;
            let txn = manager.transaction().await?;
            let result = get_connector_by_name_with_txn(db, tenant_id, name, &txn).await?;
            txn.commit().await?;
            Ok(result)
        }
        Some(txn) => get_connector_by_name_with_txn(db, tenant_id, name, txn).await,
    }
}

/// Retrieves a connector by name using the provided transaction without committing.
/// Only used internally to prevent code duplication.
async fn get_connector_by_name_with_txn(
    db: &ProjectDB,
    tenant_id: TenantId,
    name: &str,
    txn: &Transaction<'_>,
) -> Result<ConnectorDescr, DBError> {
    let query = "SELECT id, description, config, service1, service2 FROM connector WHERE name = $1 AND tenant_id = $2";
    let stmt = txn.prepare_cached(query).await?;
    let row = txn.query_opt(&stmt, &[&name, &tenant_id.0]).await?;

    // Check result
    if let Some(row) = row {
        let connector_id: ConnectorId = ConnectorId(row.get(0));
        let description: String = row.get(1);
        let config = connector_config_from_yaml_replace_services(
            db,
            tenant_id,
            row.get(2),
            vec![row.get(3), row.get(4)],
            txn,
        )
        .await?;
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
    let mut manager = db.pool.get().await?;
    let txn = manager.transaction().await?;
    let stmt = txn
        .prepare_cached(
            "SELECT name, description, config, service1, service2 FROM connector WHERE id = $1 AND tenant_id = $2",
        )
        .await?;

    let row = txn
        .query_opt(&stmt, &[&connector_id.0, &tenant_id.0])
        .await?;

    if let Some(row) = row {
        let name: String = row.get(0);
        let description: String = row.get(1);
        let config = connector_config_from_yaml_replace_services(
            db,
            tenant_id,
            row.get(2),
            vec![row.get(3), row.get(4)],
            &txn,
        )
        .await?;

        txn.commit().await?;
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
    match txn {
        None => {
            let mut manager = db.pool.get().await?;
            let txn = manager.transaction().await?;
            update_connector_with_txn(
                db,
                tenant_id,
                connector_id,
                connector_name,
                description,
                config,
                &txn,
            )
            .await?;
            txn.commit().await?;
        }
        Some(txn) => {
            update_connector_with_txn(
                db,
                tenant_id,
                connector_id,
                connector_name,
                description,
                config,
                txn,
            )
            .await?
        }
    }
    Ok(())
}

/// Updates a connector using the provided transaction without committing.
/// Only used internally to prevent code duplication.
async fn update_connector_with_txn(
    db: &ProjectDB,
    tenant_id: TenantId,
    connector_id: ConnectorId,
    connector_name: &Option<&str>,
    description: &Option<&str>,
    config: &Option<ConnectorConfig>,
    txn: &Transaction<'_>,
) -> Result<(), DBError> {
    if let Some(internal_config) = config {
        validate_connector_transport(internal_config)?;
    }
    match config {
        None => {
            let query = r#"UPDATE connector
                         SET name = COALESCE($1, name),
                             description = COALESCE($2, description)
                         WHERE id = $3 AND tenant_id = $4"#;
            let stmt = txn.prepare_cached(query).await?;
            let modified_rows = txn
                .execute(
                    &stmt,
                    &[&connector_name, &description, &connector_id.0, &tenant_id.0],
                )
                .await
                .map_err(ProjectDB::maybe_unique_violation)?;
            if modified_rows > 0 {
                Ok(())
            } else {
                Err(DBError::UnknownConnector { connector_id })
            }
        }
        Some(config) => {
            let update_query_with_config = r#"UPDATE connector
                         SET name = COALESCE($1, name),
                             description = COALESCE($2, description),
                             config = $3,
                             service1 = $4,
                             service2 = $5
                         WHERE id = $6 AND tenant_id = $7"#;
            let (config_yaml_str, service_ids) =
                connector_config_to_yaml_with_service_ids(db, tenant_id, config.clone(), txn)
                    .await?;
            let stmt = txn.prepare_cached(update_query_with_config).await?;
            let modified_rows = txn
                .execute(
                    &stmt,
                    &[
                        &connector_name,
                        &description,
                        &config_yaml_str,
                        &service_ids[0],
                        &service_ids[1],
                        &connector_id.0,
                        &tenant_id.0,
                    ],
                )
                .await
                .map_err(ProjectDB::maybe_unique_violation)?;
            if modified_rows > 0 {
                Ok(())
            } else {
                Err(DBError::UnknownConnector { connector_id })
            }
        }
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
