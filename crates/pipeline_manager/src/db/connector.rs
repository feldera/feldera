use deadpool_postgres::Transaction;
use log::debug;
use uuid::Uuid;

use crate::auth::TenantId;

use super::{DBError, ProjectDB};

use std::fmt::{self, Display};

use utoipa::ToSchema;

use crate::api::{ConnectorConfig, ConnectorConfigVariant};
use crate::db::service::{get_service_by_id, get_service_by_name};
use crate::db::ServiceId;
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
    #[serde(flatten)]
    pub config: ConnectorConfig,
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
    let query = "INSERT INTO connector (id, name, description, config, tenant_id, service1, service2) VALUES($1, $2, $3, $4, $5, $6, $7)";
    match txn {
        None => {
            let mut manager = db.pool.get().await?;
            let txn = manager.transaction().await?;
            let service_ids =
                get_service_ids_of_connector_config(db, tenant_id, config, &txn).await?;
            let stmt = txn.prepare_cached(query).await?;
            txn.execute(
                &stmt,
                &[
                    &id,
                    &name,
                    &description,
                    &config.to_yaml(),
                    &tenant_id.0,
                    &service_ids[0],
                    &service_ids[1],
                ],
            )
            .await
            .map_err(ProjectDB::maybe_unique_violation)
            .map_err(|e| {
                ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None)
            })?;
            txn.commit().await?;
        }
        Some(txn) => {
            let service_ids =
                get_service_ids_of_connector_config(db, tenant_id, config, txn).await?;
            let stmt = txn.prepare_cached(query).await?;
            txn.execute(
                &stmt,
                &[
                    &id,
                    &name,
                    &description,
                    &config.to_yaml(),
                    &tenant_id.0,
                    &service_ids[0],
                    &service_ids[1],
                ],
            )
            .await
            .map_err(ProjectDB::maybe_unique_violation)
            .map_err(|e| {
                ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None)
            })?;
        }
    };
    Ok(ConnectorId(id))
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
        let config = get_final_connector_config(
            db,
            tenant_id,
            ConnectorConfig::from_yaml_str(row.get(3)),
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
    let query = "SELECT id, description, config, service1, service2 FROM connector WHERE name = $1 AND tenant_id = $2";
    match txn {
        None => {
            let mut manager = db.pool.get().await?;
            let txn = manager.transaction().await?;
            let stmt = txn.prepare_cached(query).await?;
            let row = txn.query_opt(&stmt, &[&name, &tenant_id.0]).await?;

            // Check result
            if let Some(row) = row {
                let connector_id: ConnectorId = ConnectorId(row.get(0));
                let description: String = row.get(1);
                let config = get_final_connector_config(
                    db,
                    tenant_id,
                    ConnectorConfig::from_yaml_str(row.get(2)),
                    vec![row.get(3), row.get(4)],
                    &txn,
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
        Some(txn) => {
            let stmt = txn.prepare_cached(query).await?;
            let row = txn.query_opt(&stmt, &[&name, &tenant_id.0]).await?;

            // Check result
            if let Some(row) = row {
                let connector_id: ConnectorId = ConnectorId(row.get(0));
                let description: String = row.get(1);
                let config = get_final_connector_config(
                    db,
                    tenant_id,
                    ConnectorConfig::from_yaml_str(row.get(2)),
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
        let config = get_final_connector_config(
            db,
            tenant_id,
            ConnectorConfig::from_yaml_str(row.get(2)),
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
    let update_query_without_config = r#"UPDATE connector
                         SET name = COALESCE($1, name),
                             description = COALESCE($2, description)
                         WHERE id = $3 AND tenant_id = $4"#;
    let update_query_with_config = r#"UPDATE connector
                         SET name = COALESCE($1, name),
                             description = COALESCE($2, description),
                             config = $3,
                             service1 = $4,
                             service2 = $5
                         WHERE id = $6 AND tenant_id = $7"#;
    match config {
        None => {
            let params: &[&(dyn ToSql + Sync)] =
                &[&connector_name, &description, &connector_id.0, &tenant_id.0];
            match txn {
                None => {
                    let mut manager = db.pool.get().await?;
                    let txn = manager.transaction().await?;
                    let stmt = txn.prepare_cached(update_query_without_config).await?;
                    let modified_rows = txn
                        .execute(&stmt, params)
                        .await
                        .map_err(ProjectDB::maybe_unique_violation)?;
                    txn.commit().await?;
                    if modified_rows > 0 {
                        Ok(())
                    } else {
                        Err(DBError::UnknownConnector { connector_id })
                    }
                }
                Some(txn) => {
                    let stmt = txn.prepare_cached(update_query_without_config).await?;
                    let modified_rows = txn
                        .execute(&stmt, params)
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
        Some(config) => match txn {
            None => {
                let mut manager = db.pool.get().await?;
                let txn = manager.transaction().await?;
                let service_ids =
                    get_service_ids_of_connector_config(db, tenant_id, config, &txn).await?;
                let params: &[&(dyn ToSql + Sync)] = &[
                    &connector_name,
                    &description,
                    &config.to_yaml(),
                    &service_ids[0],
                    &service_ids[1],
                    &connector_id.0,
                    &tenant_id.0,
                ];
                let stmt = txn.prepare_cached(update_query_with_config).await?;
                let modified_rows = txn
                    .execute(&stmt, params)
                    .await
                    .map_err(ProjectDB::maybe_unique_violation)?;
                txn.commit().await?;
                if modified_rows > 0 {
                    Ok(())
                } else {
                    Err(DBError::UnknownConnector { connector_id })
                }
            }
            Some(txn) => {
                let service_ids =
                    get_service_ids_of_connector_config(db, tenant_id, config, txn).await?;
                let params: &[&(dyn ToSql + Sync)] = &[
                    &connector_name,
                    &description,
                    &config.to_yaml(),
                    &service_ids[0],
                    &service_ids[1],
                    &connector_id.0,
                    &tenant_id.0,
                ];
                let stmt = txn.prepare_cached(update_query_with_config).await?;
                let modified_rows = txn
                    .execute(&stmt, params)
                    .await
                    .map_err(ProjectDB::maybe_unique_violation)?;
                if modified_rows > 0 {
                    Ok(())
                } else {
                    Err(DBError::UnknownConnector { connector_id })
                }
            }
        },
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

/// Retrieves all the service identifiers that are referred to by name in the
/// connector configuration provided. The result is padded with [None] until
/// its length is two.
async fn get_service_ids_of_connector_config(
    db: &ProjectDB,
    tenant_id: TenantId,
    config: &ConnectorConfig,
    txn: &Transaction<'_>,
) -> Result<Vec<Option<Uuid>>, DBError> {
    let service_names = config.service_names();
    assert!(service_names.len() <= 2);
    let mut service_ids = vec![];
    for service_name in config.service_names() {
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
    assert_eq!(service_ids.len(), 2);
    Ok(service_ids)
}

/// Retrieves all the service names corresponding to the provided identifiers.
async fn get_final_connector_config(
    db: &ProjectDB,
    tenant_id: TenantId,
    deserialized_config: ConnectorConfig,
    service_ids: Vec<Option<Uuid>>,
    txn: &Transaction<'_>,
) -> Result<ConnectorConfig, DBError> {
    let mut replacement_service_names = vec![];
    for service_id in service_ids.into_iter().flatten() {
        // flatten() removes None
        replacement_service_names.push(
            get_service_by_id(db, tenant_id, ServiceId(service_id), Some(txn))
                .await?
                .name,
        );
    }
    Ok(ConnectorConfig::new_replace_service_names(
        deserialized_config,
        &replacement_service_names,
    ))
}
