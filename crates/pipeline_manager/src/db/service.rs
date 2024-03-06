use crate::api::ServiceConfig;
use crate::auth::TenantId;
use crate::db::{DBError, ProjectDB};
use deadpool_postgres::Transaction;
use log::debug;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use tokio_postgres::types::ToSql;
use utoipa::ToSchema;
use uuid::Uuid;

/// Unique service id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct ServiceId(
    #[cfg_attr(test, proptest(strategy = "super::test::limited_uuid()"))] pub Uuid,
);
impl Display for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Service descriptor.
#[derive(Deserialize, Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
pub(crate) struct ServiceDescr {
    pub service_id: ServiceId,
    pub name: String,
    pub description: String,
    #[serde(flatten)]
    pub config: ServiceConfig,
}

pub(crate) async fn new_service(
    db: &ProjectDB,
    tenant_id: TenantId,
    id: Uuid,
    name: &str,
    description: &str,
    config: &ServiceConfig,
    txn: Option<&Transaction<'_>>,
) -> Result<ServiceId, DBError> {
    debug!("new_service {name} {description} {config:?}");
    let query = "INSERT INTO service (id, name, description, config, config_type, tenant_id) VALUES($1, $2, $3, $4, $5, $6)";
    let params: &[&(dyn ToSql + Sync)] = &[
        &id,
        &name,
        &description,
        &config.to_yaml(),
        &config.config_type(),
        &tenant_id.0,
    ];
    if let Some(txn) = txn {
        let stmt = txn.prepare_cached(query).await?;
        txn.execute(&stmt, params).await
    } else {
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;
        manager.execute(&stmt, params).await
    }
    .map_err(ProjectDB::maybe_unique_violation)
    .map_err(|e| ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None))?;
    Ok(ServiceId(id))
}

pub(crate) async fn list_services(
    db: &ProjectDB,
    tenant_id: TenantId,
    filter_config_type: &Option<&str>,
) -> Result<Vec<ServiceDescr>, DBError> {
    let manager = db.pool.get().await?;
    let rows = match filter_config_type {
        None => {
            let query = "SELECT id, name, description, config, config_type FROM service WHERE tenant_id = $1";
            let stmt = manager.prepare_cached(query).await?;
            manager.query(&stmt, &[&tenant_id.0]).await?
        }
        Some(filter_config_type) => {
            let query = "SELECT id, name, description, config, config_type FROM service WHERE tenant_id = $1 AND config_type = $2";
            let stmt = manager.prepare_cached(query).await?;
            manager
                .query(&stmt, &[&tenant_id.0, &filter_config_type])
                .await?
        }
    };
    let mut result = Vec::with_capacity(rows.len());

    for row in rows {
        result.push(ServiceDescr {
            service_id: ServiceId(row.get(0)),
            name: row.get(1),
            description: row.get(2),
            config: ServiceConfig::from_yaml_str(row.get(3)),
        });
    }

    Ok(result)
}

pub(crate) async fn get_service_by_id(
    db: &ProjectDB,
    tenant_id: TenantId,
    service_id: ServiceId,
    txn: Option<&Transaction<'_>>,
) -> Result<ServiceDescr, DBError> {
    let query = "SELECT name, description, config, config_type FROM service WHERE id = $1 AND tenant_id = $2";
    let row = if let Some(txn) = txn {
        let stmt = txn.prepare_cached(query).await?;
        txn.query_opt(&stmt, &[&service_id.0, &tenant_id.0]).await?
    } else {
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;
        manager
            .query_opt(&stmt, &[&service_id.0, &tenant_id.0])
            .await?
    };

    if let Some(row) = row {
        Ok(ServiceDescr {
            service_id,
            name: row.get(0),
            description: row.get(1),
            config: ServiceConfig::from_yaml_str(row.get(2)),
        })
    } else {
        Err(DBError::UnknownService { service_id })
    }
}

pub(crate) async fn get_service_by_name(
    db: &ProjectDB,
    tenant_id: TenantId,
    name: &str,
    txn: Option<&Transaction<'_>>,
) -> Result<ServiceDescr, DBError> {
    let query = "SELECT id, description, config, config_type FROM service WHERE name = $1 AND tenant_id = $2";
    let row = if let Some(txn) = txn {
        let stmt = txn.prepare_cached(query).await?;
        txn.query_opt(&stmt, &[&name, &tenant_id.0]).await?
    } else {
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;
        manager.query_opt(&stmt, &[&name, &tenant_id.0]).await?
    };

    if let Some(row) = row {
        Ok(ServiceDescr {
            service_id: ServiceId(row.get(0)),
            name: name.to_string(),
            description: row.get(1),
            config: ServiceConfig::from_yaml_str(row.get(2)),
        })
    } else {
        Err(DBError::UnknownServiceName {
            service_name: name.to_string(),
        })
    }
}

pub(crate) async fn update_service(
    db: &ProjectDB,
    tenant_id: TenantId,
    service_id: ServiceId,
    name: &Option<&str>,
    description: &Option<&str>,
    config: &Option<ServiceConfig>,
    txn: Option<&Transaction<'_>>,
) -> Result<(), DBError> {
    let query = r#"UPDATE service
                         SET name = COALESCE($1, name),
                             description = COALESCE($2, description),
                             config = COALESCE($3, config),
                             config_type = COALESCE($4, config_type)
                         WHERE id = $5 AND tenant_id = $6"#;
    let (config_yaml, config_type) = match config {
        None => (None, None),
        Some(config) => (Some(config.to_yaml()), Some(config.config_type())),
    };
    let params: &[&(dyn ToSql + Sync)] = &[
        &name,
        &description,
        &config_yaml,
        &config_type,
        &service_id.0,
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
        Err(DBError::UnknownService { service_id })
    }
}

pub(crate) async fn delete_service(
    db: &ProjectDB,
    tenant_id: TenantId,
    service_name: &str,
) -> Result<(), DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached("DELETE FROM service WHERE name = $1 AND tenant_id = $2")
        .await?;
    let res = manager
        .execute(&stmt, &[&service_name, &tenant_id.0])
        .await?;

    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownServiceName {
            service_name: service_name.to_string(),
        })
    }
}
