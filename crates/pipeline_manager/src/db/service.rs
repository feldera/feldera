use crate::auth::TenantId;
use crate::db::{DBError, ProjectDB};
use log::debug;
use pipeline_types::config::ServiceConfig;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
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
    pub config: ServiceConfig,
}

pub(crate) async fn new_service(
    db: &ProjectDB,
    tenant_id: TenantId,
    id: Uuid,
    name: &str,
    description: &str,
    config: &ServiceConfig,
) -> Result<ServiceId, DBError> {
    debug!("new_service {name} {description} {config:?}");
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached("INSERT INTO service (id, name, description, config, tenant_id) VALUES($1, $2, $3, $4, $5)")
        .await?;
    manager
        .execute(
            &stmt,
            &[&id, &name, &description, &config.to_yaml(), &tenant_id.0],
        )
        .await
        .map_err(ProjectDB::maybe_unique_violation)
        .map_err(|e| ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None))?;
    Ok(ServiceId(id))
}

pub(crate) async fn list_services(
    db: &ProjectDB,
    tenant_id: TenantId,
) -> Result<Vec<ServiceDescr>, DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached("SELECT id, name, description, config FROM service WHERE tenant_id = $1")
        .await?;
    let rows = manager.query(&stmt, &[&tenant_id.0]).await?;

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
) -> Result<ServiceDescr, DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached(
            "SELECT name, description, config FROM service WHERE id = $1 AND tenant_id = $2",
        )
        .await?;

    let row = manager
        .query_opt(&stmt, &[&service_id.0, &tenant_id.0])
        .await?;

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
    name: String,
) -> Result<ServiceDescr, DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached(
            "SELECT id, description, config FROM service WHERE name = $1 AND tenant_id = $2",
        )
        .await?;
    let row = manager.query_opt(&stmt, &[&name, &tenant_id.0]).await?;

    if let Some(row) = row {
        Ok(ServiceDescr {
            service_id: ServiceId(row.get(0)),
            name,
            description: row.get(1),
            config: ServiceConfig::from_yaml_str(row.get(2)),
        })
    } else {
        Err(DBError::UnknownName { name })
    }
}

pub(crate) async fn update_service(
    db: &ProjectDB,
    tenant_id: TenantId,
    service_id: ServiceId,
    description: &str,
    config: &Option<ServiceConfig>,
) -> Result<(), DBError> {
    let manager = db.pool.get().await?;
    let modified_rows = match config {
        None => {
            let stmt = manager
                .prepare_cached(
                    "UPDATE service SET description = $1 WHERE id = $2 AND tenant_id = $3",
                )
                .await?;
            manager
                .execute(&stmt, &[&description, &service_id.0, &tenant_id.0])
                .await
                .map_err(DBError::from)?
        }
        Some(config) => {
            let stmt = manager
                .prepare_cached(
                    "UPDATE service SET description = $1, config = $2 WHERE id = $3 AND tenant_id = $4",
                )
                .await?;
            manager
                .execute(
                    &stmt,
                    &[&description, &config.to_yaml(), &service_id.0, &tenant_id.0],
                )
                .await
                .map_err(DBError::from)?
        }
    };
    if modified_rows > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownService { service_id })
    }
}

pub(crate) async fn delete_service(
    db: &ProjectDB,
    tenant_id: TenantId,
    service_id: ServiceId,
) -> Result<(), DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached("DELETE FROM service WHERE id = $1 AND tenant_id = $2")
        .await?;
    let res = manager
        .execute(&stmt, &[&service_id.0, &tenant_id.0])
        .await?;

    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownService { service_id })
    }
}
