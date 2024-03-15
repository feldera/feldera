use crate::auth::TenantId;
use crate::db::pipeline::convert_bigint_to_time;
use crate::db::{DBError, ProjectDB};
use crate::prober::service::{
    ServiceProbeRequest, ServiceProbeResponse, ServiceProbeStatus, ServiceProbeType,
};
use chrono::{DateTime, Utc};
use deadpool_postgres::Transaction;
use log::debug;
use pipeline_types::service::ServiceConfig;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;
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

/// Unique service probe id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct ServiceProbeId(
    #[cfg_attr(test, proptest(strategy = "super::test::limited_uuid()"))] pub Uuid,
);
impl Display for ServiceProbeId {
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
    pub config_type: String,
}

/// Service probe descriptor.
#[derive(Deserialize, Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
pub(crate) struct ServiceProbeDescr {
    pub service_probe_id: ServiceProbeId,
    pub status: ServiceProbeStatus,
    pub probe_type: ServiceProbeType,
    pub request: ServiceProbeRequest,
    pub response: Option<ServiceProbeResponse>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
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
            config_type: row.get(4),
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
            config_type: row.get(3),
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
            config_type: row.get(3),
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

pub(crate) async fn new_service_probe(
    db: &ProjectDB,
    tenant_id: TenantId,
    service_id: ServiceId,
    id: Uuid,
    request: ServiceProbeRequest,
    created_at: &DateTime<Utc>,
    txn: Option<&Transaction<'_>>,
) -> Result<ServiceProbeId, DBError> {
    debug!("new_service_probe {id}");
    let query = r#"INSERT INTO service_probe
                   (id, tenant_id, service_id, status, request, probe_type,
                    response, created_at, started_at, finished_at)
                   VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, NULL, NULL)"#;
    let status: &'static str = ServiceProbeStatus::Pending.into();
    let probe_type: &'static str = request.probe_type().into();
    let params: &[&(dyn ToSql + Sync)] = &[
        &id,
        &tenant_id.0,
        &service_id.0,
        &status,
        &request.to_yaml(),
        &probe_type,
        &created_at.timestamp(),
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
    .map_err(|e| ProjectDB::maybe_tenant_id_service_id_foreign_key_constraint_err(e, service_id))
    .map_err(|e| ProjectDB::maybe_service_id_foreign_key_constraint_err(e, service_id))
    .map_err(|e| ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None))?;
    Ok(ServiceProbeId(id))
}

pub(crate) async fn update_service_probe_set_running(
    db: &ProjectDB,
    tenant_id: TenantId,
    service_probe_id: ServiceProbeId,
    started_at: &DateTime<Utc>,
) -> Result<(), DBError> {
    let query =
        "UPDATE service_probe SET status = $1, started_at = $2 WHERE tenant_id = $3 AND id = $4 ";
    let status: &'static str = ServiceProbeStatus::Running.into();
    let params: &[&(dyn ToSql + Sync)] = &[
        &status,
        &started_at.timestamp(),
        &tenant_id.0,
        &service_probe_id.0,
    ];

    let manager = db.pool.get().await?;
    let stmt = manager.prepare_cached(query).await?;
    let modified_rows = manager.execute(&stmt, params).await?;
    if modified_rows > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownServiceProbe { service_probe_id })
    }
}

pub(crate) async fn update_service_probe_set_finished(
    db: &ProjectDB,
    tenant_id: TenantId,
    service_probe_id: ServiceProbeId,
    response: ServiceProbeResponse,
    finished_at: &DateTime<Utc>,
) -> Result<(), DBError> {
    let query = "UPDATE service_probe SET status = $1, response = $2, finished_at = $3 WHERE tenant_id = $4 AND id = $5 ";
    let status: &'static str = match response {
        ServiceProbeResponse::Success(_) => ServiceProbeStatus::Success.into(),
        ServiceProbeResponse::Error(_) => ServiceProbeStatus::Failure.into(),
    };
    let params: &[&(dyn ToSql + Sync)] = &[
        &status,
        &response.to_yaml(),
        &finished_at.timestamp(),
        &tenant_id.0,
        &service_probe_id.0,
    ];

    let manager = db.pool.get().await?;
    let stmt = manager.prepare_cached(query).await?;
    let modified_rows = manager.execute(&stmt, params).await?;
    if modified_rows > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownServiceProbe { service_probe_id })
    }
}

pub(crate) async fn next_service_probe(
    db: &ProjectDB,
) -> Result<Option<(ServiceProbeId, TenantId, ServiceProbeRequest, ServiceConfig)>, DBError> {
    let manager = db.pool.get().await?;
    let query = r#"SELECT sp.id, sp.tenant_id, sp.request, sr.config
                   FROM service_probe sp
                        LEFT JOIN service sr ON sp.service_id = sr.id
                   WHERE sp.status = 'pending' OR sp.status = 'running'
                   ORDER BY (sp.created_at, sp.id) ASC LIMIT 1"#;
    let stmt = manager.prepare_cached(query).await?;
    let row = manager.query_opt(&stmt, &[]).await?;
    if let Some(row) = row {
        Ok(Some((
            ServiceProbeId(row.get(0)),
            TenantId(row.get(1)),
            ServiceProbeRequest::from_yaml_str(row.get(2)),
            ServiceConfig::from_yaml_str(row.get(3)),
        )))
    } else {
        Ok(None)
    }
}

/// Converts row of the service_probe table from a query result to its
/// descriptor.
fn service_probe_row_to_descriptor(row: Row) -> Result<ServiceProbeDescr, DBError> {
    Ok(ServiceProbeDescr {
        service_probe_id: ServiceProbeId(row.get(0)),
        status: row.get::<_, String>(1).try_into()?,
        request: ServiceProbeRequest::from_yaml_str(row.get(2)),
        probe_type: row.get::<_, String>(3).try_into()?,
        response: row
            .get::<usize, Option<&str>>(4)
            .map(ServiceProbeResponse::from_yaml_str),
        created_at: convert_bigint_to_time("service_probe.created_at", row.get(5))?,
        started_at: match row.get(6) {
            None => None,
            Some(v) => Some(convert_bigint_to_time("service_probe.started_at", v)?),
        },
        finished_at: match row.get(7) {
            None => None,
            Some(v) => Some(convert_bigint_to_time("service_probe.finished_at", v)?),
        },
    })
}

pub(crate) async fn get_service_probe(
    db: &ProjectDB,
    tenant_id: TenantId,
    service_id: ServiceId,
    service_probe_id: ServiceProbeId,
    txn: Option<&Transaction<'_>>,
) -> Result<ServiceProbeDescr, DBError> {
    let query = r#"SELECT sp.id, sp.status, sp.request, sp.probe_type, sp.response, sp.created_at, sp.started_at, sp.finished_at
                         FROM service_probe sp
                         WHERE sp.tenant_id = $1 AND sp.service_id = $2 AND sp.id = $3"#;
    let row = if let Some(txn) = txn {
        let stmt = txn.prepare_cached(query).await?;
        txn.query_opt(&stmt, &[&tenant_id.0, &service_id.0, &service_probe_id.0])
            .await?
    } else {
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;
        manager
            .query_opt(&stmt, &[&tenant_id.0, &service_id.0, &service_probe_id.0])
            .await?
    };

    if let Some(row) = row {
        Ok(service_probe_row_to_descriptor(row)?)
    } else {
        Err(DBError::UnknownServiceProbe { service_probe_id })
    }
}

pub(crate) async fn list_service_probes(
    db: &ProjectDB,
    tenant_id: TenantId,
    service_id: ServiceId,
    limit: Option<u32>,
    probe_type: Option<ServiceProbeType>,
    txn: Option<&Transaction<'_>>,
) -> Result<Vec<ServiceProbeDescr>, DBError> {
    let query = r#"SELECT sp.id, sp.status, sp.request, sp.probe_type, sp.response, sp.created_at, sp.started_at, sp.finished_at
                         FROM service_probe sp
                         WHERE sp.tenant_id = $1 AND sp.service_id = $2
                               AND sp.probe_type = COALESCE($3, sp.probe_type)
                         ORDER BY (sp.created_at, sp.id) DESC
                         LIMIT $4"#; // NULL is the same as ALL
    let probe_type: Option<&'static str> = probe_type.map(|t| t.into());
    let params: &[&(dyn ToSql + Sync)] = &[
        &tenant_id.0,
        &service_id.0,
        &probe_type,
        &(limit.map(|v| v as i64)),
    ];
    let rows = if let Some(txn) = txn {
        let stmt = txn.prepare_cached(query).await?;
        txn.query(&stmt, params).await?
    } else {
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;
        manager.query(&stmt, params).await?
    };
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(service_probe_row_to_descriptor(row)?);
    }
    Ok(result)
}
