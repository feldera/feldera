//! Iceberg catalog clients, shared by the input and output connectors.

use anyhow::{anyhow, Result as AnyResult};
use feldera_types::transport::iceberg::{
    GlueCatalogConfig, IcebergCatalogType, RestCatalogConfig, S3TablesCatalogConfig,
};
use iceberg::{io::StorageFactory, Catalog, CatalogBuilder};
use iceberg_catalog_glue::{
    GlueCatalogBuilder, AWS_ACCESS_KEY_ID, AWS_PROFILE_NAME, AWS_REGION_NAME,
    AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, GLUE_CATALOG_PROP_CATALOG_ID, GLUE_CATALOG_PROP_URI,
    GLUE_CATALOG_PROP_WAREHOUSE,
};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_catalog_s3tables::{
    S3TablesCatalogBuilder, S3TABLES_CATALOG_PROP_ENDPOINT_URL,
    S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use std::{collections::HashMap, sync::Arc};

/// Storage backend for object stores, picked per path from its scheme
/// (`s3`/`s3a`/`gs`/`memory`/...). Used for catalogs and remote tables.
pub(crate) fn storage_factory() -> Arc<dyn StorageFactory> {
    Arc::new(OpenDalResolvingStorageFactory::new())
}

// `iceberg-catalog-s3tables` reads AWS credentials for its S3 Tables API client
// from these property keys but does not re-export them (they live in the crate's
// private `utils` module). Mirror them here. See
// crates/catalog/s3tables/src/utils.rs in the iceberg-rust fork.
const S3TABLES_PROP_ACCESS_KEY_ID: &str = "aws_access_key_id";
const S3TABLES_PROP_SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
const S3TABLES_PROP_SESSION_TOKEN: &str = "aws_session_token";
const S3TABLES_PROP_PROFILE_NAME: &str = "profile_name";
const S3TABLES_PROP_REGION_NAME: &str = "region_name";

/// Fills in FileIO properties the user did not set.
pub(crate) fn apply_fileio_defaults(props: &mut HashMap<String, String>) {
    // iceberg-rust renamed `gcs.service.path` to `gcs.service.host`; keep the old key working.
    if let Some(endpoint) = props.remove("gcs.service.path") {
        props
            .entry(iceberg::io::GCS_SERVICE_HOST.to_string())
            .or_insert(endpoint);
    }
    // OpenDAL's 10 s per-call timeout can fire on a read the connector stopped polling under
    // backpressure, and on the upload of one part of a data file; 60 s still catches a dead
    // connection.
    props
        .entry(iceberg_storage_opendal::OPENDAL_IO_TIMEOUT_MS.to_string())
        .or_insert_with(|| "60000".to_string());
}

/// The catalog settings of a connector.
pub(crate) struct CatalogConfig<'a> {
    pub catalog_type: &'a IcebergCatalogType,
    pub glue: &'a GlueCatalogConfig,
    pub rest: &'a RestCatalogConfig,
    pub s3tables: &'a S3TablesCatalogConfig,
    /// FileIO options. The catalog client reads the table's files with them.
    pub fileio: &'a HashMap<String, String>,
}

impl CatalogConfig<'_> {
    /// Connects to the catalog.
    pub(crate) async fn load(&self) -> AnyResult<Arc<dyn Catalog>> {
        let mut props = self.fileio.clone();
        let mut set = |key: &str, value: &Option<String>| {
            if let Some(value) = value {
                props.insert(key.to_string(), value.clone());
            }
        };
        match self.catalog_type {
            IcebergCatalogType::Glue => {
                let glue = self.glue;
                if glue.warehouse.is_none() {
                    return Err(anyhow!("the 'glue.warehouse' property is not set"));
                }
                set(GLUE_CATALOG_PROP_WAREHOUSE, &glue.warehouse);
                set(GLUE_CATALOG_PROP_CATALOG_ID, &glue.id);
                set(GLUE_CATALOG_PROP_URI, &glue.endpoint);
                set(AWS_ACCESS_KEY_ID, &glue.access_key_id);
                set(AWS_SECRET_ACCESS_KEY, &glue.secret_access_key);
                set(AWS_SESSION_TOKEN, &glue.session_token);
                set(AWS_PROFILE_NAME, &glue.profile_name);
                set(AWS_REGION_NAME, &glue.region);
                let catalog = GlueCatalogBuilder::default()
                    .with_storage_factory(storage_factory())
                    .load("glue".to_string(), props)
                    .await
                    .map_err(|e| anyhow!("error creating Glue catalog client: {e}"))?;
                Ok(Arc::new(catalog))
            }
            IcebergCatalogType::Rest => {
                let rest = self.rest;
                if rest.uri.is_none() {
                    return Err(anyhow!("the 'rest.uri' property is not set"));
                }
                set(REST_CATALOG_PROP_URI, &rest.uri);
                set(REST_CATALOG_PROP_WAREHOUSE, &rest.warehouse);
                set("audience", &rest.audience);
                set("resource", &rest.resource);
                set("credential", &rest.credential);
                set("oauth2-server-uri", &rest.oauth2_server_uri);
                set("prefix", &rest.prefix);
                set("scope", &rest.scope);
                set("token", &rest.token);
                for (header, value) in rest.headers.iter().flatten() {
                    props.insert(format!("header.{header}"), value.clone());
                }
                let catalog = RestCatalogBuilder::default()
                    .with_storage_factory(storage_factory())
                    .load("rest".to_string(), props)
                    .await
                    .map_err(|e| anyhow!("error creating Rest catalog client: {e}"))?;
                Ok(Arc::new(catalog))
            }
            IcebergCatalogType::S3Tables => {
                let s3tables = self.s3tables;
                if s3tables.table_bucket_arn.is_none() {
                    return Err(anyhow!(
                        "the 's3tables.table-bucket-arn' property is not set"
                    ));
                }
                set(
                    S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN,
                    &s3tables.table_bucket_arn,
                );
                set(S3TABLES_CATALOG_PROP_ENDPOINT_URL, &s3tables.endpoint);
                // These keys authenticate the S3 Tables API client. The `s3.*` FileIO
                // keys authenticate reads of the table's files. Each ignores the other's.
                set(S3TABLES_PROP_ACCESS_KEY_ID, &s3tables.access_key_id);
                set(S3TABLES_PROP_SECRET_ACCESS_KEY, &s3tables.secret_access_key);
                set(S3TABLES_PROP_SESSION_TOKEN, &s3tables.session_token);
                set(S3TABLES_PROP_PROFILE_NAME, &s3tables.profile_name);
                set(S3TABLES_PROP_REGION_NAME, &s3tables.region);
                let catalog = S3TablesCatalogBuilder::default()
                    .with_storage_factory(storage_factory())
                    .load("s3tables".to_string(), props)
                    .await
                    .map_err(|e| anyhow!("error creating S3 Tables catalog client: {e}"))?;
                Ok(Arc::new(catalog))
            }
        }
    }
}
