use std::{io::Write, marker::PhantomPinned, str::FromStr, sync::Weak, time::Duration};

use super::{error::BackoffError, prepared_statements::PreparedStatements};
use crate::{
    buffer_op,
    catalog::{RecordFormat, SerBatchReader},
    controller::{ControllerInner, EndpointId},
    flush_op,
    format::{Encoder, OutputConsumer},
    transport::OutputEndpoint,
    util::IndexedOperationType,
};
use crate::{util::indexed_operation_type, ControllerError};
use anyhow::{anyhow, Context, Result as AnyResult};
use feldera_adapterlib::transport::{AsyncErrorCallback, Step};
use feldera_types::{
    format::json::JsonFlavor,
    program_schema::{Relation, SqlIdentifier},
    transport::postgres::PostgresWriterConfig,
};
use openssl::{
    pkey::PKey,
    rsa::Rsa,
    ssl::{SslConnector, SslConnectorBuilder, SslMethod},
    x509::X509,
};
use postgres::{Client, NoTls, Statement};
use postgres_openssl::MakeTlsConnector;
use tempfile::NamedTempFile;
use tracing::{info_span, span::EnteredSpan};

pub struct PostgresOutputEndpoint {
    endpoint_id: EndpointId,
    endpoint_name: String,
    table: String,
    client: postgres::Client,
    config: PostgresWriterConfig,
    transaction: Option<postgres::Transaction<'static>>,
    prepared_statements: PreparedStatements,
    insert_buf: Vec<u8>,
    upsert_buf: Vec<u8>,
    delete_buf: Vec<u8>,
    inserts: usize,
    upserts: usize,
    deletes: usize,
    key_schema: Relation,
    value_schema: Relation,
    controller: Weak<ControllerInner>,
    num_bytes: usize,
    num_rows: usize,
    txn_start: std::time::Instant,
    _pin: PhantomPinned,
}

impl Drop for PostgresOutputEndpoint {
    fn drop(&mut self) {
        self.transaction = None;
    }
}

const PG_CONNECTION_VALIDITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Configures SSL certificates for the PostgreSQL connection if enabled.
///
/// Sets the CA certificate (`ssl_ca_pem`) and optionally the client certificate
/// and private key when provided.
fn set_certs(builder: &mut SslConnectorBuilder, config: &PostgresWriterConfig) -> AnyResult<()> {
    let Some(ca_cert) = &config.ssl_ca_pem else {
        return Ok(());
    };

    let ca_cert_path = {
        let mut file =
            NamedTempFile::new().context("failed to create tempfile to write CA certificate to")?;

        file.write_all(ca_cert.as_bytes())
            .context("failed to write CA certificate to tempfile")?;
        file.flush()
            .context("failed to flush CA certificate to tempfile")?;

        let (_, path) = file.keep()?;
        path
    };

    builder
        .set_ca_file(ca_cert_path)
        .context("failed to set CA certificate in SSL connector")?;

    if let (Some(client_cert), Some(client_key)) = (&config.ssl_client_pem, &config.ssl_client_key)
    {
        tracing::info!("postgres: using client certificate and key to connect to postgres");
        let cert = X509::from_pem(client_cert.as_bytes())
            .context("failed to parse client certificate as X509")?;
        builder
            .set_certificate(&cert)
            .context("failed to set client certificate in SSL connector")?;
        let rsa = Rsa::private_key_from_pem(client_key.as_bytes())
            .context("failed to parse client private key as RSA")?;
        let key = PKey::from_rsa(rsa).context("failed to client private key from RSA")?;
        builder
            .set_private_key(key.as_ref())
            .context("failed to set client private key")?;
    };

    Ok(())
}

fn connect(config: &PostgresWriterConfig, endpoint_name: &str) -> Result<Client, BackoffError> {
    let pgcnf = postgres::Config::from_str(&config.uri).map_err(|e| {
        BackoffError::Permanent(anyhow!("error parsing postgres connection string: {e}"))
    })?;

    let client = if config.ssl_ca_pem.is_some() {
        let mut builder = SslConnector::builder(SslMethod::tls())
            .map_err(|e| BackoffError::Permanent(anyhow!("failed to build SSL connection: {e}")))?;

        set_certs(&mut builder, config).map_err(BackoffError::Permanent)?;

        let mut connector = MakeTlsConnector::new(builder.build());

        if Some(false) == config.verify_hostname {
            let endpoint_name = endpoint_name.to_owned();
            connector.set_callback(move |ctx, _| {
                tracing::warn!("postgres: ssl: disabling hostname verification in output connector '{endpoint_name}'. The PostgreSQL server's hostname may not match the one specified in the SSL certificate.");
                ctx.set_verify_hostname(false);
                Ok(())
            });
        }

        tracing::debug!("CA certificate provided, connecting to postgres with TLS");
        pgcnf.connect(connector)
    } else {
        tracing::debug!("no CA certificates provided, connecting to postgres without TLS");
        pgcnf.connect(NoTls)
    }?;

    Ok(client)
}

impl PostgresOutputEndpoint {
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &PostgresWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
    ) -> Result<Self, ControllerError> {
        let table = config.table.to_owned();
        let mut client = connect(config, endpoint_name).map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.inner().to_string())
        })?;

        let key_schema = key_schema
            .to_owned()
            .ok_or(ControllerError::not_supported(
                "Postgres output connector requires the view to have a unique key. Please specify the `index` property in the connector configuration. For more details, see: https://docs.feldera.com/connectors/unique_keys"
            ))?;

        let prepared_statements =
            PreparedStatements::new(&key_schema, value_schema, config, &mut client).map_err(
                |e| ControllerError::output_transport_error(endpoint_name, true, e.inner()),
            )?;

        let out = Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_owned(),
            controller,
            table,
            config: config.clone(),
            client,
            transaction: None,
            prepared_statements,
            key_schema,
            num_rows: 0,
            num_bytes: 0,
            inserts: 0,
            upserts: 0,
            deletes: 0,
            insert_buf: Vec::with_capacity(config.max_buffer_size_bytes),
            upsert_buf: Vec::with_capacity(config.max_buffer_size_bytes),
            delete_buf: Vec::with_capacity(config.max_buffer_size_bytes),
            value_schema: value_schema.to_owned(),
            txn_start: std::time::Instant::now(),
            _pin: PhantomPinned,
        };

        let _guard = out.span();

        Ok(out)
    }

    fn view_name(&self) -> &SqlIdentifier {
        &self.value_schema.name
    }

    fn index_name(&self) -> &SqlIdentifier {
        &self.key_schema.name
    }

    fn transaction(&mut self) -> AnyResult<&mut postgres::Transaction<'static>> {
        self.transaction.as_mut().ok_or(anyhow!(
            "postgres: attempting to perform a transaction that hasn't been created yet"
        ))
    }

    fn exec_statement(&mut self, stmt: Statement, mut value: Vec<u8>, name: &str) {
        loop {
            match self.exec_statement_inner(stmt.clone(), &mut value, name) {
                Ok(_) => return,
                Err(e) => {
                    let retry = e.should_retry();
                    let Some(controller) = self.controller.upgrade() else {
                        tracing::warn!("controller is shutting down: aborting");
                        return;
                    };
                    controller.output_transport_error(
                        self.endpoint_id,
                        &self.endpoint_name,
                        true,
                        e.inner(),
                    );
                    if !retry {
                        return;
                    }
                    self.retry_connecting_with_backoff();
                }
            }
        }
    }

    fn exec_statement_inner(
        &mut self,
        stmt: Statement,
        value: &mut Vec<u8>,
        name: &str,
    ) -> Result<(), BackoffError> {
        if value.last() != Some(&b']') {
            value.push(b']');
        }

        if value.len() <= 2 {
            return Ok(());
        }

        self.num_bytes += value.len();

        let v: &str = std::str::from_utf8(value.as_slice()).map_err(|e| {
            BackoffError::Permanent(anyhow!("record contains non utf-8 characters: {e}"))
        })?;

        self.transaction()
            .map_err(BackoffError::Permanent)?
            .execute(&stmt, &[&v])
            .map_err(|e| {
                BackoffError::from(e).context(format!("while executing {name} statement: {v}"))
            })?;

        value.clear();
        value.push(b'[');

        Ok(())
    }

    fn flush_insert(&mut self) {
        flush_op!(
            self,
            buf = insert_buf,
            counter = inserts,
            stmt = insert,
            name = "insert"
        );
    }

    /// Executes the insert statement within the transaction and resets the buffer.
    fn insert(&mut self, mut value: Vec<u8>) {
        buffer_op!(
            self,
            buf = insert_buf,
            counter = inserts,
            stmt = insert,
            flush_fn = flush_insert,
            name = "insert",
            value = value
        );
    }

    fn flush_upsert(&mut self) {
        flush_op!(
            self,
            buf = upsert_buf,
            counter = upserts,
            stmt = upsert,
            name = "upsert"
        );
    }

    /// Executes the upsert statement within the transaction and resets the buffer.
    fn upsert(&mut self, mut value: Vec<u8>) {
        buffer_op!(
            self,
            buf = upsert_buf,
            counter = upserts,
            stmt = upsert,
            flush_fn = flush_upsert,
            name = "upsert",
            value = value
        );
    }

    fn flush_delete(&mut self) {
        flush_op!(
            self,
            buf = delete_buf,
            counter = deletes,
            stmt = delete,
            name = "delete"
        );
    }

    /// Executes the delete statement within the transaction and resets the buffer.
    fn delete(&mut self, mut value: Vec<u8>) {
        buffer_op!(
            self,
            buf = delete_buf,
            counter = deletes,
            stmt = delete,
            flush_fn = flush_delete,
            name = "delete",
            value = value
        );
    }

    fn flush(&mut self) {
        self.flush_delete();
        self.flush_insert();
        self.flush_upsert();
    }

    fn span(&self) -> EnteredSpan {
        info_span!(
            "postgres_output",
            ft = false,
            id = self.endpoint_id,
            name = self.endpoint_name,
            pg_table = self.table,
        )
        .entered()
    }

    fn retry_connecting(&mut self) -> Result<(), BackoffError> {
        if self.controller.upgrade().is_none() {
            tracing::warn!("controller is shutting down: aborting");
            return Ok(());
        };
        if self.client.is_valid(PG_CONNECTION_VALIDITY_TIMEOUT).is_ok() {
            return Ok(());
        };

        self.transaction = None;
        self.client = connect(&self.config, &self.endpoint_name)?;

        self.prepared_statements = PreparedStatements::new(
            &self.key_schema,
            &self.value_schema,
            &self.config,
            &mut self.client,
        )
        .map_err(|e| {
            e.context(format!(
                "postgres: error preparing statements after reconnecting to postgres
These statements were successfully prepared before reconnecting. Does the table {} still exist?",
                self.table
            ))
        })?;

        tracing::info!("postgres: successfully reconnected to postgres");

        Ok(())
    }

    fn retry_connecting_with_backoff(&mut self) {
        let backoff = 1000;
        let mut n_retries = 1;

        let Some(controller) = self.controller.upgrade() else {
            tracing::warn!("controller is shutting down: aborting");
            return;
        };

        loop {
            tracing::info!("retrying to connect to postgres");
            match self.retry_connecting() {
                Ok(_) => return,
                Err(e) => {
                    let retry = e.should_retry();
                    controller.output_transport_error(
                        self.endpoint_id,
                        &self.endpoint_name,
                        true,
                        e.inner(),
                    );
                    if !retry {
                        return;
                    }
                }
            }

            std::thread::sleep(
                Duration::from_millis(backoff * n_retries).min(Duration::from_secs(60)),
            );
            n_retries += 1;
        }
    }

    fn batch_start_inner(&mut self) -> Result<(), BackoffError> {
        if self.controller.upgrade().is_none() {
            tracing::warn!("controller is shutting down: aborting");
            return Ok(());
        }

        self.txn_start = std::time::Instant::now();
        let txn = self.client.transaction()?;

        // Safety
        //
        // A transaction is a reference to the `client`. The `client`'s lifetime
        // is longer than that of a transaction, which only lasts for the
        // duration of a batch.
        //
        // We transmute from Transaction<'a> to Transaction<'static> as it is
        // a reference to the `client` field in [`PostgresOutputEndpoint`].
        // This means, that moving [`PostgresOutputEndpoint`] after a batch
        // has been started, will result in `transaction` being a
        // dangling pointer.
        //
        // TODO: Consider [`std::pin::Pin`]ing the integrated connector.
        let transaction: postgres::Transaction<'static> = unsafe { std::mem::transmute(txn) };
        self.transaction = Some(transaction);

        Ok(())
    }

    fn batch_end_inner(&mut self) -> Result<(), BackoffError> {
        self.flush();

        let transaction = self
            .transaction
            .take()
            .ok_or(BackoffError::Permanent(anyhow!(
                "postgres: attempted to commit a transaction that hasn't been started"
            )))?;

        transaction.commit()?;

        let elapsed = self.txn_start.elapsed();
        tracing::debug!(
            "postgres: flushed {} rows and {} bytes in {elapsed:?}",
            self.num_rows,
            self.num_bytes
        );

        if let Some(controller) = self.controller.upgrade() {
            controller.status.output_buffer(
                self.endpoint_id,
                std::mem::take(&mut self.num_bytes),
                std::mem::take(&mut self.num_rows),
            );
        };

        Ok(())
    }
}

impl OutputConsumer for PostgresOutputEndpoint {
    fn max_buffer_size_bytes(&self) -> usize {
        self.config.max_buffer_size_bytes
    }

    fn batch_start(&mut self, _step: Step) {
        loop {
            match self.batch_start_inner() {
                Ok(_) => return,
                Err(err) => {
                    let Some(controller) = self.controller.upgrade() else {
                        tracing::warn!("controller is shutting down: aborting");
                        return;
                    };
                    let retry = err.should_retry();
                    controller.output_transport_error(
                        self.endpoint_id,
                        &self.endpoint_name,
                        true,
                        anyhow!("postgres: failed to start transaction: {}", err.inner()),
                    );
                    if !retry {
                        return;
                    }
                    self.retry_connecting_with_backoff();
                }
            }
        }
    }

    fn push_buffer(&mut self, _: &[u8], _: usize) {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _: Option<&[u8]>,
        _: Option<&[u8]>,
        _: &[(&str, Option<&[u8]>)],
        _num_records: usize,
    ) {
        unreachable!()
    }

    fn batch_end(&mut self) {
        loop {
            match self.batch_end_inner() {
                Ok(_) => return,
                Err(err) => {
                    let Some(controller) = self.controller.upgrade() else {
                        tracing::warn!("controller is shutting down: aborting");
                        return;
                    };
                    let retry = err.should_retry();
                    controller.output_transport_error(
                        self.endpoint_id,
                        &self.endpoint_name,
                        true,
                        err.inner(),
                    );
                    if !retry {
                        return;
                    }
                    self.retry_connecting_with_backoff();
                }
            }
        }
    }
}

impl Encoder for PostgresOutputEndpoint {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, batch: &dyn SerBatchReader) -> anyhow::Result<()> {
        let mut cursor = batch.cursor(RecordFormat::Json(JsonFlavor::Postgres))?;

        while cursor.key_valid() {
            if let Some(op) =
                indexed_operation_type(self.view_name(), self.index_name(), cursor.as_mut())?
            {
                cursor.rewind_vals();
                match op {
                    IndexedOperationType::Insert => {
                        let mut buf = Vec::new();
                        cursor.serialize_val(&mut buf)?;
                        self.insert(buf);
                    }
                    IndexedOperationType::Delete => {
                        let mut buf: Vec<u8> = Vec::new();
                        cursor.serialize_key(&mut buf)?;
                        self.delete(buf);
                    }
                    IndexedOperationType::Upsert => {
                        if cursor.weight() < 0 {
                            cursor.step_val();
                        }

                        let mut buf: Vec<u8> = Vec::new();
                        cursor.serialize_val(&mut buf)?;
                        self.upsert(buf);
                    }
                };

                self.num_rows += 1;
            }

            cursor.step_key();
        }

        Ok(())
    }
}

impl OutputEndpoint for PostgresOutputEndpoint {
    fn connect(&mut self, _: AsyncErrorCallback) -> anyhow::Result<()> {
        todo!()
    }

    fn max_buffer_size_bytes(&self) -> usize {
        todo!()
    }

    fn push_buffer(&mut self, _buffer: &[u8]) -> anyhow::Result<()> {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> anyhow::Result<()> {
        unreachable!()
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }

    fn batch_start(&mut self, _step: Step) -> AnyResult<()> {
        todo!()
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Weak;

    use feldera_adapterlib::errors::journal::ControllerError;
    use feldera_types::{
        program_schema::{ColumnType, Field, Relation, SqlType},
        transport::postgres::PostgresWriterConfig,
    };
    use postgres::NoTls;

    use super::PostgresOutputEndpoint;
    use crate::controller::EndpointId;

    fn int_field(name: &str) -> Field {
        Field::new(
            name.into(),
            ColumnType {
                typ: SqlType::Int,
                nullable: true,
                precision: None,
                scale: None,
                component: None,
                fields: None,
                key: None,
                value: None,
            },
        )
    }

    fn varchar_field(name: &str) -> Field {
        Field::new(
            name.into(),
            ColumnType {
                typ: SqlType::Varchar,
                nullable: true,
                precision: Some(-1),
                scale: None,
                component: None,
                fields: None,
                key: None,
                value: None,
            },
        )
    }

    fn relation(name: &str, fields: Vec<Field>, materialized: bool) -> Relation {
        Relation {
            name: name.into(),
            fields,
            materialized,
            properties: Default::default(),
        }
    }

    fn postgres_url() -> String {
        std::env::var("POSTGRES_URL")
            .unwrap_or("postgres://postgres:password@localhost:5432".to_string())
    }

    fn make_config(table: &str) -> PostgresWriterConfig {
        PostgresWriterConfig {
            uri: postgres_url(),
            table: table.into(),
            ssl_ca_pem: None,
            ssl_client_pem: None,
            ssl_client_key: None,
            verify_hostname: None,
            max_records_in_buffer: None,
            max_buffer_size_bytes: usize::pow(2, 20),
            on_conflict_do_nothing: false,
        }
    }

    fn create_endpoint(
        table: &str,
        idx_rel: Option<Relation>,
        main_rel: Relation,
    ) -> Result<PostgresOutputEndpoint, ControllerError> {
        PostgresOutputEndpoint::new(
            EndpointId::default(),
            "blah",
            &make_config(table),
            &idx_rel,
            &main_rel,
            Weak::new(),
        )
    }

    fn postgres_client() -> postgres::Client {
        postgres::Client::connect(&postgres_url(), NoTls).expect("failed to connect to postgres")
    }

    fn truncate_table(client: &mut postgres::Client, table: &str) {
        client
            .execute(&format!(r#"TRUNCATE TABLE "{table}" "#), &[])
            .expect("failed to drop table");
    }

    fn drop_table(client: &mut postgres::Client, table: &str) {
        client
            .execute(&format!(r#"DROP TABLE "{table}" "#), &[])
            .expect("failed to drop table");
    }

    #[test]
    #[serial_test::serial]
    fn test_postgres_table_name() {
        let mut client = postgres_client();
        let table = "01JWRRNVP4CGER2E3SQQKCZFNQ";

        client
            .execute(
                &format!(r#"CREATE TABLE "{table}" (id int primary key, s varchar)"#),
                &[],
            )
            .expect("failed to create test table in postgres");

        truncate_table(&mut client, table);

        let idx = relation("v1_idx", vec![int_field("id")], false);
        let main = relation("v1", vec![int_field("id"), varchar_field("s")], true);
        create_endpoint(table, Some(idx), main).unwrap();

        drop_table(&mut client, table);
    }

    #[test]
    #[serial_test::serial]
    fn test_postgres_field_name() {
        let mut client = postgres_client();
        let table = "02JWRRNVP4CGER2E3SQQKCZFNQ";
        let field = r#""01JWRRNVP4CGER2E3SQQKCZFNQ""#;

        client
            .execute(
                &format!(r#"CREATE TABLE "{table}" (id int primary key, {field} varchar)"#),
                &[],
            )
            .expect("failed to create test table in postgres");

        truncate_table(&mut client, table);

        let idx = relation("v1_idx", vec![int_field("id")], false);
        let main = relation("v1", vec![int_field("id"), varchar_field(field)], true);
        create_endpoint(table, Some(idx), main).unwrap();

        drop_table(&mut client, table);
    }

    #[test]
    #[serial_test::serial]
    fn test_postgres_key_field_name() {
        let mut client = postgres_client();
        let table = "02JWRRNVP4CGER2E3SQQKCZFNQ";
        let field = r#""01JWRRNVP4CGER2E3SQQKCZFNQ""#;

        client
            .execute(
                &format!(r#"CREATE TABLE "{table}" (id int primary key, {field} varchar)"#),
                &[],
            )
            .expect("failed to create test table in postgres");

        truncate_table(&mut client, table);

        let idx = relation(
            "v1_idx",
            vec![
                int_field("id"),
                varchar_field(r#""01JWRRNVP4CGER2E3SQQKCZFNQ""#),
            ],
            false,
        );
        let main = relation(
            "v1",
            vec![
                int_field("id"),
                varchar_field(r#""01JWRRNVP4CGER2E3SQQKCZFNQ""#),
            ],
            true,
        );
        create_endpoint("02JWRRNVP4CGER2E3SQQKCZFNQ", Some(idx), main).unwrap();

        drop_table(&mut client, table);
    }

    #[test]
    #[serial_test::serial]
    fn test_postgres_bad_field_name() {
        let mut client = postgres_client();
        let table = "02JWRRNVP4CGER2E3SQQKCZFNQ";
        let field = r#""01JWRRNVP4CGER2E3SQQKCZFNQ""#;

        client
            .execute(
                &format!(r#"CREATE TABLE "{table}" (id int primary key, {field} varchar)"#),
                &[],
            )
            .expect("failed to create test table in postgres");

        truncate_table(&mut client, table);

        let idx = relation("v1_idx", vec![int_field("id")], false);
        let main = relation(
            "v1",
            vec![int_field("id"), varchar_field("01JWRRNVP4CGER2E3SQQKCZFNQ")],
            true,
        );
        let err = create_endpoint("02JWRRNVP4CGER2E3SQQKCZFNQ", Some(idx), main)
            .err()
            .unwrap();
        assert!(err.to_string().contains("Please ensure all field"));

        drop_table(&mut client, table);
    }
}
