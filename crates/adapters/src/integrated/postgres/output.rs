use std::{
    collections::HashSet,
    marker::PhantomPinned,
    mem::take,
    pin::{pin, Pin},
    str::FromStr,
    sync::Weak,
    time::Instant,
};

use crate::{
    catalog::{CursorWithPolarity, RecordFormat, SerBatchReader, SerCursor},
    controller::{ControllerInner, EndpointId},
    format::{Encoder, OutputConsumer, MAX_DUPLICATES},
    transport::OutputEndpoint,
    util::{truncate_ellipse, IndexedOperationType},
};
use crate::{util::indexed_operation_type, ControllerError};
use anyhow::{anyhow, bail, Result as AnyResult};
use feldera_adapterlib::transport::{AsyncErrorCallback, Step};
use feldera_types::{
    format::{csv::CsvParserConfig, json::JsonFlavor},
    program_schema::{Relation, SqlIdentifier},
    transport::postgres::PostgresWriterConfig,
};
use postgres::{Client, NoTls, Statement};
use tracing::{info_span, span::EnteredSpan};

pub struct PostgresOutputEndpoint {
    endpoint_id: EndpointId,
    endpoint_name: String,
    table: String,
    client: postgres::Client,
    transaction: Option<postgres::Transaction<'static>>,
    insert: Statement,
    upsert: Statement,
    delete: Statement,
    key_schema: Relation,
    value_schema: Relation,
    controller: Weak<ControllerInner>,
    num_bytes: usize,
    num_rows: usize,
    _pin: PhantomPinned,
}

impl Drop for PostgresOutputEndpoint {
    fn drop(&mut self) {
        self.transaction = None;
    }
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

        let config = postgres::Config::from_str(&config.uri).map_err(|e| {
            ControllerError::invalid_transport_configuration(
                endpoint_name,
                &format!("error parsing postgres connection string: {e}"),
            )
        })?;

        let mut client = config.connect(NoTls).map_err(|e| {
            ControllerError::invalid_transport_configuration(
                endpoint_name,
                &format!("failed to connect to postgres: {e}"),
            )
        })?;

        let key_schema = key_schema
            .to_owned()
            .ok_or(ControllerError::not_supported(
                "Postgres output connector requires the view to have a unique key. Please specify the `index` property in the connector configuration. For more details, see: https://docs.feldera.com/connectors/unique_keys"
            ))?;

        let keys: Vec<String> = key_schema
            .fields
            .iter()
            .map(|f| f.name.sql_name())
            .collect();

        let err_msg = "\nPlease ensure all field names that are quoted in PostgreSQL are quoted correctly in Feldera as well";

        let insert = {
            let insert = format!(
                r#"INSERT INTO "{table}" SELECT * FROM jsonb_populate_recordset(NULL::"{table}", $1::jsonb)"#
            );
            client
                .prepare_typed(&insert, &[postgres::types::Type::VARCHAR])
                .map_err(|e| {
                    ControllerError::output_transport_error(
                        endpoint_name,
                        true,
                        anyhow!("failed to prepare insert statement: `{insert}`: {e} {err_msg}"),
                    )
                })?
        };

        let delete = {
            let (table_keys, d_keys): (Vec<_>, Vec<_>) = keys
                .iter()
                .map(|k| (format!(r#" "{table}".{k} "#), format!("d.{k}")))
                .unzip();

            let delete = format!(
                r#"DELETE FROM "{table}" USING (SELECT {} FROM jsonb_populate_recordset(NULL::"{table}", $1::jsonb)) as d where ({}) = ({})"#,
                keys.iter()
                    .map(|k| k.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                table_keys.join(", "),
                d_keys.join(", "),
            );
            client
                .prepare_typed(&delete, &[postgres::types::Type::VARCHAR])
                .map_err(|e| {
                    ControllerError::output_transport_error(
                        endpoint_name,
                        true,
                        anyhow!("failed to prepare delete statement: `{delete}`: {e} {err_msg}"),
                    )
                })?
        };

        let upsert = {
            let table_alias = "t";
            let new_alias = "n";
            let columns = value_schema
                .fields
                .iter()
                .map(|f| {
                    let f = f.name.sql_name();
                    format!("{f} = {new_alias}.{f}")
                })
                .collect::<Vec<_>>()
                .join(", ");

            let (table_fields, new_fields): (Vec<_>, Vec<_>) = keys
                .iter()
                .map(|f| (format!("{table_alias}.{f}"), format!("{new_alias}.{f}")))
                .unzip();

            let upsert = format!(
                r#"UPDATE "{table}" AS {table_alias} SET {columns} FROM (SELECT * FROM jsonb_populate_recordset(NULL::"{table}", $1::jsonb)) AS {new_alias} WHERE ({}) = ({})"#,
                table_fields.join(", "),
                new_fields.join(", ")
            );

            client
                .prepare_typed(&upsert, &[postgres::types::Type::VARCHAR])
                .map_err(|e| {
                    ControllerError::output_transport_error(
                        endpoint_name,
                        true,
                        anyhow!("failed to prepare update statement: `{upsert}`: {e} {err_msg}"),
                    )
                })?
        };

        let out = Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_owned(),
            controller,
            table,
            client,
            transaction: None,
            insert,
            delete,
            upsert,
            key_schema,
            num_rows: 0,
            num_bytes: 0,
            value_schema: value_schema.to_owned(),
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
        self.transaction.as_mut().ok_or(anyhow!("postgres: unreachable: attempting to perform a transaction that hasn't been created yet"))
    }

    fn exec_statement(
        &mut self,
        stmt: Statement,
        value: &mut Vec<u8>,
        name: &str,
    ) -> AnyResult<()> {
        if value.last() != Some(&b']') {
            value.push(b']');
        }

        if value.len() <= 2 {
            return Ok(());
        }

        self.num_bytes += value.len();

        let v: &str = std::str::from_utf8(value.as_slice())
            .map_err(|e| anyhow!("postgres: record contains non utf-8 characters: {e}"))?;

        self.transaction()?
            .execute(&stmt, &[&v])
            .map_err(|e| anyhow!("postgres: failed to execute {name} statement: {v}: {e}"))?;

        value.clear();
        value.push(b'[');

        Ok(())
    }

    /// Executes the insert statement within the transaction and resets the
    /// buffer back to `[`.
    pub fn insert(&mut self, value: &mut Vec<u8>) -> AnyResult<()> {
        self.exec_statement(self.insert.clone(), value, "insert")
    }

    /// Executes the upsert statement within the transaction and resets the
    /// buffer back to `[`.
    pub fn upsert(&mut self, value: &mut Vec<u8>) -> AnyResult<()> {
        self.exec_statement(self.upsert.clone(), value, "upsert")
    }

    /// Executes the delete statement within the transaction and resets the
    /// buffer back to `[`.
    pub fn delete(&mut self, value: &mut Vec<u8>) -> AnyResult<()> {
        self.exec_statement(self.delete.clone(), value, "delete")
    }

    pub fn span(&self) -> EnteredSpan {
        info_span!(
            "postgres_output",
            ft = false,
            id = self.endpoint_id,
            name = self.endpoint_name,
            pg_table = self.table,
        )
        .entered()
    }
}

impl OutputConsumer for PostgresOutputEndpoint {
    /// 1 MiB
    fn max_buffer_size_bytes(&self) -> usize {
        usize::pow(2, 20)
    }

    fn batch_start(&mut self, _: Step) {
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
        // TODO: Consider [`std::pin::Pin`]ing the integrated conenctor.
        let transaction: postgres::Transaction<'static> = unsafe {
            std::mem::transmute(
                self.client
                    .transaction()
                    .expect("postgres: failed to start transaction"),
            )
        };

        self.transaction = Some(transaction);
    }

    fn push_buffer(&mut self, _: &[u8], _: usize) {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _: Option<&[u8]>,
        _: Option<&[u8]>,
        _: &[(&str, Option<&[u8]>)],
        num_records: usize,
    ) {
        unreachable!()
    }

    fn batch_end(&mut self) {
        let transaction: postgres::Transaction<'static> = self.transaction.take().expect(
            "postgres: unreachable: attempted to commit a transaction that hasn't been started",
        );
        transaction
            .commit()
            .expect("postgres: failed to commit transaction");

        if let Some(controller) = self.controller.upgrade() {
            controller.status.output_buffer(
                self.endpoint_id,
                std::mem::take(&mut self.num_bytes),
                std::mem::take(&mut self.num_rows),
            )
        }
    }
}

impl Encoder for PostgresOutputEndpoint {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, batch: &dyn SerBatchReader) -> anyhow::Result<()> {
        let mut insert_buffer: Vec<u8> = vec![b'['];
        let mut delete_buffer: Vec<u8> = vec![b'['];
        let mut upsert_buffer: Vec<u8> = vec![b'['];

        let max_buffer_size = OutputConsumer::max_buffer_size_bytes(self);

        let mut cursor = batch.cursor(RecordFormat::Json(JsonFlavor::Postgres))?;

        while cursor.key_valid() {
            if let Some(op) =
                indexed_operation_type(self.view_name(), self.index_name(), cursor.as_mut())?
            {
                cursor.rewind_vals();
                match op {
                    IndexedOperationType::Insert => {
                        let buf = &mut insert_buffer;
                        let mut new_buf: Vec<u8> = Vec::new();
                        if buf.last() != Some(&b'[') {
                            new_buf.push(b',');
                        }
                        cursor.serialize_val(&mut new_buf)?;
                        if new_buf.len() + buf.len() > max_buffer_size {
                            self.insert(buf)?;
                        }
                        buf.append(&mut new_buf);
                    }
                    IndexedOperationType::Delete => {
                        let buf = &mut delete_buffer;
                        let mut new_buf: Vec<u8> = Vec::new();
                        if buf.last() != Some(&b'[') {
                            new_buf.push(b',');
                        }
                        cursor.serialize_key(&mut new_buf)?;
                        if new_buf.len() + buf.len() > max_buffer_size {
                            self.delete(buf)?;
                        }
                        buf.append(&mut new_buf);
                    }
                    IndexedOperationType::Upsert => {
                        if cursor.weight() < 0 {
                            cursor.step_val();
                        }

                        let buf = &mut upsert_buffer;
                        let mut new_buf: Vec<u8> = Vec::new();
                        if buf.last() != Some(&b'[') {
                            new_buf.push(b',');
                        }
                        cursor.serialize_val(&mut new_buf)?;
                        if new_buf.len() + buf.len() > max_buffer_size {
                            self.upsert(buf)?;
                        }
                        buf.append(&mut new_buf);
                    }
                };

                self.num_rows += 1;
            }

            cursor.step_key();
        }

        self.delete(&mut delete_buffer)?;
        self.insert(&mut insert_buffer)?;
        self.upsert(&mut upsert_buffer)?;

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

    fn push_buffer(&mut self, buffer: &[u8]) -> anyhow::Result<()> {
        unreachable!()
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
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
