use std::{
    collections::HashSet,
    marker::PhantomPinned,
    mem::take,
    pin::{pin, Pin},
    str::FromStr,
    sync::Weak,
    time::Instant,
};

use crate::ControllerError;
use crate::{
    catalog::{CursorWithPolarity, RecordFormat, SerBatchReader, SerCursor},
    controller::{ControllerInner, EndpointId},
    format::{Encoder, OutputConsumer, MAX_DUPLICATES},
    transport::OutputEndpoint,
    util::truncate_ellipse,
};
use anyhow::{anyhow, bail, Result as AnyResult};
use feldera_adapterlib::transport::{AsyncErrorCallback, Step};
use feldera_types::{
    format::csv::CsvParserConfig, program_schema::Relation,
    transport::postgres::PostgresWriterConfig,
};
use postgres::{Client, NoTls, Statement};
use tracing::{info_span, span::EnteredSpan};

enum OperationType {
    Insert,
    Delete,
    Upsert,
}

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
    keys: HashSet<String>,
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
                "Postgres output connector requires a unique key. Please specify the `index` property in the connector configuration. For more details, see: https://docs.feldera.com/connectors/unique_keys"
            ))?;

        let keys: HashSet<String> = key_schema
            .fields
            .iter()
            .map(|f| f.name.to_string())
            .collect();

        let insert = {
            let insert = format!(
                "INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, $1::jsonb)"
            );
            client
                .prepare_typed(&insert, &[postgres::types::Type::VARCHAR])
                .map_err(|e| {
                    ControllerError::output_transport_error(
                        endpoint_name,
                        true,
                        anyhow!("failed to prepare insert statement: `{insert}`: {e}"),
                    )
                })?
        };

        let delete = {
            let (table_keys, d_keys): (Vec<_>, Vec<_>) = keys
                .iter()
                .map(|k| (format!("{table}.{k}"), format!("d.{k}")))
                .unzip();

            let delete = format!(
                "DELETE FROM {table} USING (SELECT {} FROM jsonb_populate_recordset(NULL::{table}, $1::jsonb)) as d where ({}) = ({})",
                keys.iter().map(|k| k.as_str()).collect::<Vec<_>>().join(", "),
                table_keys.join(", "),
                d_keys.join(", "),

            );
            client
                .prepare_typed(&delete, &[postgres::types::Type::VARCHAR])
                .map_err(|e| {
                    ControllerError::output_transport_error(
                        endpoint_name,
                        true,
                        anyhow!("failed to prepare delete statement: `{delete}`: {e}"),
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
                    let f = f.name.name();
                    format!("{f} = {new_alias}.{f}")
                })
                .collect::<Vec<_>>()
                .join(", ");

            let (table_fields, new_fields): (Vec<_>, Vec<_>) = keys
                .iter()
                .map(|f| (format!("{table_alias}.{f}"), format!("{new_alias}.{f}")))
                .unzip();

            let upsert = format!(
                "UPDATE {table} AS {table_alias} SET {columns} FROM (SELECT * FROM jsonb_populate_recordset(NULL::{table}, $1::jsonb)) AS {new_alias} WHERE ({}) = ({})",
                table_fields.join(", "),
                new_fields.join(", ")
            );

            client
                .prepare_typed(&upsert, &[postgres::types::Type::VARCHAR])
                .map_err(|e| {
                    ControllerError::output_transport_error(
                        endpoint_name,
                        true,
                        anyhow!("failed to prepare update statement: `{upsert}`: {e}"),
                    )
                })?
        };

        let out = Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_owned(),
            table,
            client,
            transaction: None,
            insert,
            delete,
            upsert,
            keys,
            key_schema,
            value_schema: value_schema.to_owned(),
            _pin: PhantomPinned,
        };

        let _guard = out.span();

        Ok(out)
    }

    fn transaction(&mut self) -> AnyResult<&mut postgres::Transaction<'static>> {
        self.transaction.as_mut().ok_or(anyhow!("postgres: unreachable: attempting to perform a transaction that hasn't been created yet"))
    }

    pub fn insert(&mut self, value: &str) -> AnyResult<()> {
        if value.len() <= 2 {
            return Ok(());
        }

        let ins = self.insert.clone();
        self.transaction()?
            .execute(&ins, &[&value])
            .map_err(|e| anyhow!("postgres: failed to insert data: {value}: {e}"))?;
        Ok(())
    }

    pub fn upsert(&mut self, value: &str) -> AnyResult<()> {
        if value.len() <= 2 {
            return Ok(());
        }

        let ups = self.upsert.clone();
        self.transaction()?
            .execute(&ups, &[&value])
            .map_err(|e| anyhow!("postgres: failed to upsert data: {value}: {e}"))?;

        Ok(())
    }

    pub fn delete(&mut self, value: &str) -> AnyResult<()> {
        if value.len() <= 2 {
            return Ok(());
        }

        let del = self.delete.clone();
        self.transaction()?
            .execute(&del, &[&value])
            .map_err(|e| anyhow!("postgres: failed to delete data: {value}: {e}"))?;
        Ok(())
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

    fn non_unique_key_error(&self) -> String {
        format!(
            "Postgres connector configured with 'index={}' encountered multiple values with the same key. When configured with SQL index, the connector expects keys to be unique. Please fix the '{}' view definition to ensure that '{}' is a unique index.",
            self.key_schema.name,
            self.value_schema.name,
            self.key_schema.name,
        )
    }

    fn operation_type(
        &mut self,
        cursor: &mut dyn SerCursor,
    ) -> anyhow::Result<Option<OperationType>> {
        let mut found_insert = false;
        let mut found_delete = false;

        while cursor.val_valid() {
            let w = cursor.weight();

            if w == 0 {
                cursor.step_val();
                continue;
            }

            if w != 1 && w != -1 {
                bail!(self.non_unique_key_error());
            }

            if w == 1 {
                if found_insert {
                    bail!(self.non_unique_key_error());
                }

                found_insert = true;
            }

            if w == -1 {
                if found_delete {
                    bail!(self.non_unique_key_error());
                }

                found_delete = true;
            }

            cursor.step_val();
        }

        cursor.rewind_vals();

        Ok(match (found_insert, found_delete) {
            (true, true) => Some(OperationType::Upsert),
            (true, false) => Some(OperationType::Insert),
            (false, true) => Some(OperationType::Delete),
            (false, false) => None,
        })
    }
}

impl OutputConsumer for PostgresOutputEndpoint {
    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn batch_start(&mut self, _: Step) {
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
    }
}

impl Encoder for PostgresOutputEndpoint {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, batch: &dyn SerBatchReader) -> anyhow::Result<()> {
        let mut insert_buffer = "[".to_owned();
        let mut delete_buffer = "[".to_owned();
        let mut upsert_buffer = "[".to_owned();

        let mut cursor = batch.cursor(RecordFormat::Json(Default::default()))?;

        while cursor.key_valid() {
            if let Some(op) = self.operation_type(cursor.as_mut())? {
                match op {
                    OperationType::Insert => {
                        let buf = unsafe { insert_buffer.as_mut_vec() };
                        if buf.last() != Some(&b'[') {
                            buf.push(b',');
                        }
                        cursor.serialize_val(buf)?;
                    }
                    OperationType::Delete => {
                        let buf = unsafe { delete_buffer.as_mut_vec() };
                        if buf.last() != Some(&b'[') {
                            buf.push(b',');
                        }
                        cursor.serialize_key_fields(&self.keys, buf)?;
                    }
                    OperationType::Upsert => {
                        if cursor.weight() < 0 {
                            cursor.step_val();
                        }

                        let buf = unsafe { upsert_buffer.as_mut_vec() };
                        if buf.last() != Some(&b'[') {
                            buf.push(b',');
                        }
                        cursor.serialize_val(buf)?;
                    }
                }
            }

            cursor.step_key();
        }

        insert_buffer.push(']');
        delete_buffer.push(']');
        upsert_buffer.push(']');

        self.delete(&delete_buffer)?;
        self.insert(&insert_buffer)?;
        self.upsert(&upsert_buffer)?;

        Ok(())
    }
}

impl OutputEndpoint for PostgresOutputEndpoint {
    fn connect(&mut self, _: AsyncErrorCallback) -> anyhow::Result<()> {
        todo!()
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
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
