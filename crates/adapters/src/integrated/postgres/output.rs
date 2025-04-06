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
    util::truncate_ellipse,
};
use anyhow::{anyhow, bail, Result as AnyResult};
use feldera_adapterlib::{
    errors::metadata::ControllerError,
    transport::{AsyncErrorCallback, Step},
};
use feldera_types::{
    format::csv::CsvParserConfig, program_schema::Relation,
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
    delete: Statement,
    start: Instant,
    keys: HashSet<String>,
    _pin: PhantomPinned,
}

impl PostgresOutputEndpoint {
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &PostgresWriterConfig,
        key_schema: &Option<Relation>,
        _schema: &Relation,
        controller: Weak<ControllerInner>,
    ) -> Result<Self, ControllerError> {
        let table = config.table.to_owned();

        let config = postgres::Config::from_str(&config.uri).map_err(|e| {
            ControllerError::output_transport_error(
                endpoint_name,
                true,
                anyhow!("error parsing postgres connection string: {e}"),
            )
        })?;

        let mut client = config.connect(NoTls).map_err(|e| {
            ControllerError::output_transport_error(
                endpoint_name,
                true,
                anyhow!("failed to connect to postgres: {e}"),
            )
        })?;

        let keys: HashSet<String> = key_schema
            .as_ref()
            .map(|sch| sch.fields.iter().map(|f| f.name.to_string()).collect())
            .ok_or(ControllerError::not_supported(
                "postgres output connector requires index or primary key definition",
            ))?;

        let insert = {
            let insert = format!(
                "INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, $1::jsonb)"
            );
            client
                .prepare_typed(&insert, &[postgres::types::Type::VARCHAR])
                .unwrap_or_else(|e| panic!("failed to prepare insert statement: `{insert}`: {e}"))
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
                .unwrap_or_else(|e| panic!("failed to prepare delete statement: `{delete}`: {e}"))
        };

        let out = Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_owned(),
            table,
            start: Instant::now(),
            client,
            transaction: None,
            insert,
            delete,
            keys,
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
        self.transaction()?.execute(&ins, &[&value])?;
        Ok(())
    }

    pub fn delete(&mut self, value: &str) -> AnyResult<()> {
        if value.len() <= 2 {
            return Ok(());
        }

        let del = self.delete.clone();
        self.transaction()?.execute(&del, &[&value])?;
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
        self.start = Instant::now();
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
        let elapsed = self.start.elapsed();

        tracing::info!("transaction took: {elapsed:?}",);
    }
}

impl Encoder for PostgresOutputEndpoint {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, batch: &dyn SerBatchReader) -> anyhow::Result<()> {
        let mut insert_buffer = "[".to_owned();
        let mut delete_buffer = "[".to_owned();

        let mut cursor =
            CursorWithPolarity::new(batch.cursor(RecordFormat::Json(Default::default()))?);

        while cursor.key_valid() {
            if !cursor.val_valid() {
                cursor.step_key();
                continue;
            }
            let mut w = cursor.weight();

            if !(-MAX_DUPLICATES..=MAX_DUPLICATES).contains(&w) {
                let mut key_str = String::new();
                let _ = cursor.serialize_val(unsafe { key_str.as_mut_vec() });
                bail!(
                        "Unable to output record '{}' with very large weight {w}. Consider adjusting your SQL queries to avoid duplicate output records, e.g., using 'SELECT DISTINCT'.",
                        &key_str
                    );
            }

            while w != 0 {
                match w {
                    x if x < 0 => {
                        let buf = unsafe { delete_buffer.as_mut_vec() };
                        if buf.last() != Some(&b'[') {
                            buf.push(b',');
                        }
                        cursor.serialize_key_fields(&self.keys, buf)?;
                        w += 1;
                    }
                    x if x > 0 => {
                        let buf = unsafe { insert_buffer.as_mut_vec() };
                        if buf.last() != Some(&b'[') {
                            buf.push(b',');
                        }
                        cursor.serialize_val(buf)?;
                        w -= 1;
                    }
                    _ => continue,
                }
            }

            cursor.step_key();
        }

        insert_buffer.push(']');
        delete_buffer.push(']');

        self.insert(&insert_buffer)?;
        self.delete(&delete_buffer)?;

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
