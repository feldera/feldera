use crate::controller::{ControllerInner, EndpointId};
use crate::format::InputBuffer;
use crate::transport::{
    InputEndpoint, InputQueue, InputReaderCommand, IntegratedInputEndpoint, NonFtInputReaderCommand,
};
use crate::{
    ControllerError, InputConsumer, InputReader, PipelineState, RecordFormat,
    TransportInputEndpoint,
};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use dbsp::circuit::tokio::TOKIO;
use dbsp::InputHandle;

use feldera_adapterlib::catalog::{DeCollectionStream, InputCollectionHandle};
use feldera_adapterlib::format::ParseError;
use feldera_types::config::{FtModel, InputEndpointConfig};
use feldera_types::format::json::JsonFlavor;
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlType};
use feldera_types::transport::postgres::PostgresReaderConfig;

use chrono::{TimeZone, Utc};
use futures::{pin_mut, TryFutureExt};
use futures_util::{StreamExt, TryStreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::fmt::format;
use std::str::FromStr;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch::{channel, Receiver, Sender};
use tokio::time::sleep;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::types::{FromSql, Type};
use tokio_postgres::{Client, Connection, NoTls, Socket};
use tokio_util::time;
use tracing::{debug, error, info, trace};
use tracing_subscriber::fmt::fmt;
use url::Url;
use utoipa::ToSchema;
use uuid::Uuid;

/// Integrated input connector that reads from a delta table.
pub struct PostgresInputEndpoint {
    inner: Arc<PostgresInputEndpointInner>,
}

impl PostgresInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &PostgresReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        Self {
            inner: Arc::new(PostgresInputEndpointInner::new(
                endpoint_name,
                config.clone(),
                consumer,
            )),
        }
    }
}

impl InputEndpoint for PostgresInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        None
    }
}
impl IntegratedInputEndpoint for PostgresInputEndpoint {
    fn open(
        self: Box<Self>,
        input_handle: &InputCollectionHandle,
        _resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(PostgresInputReader::new(
            &self.inner,
            input_handle,
        )?))
    }
}

struct PostgresInputReader {
    sender: Sender<PipelineState>,
    inner: Arc<PostgresInputEndpointInner>,
}

impl PostgresInputReader {
    fn new(
        endpoint: &Arc<PostgresInputEndpointInner>,
        input_handle: &InputCollectionHandle,
    ) -> AnyResult<Self> {
        let (sender, receiver) = channel(PipelineState::Paused);
        let endpoint_clone = endpoint.clone();
        let receiver_clone = receiver.clone();

        // Used to communicate the status of connector initialization.
        let (init_status_sender, mut init_status_receiver) =
            mpsc::channel::<Result<(), ControllerError>>(1);

        let input_stream = input_handle
            .handle
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Datagen))?;
        let schema = input_handle.schema.clone();

        std::thread::spawn(move || {
            TOKIO.block_on(async {
                let _ = endpoint_clone
                    .worker_task(input_stream, schema, receiver_clone, init_status_sender)
                    .await;
            })
        });

        init_status_receiver.blocking_recv().ok_or_else(|| {
            ControllerError::input_transport_error(
                &endpoint.endpoint_name,
                true,
                anyhow!("worker thread terminated unexpectedly during initialization"),
            )
        })??;

        Ok(Self {
            sender,
            inner: endpoint.clone(),
        })
    }
}

impl InputReader for PostgresInputReader {
    fn request(&self, command: InputReaderCommand) {
        match command.as_nonft().unwrap() {
            NonFtInputReaderCommand::Queue => self.inner.queue.queue(),
            NonFtInputReaderCommand::Transition(state) => drop(self.sender.send_replace(state)),
        }
    }

    fn is_closed(&self) -> bool {
        self.inner.queue.is_empty() && self.sender.is_closed()
    }
}

impl Drop for PostgresInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

struct PostgresInputEndpointInner {
    endpoint_name: String,
    config: PostgresReaderConfig,
    consumer: Box<dyn InputConsumer>,
    queue: InputQueue,
}

impl PostgresInputEndpointInner {
    fn new(
        endpoint_name: &str,
        config: PostgresReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        let queue = InputQueue::new(consumer.clone());

        Self {
            endpoint_name: endpoint_name.to_string(),
            config,
            consumer,
            queue,
        }
    }

    async fn worker_task(
        self: Arc<Self>,
        input_stream: Box<dyn DeCollectionStream>,
        schema: Relation,
        receiver: Receiver<PipelineState>,
        init_status_sender: mpsc::Sender<Result<(), ControllerError>>,
    ) {
        let mut receiver_clone = receiver.clone();
        select! {
            _ = Self::worker_task_inner(self.clone(), input_stream, schema, receiver, init_status_sender) => {
                debug!("postgres {}: worker task terminated",
                    &self.endpoint_name,
                );
            }
            _ = receiver_clone.wait_for(|state| state == &PipelineState::Terminated) => {
                debug!("postgres {}: received termination command; worker task canceled",
                    &self.endpoint_name,
                );
            }
        }
    }

    async fn worker_task_inner(
        self: Arc<Self>,
        mut input_stream: Box<dyn DeCollectionStream>,
        schema: Relation,
        mut receiver: Receiver<PipelineState>,
        init_status_sender: mpsc::Sender<Result<(), ControllerError>>,
    ) {
        let client = match self.connect_to_postgres().await {
            Err(e) => {
                let _ = init_status_sender.send(Err(e)).await;
                return;
            }
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("connection error: {}", e);
                    }
                });
                client
            }
        };

        let _r = init_status_sender.send(Ok(())).await;
        wait_running(&mut receiver).await;

        let rows = match client
            .query(self.config.query.as_str(), &[])
            .await
            .map_err(|e| {
                ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("error executing query: {e}"),
                )
            }) {
            Ok(rows) => rows,
            Err(e) => {
                error!(
                    "postgres {}: error reading from postgres: {e}",
                    &self.endpoint_name
                );
                let _r = init_status_sender.send(Err(e)).await;
                return;
            }
        };

        let mut last_event_number = 0;
        let mut bytes = 0;
        let mut errors = Vec::new();
        for row in rows {
            let columns = row.columns();
            let mut dynamic_values = serde_json::Map::new();

            for (col_idx, col) in columns.iter().enumerate() {
                if !schema
                    .fields
                    .iter()
                    .map(|f| &f.name)
                    .any(|n| n == &col.name())
                {
                    // we ignore fields loaded from postgres that are not in the feldera table we ingest into
                    continue;
                }

                let col_name = col.name();
                let col_type = col.type_();

                let value = match *col_type {
                    Type::BOOL => {
                        let v: Option<bool> = row.get(col_idx);
                        json!(v)
                    }
                    Type::BOOL_ARRAY => {
                        let v: Option<Vec<bool>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::BYTEA => {
                        let v: Option<Vec<u8>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::BYTEA_ARRAY => {
                        let v: Option<Vec<Vec<u8>>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::BPCHAR | Type::CHAR | Type::VARCHAR | Type::TEXT | Type::NAME => {
                        let v: Option<String> = row.get(col_idx);
                        json!(v)
                    }
                    Type::BPCHAR_ARRAY
                    | Type::CHAR_ARRAY
                    | Type::VARCHAR_ARRAY
                    | Type::TEXT_ARRAY
                    | Type::NAME_ARRAY => {
                        let v: Option<Vec<String>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::DATE => {
                        let v: Option<chrono::NaiveDate> = row.get(col_idx);
                        json!(v)
                    }
                    Type::DATE_ARRAY => {
                        let v: Option<Vec<chrono::NaiveDate>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::TIME => {
                        let v: Option<chrono::NaiveTime> = row.get(col_idx);
                        json!(v)
                    }
                    Type::TIME_ARRAY => {
                        let v: Option<Vec<chrono::NaiveTime>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::TIMESTAMP => {
                        let v: Option<chrono::NaiveDateTime> = row.get(col_idx);
                        let vutc = v.as_ref().map(|v| Utc.from_utc_datetime(v).to_rfc3339());
                        json!(vutc)
                    }
                    Type::TIMESTAMP_ARRAY => {
                        let v: Option<Vec<chrono::NaiveDateTime>> = row.get(col_idx);
                        let vutc: Option<Vec<String>> = v.map(|v| {
                            v.into_iter()
                                .map(|v| Utc.from_utc_datetime(&v).to_rfc3339())
                                .collect::<Vec<String>>()
                        });
                        json!(vutc)
                    }
                    Type::INT2 => {
                        let v: Option<i16> = row.get(col_idx);
                        json!(v)
                    }
                    Type::INT2_ARRAY => {
                        let v: Option<Vec<i16>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::INT4 => {
                        let v: Option<i32> = row.get(col_idx);
                        json!(v)
                    }
                    Type::INT4_ARRAY => {
                        let v: Option<Vec<i32>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::INT8 => {
                        let v: Option<i64> = row.get(col_idx);
                        json!(v)
                    }
                    Type::INT8_ARRAY => {
                        let v: Option<Vec<i64>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::FLOAT4 => {
                        let v: Option<f32> = row.get(col_idx);
                        json!(v)
                    }
                    Type::FLOAT4_ARRAY => {
                        let v: Option<Vec<f32>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::FLOAT8 => {
                        let v: Option<f64> = row.get(col_idx);
                        json!(v)
                    }
                    Type::FLOAT8_ARRAY => {
                        let v: Option<Vec<f64>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::UUID => {
                        let v: Option<Uuid> = row.get(col_idx);
                        json!(v)
                    }
                    Type::UUID_ARRAY => {
                        let v: Option<Vec<Uuid>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::NUMERIC => {
                        let v: Option<Decimal> = row.get(col_idx);
                        json!(v)
                    }
                    Type::NUMERIC_ARRAY => {
                        let v: Option<Vec<Decimal>> = row.get(col_idx);
                        json!(v)
                    }
                    Type::JSON => {
                        let v: Option<Value> = row.get(col_idx);
                        json!(v)
                    }
                    Type::JSON_ARRAY => {
                        let v: Option<Vec<Value>> = row.get(col_idx);
                        json!(v)
                    }
                    _ => {
                        // note that it's an unrecognized type:
                        errors.push(
                            ParseError::bin_envelope_error(
                                format!("error deserializing table records from PostgreSQL: unrecognized column type '{col_type}' for column '{col_name}'"),
                                &[],
                                None,
                            ));
                        Value::Null
                    }
                };
                dynamic_values.insert(col_name.to_string(), value);
                last_event_number += 1;
            }
            let value = json!(dynamic_values).to_string();
            debug!("postgres about to insert {value}");
            let ret = input_stream.insert(value.as_ref());
            if let Err(ret) = ret {
                errors.push(ParseError::text_event_error(
                    "Failed to deserialize table record from PostgreSQL",
                    ret,
                    last_event_number,
                    Some(value.as_str()),
                    None,
                ));
            }
            bytes += value.len();
            if bytes >= 1024 * 1024 * 2 {
                self.queue.push((input_stream.take_all(), errors), bytes);
                bytes = 0;
                errors = Vec::new();
            }
        }

        self.queue.push((input_stream.take_all(), errors), bytes);
        self.consumer.eoi();
    }

    /// Open existing postgres connection.
    async fn connect_to_postgres(
        &self,
    ) -> Result<(Client, Connection<Socket, NoTlsStream>), ControllerError> {
        debug!(
            "postgres {}: opening connection to '{}'",
            &self.endpoint_name, &self.config.uri
        );

        let (client, connection) = tokio_postgres::connect(self.config.uri.as_str(), NoTls)
            .await
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!(
                        "Unable to connect to postgres instance '{}': {e}",
                        self.config.uri
                    ),
                )
            })?;

        info!(
            "postgres {}: opened connection to '{}'",
            &self.endpoint_name, &self.config.uri,
        );

        Ok((client, connection))
    }
}

/// Block until the state is `Running`.
async fn wait_running(receiver: &mut Receiver<PipelineState>) {
    // An error indicates that the channel was closed.  It's ok to ignore
    // the error as this situation will be handled by the top-level select,
    // which will abort the worker thread.
    let _ = receiver
        .wait_for(|state| state == &PipelineState::Running)
        .await;
}
