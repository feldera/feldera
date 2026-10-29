use crate::transport::{
    InputEndpoint, InputQueue, InputReaderCommand, IntegratedInputEndpoint, NonFtInputReaderCommand,
};
use crate::{ControllerError, InputConsumer, InputReader, PipelineState, RecordFormat};
use anyhow::{Result as AnyResult, anyhow};
use feldera_adapterlib::catalog::{DeCollectionStream, InputCollectionHandle};
use feldera_adapterlib::format::ParseError;
use feldera_storage::tokio::TOKIO;
use feldera_types::config::FtModel;
use feldera_types::format::json::JsonFlavor;
use feldera_types::program_schema::Relation;
use feldera_types::transport::postgres::PostgresReaderConfig;

use super::tls::make_tls_connector;
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch::{Receiver, Sender, channel};
use tokio_postgres::config::Host;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, error, info};
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

        // Used to communicate the status of connector initialization.
        let (init_status_sender, mut init_status_receiver) =
            mpsc::channel::<Result<(), ControllerError>>(1);

        let input_stream = input_handle
            .handle
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Datagen))?;
        let schema = input_handle.schema.clone();

        thread::Builder::new()
            .name("postgres-input-tokio-wrapper".to_string())
            .spawn(move || {
                TOKIO.block_on(async {
                    let _ = endpoint_clone
                        .worker_task(input_stream, schema, receiver, init_status_sender)
                        .await;
                })
            })
            .expect("failed to create Postgres input connector tokio wrapper thread");

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
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        match command.as_nonft().unwrap() {
            NonFtInputReaderCommand::Queue => self.inner.queue.queue(),
            NonFtInputReaderCommand::Transition(state) => drop(self.sender.send_replace(state)),
        }
    }

    fn is_closed(&self) -> bool {
        // This reports the reader as closed once its worker exits, which also
        // happens after the end of input.  [InputReader::is_closed] does not
        // allow that, but here it does no harm: [Self::request] answers
        // [InputReaderCommand::Queue] itself, without the worker, so the
        // controller never finds the reader closed without an answer, and so
        // never removes it.
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
            Ok(client) => client,
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
                // Initialization already succeeded, so nobody is receiving on
                // `init_status_sender` anymore.  Report the failure to the
                // controller instead; sending it on the init channel would
                // leave the pipeline stalled without an error.
                self.consumer
                    .error(true, anyhow!("error reading from Postgres: {e}"), None);
                return;
            }
        };

        let mut last_event_number = 0;
        let mut bytes = 0;
        let mut errors = Vec::new();

        // Set the ingestion timestamp to the time we start reading the next batch.
        let mut timestamp = Utc::now();

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
            let ret = input_stream.insert(value.as_ref(), &None);
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
                self.queue
                    .push((input_stream.take_all(), errors), timestamp);
                timestamp = Utc::now();
                bytes = 0;
                errors = Vec::new();
            }
        }

        self.queue
            .push((input_stream.take_all(), errors), timestamp);
        self.consumer.eoi();
    }

    /// Open a postgres connection, optionally with TLS.
    async fn connect_to_postgres(&self) -> Result<Client, ControllerError> {
        connect(&self.endpoint_name, &self.config).await
    }
}

/// The hosts, ports and database of a Postgres URI, for logs and errors.
///
/// The URI can hold a password, so the URI itself never goes into a log or an
/// error. `None` when the URI does not parse; the connection attempt then
/// reports the parse error on its own.
fn connection_target(uri: &str) -> Option<String> {
    let config = tokio_postgres::Config::from_str(uri).ok()?;
    let hosts = config
        .get_hosts()
        .iter()
        .map(|host| match host {
            Host::Tcp(host) => host.clone(),
            #[cfg(unix)]
            Host::Unix(path) => path.display().to_string(),
        })
        .collect::<Vec<_>>()
        .join(",");
    let ports = config
        .get_ports()
        .iter()
        .map(|port| port.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let dbname = config.get_dbname().unwrap_or_default();
    Some(format!("{hosts}:{ports}/{dbname}"))
}

/// Open a postgres connection for `config`, optionally with TLS.
async fn connect(
    endpoint_name: &str,
    config: &PostgresReaderConfig,
) -> Result<Client, ControllerError> {
    // Shown instead of the URI, which can hold the password.
    let target = connection_target(&config.uri)
        .map(|target| format!(" at '{target}'"))
        .unwrap_or_default();
    debug!("postgres {endpoint_name}: opening connection{target}");

    let connector = make_tls_connector(&config.tls, endpoint_name).map_err(|e| {
        ControllerError::invalid_transport_configuration(
            endpoint_name,
            &format!("failed to configure TLS: {e}"),
        )
    })?;

    let make_conn_err = |e: tokio_postgres::Error| {
        ControllerError::invalid_transport_configuration(
            endpoint_name,
            &format!("Unable to connect to postgres instance{target}: {e}"),
        )
    };

    fn spawn_pg_connection_monitor(
        name: String,
        conn: impl Future<Output = Result<(), tokio_postgres::Error>> + Send + 'static,
    ) {
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("postgres {name}: connection error: {e}");
            }
        });
    }

    let client = if let Some(connector) = connector {
        debug!("postgres {endpoint_name}: CA certificate provided, connecting with TLS");
        let (client, connection) = tokio_postgres::connect(config.uri.as_str(), connector)
            .await
            .map_err(make_conn_err)?;
        spawn_pg_connection_monitor(endpoint_name.to_string(), connection);
        client
    } else {
        debug!("postgres {endpoint_name}: no CA certificates provided, connecting without TLS");
        let (client, connection) = tokio_postgres::connect(config.uri.as_str(), NoTls)
            .await
            .map_err(make_conn_err)?;
        spawn_pg_connection_monitor(endpoint_name.to_string(), connection);
        client
    };

    info!("postgres {endpoint_name}: opened connection{target}");

    Ok(client)
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

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_types::transport::postgres::PostgresTlsConfig;

    const PASSWORD: &str = "hunter2-do-not-print";

    /// A Unix socket directory that does not exist, so a connection through
    /// it fails at once. No port is involved, so no other test can take it.
    const NO_SOCKET_DIR: &str = "/nonexistent-feldera-2136/pg";

    fn config(uri: &str) -> PostgresReaderConfig {
        PostgresReaderConfig {
            uri: uri.to_string(),
            query: "SELECT 1".to_string(),
            tls: PostgresTlsConfig::default(),
        }
    }

    /// A failed connection names the endpoint and the host but not the
    /// password, in the message and in the serialized error alike.
    #[test]
    fn connect_error_omits_the_password() {
        let uri = format!("postgresql://alice:{PASSWORD}@/orders?host={NO_SOCKET_DIR}");
        let error = TOKIO
            .block_on(connect("pg_in", &config(&uri)))
            .expect_err("the connection is refused");

        for text in [error.to_string(), serde_json::to_string(&error).unwrap()] {
            assert!(!text.contains(PASSWORD), "password in error: {text}");
            assert!(!text.contains(&uri), "URI in error: {text}");
            assert!(text.contains("pg_in"), "endpoint missing: {text}");
            assert!(text.contains(NO_SOCKET_DIR), "host missing: {text}");
            assert!(text.contains("orders"), "database missing: {text}");
        }
    }

    /// The target summary never holds the password, for either URI syntax.
    #[test]
    fn connection_target_omits_the_password() {
        let url = format!("postgresql://alice:{PASSWORD}@db.example:5433/orders");
        assert_eq!(
            connection_target(&url).as_deref(),
            Some("db.example:5433/orders")
        );
        let keywords =
            format!("host=db.example port=5433 user=alice password={PASSWORD} dbname=orders");
        assert_eq!(
            connection_target(&keywords).as_deref(),
            Some("db.example:5433/orders")
        );
        // A URI without a port names the default one.
        assert_eq!(
            connection_target("postgresql://alice@db.example/orders").as_deref(),
            Some("db.example:5432/orders")
        );
        assert_eq!(connection_target("not a uri at all ="), None);
    }
}
