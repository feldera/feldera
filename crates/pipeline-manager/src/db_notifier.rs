// Machinery to subscribe to notifications from database tables.
// Intended to be used by various reconciliation loops.
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::tenant::TenantId;
use futures_util::{stream, StreamExt};
use log::{debug, error, trace, warn};
use postgres_openssl::TlsStream;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::{tls::NoTlsStream, AsyncMessage, Connection, Socket};
use uuid::Uuid;

const RETRY_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_secs(2);

#[derive(Debug, PartialEq, Eq)]
pub enum DbNotification {
    Pipeline(Operation, TenantId, PipelineId),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Operation {
    Add,
    Delete,
    Update,
}

// Rust complains that fields in this enum are never read, even though they are read by `Debug`.
#[allow(dead_code)]
#[derive(Debug)]
enum NotificationError {
    MissingPayloadComponents(String),
    InvalidOperationError(String),
    InvalidUuid(uuid::Error),
    InvalidChannel(String),
    SyncError(DBError),
}

impl From<uuid::Error> for NotificationError {
    fn from(value: uuid::Error) -> Self {
        NotificationError::InvalidUuid(value)
    }
}

fn parse_operation(s: &str) -> Result<Operation, NotificationError> {
    match s {
        "A" => Ok(Operation::Add),
        "U" => Ok(Operation::Update),
        "D" => Ok(Operation::Delete),
        _ => Err(NotificationError::InvalidOperationError(s.to_string())),
    }
}

/// Listen for changes to a table (currently set up for the program, pipeline
/// and pipeline_runtime_state tables).
///
/// Partly inspired by the Kubernetes Informers machinery.
///
/// Calling listen() subscribes to a stream of `DbNotification` messages,
/// received on the rx-half of the tx `UnboundedSender`. listen() will retry in
/// case of connection failures. After establishing a connection, it will first
/// issue a notification for every entry in the subscribed tables. These will
/// show up as `Operation::Add` events.
///
/// TODO: Unlike Kubernetes informers, we do not supply the entire object per
/// event. Ideally, we'd also be checking object versions and only issue
/// notifications for version changes, and discard notifications about older
/// versions. In the absence of such a check, there is a potential race +
/// redundnacy between sync() calls and receiving notifications from the
/// registered LISTEN calls
pub async fn listen(
    conn: Arc<tokio::sync::Mutex<StoragePostgres>>,
    tx: UnboundedSender<DbNotification>,
) {
    let db_config = conn.lock().await.db_config.clone();
    loop {
        enum ConnWrapper {
            Tls(Connection<Socket, TlsStream<Socket>>),
            NoTls(Connection<Socket, NoTlsStream>),
        }

        let (client, mut connection) = if let Some(connector) = db_config
            .tls_connector()
            .expect("Unable to get Tls connector")
        {
            let (client, connection) = conn.lock().await.config.connect(connector).await.unwrap();
            (client, ConnWrapper::Tls(connection))
        } else {
            let (client, connection) = conn
                .lock()
                .await
                .config
                .connect(tokio_postgres::NoTls)
                .await
                .unwrap();
            (client, ConnWrapper::NoTls(connection))
        };

        let client = Arc::new(client);
        // Not clear why the calls to LISTEN block forever when we call it from the same
        // thread. But we need to call LISTEN before we can poll for async
        // notifications
        let client_copy = client.clone();
        tokio::spawn(async move {
            client_copy.batch_execute("LISTEN pipeline;").await.unwrap();
        });
        let mut stream = stream::poll_fn(move |cx| match &mut connection {
            ConnWrapper::Tls(c) => c.poll_message(cx),
            ConnWrapper::NoTls(c) => c.poll_message(cx),
        });

        let res = sync(&conn, &tx).await;
        if res.is_err() {
            error!("Synchronize attempt returned an error {:?}", res);
            tokio::time::sleep(RETRY_INTERVAL).await;
            continue;
        }
        loop {
            trace!("Waiting for notification");
            match stream.next().await {
                Some(Ok(msg)) => {
                    trace!("Reconciler received an AsyncMessage: {:?}", msg);
                    match msg {
                        AsyncMessage::Notification(n) => {
                            match parse_notification(n.channel(), n.payload()) {
                                Ok(n) => {
                                    debug!("Handling notification {:?}", n);
                                    let res = tx.send(n);
                                    if res.is_err() {
                                        error!("Received send error {:?}", res);
                                    }
                                }
                                Err(e) => {
                                    error!("Unexpected notification from the DB {:?}", e);
                                }
                            }
                        }
                        AsyncMessage::Notice(n) => {
                            // While these use the same struct as DbError,
                            // they are not errors. A warning seems sufficient.
                            // https://www.postgresql.org/docs/current/libpq-notice-processing.html
                            warn!("Received a notice: {n:?}");
                        }
                        // Needed because AsyncMessage is marked non-exhaustive
                        _ => error!("Receieved AsyncMessage that isn't a notice or notification"),
                    }
                }
                // Both Err and None are terminal messages. We should exit then.
                Some(Err(e)) => {
                    error!("Reconciler received an error: {:?}", e);
                    break;
                }
                None => {
                    error!("Reconciler received 'None'");
                    break;
                }
            }
        }
        // The connection will get dropped if the client is dropped. Keep
        // ownership of the client till we're done with the above loop.
        //
        // Also see: https://github.com/sfackler/rust-postgres/issues/591,
        drop(client);
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}

/// Synchronizes with the current state of the pipeline and program tables,
/// issuing an Add event per row.
async fn sync(
    db: &Arc<tokio::sync::Mutex<StoragePostgres>>,
    tx: &UnboundedSender<DbNotification>,
) -> Result<(), NotificationError> {
    match db.lock().await.list_pipeline_ids_across_all_tenants().await {
        Ok(list) => {
            for (tenant_id, pipeline_id) in list {
                // The first synchronization always appears as an Insert
                let res = tx.send(DbNotification::Pipeline(
                    Operation::Add,
                    tenant_id,
                    pipeline_id,
                ));
                if res.is_err() {
                    error!("Received send error {:?}", res);
                }
            }
            Ok(())
        }
        Err(e) => Err(NotificationError::SyncError(e)),
    }
}

/// Parse notifications generated by the database.
/// The channel corresponds to the relation name.
/// The payload is a String with the following shape:
/// "Operation TenantId PipelineId"
fn parse_notification(channel: &str, payload: &str) -> Result<DbNotification, NotificationError> {
    if channel != "pipeline" {
        return Err(NotificationError::InvalidChannel(channel.to_string()));
    }
    let mut split = payload.split(' ');
    let op_type = split.next();
    let tenant_id = split.next();
    let pipeline_id = split.next();
    if op_type.is_none() | tenant_id.is_none() || pipeline_id.is_none() {
        error!("Received invalid payload {:?}", payload);
        return Err(NotificationError::MissingPayloadComponents(
            payload.to_string(),
        ));
    }
    let operation = parse_operation(op_type.unwrap())?;
    let tenant_id = TenantId(Uuid::parse_str(tenant_id.unwrap())?);
    let pipeline_id = Uuid::parse_str(pipeline_id.unwrap())?;
    match channel {
        "pipeline" => Ok(DbNotification::Pipeline(
            operation,
            tenant_id,
            PipelineId(pipeline_id),
        )),
        _ => unreachable!("Invalid channel"),
    }
}

#[cfg(test)]
mod test {
    use super::listen;
    use crate::db::types::pipeline::{PipelineDescr, PipelineId};
    use crate::db::types::program::{CompilationProfile, ProgramConfig};
    use crate::{
        auth::TenantRecord,
        db::storage::Storage,
        db_notifier::{DbNotification, Operation},
    };
    use serde_json::json;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    pub async fn notifier_changes() {
        let (db, _temp) = crate::db::test::setup_pg().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let db = Arc::new(tokio::sync::Mutex::new(db));
        tokio::spawn(listen(db.clone(), tx));
        let tenant_id = TenantRecord::default().id;
        for i in 0..10 {
            // Create
            let pipeline_id = PipelineId(Uuid::now_v7());
            let _ = db
                .lock()
                .await
                .new_pipeline(
                    tenant_id,
                    pipeline_id.0,
                    "v0",
                    PipelineDescr {
                        name: format!("example{i}"),
                        description: "Description of example".to_string(),
                        runtime_config: json!({}),
                        program_code: "CREATE TABLE example ( col1 INT );".to_string(),
                        udf_rust: "".to_string(),
                        udf_toml: "".to_string(),
                        program_config: serde_json::to_value(ProgramConfig {
                            profile: Some(CompilationProfile::Unoptimized),
                            cache: true,
                        })
                        .unwrap(),
                    },
                )
                .await
                .unwrap();

            // Check creation notification was sent
            let notification = rx.recv().await.unwrap();
            assert_eq!(
                DbNotification::Pipeline(Operation::Add, tenant_id, pipeline_id),
                notification,
            );

            // Update
            let _ = db
                .lock()
                .await
                .update_pipeline(
                    tenant_id,
                    &format!("example{i}"),
                    &Some(format!("example{i}-renamed")),
                    &Some("Description of example2".to_string()),
                    "v0",
                    &None,
                    &Some("CREATE TABLE example ( col1 VARCHAR );".to_string()),
                    &None,
                    &None,
                    &None,
                )
                .await;

            // Check update notification was sent
            let notification = rx.recv().await.unwrap();
            assert_eq!(
                DbNotification::Pipeline(Operation::Update, tenant_id, pipeline_id),
                notification,
            );

            // Delete
            db.lock()
                .await
                .delete_pipeline(tenant_id, &format!("example{i}-renamed"))
                .await
                .unwrap();

            // Check delete notification was sent
            let notification = rx.recv().await.unwrap();
            assert_eq!(
                DbNotification::Pipeline(Operation::Delete, tenant_id, pipeline_id),
                notification,
            );
        }
    }

    #[tokio::test]
    pub async fn notifier_sync() {
        let (db, _temp) = crate::db::test::setup_pg().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let db = Arc::new(tokio::sync::Mutex::new(db));

        // Create a pipeline
        let tenant_id = TenantRecord::default().id;
        let pipeline_id = PipelineId(Uuid::now_v7());
        let _ = db
            .lock()
            .await
            .new_pipeline(
                tenant_id,
                pipeline_id.0,
                "v0",
                PipelineDescr {
                    name: "example1".to_string(),
                    description: "Description of example1".to_string(),
                    runtime_config: json!({}),
                    program_code: "CREATE TABLE example1 ( col1 INT );".to_string(),
                    udf_rust: "".to_string(),
                    udf_toml: "".to_string(),
                    program_config: serde_json::to_value(ProgramConfig {
                        profile: Some(CompilationProfile::Unoptimized),
                        cache: true,
                    })
                    .unwrap(),
                },
            )
            .await
            .unwrap();

        // Check that a notifier which is created later still issues the add notification
        tokio::spawn(listen(db.clone(), tx));
        let notification = rx.recv().await.unwrap();
        assert_eq!(
            DbNotification::Pipeline(Operation::Add, tenant_id, pipeline_id),
            notification,
        );
    }
}
