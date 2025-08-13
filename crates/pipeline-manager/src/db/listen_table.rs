use crate::db::error::DBError;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::tenant::TenantId;
use futures_util::{stream, StreamExt};
use log::{error, warn};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::error::TrySendError;
use tokio_postgres::AsyncMessage;
use uuid::Uuid;

/// Interval at which to retry listening.
const LISTEN_RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// Upper bound on the number of pipeline listen notifications that can be buffered in the channel.
pub const PIPELINE_NOTIFY_CHANNEL_CAPACITY: usize = 1000000;

/// Notification about an operation (add, update, delete) that occurred to a pipeline table row.
#[derive(Debug, PartialEq, Eq)]
pub struct PipelineNotification {
    pub operation: Operation,
    tenant_id: TenantId,
    pub pipeline_id: PipelineId,
}

/// Pipeline table row operation.
#[derive(Debug, PartialEq, Eq)]
pub enum Operation {
    Add,
    Update,
    Delete,
}

/// The entire listening mechanism failed, causing it to have to be restarted.
#[derive(ThisError, Debug)]
pub enum ListenError {
    #[error("cannot make TLS connector: {error}")]
    CannotMakeTlsConnector { error: DBError },
    #[error("unable to connect to Postgres: {error}")]
    CannotConnect { error: tokio_postgres::Error },
    #[error("notification receiver closed")]
    NotificationReceiverClosed,
    #[error("notification stream encountered an error: {error}")]
    StreamError { error: tokio_postgres::Error },
    #[error("notification stream returned None, indicating it finished")]
    StreamNone,
    #[error("join encountered an error: {error}")]
    JoinError { error: String },
}

// Unable to parse a notification received from the database listening.
#[derive(ThisError, Debug, PartialEq)]
enum NotificationError {
    #[error("invalid channel: {channel}")]
    InvalidChannel { channel: String },
    #[error("missing payload components: {payload}")]
    MissingPayloadComponents { payload: String },
    #[error("invalid operation in payload: {payload_operation}")]
    InvalidOperationError { payload_operation: String },
    #[error("invalid uuid as component payload: {payload_component}")]
    InvalidUuid { payload_component: String },
}

/// Listens for changes (add, update, delete) to the pipeline table, and send notifications using
/// the provided channel provider.
///
/// Limitations:
/// - It does not perform a full synchronization initially
/// - There is no guarantee it will capture every change to the pipeline table
/// - It does best effort to reconnect when it loses connection
///
/// In practice, it generally does work, and can be used to more quickly detect pipeline table
/// changes. It should be used as a supplement to preempt regular polling at an interval.
pub async fn listen_table(
    db: Arc<tokio::sync::Mutex<StoragePostgres>>,
    notification_sender: tokio::sync::mpsc::Sender<PipelineNotification>,
) {
    loop {
        match attempt_listen_table(db.clone(), notification_sender.clone()).await {
            Ok(()) => {
                error!(
                    "Listening for pipeline table changes was interrupted -- trying again in {}s. No error was returned.",
                    LISTEN_RETRY_INTERVAL.as_secs()
                );
            }
            Err(e) => {
                error!(
                    "Listening for pipeline table changes was interrupted -- trying again in {}s. Error: {e}",
                    LISTEN_RETRY_INTERVAL.as_secs()
                );
            }
        }
        tokio::time::sleep(LISTEN_RETRY_INTERVAL).await;
    }
}

/// Attempts to listen for changes (add, update, delete) to the pipeline table, and send
/// notifications using the provided channel provider. Returns an error if it failed.
async fn attempt_listen_table(
    db: Arc<tokio::sync::Mutex<StoragePostgres>>,
    notification_sender: tokio::sync::mpsc::Sender<PipelineNotification>,
) -> Result<(), ListenError> {
    // Create a new connection to the database using the existing configuration
    let db_config = db.lock().await.db_config.clone();
    let connector = db_config
        .tls_connector()
        .map_err(|error| ListenError::CannotMakeTlsConnector { error })?;
    let (client, mut connection) = db
        .lock()
        .await
        .config
        .connect(connector)
        .await
        .map_err(|error| ListenError::CannotConnect { error })?;

    // Spawn a separate task for the connection, which is needed for the client to query
    let join_handle = tokio::spawn(async move {
        let mut stream = stream::poll_fn(move |cx| connection.poll_message(cx));
        loop {
            match stream.next().await {
                Some(Ok(message)) => {
                    match message {
                        AsyncMessage::Notification(n) => {
                            match parse_notification(n.channel(), n.payload()) {
                                Ok(n) => {
                                    if let Err(e) = notification_sender.try_send(n) {
                                        match e {
                                            TrySendError::Full(_n) => {
                                                error!("Notifier is unable to send notification out on channel because it has reached capacity");
                                            }
                                            TrySendError::Closed(_n) => {
                                                break ListenError::NotificationReceiverClosed;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Notifier is unable to parse notification: {e:?}");
                                }
                            }
                        }
                        AsyncMessage::Notice(notice) => {
                            // Not a true error, as such a warning is sufficient.
                            // See also: https://www.postgresql.org/docs/current/libpq-notice-processing.html
                            warn!("Notifier received a notice: {notice:?}");
                        }
                        // AsyncMessage is marked non-exhaustive
                        _ => {
                            error!("Notifier received AsyncMessage that isn't a notification or notice")
                        }
                    }
                }
                // Both `Some(Err(...))` and `None` are terminal messages
                Some(Err(error)) => {
                    break ListenError::StreamError { error };
                }
                None => {
                    break ListenError::StreamNone;
                }
            }
        }
    });

    // Start listening to the pipeline table trigger events. The connection will get dropped if the
    // client is dropped, as such we keep ownership till the connection stream task has exited.
    // See also: https://github.com/sfackler/rust-postgres/issues/591
    if let Err(e) = client.batch_execute("LISTEN pipeline;").await {
        error!(
            "Notifier unable to execute LISTEN -- waiting for connection to terminate. Error: {e}"
        );
        drop(client); // Cause the connection stream to exit
        match join_handle.await {
            // The task will only exit if it encountered an error
            Ok(e) => Err(e),
            // Join failed (due to a panic)
            Err(e) => Err(ListenError::JoinError {
                error: e.to_string(),
            }),
        }
    } else {
        let result = match join_handle.await {
            Ok(e) => Err(e),
            Err(e) => Err(ListenError::JoinError {
                error: e.to_string(),
            }),
        };
        drop(client);
        result
    }
}

/// Parses the notification from the database.
/// - The `channel` corresponds to the relation name (must be `pipeline`)
/// - The `payload` is a String with the following shape: `Operation TenantId PipelineId`
///
/// Returns an error if the above is not the case, thus being unable to parse it.
fn parse_notification(
    channel: &str,
    payload: &str,
) -> Result<PipelineNotification, NotificationError> {
    // Only the pipeline table is supported
    if channel != "pipeline" {
        return Err(NotificationError::InvalidChannel {
            channel: channel.to_string(),
        });
    }

    // Split into three components
    let [op_type, tenant_id, pipeline_id]: [&str; 3] = payload
        .splitn(3, ' ')
        .collect::<Vec<&str>>()
        .try_into()
        .map_err(|_| NotificationError::MissingPayloadComponents {
            payload: payload.to_string(),
        })?;

    // Parse each component
    let operation = match op_type {
        "A" => Ok(Operation::Add),
        "U" => Ok(Operation::Update),
        "D" => Ok(Operation::Delete),
        payload_operation => Err(NotificationError::InvalidOperationError {
            payload_operation: payload_operation.to_string(),
        }),
    }?;
    let tenant_id =
        TenantId(
            Uuid::parse_str(tenant_id).map_err(|_e| NotificationError::InvalidUuid {
                payload_component: tenant_id.to_string(),
            })?,
        );
    let pipeline_id =
        PipelineId(
            Uuid::parse_str(pipeline_id).map_err(|_e| NotificationError::InvalidUuid {
                payload_component: pipeline_id.to_string(),
            })?,
        );

    Ok(PipelineNotification {
        operation,
        tenant_id,
        pipeline_id,
    })
}

#[cfg(test)]
mod test {
    use super::{
        listen_table, NotificationError, PipelineNotification, PIPELINE_NOTIFY_CHANNEL_CAPACITY,
    };
    use super::{parse_notification, Operation};
    use crate::db::types::pipeline::{PipelineDescr, PipelineId};
    use crate::db::types::program::ProgramConfig;
    use crate::db::types::tenant::TenantId;
    use crate::{auth::TenantRecord, db::storage::Storage};
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;
    use uuid::Uuid;

    #[tokio::test]
    pub async fn notification_parsing_success() {
        assert_eq!(
            parse_notification(
                "pipeline",
                "A 00000000-0000-0000-0000-000000000001 00000000-0000-0000-0000-000000000002"
            ),
            Ok(PipelineNotification {
                operation: Operation::Add,
                tenant_id: TenantId(Uuid::from_u128(0x1)),
                pipeline_id: PipelineId(Uuid::from_u128(0x2)),
            })
        );
        assert_eq!(
            parse_notification(
                "pipeline",
                "U 00000000-0000-0000-0000-000000000001 00000000-0000-0000-0000-000000000002"
            ),
            Ok(PipelineNotification {
                operation: Operation::Update,
                tenant_id: TenantId(Uuid::from_u128(0x1)),
                pipeline_id: PipelineId(Uuid::from_u128(0x2)),
            })
        );
        assert_eq!(
            parse_notification(
                "pipeline",
                "D 00000000-0000-0000-0000-000000000001 00000000-0000-0000-0000-000000000002"
            ),
            Ok(PipelineNotification {
                operation: Operation::Delete,
                tenant_id: TenantId(Uuid::from_u128(0x1)),
                pipeline_id: PipelineId(Uuid::from_u128(0x2)),
            })
        );
    }

    #[tokio::test]
    pub async fn notification_parsing_error() {
        // Invalid channel
        assert_eq!(
            parse_notification(
                "example",
                "A 00000000-0000-0000-0000-000000000001 00000000-0000-0000-0000-000000000002"
            ),
            Err(NotificationError::InvalidChannel {
                channel: "example".to_string()
            })
        );

        // Invalid split
        assert_eq!(
            parse_notification("pipeline", "A"),
            Err(NotificationError::MissingPayloadComponents {
                payload: "A".to_string()
            })
        );
        assert_eq!(
            parse_notification("pipeline", "A 00000000-0000-0000-0000-000000000001"),
            Err(NotificationError::MissingPayloadComponents {
                payload: "A 00000000-0000-0000-0000-000000000001".to_string()
            })
        );

        // Invalid operation
        assert_eq!(
            parse_notification(
                "pipeline",
                "C 00000000-0000-0000-0000-000000000001 00000000-0000-0000-0000-000000000002"
            ),
            Err(NotificationError::InvalidOperationError {
                payload_operation: "C".to_string()
            })
        );

        // Invalid tenant identifier
        assert_eq!(
            parse_notification(
                "pipeline",
                "A 00000000-0000-0000-0000-00000000000x 00000000-0000-0000-0000-000000000002"
            ),
            Err(NotificationError::InvalidUuid {
                payload_component: "00000000-0000-0000-0000-00000000000x".to_string()
            })
        );

        // Invalid pipeline identifier
        assert_eq!(
            parse_notification(
                "pipeline",
                "A 00000000-0000-0000-0000-000000000001 00000000-0000-0000-0000-00000000000y"
            ),
            Err(NotificationError::InvalidUuid {
                payload_component: "00000000-0000-0000-0000-00000000000y".to_string()
            })
        );
    }

    #[tokio::test]
    pub async fn listening_works() {
        let (db, _temp) = crate::db::test::setup_pg().await;
        let (notification_sender, mut notification_receiver) =
            tokio::sync::mpsc::channel(PIPELINE_NOTIFY_CHANNEL_CAPACITY);
        let db = Arc::new(tokio::sync::Mutex::new(db));
        tokio::spawn(listen_table(db.clone(), notification_sender));
        let tenant_id = TenantRecord::default().id;

        // Wait until the LISTEN is issued. A generous timeout is used here, such that if within
        // this time it is not yet listening, something else is very likely wrong.
        sleep(Duration::from_secs(5)).await;

        // Create (add)
        let pipeline_id = PipelineId(Uuid::now_v7());
        let _ = db
            .lock()
            .await
            .new_pipeline(
                tenant_id,
                pipeline_id.0,
                "v0",
                PipelineDescr {
                    name: "example".to_string(),
                    description: "Description of example".to_string(),
                    runtime_config: json!({}),
                    program_code: "CREATE TABLE example ( col1 INT );".to_string(),
                    udf_rust: "".to_string(),
                    udf_toml: "".to_string(),
                    program_config: serde_json::to_value(ProgramConfig::default()).unwrap(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), notification_receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            PipelineNotification {
                operation: Operation::Add,
                tenant_id,
                pipeline_id,
            }
        );

        // Update
        let _ = db
            .lock()
            .await
            .update_pipeline(
                tenant_id,
                "example",
                &Some("example-renamed".to_string()),
                &Some("Description of example2".to_string()),
                "v0",
                &None,
                &Some("CREATE TABLE example ( col1 VARCHAR );".to_string()),
                &None,
                &None,
                &None,
            )
            .await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), notification_receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            PipelineNotification {
                operation: Operation::Update,
                tenant_id,
                pipeline_id,
            }
        );

        // Delete
        db.lock()
            .await
            .delete_pipeline(tenant_id, "example-renamed")
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), notification_receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            PipelineNotification {
                operation: Operation::Delete,
                tenant_id,
                pipeline_id,
            }
        );
    }
}
