/// Machinery to subscribe to notifications from database tables.
/// Intended to be used by various reconciliation loops.
use std::sync::Arc;

use log::{debug, error, trace, warn};

use tokio::sync::mpsc::UnboundedSender;

use futures_util::{stream, StreamExt};
use tokio_postgres::AsyncMessage;
use uuid::Uuid;

use crate::{
    auth::TenantId,
    db::{storage::Storage, DBError, PipelineId, ProgramId, ProjectDB},
};

const RETRY_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_secs(2);

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
pub async fn listen(conn: Arc<tokio::sync::Mutex<ProjectDB>>, tx: UnboundedSender<DbNotification>) {
    loop {
        let (client, mut connection) = conn
            .lock()
            .await
            .config
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        let client = Arc::new(client);
        // Not clear why the calls to LISTEN block forever when we call it from the same
        // thread. But we need to call LISTEN before we can poll for async
        // notifications
        let client_copy = client.clone();
        tokio::spawn(async move {
            client_copy
                .batch_execute("LISTEN program; LISTEN pipeline; LISTEN pipeline_runtime_state;")
                .await
                .unwrap();
        });
        let mut stream = stream::poll_fn(move |cx| connection.poll_message(cx));
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
    db: &Arc<tokio::sync::Mutex<ProjectDB>>,
    tx: &UnboundedSender<DbNotification>,
) -> Result<(), NotificationError> {
    match db.lock().await.all_programs().await {
        Ok(list) => {
            for (tenant_id, program_descr) in list {
                // The first synchronization always appears as an Insert
                let res = tx.send(DbNotification::Program(
                    Operation::Add,
                    tenant_id,
                    program_descr.program_id,
                ));
                if res.is_err() {
                    error!("Received send error {:?}", res);
                }
            }
        }
        Err(e) => return Err(NotificationError::SyncError(e)),
    };
    match db.lock().await.all_pipelines().await {
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
/// The payload is a String with the following shape: "Operation TenantId
/// ProgramId"
fn parse_notification(channel: &str, payload: &str) -> Result<DbNotification, NotificationError> {
    if channel != "program" && channel != "pipeline" && channel != "pipeline_runtime_state" {
        return Err(NotificationError::InvalidChannel(channel.to_string()));
    }
    let mut split = payload.split(' ');
    let op_type = split.next();
    let tenant_id = split.next();
    let program_id = split.next();
    if op_type.is_none() | tenant_id.is_none() || program_id.is_none() {
        error!("Received invalid payload {:?}", payload);
        return Err(NotificationError::MissingPayloadComponents(
            payload.to_string(),
        ));
    }
    let operation = parse_operation(op_type.unwrap())?;
    let tenant_id = TenantId(Uuid::parse_str(tenant_id.unwrap())?);
    let program_or_pipeline_id = Uuid::parse_str(program_id.unwrap())?;
    match channel {
        "program" => Ok(DbNotification::Program(
            operation,
            tenant_id,
            ProgramId(program_or_pipeline_id),
        )),
        // TODO: it could be beneficial to differentiate between both channels.
        // Given that we do not return the full object, there is limited value in
        // doing so right now.
        "pipeline" | "pipeline_runtime_state" => Ok(DbNotification::Pipeline(
            operation,
            tenant_id,
            PipelineId(program_or_pipeline_id),
        )),
        _ => unreachable!("Invalid channel"),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum DbNotification {
    Program(Operation, TenantId, ProgramId),
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use pipeline_types::config::RuntimeConfig;
    use uuid::Uuid;

    use crate::{
        auth::TenantRecord,
        compiler::ProgramConfig,
        config::CompilationProfile,
        db::{storage::Storage, PipelineId, ProgramId},
        db_notifier::{DbNotification, Operation},
    };

    use super::listen;

    #[tokio::test]
    pub async fn notifier_changes() {
        let (conn, _temp) = crate::db::test::setup_pg().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let conn = Arc::new(tokio::sync::Mutex::new(conn));
        tokio::spawn(listen(conn.clone(), tx));
        let tenant_id = TenantRecord::default().id;
        for i in 0..10 {
            // Test inserts
            let program_id = Uuid::now_v7();
            let _ = conn
                .lock()
                .await
                .new_program(
                    tenant_id,
                    program_id,
                    &format!("test{i}").to_string(),
                    "program desc",
                    "ignored",
                    &ProgramConfig {
                        profile: Some(CompilationProfile::Unoptimized),
                    },
                    None,
                )
                .await
                .unwrap();
            let rc = RuntimeConfig::from_yaml("");
            let pipeline_id = Uuid::now_v7();
            let _ = conn
                .lock()
                .await
                .new_pipeline(
                    tenant_id,
                    pipeline_id,
                    &None,
                    &format!("{i}"),
                    "2",
                    &rc,
                    &Some(vec![]),
                    None,
                )
                .await
                .unwrap();

            let program_id = ProgramId(program_id);
            let pipeline_id = PipelineId(pipeline_id);
            let n = rx.recv().await.unwrap();
            assert_eq!(
                DbNotification::Program(Operation::Add, tenant_id, program_id),
                n,
            );
            let n = rx.recv().await.unwrap();
            assert_eq!(
                DbNotification::Pipeline(Operation::Add, tenant_id, pipeline_id),
                n,
            );

            // Updates
            let updated_program_name = format!("updated_test{i}");
            let _ = conn
                .lock()
                .await
                .update_program(
                    tenant_id,
                    program_id,
                    &Some(updated_program_name.clone()),
                    &Some("some new description".to_string()),
                    &None,
                    &None,
                    &None,
                    &None,
                    None,
                    None,
                )
                .await;
            let pipeline_name = &format!("{i}");
            let _ = conn
                .lock()
                .await
                .update_pipeline(
                    tenant_id,
                    pipeline_id,
                    &None,
                    pipeline_name,
                    "some new description",
                    &None,
                    &None,
                    None,
                )
                .await;
            let n = rx.recv().await.unwrap();
            assert_eq!(
                DbNotification::Program(Operation::Update, tenant_id, program_id),
                n,
            );
            let n = rx.recv().await.unwrap();
            assert_eq!(
                DbNotification::Pipeline(Operation::Update, tenant_id, pipeline_id),
                n,
            );

            // Deletes
            conn.lock()
                .await
                .delete_program(tenant_id, &updated_program_name)
                .await
                .unwrap();
            conn.lock()
                .await
                .delete_pipeline(tenant_id, pipeline_name)
                .await
                .unwrap();

            let n = rx.recv().await.unwrap();
            assert_eq!(
                DbNotification::Program(Operation::Delete, tenant_id, program_id),
                n,
            );
            let n = rx.recv().await.unwrap();
            assert_eq!(
                DbNotification::Pipeline(Operation::Delete, tenant_id, pipeline_id),
                n,
            );
        }
    }

    #[tokio::test]
    pub async fn notifier_sync() {
        let (conn, _temp) = crate::db::test::setup_pg().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let conn = Arc::new(tokio::sync::Mutex::new(conn));

        // Create some programs and pipelines before listening for changes
        let tenant_id = TenantRecord::default().id;
        let program_id = Uuid::now_v7();
        let _ = conn
            .lock()
            .await
            .new_program(
                tenant_id,
                program_id,
                "test0",
                "program desc",
                "ignored",
                &ProgramConfig {
                    profile: Some(CompilationProfile::Unoptimized),
                },
                None,
            )
            .await
            .unwrap();
        let rc = RuntimeConfig::from_yaml("");
        let pipeline_id = Uuid::now_v7();
        let _ = conn
            .lock()
            .await
            .new_pipeline(
                tenant_id,
                pipeline_id,
                &None,
                "test1",
                "2",
                &rc,
                &Some(vec![]),
                None,
            )
            .await
            .unwrap();
        let program_id = ProgramId(program_id);
        let pipeline_id = PipelineId(pipeline_id);
        tokio::spawn(listen(conn.clone(), tx));
        let n = rx.recv().await.unwrap();
        assert_eq!(
            DbNotification::Program(Operation::Add, tenant_id, program_id),
            n,
        );
        let n = rx.recv().await.unwrap();
        assert_eq!(
            DbNotification::Pipeline(Operation::Add, tenant_id, pipeline_id),
            n,
        );
    }
}
