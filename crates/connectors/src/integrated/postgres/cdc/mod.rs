//! Postgres CDC input connector, built on etl.

mod destination;
mod key;
mod member;
mod monitor;
mod pipeline;
mod reconcile;
#[cfg(any(test, feature = "testing"))]
pub mod tests;

use crate::transport::{
    InputEndpoint, InputQueue, InputReaderCommand, IntegratedInputEndpoint, NonFtInputReaderCommand,
};
use crate::{ControllerError, InputConsumer, InputReader, PipelineState, RecordFormat};
use anyhow::{Result as AnyResult, anyhow};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use etl::config::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig,
};
use etl::data::{ArrayCell, Cell, Date, OldTableRow, PgTime, TableRow, Timestamp, UpdatedTableRow};
use etl::destination::{
    Destination, DestinationWriteStatus, DropTableForCopyResult, TableCopyBatchId,
    WriteEventsDurability, WriteEventsResult, WriteTableRowsResult,
};
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::event::Event;
use etl::pipeline::Pipeline;
use etl::schema::{ReplicatedTableSchema, TableId};
use etl::store::{
    CachedStore, PostgresStore, SchemaStore, StateStore, TableRetryPolicy, TableState,
    TableStateType,
};
use feldera_adapterlib::catalog::{DeCollectionStream, InputCollectionHandle};
use feldera_adapterlib::format::ParseError;
use feldera_adapterlib::transport::{Resume, Step, Watermark};
use feldera_types::config::FtModel;
use feldera_types::coordination::Completion;
use feldera_types::format::json::JsonFlavor;
use feldera_types::transport::postgres::{
    PostgresCdcBatchConfig, PostgresCdcMemoryBackpressureConfig, PostgresCdcReaderConfig,
    PostgresTlsConfig,
};
use serde_json::{Value, json};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch::{Receiver, Sender, channel};
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};
use url::Url;
use xxhash_rust::xxh3::xxh3_64;

use super::tls::{make_etl_tls_config, make_tls_connector};
use destination::*;
use key::*;
use member::*;
use monitor::*;
use pipeline::*;
use reconcile::*;

#[cfg(any(test, feature = "testing"))]
pub(crate) use key::pipeline_id;
pub use pipeline::PostgresCdcInputEndpoint;
#[cfg(any(test, feature = "testing"))]
pub(crate) use pipeline::{
    ETL_STOP_GRACE, RUNTIME_SHUTDOWN_TIMEOUT, STOPPED_PAST_GRACE, WATCHER_SHUTDOWN_TIMEOUT,
};
