//! I/O adapter framework for DBSP.
//!
//! An infrastructure to ingest data into a DBSP circuit from external
//! data sources and to stream the outputs of the circuit to external
//! consumers.
//!
//! Defines the APIs to integrate different transport technologies
//! (files, URLs, Kafka streams, database connections, etc.) and data
//! formats (CSV, bincode, JSON, etc.) into the DBSP input and output
//! pipelines.
//!
//! This crate is primarily for use in general-purpose applications to
//! support users plugging in a variety of inputs and outputs in a flexible
//! way.  It is likely to be more than needed for simple applications that
//! support specific inputs and outputs.
//!
//! ## Overview
//!
//! The data ingestion pipeline consists of two kinds of adapters: **data
//! transport** adapters and **data format** adapters.
//!
//! ```text
//!                                              ┌──────────────┐
//!                                              │  controller  │
//!                                              │  ┌────────┐  │
//!                                              │  │ catalog│  │
//!                                              │  ├────────┤  │
//!                                              │  │ config │  │
//!                                              │  ├────────┤  │
//!                    control commands          │  │  stats │  │
//!               ┌──┬───────────────────────────┤  └────────┘  │
//!               │  │                           │              │
//!               │  │                           └──────────────┘
//!               │  │
//!               │  │
//!               │  │                             ┌───────┐
//!               ▼  │                             │       │       queue
//!             ┌────────┐     ┌──────┐       ┌────┴─┐   ┌─┴────┐  ┌─┬─┬─┐  ┌───────┐  ┌────────┐
//!          ──►│endpoint├────►│parser├──────►│handle│   │handle├─►│ │ │ ├─►│encoder├─►│endpoint├─►
//!             └────────┘     └──────┘       └────┬─┘   └─┬────┘  └─┴─┴─┘  └───────┘  └────────┘
//!                  │                             │circuit│
//!                  ▼                             │       │
//! transport-  ┌────────┐bytes┌──────┐records┌────┴─┐   ┌─┴────┐  ┌─┬─┬─┐  ┌───────┐  ┌────────┐
//! specific ──►│endpoint├────►│parser├──────►│handle│   │handle├─►│ │ │ ├─►│encoder├─►│endpoint├─►
//! protocol    └────────┘     └──────┘       └────┬─┘   └─┬────┘  └─┴─┴─┘  └───────┘  └────────┘
//!                 ▲             ▲                │       │                    ▲           ▲
//!                 │             │                └───────┘                    │           │
//!            ┌────┴────┐    ┌───┴────┐                                    ┌───┴────┐ ┌────┴────┐
//!            │  input  │    │ input  │                                    │ output │ │  output │
//!            │transport│    │ format │                                    │ format │ │transport│
//!            └─────────┘    └────────┘                                    └────────┘ └─────────┘
//! ```
//!
//! A data transport implements support for a specific streaming technology like
//! Kafka. It provides an API to create transport **endpoints**, that connect to
//! specified data sources, e.g., Kafka topics.  An endpoint reads raw binary
//! data from the source and provides basic flow control and error reporting
//! facilities, but is agnostic of the contents or format of the data.
//!
//! A data format adapter implements support for a data encapsulation format
//! like CSV, JSON, or bincode.  It provides an API to create **parsers**, which
//! transform raw binary data into a stream of **records** and push this data to
//! the DBSP circuit.
//!
//! Similar to input pipelines, an output pipeline consists of an (output)
//! transport endpoint and an encoder that serializes output batches into a
//! particular format.  Output batches produced by the circuit are placed in
//! lock-free queueus.  Each output pipeline runs in a separate thread that
//! dequeues the batches and pushes them to the encoder.
//!
//! The [`Controller`] component serves as a centralized control plane that
//! coordinates the creation, reconfiguration, teardown of the pipeline, and
//! implements runtime flow control.  It instantiates the pipeline according to
//! a user-provided configuration (see below) and exposes an API to reconfigure
//! and monitor the pipeline at runtime.
//!
//! ## Adapter API
//!
//! The transport adapter API consists of the following traits:
//!
//! * [`TransportInputEndpoint`] represents a configured data connection, e.g., a file,
//!   an S3 bucket or a Kafka topic.  By providing an [`InputConsumer`], a
//!   client may open an endpoint and thereby obtain an [`InputReader`].
//!
//! * [`InputReader`] allows a client to request reading an endpoint's data, and
//!   pause and resume reading.  A reader operates asynchronously in a
//!   background thread and passes updates to the [`InputConsumer`].
//!
//! * [`InputConsumer`] is provided by the client, not the API.  An
//!   [`InputReader`] with new data or status information provides it by calling
//!   the consumer's methods.
//!
//! * [`OutputEndpoint`] represents an individual outgoing data connection,
//!   e.g., a file, an S3 bucket or a Kafka topic.
//!
//! The format adapter API consists of:
//!
//! * [`InputFormat`] - a factory trait that creates [`Parser`] instances
//!
//! * [`Parser`] - a parser that consumes a raw binary stream and produces
//!   records into an internal [`InputBuffer`].
//!
//! * [`InputBuffer`] - stores records temporarily before pushing them into a
//!   circuit's input handle.
//!
//! * [`OutputFormat`] - a factory trait that creates [`Encoder`] instances
//!
//! * [`Encoder`] - an encoder that consumes batches of records and serializes
//!   them into binary buffers.
//!
//! ## Controller API
//!
//! A [`Controller`] is instantiated with
//!
//! * a [`PipelineConfig`] object, which specifies input and output pipeline
//!   configurations as well as global controller configuration settings, and
//!
//! * a [`Catalog`] object, which stores dictionaries of input and output
//!   streams of the circuit.
//!
//! # Fault tolerance
//!
//! This crate implements support for "fault tolerant" circuits, that is,
//! circuits whose operation can resume gracefully after a crash.  A crash will
//! not cause a fault tolerant circuit to drop input or process it more than
//! once, or to drop output or produce duplicate output, or to corrupt its
//! internal state.
//!
//! The form of fault tolerance implemented in this crate can recover from
//! crashes that kill processes or disrupt networking, but not crashes that lose
//! storage or corrupt computations.
//!
//! A fault-tolerant circuit requires all of its input and output endpoints to
//! be fault-tolerant:
//!
//! * A fault-tolerant input endpoint divides input into numbered [`Step`]s that
//!   can be retrieved repeatedly with the same content, despite crashes.
//!   [`InputEndpoint::is_fault_tolerant`] reports whether an input endpoint is
//!   fault tolerant.
//!
//! * A fault-tolerant output endpoint divides its output into numbered
//!   [`Step`]s such that, if a step with a given number is output more than
//!   once, the output endpoint discards the duplicate.
//!
//! Fault tolerance works only with deterministic circuits, that is, ones that,
//! given a sequence of inputs, will always produce the same sequence of
//! outputs.  Most circuits used to analyze data are deterministic.
//!
//! [`Step`]: crate::transport::Step

use bytemuck::NoUninit;
use num_derive::FromPrimitive;
use rustls::crypto::CryptoProvider;
use serde::Serialize;

pub(crate) mod adhoc;
mod catalog;
mod controller;
pub mod format;
pub mod integrated;
pub mod server;
pub mod static_compile;
pub mod transport;
pub(crate) mod util;

#[cfg(test)]
pub mod test;

pub use integrated::{create_integrated_output_endpoint, IntegratedOutputEndpoint};

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, FromPrimitive, Serialize, NoUninit)]
#[repr(u8)]
pub enum PipelineState {
    /// All input endpoints are paused (or are in the process of being paused).
    #[default]
    Paused,

    /// Controller is running.
    Running,

    /// Controller is being terminated.
    Terminated,
}

// Re-export `DetailedError`.
pub use dbsp::DetailedError;

pub use server::{ErrorResponse, PipelineError};

pub use catalog::{
    Catalog, CircuitCatalog, DeCollectionHandle, DeCollectionStream, RecordFormat, SerBatch,
    SerCollectionHandle, SerCursor,
};
pub use format::{
    Encoder, InputBuffer, InputFormat, OutputConsumer, OutputFormat, ParseError, Parser,
};

pub use controller::{
    ConfigError, ConnectorConfig, Controller, ControllerError, ControllerStatus, FormatConfig,
    InputEndpointConfig, OutputEndpointConfig, PipelineConfig, RuntimeConfig, TransportConfig,
};
pub use transport::{
    AsyncErrorCallback, InputConsumer, InputEndpoint, InputReader, OutputEndpoint,
    TransportInputEndpoint,
};

/// Some dependencies of this crate use the `rustls` library. This library has two features
/// `ring` and `aws-lc-rs`. When both are enabled, the library requires a process-wide default
/// crypto provider to be configured. While no single dependency enables both these features,
/// Rust's feature unification may end up enabling both of them, depending on the exact rustls
/// dependency versions in use.
///
/// Bottom line: this function must be called in the `main` function in this crate, as well as
/// in all tests that exercise libraries that use `rustls` internally.
fn ensure_default_crypto_provider() {
    let _ = CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider());
}
