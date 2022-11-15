//! I/O adapter framework for DBSP.
//!
//! An infrastructure to ingest data into a DBSP circuit from external
//! data sources and to stream the outputs of the circuit to external
//! consumers.
//!
//! Defines the APIs to integrate different transport technologies
//! (files, Kafka streams, database connections, etc.) and data
//! formats (CSV, bincode, JSON, etc.) into the DBSP input and output
//! pipelines.
//!
//! ## Overview
//!
//! The data ingestion pipeline consists of two kinds of adapters: **data
//! transport** adapters and **data format** adapters.
//!
//! ```text
//!                                                       ┌──────────────┐
//!                                                       │  controller  │
//!                                                       │  ┌────────┐  │
//!                                                       │  │ catalog│  │
//!                                                       │  ├────────┤  │
//!                                                       │  │ config │  │
//!                                                       │  ├────────┤  │
//!                         control commands              │  │  stats │  │
//!                  ┌──┬─────────────────────────────────┤  └────────┘  │
//!                  │  │                                 │              │
//!                  │  │                                 └──────────────┘
//!                  │  │        
//!                  │  │       
//!                  │  │                                   ┌───────────┐
//!                  ▼  │                                   │           │         queue
//!                ┌────┴───┐          ┌──────┐        ┌────┴─┐       ┌─┴────┐  ┌─┬─┬─┬─┐   ┌───────┐   ┌────────┐
//!          ─────►│endpoint├─────────►│parser├───────►│handle│       │handle├──┤ │ │ │ ├──►│encoder├──►│endpoint├──►
//!                └────────┘          └──────┘        └────┬─┘       └─┬────┘  └─┴─┴─┴─┘   └───────┘   └────────┘
//!                     ▼                                   │  circuit  │
//! transport-     ┌────────┐bytes     ┌──────┐records ┌────┴─┐       ┌─┴────┐  ┌─┬─┬─┬─┐   ┌───────┐   ┌────────┐
//! specific ─────►│endpoint├─────────►│parser├───────►│handle│       │handle├──┤ │ │ │ ├──►│encoder├──►│endpoint├──►
//! protocol       └────────┘          └──────┘        └────┬─┘       └─┬────┘  └─┴─┴─┴─┘   └───────┘   └────────┘
//!                    ▲                  ▲                 │           │                       ▲            ▲
//!                    │                  │                 └───────────┘                       │            │
//!               ┌────┴────┐         ┌───┴────┐                                            ┌───┴────┐  ┌────┴────┐
//!               │  input  │         │ input  │                                            │ output │  │  output │
//!               │transport│         │ format │                                            │ format │  │transport│
//!               └─────────┘         └────────┘                                            └────────┘  └─────────┘
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
//! The transport adapter API consists of two traits:
//!
//! * [`InputTransport`] is a factory trait that creates [`InputEndpoint`]
//!   instances.
//!
//! * [`InputEndpoint`] represents an individual data connection, e.g., a file,
//!   an S3 bucket or a Kafka topic.
//!
//! * [`OutputTransport`] is a factory trait that creates [`OutputEndpoint`]
//!   instances.
//!
//! * [`OutputEndpoint`] represents an individual outgoing data connection,
//!   e.g., a file, an S3 bucket or a Kafka topic.
//!
//! Similarly, the format adapter API consists of:
//!
//! * [`InputFormat`] - a factory trait that creates [`Parser`] instances
//!
//! * [`Parser`] - a parser that consumes a raw binary stream and outputs a
//!   stream of records.
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
//! * a [`ControllerConfig`] object, which specifies input and output pipeline
//!   configurations as well as global controller configuration settings, and
//!
//! * a [`Catalog`] object, which stores dictionaries of input and output
//!   streams of the circuit.

use num_derive::FromPrimitive;

mod catalog;
mod controller;
mod deinput;
mod format;
mod seroutput;
mod transport;

#[cfg(test)]
mod test;

#[derive(Copy, Clone, PartialEq, Eq, FromPrimitive)]
pub enum PipelineState {
    /// All input endpoints are paused (or are in the process of being paused).
    Paused = 0,

    /// Controller is running.
    Running = 1,

    /// Controller is being terminated.
    Terminated = 2,
}

pub use catalog::Catalog;
pub use deinput::{
    DeCollectionHandle, DeMapHandle, DeScalarHandle, DeScalarHandleImpl, DeSetHandle, DeZSetHandle,
};
pub use format::{Encoder, InputFormat, OutputConsumer, OutputFormat, Parser};
pub use seroutput::{SerBatch, SerCursor, SerOutputBatchHandle};

pub use controller::{Controller, ControllerConfig};
pub use transport::{
    FileInputTransport, InputConsumer, InputEndpoint, InputTransport, OutputEndpoint,
    OutputTransport,
};
