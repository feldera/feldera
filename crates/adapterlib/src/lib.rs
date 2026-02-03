use bytemuck::NoUninit;
pub use dbsp::DetailedError as DbspDetailedError;
use num_derive::FromPrimitive;
use serde::Serialize;

pub mod catalog;
mod connector_metadata;
pub mod errors;
pub mod format;
pub mod transport;
pub mod utils;

pub use connector_metadata::ConnectorMetadata;

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
