use bytemuck::NoUninit;
use num_derive::FromPrimitive;

pub mod catalog;
pub mod errors;
pub mod format;
pub mod transport;

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

pub use dbsp::DetailedError as DbspDetailedError;
use serde::Serialize;
