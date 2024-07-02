use crate::ControllerError;
use dbsp::{profile::GraphProfile, DBSPHandle};
use std::path::PathBuf;

/// Trait for DBSP circuit handle objects.
pub trait DbspCircuitHandle {
    fn step(&mut self) -> Result<(), ControllerError>;

    fn enable_cpu_profiler(&mut self) -> Result<(), ControllerError>;

    fn dump_profile(&mut self, dir_path: &str) -> Result<PathBuf, ControllerError>;

    fn graph_profile(&mut self) -> Result<GraphProfile, ControllerError>;

    fn kill(self: Box<Self>) -> std::thread::Result<()>;
}

impl DbspCircuitHandle for DBSPHandle {
    fn step(&mut self) -> Result<(), ControllerError> {
        DBSPHandle::step(self).map_err(ControllerError::dbsp_error)
    }

    fn enable_cpu_profiler(&mut self) -> Result<(), ControllerError> {
        DBSPHandle::enable_cpu_profiler(self).map_err(ControllerError::dbsp_error)
    }

    fn dump_profile(&mut self, dir_path: &str) -> Result<PathBuf, ControllerError> {
        DBSPHandle::dump_profile(self, dir_path).map_err(ControllerError::dbsp_error)
    }

    fn graph_profile(&mut self) -> Result<GraphProfile, ControllerError> {
        DBSPHandle::graph_profile(self).map_err(ControllerError::dbsp_error)
    }

    fn kill(self: Box<Self>) -> std::thread::Result<()> {
        DBSPHandle::kill(*self)
    }
}
