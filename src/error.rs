use crate::{RuntimeError, SchedulerError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    Scheduler(SchedulerError),
    Runtime(RuntimeError),
    Custom(String),
}
