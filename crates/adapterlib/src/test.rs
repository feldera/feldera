//! Test fixtures that the connector suites of every engine share.
//!
//! Built for this crate's tests and, through the `testing` feature, for the tests of the crates
//! that run the shared suites against an engine.

mod canonical;
mod engine;
mod mock_input_consumer;
mod mock_output_callbacks;
mod mock_output_consumer;
mod updates;
mod wait;
mod zset;

pub use engine::{
    Capability, ConnectorTestEngine, InputOptions, TestCollection, wait_for_rows,
    wait_for_rows_within,
};
pub use mock_input_consumer::{
    ErrorCallback, MockInputConsumer, MockInputConsumerState, MockInputParser, MockInputParserState,
};
pub use mock_output_callbacks::MockOutputCallbacks;
pub use mock_output_consumer::MockOutputConsumer;
pub use updates::{Updates, WEIGHT_COLUMN};
pub use wait::{async_wait, default_timeout_ms, wait};
pub use zset::ZSet;
