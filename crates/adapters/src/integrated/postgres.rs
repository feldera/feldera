mod error;
mod input;
pub(crate) mod output;
mod output_macros;
mod prepared_statements;
mod tls;

#[cfg(feature = "with-postgres-cdc")]
pub(crate) mod cdc_input;

#[cfg(any(test, feature = "testing"))]
pub mod test;

pub use input::PostgresInputEndpoint;
pub use output::PostgresOutputEndpoint;

#[cfg(feature = "with-postgres-cdc")]
pub use cdc_input::PostgresCdcInputEndpoint;
