mod error;
mod input;
pub(crate) mod output;
mod output_macros;
mod prepared_statements;
mod tls;

#[cfg(feature = "with-postgres-cdc")]
pub(crate) mod cdc_input;

#[cfg(test)]
mod test;

#[cfg(all(any(test, feature = "testing"), feature = "with-postgres-cdc"))]
pub mod cdc_privilege_tests;
#[cfg(all(any(test, feature = "testing"), feature = "with-postgres-cdc"))]
pub mod cdc_scenario_tests;
#[cfg(all(any(test, feature = "testing"), feature = "with-postgres-cdc"))]
pub mod cdc_tests;
#[cfg(any(test, feature = "testing"))]
pub mod tests;

pub use input::PostgresInputEndpoint;
pub use output::PostgresOutputEndpoint;

#[cfg(feature = "with-postgres-cdc")]
pub use cdc_input::PostgresCdcInputEndpoint;
