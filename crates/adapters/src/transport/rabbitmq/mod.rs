mod connection;
mod input;
mod output;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod e2e;

pub use input::RabbitmqInputEndpoint;
pub use output::RabbitmqOutputEndpoint;
