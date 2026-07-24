mod input;
mod output;
#[cfg(test)]
mod test;

pub use input::S2InputEndpoint;
#[cfg(test)]
pub(crate) use input::{S2CheckpointMetadata as S2Metadata, make_replay_read_input};
pub use output::S2OutputEndpoint;
