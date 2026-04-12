mod input;
mod output;
#[cfg(test)]
mod test;

#[cfg(test)]
pub(crate) use input::Metadata as S2Metadata;
pub use input::S2InputEndpoint;
pub use output::S2OutputEndpoint;
