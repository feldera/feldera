mod input;
#[cfg(test)]
mod test;

#[cfg(test)]
pub(crate) use input::Metadata as S2Metadata;
pub use input::S2InputEndpoint;
