mod input;
mod output;

pub(self) static MAX_SOCKETS_PER_ENDPOINT: usize = 5;

pub use input::HttpInputTransport;
pub use output::HttpOutputTransport;
