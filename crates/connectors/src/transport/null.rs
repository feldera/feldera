use super::OutputEndpoint;
use anyhow::{Error as AnyError, Result as AnyResult};

/// Output transport endpoint that discards all data (aka /dev/null).
///
/// Useful as a sink when only the postprocessor side effects matter,
/// or in tests where no persistent output is needed.
pub(crate) struct NullOutputEndpoint;

impl OutputEndpoint for NullOutputEndpoint {
    fn connect(
        &mut self,
        _async_error_callback: Box<dyn Fn(bool, AnyError, Option<&'static str>) + Send + Sync>,
    ) -> AnyResult<()> {
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn push_buffer(&mut self, _buffer: &[u8]) -> AnyResult<()> {
        Ok(())
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        // Discarding data trivially satisfies fault-tolerance requirements:
        // there is nothing to deduplicate and nothing to make visible.
        true
    }
}
