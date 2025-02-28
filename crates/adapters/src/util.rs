use std::borrow::Cow;
use std::path::Path;
use std::{
    error::Error,
    fs::File,
    io::{Error as IoError, Write},
};

use tempfile::NamedTempFile;

pub(crate) fn root_cause(mut err: &dyn Error) -> &dyn Error {
    while let Some(source) = err.source() {
        err = source;
    }
    err
}

pub(crate) fn truncate_ellipse<'a>(s: &'a str, len: usize, ellipse: &str) -> Cow<'a, str> {
    if s.len() <= len {
        return Cow::Borrowed(s);
    } else if len == 0 {
        return Cow::Borrowed("");
    }

    let result = s.chars().take(len).chain(ellipse.chars()).collect();
    Cow::Owned(result)
}

pub(crate) fn write_file_atomically<P>(path: P, data: &[u8]) -> Result<(), IoError>
where
    P: AsRef<Path>,
{
    // Find the file's directory and open it.
    let dir = path.as_ref().parent().unwrap_or(Path::new("."));
    let dir_handle = File::open(dir)?;

    // Write and rename and sync the file.
    let mut temp = NamedTempFile::new_in(dir)?;
    temp.write_all(data)?;
    temp.as_file().sync_data()?;
    temp.persist(path)?;

    // Then sync the directory to make sure that the rename is committed.
    dir_handle.sync_data()?;

    Ok(())
}

/// For logging with a non-constant level.  From
/// <https://github.com/tokio-rs/tracing/issues/2730>
#[macro_export]
macro_rules! dyn_event {
    ($lvl:expr, $($arg:tt)+) => {
        match $lvl {
            ::tracing::Level::TRACE => ::tracing::trace!($($arg)+),
            ::tracing::Level::DEBUG => ::tracing::debug!($($arg)+),
            ::tracing::Level::INFO => ::tracing::info!($($arg)+),
            ::tracing::Level::WARN => ::tracing::warn!($($arg)+),
            ::tracing::Level::ERROR => ::tracing::error!($($arg)+),
        }
    };
}
