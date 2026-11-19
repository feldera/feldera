//! A library error as the pipeline may show it.

use crate::credential_scrub::redact_connection_credentials;
use crate::secret_redaction::secret_redactor;
use std::error::Error;
use std::fmt;

/// An error from a driver or client library, shown without credentials.
///
/// A library can echo the connection string or the option value it was
/// given, and a log line or a message can interpolate the error anywhere.
/// `Display` and `Debug` print the text of the error and of its sources
/// with connection credentials removed and registered secret values
/// replaced, so whatever is interpolated is already safe. The raw error
/// stays inside for classification by kind or code; this type never prints
/// it and has no `source()`, so a chain walker cannot reach it either.
///
/// Wrap a library error where it enters our code, so that the raw type does
/// not cross a function signature:
///
/// ```ignore
/// let client = connect(uri).await.map_err(LibraryError::wrap)?;
/// ```
pub struct LibraryError {
    inner: Inner,
}

/// The raw error. An `anyhow::Error` is kept as is: boxed into a
/// `dyn Error`, it would hide the original error from a downcast.
enum Inner {
    Std(Box<dyn Error + Send + Sync + 'static>),
    Anyhow(anyhow::Error),
}

impl LibraryError {
    /// Wraps `error`, keeping it for classification.
    pub fn wrap<E: Error + Send + Sync + 'static>(error: E) -> Self {
        Self {
            inner: Inner::Std(Box::new(error)),
        }
    }

    /// The raw error, for classification by kind or code only. Its text can
    /// hold credentials: never format it.
    pub fn raw(&self) -> &(dyn Error + Send + Sync + 'static) {
        match &self.inner {
            Inner::Std(error) => &**error,
            Inner::Anyhow(error) => error.as_ref(),
        }
    }

    /// The raw error as `E`, for classification only; see [`Self::raw`].
    pub fn downcast_ref<E: Error + Send + Sync + 'static>(&self) -> Option<&E> {
        match &self.inner {
            Inner::Std(error) => error.downcast_ref::<E>(),
            Inner::Anyhow(error) => error.downcast_ref::<E>(),
        }
    }

    /// The sanitized text: the error and its sources, joined by `: `.
    pub fn text(&self) -> String {
        let raw = self.raw();
        let mut text = raw.to_string();
        let mut source = raw.source();
        while let Some(cause) = source {
            text.push_str(": ");
            text.push_str(&cause.to_string());
            source = cause.source();
        }
        let text = redact_connection_credentials(&text);
        secret_redactor().redact(&text).into_owned()
    }
}

impl fmt::Display for LibraryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text())
    }
}

impl fmt::Debug for LibraryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text())
    }
}

/// No `source()`: the chain ends in the sanitized text.
impl Error for LibraryError {}

impl From<anyhow::Error> for LibraryError {
    fn from(error: anyhow::Error) -> Self {
        Self {
            inner: Inner::Anyhow(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LibraryError;
    use crate::secret_redaction::secret_redactor;
    use std::fmt;

    /// A library error with a source, shaped like a driver's.
    #[derive(Debug)]
    struct Driver {
        message: String,
        cause: Option<Box<Driver>>,
    }

    impl fmt::Display for Driver {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.message)
        }
    }

    impl std::error::Error for Driver {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.cause.as_deref().map(|cause| cause as _)
        }
    }

    fn driver(message: &str, cause: Option<&str>) -> Driver {
        Driver {
            message: message.to_string(),
            cause: cause.map(|cause| Box::new(driver(cause, None))),
        }
    }

    /// The connection string a library echoes loses its password, in
    /// `Display` and in `Debug` alike.
    #[test]
    fn the_text_has_no_credentials() {
        let error = LibraryError::wrap(driver(
            "cannot open postgres://u:library-pw-2136@db.example/app",
            None,
        ));
        assert_eq!(
            error.to_string(),
            "cannot open postgres://u:[REDACTED]@db.example/app"
        );
        assert_eq!(format!("{error:?}"), error.to_string());
    }

    /// A registered secret value in the text shows as its reference.
    #[test]
    fn a_registered_value_shows_as_its_reference() {
        let reference = "${secret:kubernetes:library/token}";
        secret_redactor().register(reference, "library-token-value-2136");
        let error = LibraryError::wrap(driver(
            "unauthorized: token library-token-value-2136 rejected",
            None,
        ));
        assert_eq!(
            error.to_string(),
            format!("unauthorized: token {reference} rejected")
        );
    }

    /// The sources of the error stay in the text, sanitized, and a chain
    /// walker finds no raw source behind the wrapper.
    #[test]
    fn sources_are_joined_and_hidden_from_chain_walkers() {
        let error = LibraryError::wrap(driver(
            "query failed",
            Some("connect to host=h password=library-kw-2136 dbname=db refused"),
        ));
        assert_eq!(
            error.to_string(),
            "query failed: connect to host=h password=[REDACTED] dbname=db refused"
        );
        assert!(std::error::Error::source(&error).is_none());
        let chained = anyhow::Error::new(error).context("reading orders");
        let walked = format!("{chained:#}");
        assert!(!walked.contains("library-kw-2136"), "{walked}");
        assert!(
            walked.starts_with("reading orders: query failed: "),
            "{walked}"
        );
    }

    /// The raw error is there for classification.
    #[test]
    fn the_raw_error_is_available_for_classification() {
        let error = LibraryError::wrap(driver("kind 42", None));
        assert_eq!(error.downcast_ref::<Driver>().unwrap().message, "kind 42");
        assert_eq!(error.raw().to_string(), "kind 42");
    }

    /// An `anyhow` chain wraps too, with its contexts kept.
    #[test]
    fn an_anyhow_error_wraps_with_its_contexts() {
        let error: LibraryError = anyhow::Error::new(driver(
            "Invalid value \"hunter2x\" for configuration property \"sasl.password\"",
            None,
        ))
        .context("creating the Kafka client")
        .into();
        assert_eq!(
            error.to_string(),
            "creating the Kafka client: Invalid value \"[REDACTED]\" for configuration property \"sasl.password\""
        );
    }

    /// The original error behind an `anyhow` chain is there for
    /// classification too.
    #[test]
    fn an_anyhow_error_keeps_its_original_for_classification() {
        let error: LibraryError = anyhow::Error::new(driver("kind 7", None))
            .context("connecting")
            .into();
        assert_eq!(error.downcast_ref::<Driver>().unwrap().message, "kind 7");
        assert_eq!(error.raw().to_string(), "connecting");
    }
}
