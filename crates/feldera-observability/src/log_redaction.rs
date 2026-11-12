//! Redaction of log lines before they are written.
//!
//! A [`LineRedactor`] rewrites the output of the text log and of the JSON
//! log. The text log passes each formatted line through a
//! [`RedactingMakeWriter`]. The JSON log passes each event through
//! [`LineRedactor::redact_json`] before it serializes the event.

use serde_json::Value;
use std::borrow::Cow;
use std::io;
use tracing_subscriber::fmt::MakeWriter;

/// Rewrites a formatted log line before it is written.
pub trait LineRedactor: Send + Sync + 'static {
    fn redact<'a>(&self, line: &'a [u8]) -> Cow<'a, [u8]>;

    /// Redact strings before a structured formatter escapes them.
    fn redact_json(&self, value: &mut Value) {
        match value {
            Value::String(text) => {
                if let Cow::Owned(bytes) = self.redact(text.as_bytes()) {
                    *text = String::from_utf8_lossy(&bytes).into_owned();
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|item| self.redact_json(item)),
            Value::Object(fields) => {
                let mut renamed = Vec::new();
                for (key, field) in fields.iter_mut() {
                    self.redact_json(field);
                    if let Cow::Owned(bytes) = self.redact(key.as_bytes()) {
                        renamed.push((key.clone(), String::from_utf8_lossy(&bytes).into_owned()));
                    }
                }
                for (key, redacted) in renamed {
                    if let Some(field) = fields.remove(&key) {
                        fields.insert(redacted, field);
                    }
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
}

/// Makes writers that pass every line through a [`LineRedactor`].
pub struct RedactingMakeWriter<R, W> {
    redactor: R,
    inner: W,
}

impl<R, W> RedactingMakeWriter<R, W> {
    pub fn new(redactor: R, inner: W) -> Self {
        Self { redactor, inner }
    }
}

impl<'a, R: LineRedactor, W: MakeWriter<'a>> MakeWriter<'a> for RedactingMakeWriter<R, W> {
    type Writer = RedactingWriter<'a, R, W::Writer>;

    fn make_writer(&'a self) -> Self::Writer {
        RedactingWriter {
            redactor: &self.redactor,
            inner: self.inner.make_writer(),
        }
    }
}

pub struct RedactingWriter<'a, R, W> {
    redactor: &'a R,
    inner: W,
}

impl<R: LineRedactor, W: io::Write> io::Write for RedactingWriter<'_, R, W> {
    // The fmt layer hands over one formatted event per call, so a line is redacted whole.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write_all(&self.redactor.redact(buf))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::{LineRedactor, RedactingMakeWriter};
    use std::borrow::Cow;
    use std::io;
    use std::sync::{Arc, Mutex};
    use tracing::error;
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::layer::SubscriberExt;

    struct Replace {
        value: &'static str,
        replacement: &'static str,
    }

    impl LineRedactor for Replace {
        fn redact<'a>(&self, line: &'a [u8]) -> Cow<'a, [u8]> {
            let text = String::from_utf8_lossy(line);
            if text.contains(self.value) {
                Cow::Owned(text.replace(self.value, self.replacement).into_bytes())
            } else {
                Cow::Borrowed(line)
            }
        }
    }

    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for SharedBuffer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl MakeWriter<'_> for SharedBuffer {
        type Writer = SharedBuffer;

        fn make_writer(&self) -> Self::Writer {
            self.clone()
        }
    }

    /// A value in a log event reaches the output as its replacement.
    #[test]
    fn logged_value_is_replaced() {
        let buffer = SharedBuffer::default();
        let redactor = Replace {
            value: "logged-secret-value",
            replacement: "${secret:kubernetes:pg/uri}",
        };
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(RedactingMakeWriter::new(redactor, buffer.clone())),
        );

        tracing::subscriber::with_default(subscriber, || {
            error!("connection to logged-secret-value refused");
        });

        let log = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert!(!log.contains("logged-secret-value"), "{log}");
        assert!(
            log.contains("connection to ${secret:kubernetes:pg/uri} refused"),
            "{log}"
        );
    }
}
