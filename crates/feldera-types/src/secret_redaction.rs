//! Redaction of resolved secret values in diagnostics.
//!
//! The secret resolver registers each `${secret:...}` value it resolves.
//! Error responses, connector error lists, the panic hook and the pipeline
//! log pass through the registry, so a resolved value that a message copies
//! by mistake leaves the process as its reference, such as
//! `${secret:kubernetes:pg/uri}`. This is a second layer only: a diagnostic
//! is built from reviewed fields at its source and must not depend on it.
//!
//! The registry is process-wide and never logged or serialized. A value
//! registered twice keeps its first reference. Unit tests share it, so a
//! test registers a value that no other message can contain.

use aho_corasick::{AhoCorasick, MatchKind};
use serde_json::Value;
use std::borrow::Cow;
use std::sync::{PoisonError, RwLock};
use url::Url;

/// Shortest value the registry redacts, measured once the value is trimmed.
/// A shorter value matches too much unrelated text, such as a port number or
/// a common word, and is not registered. A derived form shorter than this is
/// skipped for the same reason.
pub const MIN_REDACTED_LEN: usize = 8;

#[derive(Default)]
struct Entries {
    values: Vec<String>,
    replacements: Vec<String>,
    /// Finds every value in one pass over a line, longest match first.
    automaton: Option<AhoCorasick>,
}

/// Registry of resolved secret values and the text that replaces each one.
#[derive(Default)]
pub struct SecretRedactor {
    entries: RwLock<Entries>,
}

static GLOBAL: SecretRedactor = SecretRedactor::new();

/// The registry of this process.
pub fn secret_redactor() -> &'static SecretRedactor {
    &GLOBAL
}

impl SecretRedactor {
    pub const fn new() -> Self {
        Self {
            entries: RwLock::new(Entries {
                values: Vec::new(),
                replacements: Vec::new(),
                automaton: None,
            }),
        }
    }

    /// Registers `value`, resolved from `reference`.
    ///
    /// The value and its trimmed form are replaced by the reference. The
    /// forms of the value that a connector or a formatter can derive are
    /// replaced by `[REDACTED <reference>]`: the percent-encoded,
    /// percent-decoded and debug-escaped forms, and the password of a value
    /// that is a URL.
    pub fn register(&self, reference: &str, value: &str) {
        if value.trim().len() < MIN_REDACTED_LEN {
            return;
        }
        let derived = format!("[REDACTED {reference}]");
        let mut forms = vec![
            (value.to_string(), reference.to_string()),
            (value.trim().to_string(), reference.to_string()),
            (urlencoding::encode(value).into_owned(), derived.clone()),
            (debug_escaped(value), derived.clone()),
        ];
        if let Ok(decoded) = urlencoding::decode(value) {
            forms.push((decoded.into_owned(), derived.clone()));
        }
        if let Some(password) = Url::parse(value)
            .ok()
            .and_then(|url| url.password().map(str::to_owned))
        {
            if let Ok(decoded) = urlencoding::decode(&password) {
                forms.push((debug_escaped(&decoded), derived.clone()));
                forms.push((decoded.into_owned(), derived.clone()));
            }
            forms.push((debug_escaped(&password), derived.clone()));
            forms.push((password, derived));
        }

        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        for (value, replacement) in forms {
            if value.len() < MIN_REDACTED_LEN || entries.values.contains(&value) {
                continue;
            }
            entries.values.push(value);
            entries.replacements.push(replacement);
        }
        // Leftmost-longest: a URL is replaced before the password inside it.
        entries.automaton = Some(
            AhoCorasick::builder()
                .match_kind(MatchKind::LeftmostLongest)
                .build(&entries.values)
                .expect("plain string patterns build"),
        );
    }

    /// Replaces every registered value in `text`.
    pub fn redact<'a>(&self, text: &'a str) -> Cow<'a, str> {
        let entries = self.entries.read().unwrap_or_else(PoisonError::into_inner);
        match &entries.automaton {
            Some(automaton) if automaton.is_match(text) => {
                Cow::Owned(automaton.replace_all(text, &entries.replacements))
            }
            _ => Cow::Borrowed(text),
        }
    }

    /// Replaces every registered value in `bytes`, read as UTF-8.
    pub fn redact_bytes<'a>(&self, bytes: &'a [u8]) -> Cow<'a, [u8]> {
        let text = String::from_utf8_lossy(bytes);
        match self.redact(&text) {
            Cow::Borrowed(_) => Cow::Borrowed(bytes),
            Cow::Owned(redacted) => Cow::Owned(redacted.into_bytes()),
        }
    }

    /// Replaces every registered value in the strings of `value`.
    pub fn redact_json(&self, value: &mut Value) {
        match value {
            Value::String(text) => {
                if let Cow::Owned(redacted) = self.redact(text) {
                    *text = redacted;
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|item| self.redact_json(item)),
            Value::Object(fields) => {
                for (key, mut value) in std::mem::take(fields) {
                    self.redact_json(&mut value);
                    fields.insert(self.redact(&key).into_owned(), value);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
}

/// `value` as `{:?}` prints it, without the quotes.
fn debug_escaped(value: &str) -> String {
    let escaped = format!("{value:?}");
    escaped[1..escaped.len() - 1].to_string()
}

#[cfg(test)]
mod tests {
    use super::{MIN_REDACTED_LEN, SecretRedactor};
    use serde_json::json;

    const REFERENCE: &str = "${secret:kubernetes:pg/uri}";

    #[test]
    fn value_becomes_its_reference() {
        let redactor = SecretRedactor::new();
        redactor.register(REFERENCE, "hunter2-hunter2");
        assert_eq!(
            redactor.redact("password hunter2-hunter2 rejected"),
            format!("password {REFERENCE} rejected")
        );
        assert_eq!(
            redactor.redact_bytes(b"ok hunter2-hunter2\n").as_ref(),
            format!("ok {REFERENCE}\n").as_bytes()
        );
    }

    #[test]
    fn url_password_and_encoded_forms_are_redacted() {
        let redactor = SecretRedactor::new();
        let uri = "postgres://feldera:p%40ss-word-2136@db.internal:5432/app";
        redactor.register(REFERENCE, uri);
        assert_eq!(redactor.redact(uri), REFERENCE);
        assert_eq!(
            redactor.redact("password \"p@ss-word-2136\" rejected"),
            format!("password \"[REDACTED {REFERENCE}]\" rejected")
        );
        assert_eq!(
            redactor.redact("password \"p%40ss-word-2136\" rejected"),
            format!("password \"[REDACTED {REFERENCE}]\" rejected")
        );
        assert_eq!(
            redactor.redact(&urlencoding::encode(uri)),
            format!("[REDACTED {REFERENCE}]")
        );
    }

    #[test]
    fn trimmed_and_escaped_forms_are_redacted() {
        let redactor = SecretRedactor::new();
        redactor.register(REFERENCE, "quote\"and\\slash\n");
        assert_eq!(
            redactor.redact("x quote\"and\\slash y"),
            format!("x {REFERENCE} y")
        );
        assert_eq!(
            redactor.redact("x quote\\\"and\\\\slash\\n y"),
            format!("x [REDACTED {REFERENCE}] y")
        );
    }

    #[test]
    fn short_values_are_not_registered() {
        let redactor = SecretRedactor::new();
        let short = "a".repeat(MIN_REDACTED_LEN - 1);
        redactor.register(REFERENCE, &short);
        assert_eq!(redactor.redact(&short), short);
        // The floor counts the trimmed value.
        redactor.register(REFERENCE, &format!("{short}\n"));
        assert_eq!(redactor.redact(&short), short);
    }

    #[test]
    fn first_registration_wins_and_several_values_share_a_line() {
        let redactor = SecretRedactor::new();
        redactor.register(REFERENCE, "shared-secret-value");
        redactor.register("${secret:kubernetes:other/key}", "shared-secret-value");
        redactor.register("${secret:kubernetes:kafka/pw}", "kafka-secret-value");
        assert_eq!(
            redactor.redact("a shared-secret-value b kafka-secret-value c"),
            format!("a {REFERENCE} b ${{secret:kubernetes:kafka/pw}} c")
        );
    }

    #[test]
    fn json_strings_are_redacted() {
        let redactor = SecretRedactor::new();
        redactor.register(REFERENCE, "json-secret-value");
        let mut details = json!({
            "endpoint": "pg_in",
            "error": "connect json-secret-value failed",
            "chain": [{"cause": "json-secret-value"}, 1, null, true],
        });
        redactor.redact_json(&mut details);
        assert_eq!(
            details,
            json!({
                "endpoint": "pg_in",
                "error": format!("connect {REFERENCE} failed"),
                "chain": [{"cause": REFERENCE}, 1, null, true],
            })
        );
    }

    #[test]
    fn escaped_url_passwords_and_json_keys_are_redacted() {
        let redactor = SecretRedactor::new();
        redactor.register(REFERENCE, "postgres://u:pass%22word2136@db/app");
        assert_eq!(
            redactor.redact(r#"password pass\"word2136"#),
            format!("password [REDACTED {REFERENCE}]")
        );
        redactor.register(REFERENCE, "json-key-secret-2136");
        let mut details = json!({"json-key-secret-2136": "details"});
        redactor.redact_json(&mut details);
        assert_eq!(details, json!({REFERENCE: "details"}));
    }
}
