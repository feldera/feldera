//! Credential scrubbing for the diagnostics the manager stores and exports.
//!
//! The pipeline process knows each resolved secret value and replaces it by
//! its reference before the value reaches an error, a log line, or `/stats`.
//! The manager does not resolve secrets, so it cannot use that registry. Old
//! runtimes can still send it connection strings in errors. It removes URL
//! userinfo passwords and omits diagnostics containing query or keyword
//! passwords when their boundaries cannot be established safely.
//!
//! The manager applies this to the diagnostics it writes (a pipeline on a
//! runtime from before the fix still reports such errors) and to the stored
//! support collections it exports (collections from before the fix are
//! exported as they were stored).
//!
//! Keyword and query passwords, and ambiguous userinfo, cause the whole
//! diagnostic string to be replaced. Free text does not have reliable
//! connection-string boundaries; guessing a password's end can disclose it.

use serde_json::Value;
use std::borrow::Cow;

/// Replacement for the password component of a URL.
pub const REDACTED: &str = "[REDACTED]";

/// Ends the authority part of a URL: path, query, fragment, or a character
/// that cannot be part of an authority in free text or JSON.
fn ends_authority(c: char) -> bool {
    matches!(c, '/' | '?' | '#' | '\\' | '<' | '>') || c.is_whitespace()
}

/// Replaces URL userinfo passwords. Query/keyword passwords and ambiguous
/// userinfo cause the whole string to be replaced with [`REDACTED`].
pub fn redact_connection_credentials(text: &str) -> Cow<'_, str> {
    // Postgres also accepts query parameters and keyword connection strings.
    // Decode parameter names too: `?%70assword=...` is still a password.
    // Omit the whole string because quoted/escaped keyword values can contain
    // arbitrary delimiters. Codes and endpoint identities live in other fields.
    let decoded = urlencoding::decode_binary(text.as_bytes());
    let lower = String::from_utf8_lossy(&decoded).to_ascii_lowercase();
    if lower
        .split("password")
        .skip(1)
        .any(|tail| tail.trim_start().starts_with('='))
    {
        return Cow::Owned(REDACTED.to_string());
    }
    let mut out = String::new();
    let mut changed = false;
    let mut rest = text;
    while let Some(scheme_end) = rest.find("://") {
        let authority_start = scheme_end + 3;
        let authority_end = rest[authority_start..]
            .find(ends_authority)
            .map_or(rest.len(), |i| authority_start + i);
        let authority = &rest[authority_start..authority_end];
        // The last '@' separates userinfo from host, so a password that
        // contains '@' is still covered.
        let password = authority.rfind('@').and_then(|at| {
            let colon = authority[..at].find(':')?;
            Some((authority_start + colon + 1, authority_start + at))
        });
        if password.is_none()
            && !authority.contains('@')
            && rest[authority_end..]
                .split("://")
                .next()
                .is_some_and(|tail| tail.contains('@'))
        {
            // A delimiter may have ended a password before its '@'. The
            // remaining text is unproven, so leave no part of it visible.
            return Cow::Owned(REDACTED.to_string());
        }
        match password {
            Some((start, end)) => {
                out.push_str(&rest[..start]);
                out.push_str(REDACTED);
                changed = true;
                rest = &rest[end..];
            }
            None => {
                out.push_str(&rest[..authority_end]);
                rest = &rest[authority_end..];
            }
        }
    }
    if !changed {
        Cow::Borrowed(text)
    } else {
        out.push_str(rest);
        Cow::Owned(out)
    }
}

/// Applies [`redact_connection_credentials`] to every string in `value`, keys included.
pub fn redact_connection_credentials_in_json(value: &mut Value) {
    match value {
        Value::String(s) => {
            if let Cow::Owned(redacted) = redact_connection_credentials(s) {
                *s = redacted;
            }
        }
        Value::Array(items) => items
            .iter_mut()
            .for_each(redact_connection_credentials_in_json),
        Value::Object(fields) => {
            let mut renamed = Vec::new();
            for (key, field) in fields.iter_mut() {
                redact_connection_credentials_in_json(field);
                if let Cow::Owned(redacted) = redact_connection_credentials(key) {
                    renamed.push((key.clone(), redacted));
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn supported_postgres_password_forms_are_redacted() {
        for uri in [
            "postgres://u:quote'secret2136@host/db",
            "postgres://u:quote\"secret2136@host/db",
            "postgres://u@host/db?password=query-secret-2136",
            "postgres://u@host/db?%70assword=encoded-secret-2136",
            "host=host user=u password=keyword-secret-2136 dbname=db",
            "host=host user=u password = 'quoted secret 2136' dbname=db",
        ] {
            let config = uri.parse::<tokio_postgres::Config>().unwrap();
            let password = std::str::from_utf8(config.get_password().unwrap()).unwrap();
            let message = format!(
                "Unable to connect to postgres instance '{uri}': error connecting to server"
            );
            let redacted = redact_connection_credentials(&message);
            assert!(!redacted.contains(password), "{redacted}");
            let mut json = json!({"message": message, "details": {uri: message}});
            redact_connection_credentials_in_json(&mut json);
            assert!(!json.to_string().contains("secret"), "{json}");
        }
        // Even a rejected URI was echoed by the old connection-error path.
        // Invalid UTF-8 in its value must not hide an encoded parameter name.
        let uri = "postgres://u@host/db?%70assword=byte%FFsecret2136";
        assert!(uri.parse::<tokio_postgres::Config>().is_err());
        assert_eq!(redact_connection_credentials(uri), REDACTED);
    }

    #[test]
    fn url_password_becomes_the_marker() {
        assert_eq!(
            redact_connection_credentials(
                "error connecting to postgres://feldera:s3cret@db.internal:5432/app"
            ),
            "error connecting to postgres://feldera:[REDACTED]@db.internal:5432/app"
        );
    }

    #[test]
    fn ambiguous_userinfo_is_omitted() {
        for text in [
            "postgres://u:secret with space@host/db",
            "postgres://u:secret\\value@host/db",
            "postgres:// user:secret@host/db",
        ] {
            assert_eq!(redact_connection_credentials(text), REDACTED);
        }
    }

    #[test]
    fn every_url_in_the_text_is_covered() {
        let text = "a=redis://:pw1@h1 b=amqp://u:pw2@h2/v c=https://u:pw3@h3?x=1";
        assert_eq!(
            redact_connection_credentials(text),
            "a=redis://:[REDACTED]@h1 b=amqp://u:[REDACTED]@h2/v c=https://u:[REDACTED]@h3?x=1"
        );
    }

    #[test]
    fn a_password_with_an_at_sign_is_covered_whole() {
        assert_eq!(
            redact_connection_credentials("postgres://u:p@ss@host/db"),
            "postgres://u:[REDACTED]@host/db"
        );
    }

    #[test]
    fn a_url_inside_json_escaped_text_is_covered() {
        assert_eq!(
            redact_connection_credentials(r#"{"error":"postgres://u:pw@host\n"}"#),
            r#"{"error":"postgres://u:[REDACTED]@host\n"}"#
        );
    }

    #[test]
    fn text_without_a_password_is_unchanged() {
        for text in [
            "",
            "connection refused",
            "postgres://db.internal:5432/app",
            "postgres://feldera@db.internal/app",
            "user:password@ without a scheme",
            "see https://docs.feldera.com/connectors",
        ] {
            assert!(matches!(redact_connection_credentials(text), Cow::Borrowed(t) if t == text));
        }
    }

    #[test]
    fn json_strings_and_keys_are_covered() {
        let mut value = json!({
            "message": "postgres://u:pw@h/db",
            "details": {"postgres://k:pw@h": ["postgres://u:pw@h/db", 1, null]}
        });
        redact_connection_credentials_in_json(&mut value);
        assert_eq!(
            value,
            json!({
                "message": "postgres://u:[REDACTED]@h/db",
                "details": {"postgres://k:[REDACTED]@h": ["postgres://u:[REDACTED]@h/db", 1, null]}
            })
        );
    }
}
