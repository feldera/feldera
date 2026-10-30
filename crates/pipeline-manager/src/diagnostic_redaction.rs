//! Credential scrubbing for the diagnostics the manager stores and exports.
//!
//! The pipeline process knows each resolved secret value and replaces it by
//! its reference before the value reaches an error, a log line, or `/stats`.
//! The manager does not resolve secrets, so it cannot use that registry. Old
//! runtimes can still send it connection strings in errors, and stored
//! support collections from before the fix hold them too. The manager
//! replaces the password of a URL's userinfo and the value of a `password=`
//! parameter, one line at a time, so the rest of a diagnostic or a log
//! stays readable.
//!
//! The manager applies this to the diagnostics it writes (a pipeline on a
//! runtime from before the fix still reports such errors), to the
//! diagnostics it reads back, and to the stored support collections it
//! exports.

use serde_json::Value;
use std::borrow::Cow;

/// Replacement for the password component of a URL or of a parameter.
pub const REDACTED: &str = "[REDACTED]";

/// Replaces connection passwords in `text`, line by line: the password of a
/// URL's userinfo, and the value of a `password=` parameter in a query
/// string or a keyword connection string.
pub fn redact_connection_credentials(text: &str) -> Cow<'_, str> {
    let mut out = String::new();
    let mut changed = false;
    for line in text.split_inclusive('\n') {
        match redact_line(line) {
            Cow::Borrowed(line) => out.push_str(line),
            Cow::Owned(redacted) => {
                changed = true;
                out.push_str(&redacted);
            }
        }
    }
    if changed {
        Cow::Owned(out)
    } else {
        Cow::Borrowed(text)
    }
}

fn redact_line(line: &str) -> Cow<'_, str> {
    match redact_url_passwords(line) {
        Cow::Borrowed(_) => redact_password_parameters(line),
        Cow::Owned(redacted) => Cow::Owned(redact_password_parameters(&redacted).into_owned()),
    }
}

/// Replaces the userinfo password of every URL in `line`. A URL runs to the
/// next whitespace; its last `@` ends the userinfo, so a password that holds
/// `@`, `/`, `?` or a quote is covered whole. A `@` later on the line, such
/// as an email address, is not part of the URL.
fn redact_url_passwords(line: &str) -> Cow<'_, str> {
    let mut out = String::new();
    let mut changed = false;
    let mut rest = line;
    while let Some(scheme_end) = rest.find("://") {
        let url_start = scheme_end + 3;
        let url_end = rest[url_start..]
            .find(char::is_whitespace)
            .map_or(rest.len(), |i| url_start + i);
        let url = &rest[url_start..url_end];
        let password = url
            .rfind('@')
            .and_then(|at| url[..at].find(':').map(|colon| (colon + 1, at)));
        match password {
            Some((start, end)) if start < end => {
                out.push_str(&rest[..url_start + start]);
                out.push_str(REDACTED);
                changed = true;
                rest = &rest[url_start + end..];
            }
            _ => {
                out.push_str(&rest[..url_end]);
                rest = &rest[url_end..];
            }
        }
    }
    if changed {
        out.push_str(rest);
        Cow::Owned(out)
    } else {
        Cow::Borrowed(line)
    }
}

/// Replaces the value of every `password=` parameter in `line`, in any
/// letter case and with spaces around the `=`. A quoted value runs to its
/// closing quote, any other to the next whitespace, `&`, `;` or quote.
fn redact_password_parameters(line: &str) -> Cow<'_, str> {
    const KEY: &str = "password";
    let lower = line.to_ascii_lowercase();
    let mut out = String::new();
    let mut changed = false;
    let mut from = 0;
    while let Some(found) = lower[from..].find(KEY) {
        let key_end = from + found + KEY.len();
        let after_key = line[key_end..].trim_start();
        let Some(after_equals) = after_key.strip_prefix('=') else {
            out.push_str(&line[from..key_end]);
            from = key_end;
            continue;
        };
        let value_start = line.len() - after_equals.trim_start().len();
        let value = &line[value_start..];
        let value_len = match value.chars().next() {
            Some(quote @ ('\'' | '"')) => value[1..]
                .find(quote)
                .map_or(value.len(), |close| close + 2),
            Some(_) => value
                .find(|c: char| c.is_whitespace() || matches!(c, '&' | ';' | '\'' | '"'))
                .unwrap_or(value.len()),
            None => 0,
        };
        out.push_str(&line[from..value_start]);
        if value_len > 0 {
            out.push_str(REDACTED);
            changed = true;
        }
        from = value_start + value_len;
    }
    if changed {
        out.push_str(&line[from..]);
        Cow::Owned(out)
    } else {
        Cow::Borrowed(line)
    }
}

/// Applies [`redact_connection_credentials`] to every string in `value`,
/// keys included. Two keys that redact to the same text are kept apart by a
/// counter, so no value is dropped.
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
                let Some(field) = fields.remove(&key) else {
                    continue;
                };
                let mut name = redacted.clone();
                let mut n = 1;
                while fields.contains_key(&name) {
                    n += 1;
                    name = format!("{redacted} ({n})");
                }
                fields.insert(name, field);
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
            "postgres://u@host/db?password=query-secret-2136&sslmode=require",
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
            // The rest of the diagnostic stays.
            assert!(
                redacted.contains("error connecting to server"),
                "{redacted}"
            );
            assert!(redacted.contains("host"), "{redacted}");
            let mut json = json!({"message": message, "details": {uri: message}});
            redact_connection_credentials_in_json(&mut json);
            assert!(!json.to_string().contains("secret"), "{json}");
        }
    }

    /// One line with a credential leaves the other lines
    /// of a log as they are, and a `@` after a URL is not its userinfo.
    #[test]
    fn only_the_credential_changes_in_a_multi_line_log() {
        let logs = "INFO fetching s3://bucket/data.parquet\n\
                    INFO user alice@example.com logged in\n\
                    ERROR connect postgres://u:log-secret-2136@h/db refused\n\
                    WARN set password=<value> in config\n";
        assert_eq!(
            redact_connection_credentials(logs),
            "INFO fetching s3://bucket/data.parquet\n\
             INFO user alice@example.com logged in\n\
             ERROR connect postgres://u:[REDACTED]@h/db refused\n\
             WARN set password=[REDACTED] in config\n"
        );
        let same_line = "INFO fetching s3://bucket/data.parquet for alice@example.com";
        assert!(matches!(
            redact_connection_credentials(same_line),
            Cow::Borrowed(t) if t == same_line
        ));
    }

    /// A password runs to the last `@` of the URL: a delimiter inside it
    /// does not end it early.
    #[test]
    fn a_password_with_a_delimiter_is_covered_whole() {
        for (text, expected) in [
            (
                "postgres://u:pa/ss@host/db",
                "postgres://u:[REDACTED]@host/db",
            ),
            (
                "postgres://u:secret\\value@host/db",
                "postgres://u:[REDACTED]@host/db",
            ),
            (
                "postgres://u:quote'secret@host/db' failed",
                "postgres://u:[REDACTED]@host/db' failed",
            ),
        ] {
            assert_eq!(redact_connection_credentials(text), expected);
        }
    }

    /// Only the value of a password parameter goes; an empty value, or the
    /// word without `=`, changes nothing.
    #[test]
    fn password_parameter_values_are_replaced() {
        assert_eq!(
            redact_connection_credentials("Password = 'a b' x password=c&d password=e;f"),
            "Password = [REDACTED] x password=[REDACTED]&d password=[REDACTED];f"
        );
        for text in ["password=", "the password is wrong", "password"] {
            assert!(matches!(
                redact_connection_credentials(text),
                Cow::Borrowed(t) if t == text
            ));
        }
    }

    /// Two keys that redact to the same text keep both values.
    #[test]
    fn colliding_keys_keep_both_values() {
        let mut value =
            json!({"postgres://a:x@h": 1, "postgres://a:y@h": 2, "postgres://a:[REDACTED]@h": 3});
        redact_connection_credentials_in_json(&mut value);
        let mut values: Vec<_> = value.as_object().unwrap().values().cloned().collect();
        values.sort_by_key(|v| v.as_i64());
        assert_eq!(values, vec![json!(1), json!(2), json!(3)]);
        assert!(value.get("postgres://a:[REDACTED]@h").is_some(), "{value}");
        assert!(
            value.get("postgres://a:[REDACTED]@h (2)").is_some(),
            "{value}"
        );
        assert!(
            value.get("postgres://a:[REDACTED]@h (3)").is_some(),
            "{value}"
        );
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
