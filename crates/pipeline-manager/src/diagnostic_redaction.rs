//! Credential scrubbing for the diagnostics the manager stores and exports.
//!
//! The pipeline process knows each resolved secret value and replaces it by
//! its reference before the value reaches an error, a log line, or `/stats`.
//! The manager does not resolve secrets, so it cannot use that registry. Old
//! runtimes can still send it connection strings in errors, and stored
//! support collections from before the fix hold them too. The manager
//! replaces the password of a URL's userinfo and the value of a `password=`
//! parameter and keeps the text around them. Where the end of a credential
//! cannot be proven, the text is cut off at the credential: what comes
//! before it stays, what comes after it goes.
//!
//! The manager applies this to the diagnostics it writes (a pipeline on a
//! runtime from before the fix still reports such errors), to the
//! diagnostics it reads back, and to the stored support collections it
//! exports.

use serde_json::Value;
use std::borrow::Cow;

/// Replacement for the password component of a URL or of a parameter, and
/// for the rest of a text cut off at an unbounded credential.
pub const REDACTED: &str = "[REDACTED]";

/// Replaces connection passwords in `text` and keeps the text around them.
///
/// Free text has no reliable connection-string boundaries. A password with
/// whitespace in it, an unterminated quote, or a query value without a `&`
/// after it has no provable end, and a percent-encoded parameter name hides
/// where its value starts. In those cases the text is cut off at the
/// credential. The scan does not split lines first: a quoted or escaped
/// keyword password can contain a newline.
pub fn redact_connection_credentials(text: &str) -> Cow<'_, str> {
    match redact_url_passwords(text) {
        Cow::Borrowed(_) => redact_password_parameters(text),
        Cow::Owned(urls) => Cow::Owned(redact_password_parameters(&urls).into_owned()),
    }
}

/// Whether `query` holds a `password=` parameter that only shows once
/// decoded, such as `%70assword=`. The driver decodes parameter names, the
/// scrub reads the original text, so such a value cannot be located safely.
fn hides_a_password_parameter(query: &str) -> bool {
    let decoded = urlencoding::decode_binary(query.as_bytes());
    if decoded == query.as_bytes() {
        return false;
    }
    let count = |text: &str| {
        text.to_ascii_lowercase()
            .split("password")
            .skip(1)
            .filter(|tail| tail.trim_start().starts_with('='))
            .count()
    };
    count(&String::from_utf8_lossy(&decoded)) > count(query)
}

/// Replaces the userinfo password of every URL in `text`. A URL runs to the
/// next whitespace; its last `@` ends the userinfo, so a password that holds
/// `@`, `/`, `?` or a quote is covered whole. Whitespace does not prove that
/// a password ended: a URL with a userinfo `:` and no `@`, followed by an
/// `@` before the next URL, may be a password split by whitespace, and the
/// text is cut off at that URL. A port, digits after the host, is not such
/// a colon.
///
/// A URL that holds an `@` is complete, and a later `@`, such as an email
/// address in the same log, is not part of it. The cost: a password with
/// both an unencoded `@` and whitespace is covered only up to its first
/// `@`. `postgres://u:pa@ss word@host/db` becomes
/// `postgres://u:[REDACTED]@ss word@host/db`. The other reading would cut
/// off a log at each URL with a password that an email address follows.
fn redact_url_passwords(text: &str) -> Cow<'_, str> {
    let mut out = String::new();
    let mut changed = false;
    let mut rest = text;
    while let Some(scheme_end) = rest.find("://") {
        let url_start = scheme_end + 3;
        let url_end = rest[url_start..]
            .find(char::is_whitespace)
            .map_or(rest.len(), |i| url_start + i);
        let url = &rest[url_start..url_end];
        let tail = rest[url_end..].split("://").next().unwrap_or_default();
        let authority = url.split(['/', '?', '#']).next().unwrap_or_default();
        let userinfo_colon = authority.split_once(':').is_some_and(|(_, after)| {
            after.is_empty() || !after.bytes().all(|b| b.is_ascii_digit())
        });
        if !url.contains('@') && (url.is_empty() || userinfo_colon) && tail.contains('@') {
            out.push_str(&rest[..url_start]);
            out.push_str(REDACTED);
            return Cow::Owned(out);
        }
        if let Some(query_start) = url.find('?')
            && hides_a_password_parameter(&url[query_start + 1..])
        {
            out.push_str(&rest[..url_start + query_start + 1]);
            out.push_str(REDACTED);
            return Cow::Owned(out);
        }
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
        Cow::Borrowed(text)
    }
}

/// Length of a libpq keyword value, including escapes. A `'` opens a quoted
/// value; an unquoted value ends at unescaped whitespace, not at `&` or `;`.
/// libpq does not quote with `"`, but other connection strings do: a value
/// that starts with `"` is covered up to its closing quote and up to the
/// next whitespace, whichever is longer, so both readings are inside the
/// replaced span. An unterminated `'` or a trailing backslash has no end.
fn keyword_value_len(value: &str) -> Option<usize> {
    match value.chars().next() {
        Some('\'') => quoted_value_len(value, '\''),
        Some('"') => {
            let unquoted = unquoted_value_len(value)?;
            Some(quoted_value_len(value, '"').map_or(unquoted, |quoted| quoted.max(unquoted)))
        }
        _ => unquoted_value_len(value),
    }
}

/// Length of `value` up to and including its closing `quote`, honoring
/// backslash escapes. `None` when the quote never closes.
fn quoted_value_len(value: &str, quote: char) -> Option<usize> {
    let mut chars = value[quote.len_utf8()..].char_indices();
    while let Some((index, c)) = chars.next() {
        if c == '\\' {
            chars.next()?;
        } else if c == quote {
            return Some(quote.len_utf8() + index + c.len_utf8());
        }
    }
    None
}

/// Length of `value` up to its first unescaped whitespace. `None` when a
/// backslash has nothing to escape.
fn unquoted_value_len(value: &str) -> Option<usize> {
    let mut chars = value.char_indices();
    while let Some((index, c)) = chars.next() {
        if c == '\\' {
            chars.next()?;
        } else if c.is_whitespace() {
            return Some(index);
        }
    }
    Some(value.len())
}

/// Replaces the value of every `password=` parameter, in any letter case and
/// with spaces around the `=`. In a URL query the value runs to the next
/// `&`; in a keyword connection string it is a libpq value. A query value
/// with whitespace in it or without a `&` after it, an unterminated quote,
/// or a trailing backslash has no provable end: the text is cut off at the
/// value.
fn redact_password_parameters(text: &str) -> Cow<'_, str> {
    const KEY: &str = "password";
    let lower = text.to_ascii_lowercase();
    let mut out = String::new();
    let mut changed = false;
    let mut from = 0;
    while let Some(found) = lower[from..].find(KEY) {
        let key_start = from + found;
        let key_end = key_start + KEY.len();
        let after_key = text[key_end..].trim_start();
        let Some(after_equals) = after_key.strip_prefix('=') else {
            out.push_str(&text[from..key_end]);
            from = key_end;
            continue;
        };
        let value_start = text.len() - after_equals.trim_start().len();
        let value = &text[value_start..];
        // The token before the key tells a URL query from a keyword string.
        let prefix = text[..key_start]
            .rsplit(char::is_whitespace)
            .next()
            .unwrap_or_default();
        let in_query = prefix.contains("://") && prefix.contains('?');
        let value_len = if in_query {
            value
                .find('&')
                .filter(|end| !value[..*end].chars().any(char::is_whitespace))
        } else {
            keyword_value_len(value)
        };
        out.push_str(&text[from..value_start]);
        let Some(value_len) = value_len else {
            out.push_str(REDACTED);
            return Cow::Owned(out);
        };
        if value_len > 0 {
            out.push_str(REDACTED);
            changed = true;
        }
        from = value_start + value_len;
    }
    if changed {
        out.push_str(&text[from..]);
        Cow::Owned(out)
    } else {
        Cow::Borrowed(text)
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

    /// Where a credential has no provable end, the text is cut off at it:
    /// the text before it stays, the text after it goes.
    #[test]
    fn an_unbounded_credential_cuts_the_text_from_its_start() {
        for (text, expected) in [
            (
                "postgres://u@host/db?%70assword=encoded-secret-2136",
                "postgres://u@host/db?[REDACTED]",
            ),
            (
                "postgres://u@host/db?%70assword=byte%FFsecret2136",
                "postgres://u@host/db?[REDACTED]",
            ),
            (
                "postgres://u:secret with space@host/db",
                "postgres://[REDACTED]",
            ),
            ("postgres:// user:secret@host/db", "postgres://[REDACTED]"),
            (
                "postgres://u:secret\ncontinuation@host/db",
                "postgres://[REDACTED]",
            ),
            (
                "postgres://u@host/db?password=a b&sslmode=require",
                "postgres://u@host/db?password=[REDACTED]",
            ),
            (
                "connect 'postgres://u@h/db?password=tail-secret-2136': refused",
                "connect 'postgres://u@h/db?password=[REDACTED]",
            ),
            (
                "password='unterminated secret\ncontinuation",
                "password=[REDACTED]",
            ),
            ("password=secret\\", "password=[REDACTED]"),
            (
                "INFO line one\nERROR connect postgres://u:secret with space@host/db\nINFO line three",
                "INFO line one\nERROR connect postgres://[REDACTED]",
            ),
        ] {
            assert_eq!(redact_connection_credentials(text), expected, "{text}");
        }
    }

    /// A port is digits after the host, not the start of a password: a
    /// later `@` leaves the log alone.
    #[test]
    fn a_port_is_not_a_userinfo_colon() {
        let logs = "INFO GET http://localhost:8080/v0/stats\n\
                    INFO user alice@example.com logged in\n\
                    ERROR connect postgres://u:log-secret-2136@h:5432/db refused\n";
        assert_eq!(
            redact_connection_credentials(logs),
            "INFO GET http://localhost:8080/v0/stats\n\
             INFO user alice@example.com logged in\n\
             ERROR connect postgres://u:[REDACTED]@h:5432/db refused\n"
        );
        let same_line = "http://minio:9000 for bob@x";
        assert!(matches!(
            redact_connection_credentials(same_line),
            Cow::Borrowed(t) if t == same_line
        ));
    }

    /// A URL that holds an `@` is complete: text after it stays, also when
    /// it holds another `@`. A password with an unencoded `@` and whitespace
    /// is then covered only up to its first `@`.
    #[test]
    fn a_url_with_an_at_sign_is_complete() {
        for (text, expected) in [
            (
                "ERROR postgres://u:pw@host:5432 refused; contact admin@example.com\nINFO after",
                "ERROR postgres://u:[REDACTED]@host:5432 refused; contact admin@example.com\nINFO after",
            ),
            (
                "postgres://u:pa@ss word@host/db",
                "postgres://u:[REDACTED]@ss word@host/db",
            ),
        ] {
            assert_eq!(redact_connection_credentials(text), expected, "{text}");
        }
        let no_password = "connect to kafka://broker:9092 failed for bob@corp";
        assert!(matches!(
            redact_connection_credentials(no_password),
            Cow::Borrowed(t) if t == no_password
        ));
    }

    /// A percent-escape elsewhere in a log, or an encoded byte in a bounded
    /// query value, is no reason to drop the log.
    #[test]
    fn an_escape_elsewhere_keeps_the_log() {
        let logs = "GET /v0/pipelines?name=a%20b\n\
                    ERROR host=h password=kw-secret-2136 dbname=db\n\
                    WARN postgres://u@h/db?password=x%26y&sslmode=require\n\
                    INFO done\n";
        assert_eq!(
            redact_connection_credentials(logs),
            "GET /v0/pipelines?name=a%20b\n\
             ERROR host=h password=[REDACTED] dbname=db\n\
             WARN postgres://u@h/db?password=[REDACTED]&sslmode=require\n\
             INFO done\n"
        );
    }

    /// A `"` is a plain character to libpq and a quote to other connection
    /// strings: the replaced span covers both readings.
    #[test]
    fn a_double_quoted_value_is_covered_under_both_readings() {
        for (text, expected) in [
            (
                "password=\"x y\" dbname=db",
                "password=[REDACTED] dbname=db",
            ),
            (
                "password=\"head\"secret-tail-2136 dbname=db",
                "password=[REDACTED] dbname=db",
            ),
            ("password=\"open x", "password=[REDACTED] x"),
        ] {
            assert_eq!(redact_connection_credentials(text), expected, "{text}");
        }
    }

    #[test]
    fn escaped_and_multiline_keyword_passwords_are_covered_whole() {
        for value in [
            r"'head\'secret-tail-2136'",
            r"'head\\secret-tail-2136'",
            "'head\nsecret-tail-2136'",
            "head\\\nsecret-tail-2136",
            r"head\ secret-tail-2136",
            "head&secret-tail-2136",
            "head;secret-tail-2136",
        ] {
            let config = format!("host=host password={value} dbname=db");
            let parsed = config.parse::<tokio_postgres::Config>().unwrap();
            assert!(
                parsed
                    .get_password()
                    .unwrap()
                    .ends_with(b"secret-tail-2136")
            );
            assert_eq!(
                redact_connection_credentials(&config),
                "host=host password=[REDACTED] dbname=db"
            );
        }
    }

    #[test]
    fn postgres_query_fragments_and_unquoted_double_quotes_are_credentials() {
        for (config, expected) in [
            (
                "postgres://u@host/db?password=head#secret-tail-2136&sslmode=disable",
                "postgres://u@host/db?password=[REDACTED]&sslmode=disable",
            ),
            (
                "host=host password=\"head\"secret-tail-2136 dbname=db",
                "host=host password=[REDACTED] dbname=db",
            ),
        ] {
            let parsed = config.parse::<tokio_postgres::Config>().unwrap();
            assert!(
                parsed
                    .get_password()
                    .unwrap()
                    .ends_with(b"secret-tail-2136")
            );
            assert_eq!(redact_connection_credentials(config), expected);
        }
    }

    /// Fred's QA cases: one line with a credential leaves the other lines
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
            "Password = [REDACTED] x password=[REDACTED] password=[REDACTED]"
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
