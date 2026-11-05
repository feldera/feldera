//! The credential scrub the manager applies to the diagnostics it writes, reads
//! back, and exports: see [`feldera_types::credential_scrub`]. The manager
//! does not resolve secrets, so it cannot use the pipeline's registry; an
//! older runtime can still send it connection strings in errors, and stored
//! support collections from before the fix hold them too.

pub use feldera_types::credential_scrub::{
    REDACTED, redact_connection_credentials, redact_connection_credentials_in_json,
};

/// The scrub against the driver's own grammar: what `tokio_postgres` reads
/// as the password never survives.
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
}
