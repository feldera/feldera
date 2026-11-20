use anyhow::anyhow;
use postgres::error::{DbError, Severity, SqlState};

pub(super) enum BackoffError {
    Temporary(anyhow::Error),
    Permanent(anyhow::Error),
}

impl BackoffError {
    /// Classifies a failure to open a connection.
    ///
    /// Almost every way that connecting can fail is worth waiting out: the
    /// server may be starting up or recovering from a crash, refusing
    /// connections during maintenance, out of connection slots, or unreachable.
    /// So this denies a short list rather than allowing one, unlike
    /// [`From<postgres::Error>`], which classifies statements failing on a
    /// connection that is already established.
    ///
    /// Only a configuration the connector cannot outlast is permanent. Retrying
    /// a wrong password or a database that does not exist would spin until
    /// someone changes the connector's configuration, which restarts it anyway.
    pub fn connecting(value: postgres::Error) -> Self {
        let permanent = value.code().is_some_and(|code| {
            [
                SqlState::INVALID_PASSWORD,
                SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                SqlState::INVALID_CATALOG_NAME,
                SqlState::INSUFFICIENT_PRIVILEGE,
            ]
            .contains(code)
        });

        // Chain rather than interpolate, so that the server's message survives:
        // see the note in `From<postgres::Error>`.
        if permanent {
            Self::Permanent(anyhow::Error::new(value).context("cannot connect to postgres"))
        } else {
            Self::Temporary(anyhow::Error::new(value).context("cannot connect to postgres yet"))
        }
    }

    pub fn should_retry(&self) -> bool {
        match self {
            BackoffError::Temporary(_) => true,
            BackoffError::Permanent(_) => false,
        }
    }

    /// Flattens the error and everything it is chained to into one message.
    ///
    /// `{:#}` walks the chain, so the server's message and its DETAIL survive,
    /// which is the whole reason the postgres error is chained rather than
    /// interpolated. `{:?}` walks it too, but also prints the backtrace anyhow
    /// captured, which runs to kilobytes whenever `RUST_BACKTRACE` is set and
    /// leaves no room under the cap the endpoint status applies.
    pub fn inner(self) -> anyhow::Error {
        match self {
            BackoffError::Permanent(error) | BackoffError::Temporary(error) => {
                anyhow!("{error:#}")
            }
        }
    }

    /// Byte length of the formatted error chain, so a caller embedding a
    /// payload in its context can size it to what [`Self::inner`] leaves over.
    pub fn message_len(&self) -> usize {
        match self {
            BackoffError::Permanent(error) | BackoffError::Temporary(error) => {
                format!("{error:?}").len()
            }
        }
    }

    pub fn context(self, context: String) -> Self {
        match self {
            BackoffError::Temporary(error) => BackoffError::Temporary(error.context(context)),
            BackoffError::Permanent(error) => BackoffError::Permanent(error.context(context)),
        }
    }
}

/// Whether a statement that failed on an established connection can succeed on
/// a new one.
///
/// `closed` is [`postgres::Error::is_closed`], `code` is the SQLSTATE of the
/// error, and `session_ended` is [`session_ended`].
///
/// The rule is about the session, not about the statement: when the session is
/// gone, so is its transaction, and the same rows can be written again through
/// a new session. A statement that the server rejected in a healthy session
/// fails the same way each time, so it is not transient.
fn is_transient(closed: bool, code: Option<&SqlState>, session_ended: bool) -> bool {
    // The error has no SQLSTATE when the OS refused or dropped the connection.
    let Some(code) = code else {
        return true;
    };

    closed
        || session_ended
        // Class 08, connection exception. A connection pooler in front of the
        // server is one source of these: PgBouncer reports each error of its
        // own as 08P01, such as a server that it cannot log in to, or a server
        // connection that died under a transaction.
        || code.code().starts_with("08")
        // The server is stopping, restarting after a crash, or starting up.
        || [
            SqlState::ADMIN_SHUTDOWN,
            SqlState::CRASH_SHUTDOWN,
            SqlState::CANNOT_CONNECT_NOW,
        ]
        .contains(code)
}

/// Whether the sender of `error` ended the session with it.
///
/// FATAL ends the session of the client and PANIC ends all sessions. The
/// SQLSTATE cannot stand in for this: a session can end for a timeout, a
/// conflict with recovery, or a reason that only a pooler knows.
fn session_ended(error: &DbError) -> bool {
    match error.parsed_severity() {
        Some(severity) => matches!(severity, Severity::Fatal | Severity::Panic),
        // Only the localizable severity is present: the sender is a server
        // older than 9.6, or a pooler, and poolers do not localize.
        None => matches!(error.severity(), "FATAL" | "PANIC"),
    }
}

/// Classifies a statement failing on an established connection.
///
/// Use [`BackoffError::connecting`] for a failure to open the connection in the
/// first place, where far more of the failures are transient.
impl From<postgres::Error> for BackoffError {
    fn from(value: postgres::Error) -> Self {
        let code = value.code().cloned();
        let temporary = is_transient(
            value.is_closed(),
            code.as_ref(),
            value.as_db_error().is_some_and(session_ended),
        );

        // Keep the postgres error as the source instead of interpolating it:
        // `Display for postgres::Error` reports only the kind ("db error"), and
        // the server's message and DETAIL reach the report solely through the
        // error chain, which `BackoffError::inner` formats in full.
        if temporary {
            Self::Temporary(
                anyhow::Error::new(value)
                    .context(format!("postgres error: transient: SqlState: {code:?}")),
            )
        } else {
            Self::Permanent(
                anyhow::Error::new(value)
                    .context(format!("postgres error: permanent: SqlState: {code:?}")),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_transient;
    use postgres::error::SqlState;

    /// A statement that the server rejects in a healthy session is permanent.
    #[test]
    fn rejected_statement_is_permanent() {
        for code in [
            SqlState::UNIQUE_VIOLATION,
            SqlState::NOT_NULL_VIOLATION,
            SqlState::UNDEFINED_TABLE,
            SqlState::SYNTAX_ERROR,
        ] {
            assert!(!is_transient(false, Some(&code), false), "{code:?}");
        }
    }

    /// PgBouncer reports its own errors as 08P01. One of them repeats the
    /// 57P03 of a server that is starting up.
    #[test]
    fn pooler_and_startup_errors_are_transient() {
        for code in [
            SqlState::PROTOCOL_VIOLATION,
            SqlState::CANNOT_CONNECT_NOW,
            SqlState::CRASH_SHUTDOWN,
            SqlState::ADMIN_SHUTDOWN,
            SqlState::CONNECTION_FAILURE,
        ] {
            assert!(is_transient(false, Some(&code), false), "{code:?}");
        }
    }

    /// An error that ends the session is transient, whatever its SQLSTATE: a
    /// session timeout is one example.
    #[test]
    fn error_that_ends_the_session_is_transient() {
        let code = SqlState::IDLE_IN_TRANSACTION_SESSION_TIMEOUT;
        assert!(!is_transient(false, Some(&code), false));
        assert!(is_transient(false, Some(&code), true));
    }

    #[test]
    fn closed_connection_is_transient() {
        assert!(is_transient(true, Some(&SqlState::UNIQUE_VIOLATION), false));
        assert!(is_transient(false, None, false));
    }
}
