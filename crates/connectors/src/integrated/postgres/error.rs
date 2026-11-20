use anyhow::anyhow;
use feldera_types::library_error::LibraryError;
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
    ///
    /// This is the boundary where the raw driver error is classified and
    /// wrapped; it is one of the two places allowed to name the raw type.
    #[allow(clippy::disallowed_types)]
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
        // see the note in `From<postgres::Error>`. The wrapper keeps the
        // message and its DETAIL in its text, without credentials.
        let value = LibraryError::wrap(value);
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

    /// Whether the server answered with an error and left the session open.
    ///
    /// This separates a server that is there and did not run a statement from
    /// a server or a pooler that is not available.
    ///
    /// A connection error (class 08) ends the session whatever its severity:
    /// PgBouncer before 1.16 sends its own errors as ERROR and then closes
    /// the session.
    ///
    /// The driver error is inside the [`LibraryError`] that the two
    /// classifiers wrap it in; this only reads its code.
    #[allow(clippy::disallowed_types)]
    pub fn session_stays_open(&self) -> bool {
        let (BackoffError::Temporary(error) | BackoffError::Permanent(error)) = self;
        let driver_error = error
            .downcast_ref::<LibraryError>()
            .and_then(|e| e.downcast_ref::<postgres::Error>());
        driver_error.is_some_and(|e| {
            !e.is_closed()
                && e.as_db_error()
                    .is_some_and(|db| !session_ended(db) && !db.code().code().starts_with("08"))
        })
    }

    /// The error and everything it is chained to, as one line of text.
    pub fn message(&self) -> String {
        let (BackoffError::Temporary(error) | BackoffError::Permanent(error)) = self;
        format!("{error:#}")
    }

    pub fn context(self, context: String) -> Self {
        match self {
            BackoffError::Temporary(error) => BackoffError::Temporary(error.context(context)),
            BackoffError::Permanent(error) => BackoffError::Permanent(error.context(context)),
        }
    }
}

/// SQLSTATE classes of a failure that the statement and its rows usually do
/// not cause. The server cannot run the statement now. It can usually run it
/// later, or through a different session.
///
/// Some errors in these classes come back each time, for example a statement
/// that is always slower than the statement timeout, or an internal error that
/// a trigger raises. The limit on the attempts to write a batch bounds the
/// cost of such an error.
///
/// | Class | Meaning | Examples |
/// |---|---|---|
/// | 08 | Connection exception | PgBouncer reports each error of its own as 08P01, such as a server that it cannot log in to |
/// | 40 | Transaction rollback | Serialization failure, deadlock |
/// | 53 | Insufficient resources | Disk full, out of memory, too many connections |
/// | 57 | Operator intervention | Shutdown, startup, a query that an operator or a timeout cancelled |
/// | 58 | System error | I/O error |
/// | XX | Internal error | Some managed services report "server unavailable" as XX000 |
const TRANSIENT_SQLSTATE_CLASSES: [&str; 6] = ["08", "40", "53", "57", "58", "XX"];

/// Whether a statement that failed on an established connection can succeed
/// when the worker writes the batch again.
///
/// `closed` is [`postgres::Error::is_closed`], `code` is the SQLSTATE of the
/// error, and `session_ended` is [`session_ended`].
///
/// The rule is about the session and the server, not about the statement. The
/// transaction is gone in each of these cases, and the worker writes the same
/// rows again through a new session. A statement that the server rejected for
/// its rows fails the same way each time, so it is not transient.
fn is_transient(closed: bool, code: Option<&SqlState>, session_ended: bool) -> bool {
    // The error has no SQLSTATE when the OS refused or dropped the connection.
    let Some(code) = code else {
        return true;
    };

    closed
        || session_ended
        || TRANSIENT_SQLSTATE_CLASSES
            .iter()
            .any(|class| code.code().starts_with(class))
        // The session cannot write. After a failover, a pooler can hold a
        // session on the old primary, which is then a read-only replica. The
        // session stays open, so only a new session can correct this.
        || *code == SqlState::READ_ONLY_SQL_TRANSACTION
        // A different transaction held a lock for longer than `lock_timeout`.
        || *code == SqlState::LOCK_NOT_AVAILABLE
}

/// Whether the sender of `error` ended the session with it.
///
/// FATAL ends the session of the client and PANIC ends all sessions. The
/// SQLSTATE cannot stand in for this: a session can end for a timeout, a
/// conflict with recovery, or a reason that only a pooler knows.
fn session_ended(error: &DbError) -> bool {
    match error.parsed_severity() {
        Some(severity) => matches!(severity, Severity::Fatal | Severity::Panic),
        // Only the localizable severity is present. The sender is a pooler:
        // PgBouncer sends no other severity field, and it does not localize.
        // A PostgreSQL server older than 9.6 also sends only this field.
        None => matches!(error.severity(), "FATAL" | "PANIC"),
    }
}

/// Classifies a statement failing on an established connection.
///
/// Use [`BackoffError::connecting`] for a failure to open the connection in the
/// first place, where far more of the failures are transient.
///
/// This is the other boundary that classifies and wraps the raw driver error.
#[allow(clippy::disallowed_types)]
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
        // error chain, which `BackoffError::inner` formats in full. The
        // wrapper joins that chain into one sanitized text.
        let value = LibraryError::wrap(value);
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
    use super::{BackoffError, is_transient};
    use postgres::error::SqlState;

    /// The driver's own message survives the wrapper, so the report still
    /// says what went wrong.
    #[test]
    fn the_driver_message_survives_classification() {
        let error = "host=h port=notaport"
            .parse::<tokio_postgres::Config>()
            .unwrap_err();
        let text = BackoffError::connecting(error).inner().to_string();
        assert!(
            text.starts_with("cannot connect to postgres yet: "),
            "{text}"
        );
        assert!(text.contains("port"), "{text}");
    }

    /// A statement that the server rejects in a healthy session is permanent.
    #[test]
    fn rejected_statement_is_permanent() {
        for code in [
            SqlState::UNIQUE_VIOLATION,
            SqlState::NOT_NULL_VIOLATION,
            SqlState::FOREIGN_KEY_VIOLATION,
            SqlState::INVALID_TEXT_REPRESENTATION,
            SqlState::INSUFFICIENT_PRIVILEGE,
            SqlState::UNDEFINED_TABLE,
            SqlState::SYNTAX_ERROR,
            SqlState::IN_FAILED_SQL_TRANSACTION,
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

    /// A session that stays open and cannot write is transient: a new session
    /// can reach a server that accepts writes.
    #[test]
    fn read_only_session_is_transient() {
        let code = SqlState::READ_ONLY_SQL_TRANSACTION;
        assert!(is_transient(false, Some(&code), false));
    }

    /// A failure of the server, or a conflict with a different transaction,
    /// is transient also when the session stays open.
    #[test]
    fn server_failures_and_conflicts_are_transient() {
        for code in [
            SqlState::T_R_SERIALIZATION_FAILURE,
            SqlState::T_R_DEADLOCK_DETECTED,
            SqlState::LOCK_NOT_AVAILABLE,
            SqlState::DISK_FULL,
            SqlState::OUT_OF_MEMORY,
            SqlState::QUERY_CANCELED,
            SqlState::IO_ERROR,
            SqlState::INTERNAL_ERROR,
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
