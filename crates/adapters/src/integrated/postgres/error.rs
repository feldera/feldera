use anyhow::anyhow;
use postgres::error::SqlState;

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

    pub fn inner(self) -> anyhow::Error {
        match self {
            BackoffError::Permanent(error) | BackoffError::Temporary(error) => {
                // include the context info
                anyhow!("{error:?}")
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

/// Classifies a statement failing on an established connection.
///
/// Use [`BackoffError::connecting`] for a failure to open the connection in the
/// first place, where far more of the failures are transient.
impl From<postgres::Error> for BackoffError {
    fn from(value: postgres::Error) -> Self {
        let code = value.code().cloned();
        let temporary = value.is_closed()
            || code.as_ref().is_some_and(|c| {
                [
                    SqlState::CONNECTION_FAILURE,
                    SqlState::CONNECTION_DOES_NOT_EXIST,
                    SqlState::CONNECTION_EXCEPTION,
                    SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
                    SqlState::ADMIN_SHUTDOWN,
                ]
                .contains(c)
            })
            // value.code() is none when connection is refused by the OS
            || code.is_none();

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
