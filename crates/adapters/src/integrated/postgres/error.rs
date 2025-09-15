use anyhow::anyhow;

pub(super) enum BackoffError {
    Temporary(anyhow::Error),
    Permanent(anyhow::Error),
}

impl BackoffError {
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

impl From<postgres::Error> for BackoffError {
    fn from(value: postgres::Error) -> Self {
        use postgres::error::SqlState;

        if value.is_closed()
            || value.code().is_some_and(|c| {
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
            || value.code().is_none()
        {
            Self::Temporary(anyhow!("failed to connect to postgres: {value}"))
        } else {
            Self::Permanent(anyhow!(
                "postgres error: permanent: SqlState: {:?}: {value}",
                value.code()
            ))
        }
    }
}
