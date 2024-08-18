use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// Subscription options docs: https://cloud.google.com/pubsub/docs/subscription-properties

/// Google Pub/Sub input connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct PubSubInputConfig {
    /// Set in order to use a Pub/Sub [emulator](https://cloud.google.com/pubsub/docs/emulator)
    /// instead of the production service, e.g., 'localhost:8681'.
    pub emulator: Option<String>,

    /// The content of a Google Cloud credentials JSON file.
    ///
    /// When this option is specified, the connector will use the provided credentials for
    /// authentication.  Otherwise, it will use Application Default Credentials (ADC) configured
    /// in the environment where the Feldera service is running.  See
    /// [Google Cloud documentation](https://cloud.google.com/docs/authentication/provide-credentials-adc)
    /// for information on configuring application default credentials.
    ///
    /// When running Feldera in an environment where ADC are not configured,
    /// e.g., a Docker container, use this option to ship Google Cloud credentials from another environment.
    /// For example, if you use the
    /// [`gcloud auth application-default login`](https://cloud.google.com/pubsub/docs/authentication#client-libs)
    /// command for authentication in your local development environment, ADC are stored in the
    /// `.config/gcloud/application_default_credentials.json` file in your home directory.
    pub credentials: Option<String>,

    /// Override the default service endpoint 'pubsub.googleapis.com'
    pub endpoint: Option<String>,

    /// gRPC channel pool size.
    pub pool_size: Option<u32>,

    /// gRPC request timeout.
    pub timeout_seconds: Option<u32>,

    /// gRPC connection timeout.
    pub connect_timeout_seconds: Option<u32>,

    /// Google Cloud project_id.
    ///
    /// When not specified, the connector will use the project id associated
    /// with the authenticated account.
    pub project_id: Option<String>,

    /// Subscription name.
    pub subscription: String,

    /// Reset subscription's backlog to a given snapshot on startup,
    /// using the Pub/Sub `Seek` API.
    ///
    /// This option is mutually exclusive with the `timestamp` option.
    pub snapshot: Option<String>,

    /// Reset subscription's backlog to a given timestamp on startup,
    /// using the Pub/Sub `Seek` API.
    ///
    /// The value of this option is an ISO 8601-encoded UTC time, e.g., "2024-08-17T16:39:57-08:00".
    ///
    /// This option is mutually exclusive with the `snapshot` option.
    pub timestamp: Option<String>,
}
