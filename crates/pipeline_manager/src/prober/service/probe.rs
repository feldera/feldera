use crate::db::DBError;
use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;
use utoipa::ToSchema;

/// Service probe status.
///
/// State transition diagram:
/// ```text
///    Pending
///       │
///       │ (Prober server picks up the probe)
///       │
///       ▼
///   ⌛Running ───► Failure
///       │
///       ▼
///    Success
/// ```
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ServiceProbeStatus {
    /// Probe has not been started.
    Pending,
    /// Probe has been picked up by the probe server and is running.
    Running,
    /// Probe has finished running and it succeeded.
    Success,
    /// Probe has finished running and it failed.
    Failure,
}

impl TryFrom<String> for ServiceProbeStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "success" => Ok(Self::Success),
            "failure" => Ok(Self::Failure),
            _ => Err(DBError::unknown_service_probe_status(value)),
        }
    }
}

impl From<ServiceProbeStatus> for &'static str {
    fn from(val: ServiceProbeStatus) -> Self {
        match val {
            ServiceProbeStatus::Pending => "pending",
            ServiceProbeStatus::Running => "running",
            ServiceProbeStatus::Success => "success",
            ServiceProbeStatus::Failure => "failure",
        }
    }
}

/// Enumeration of all possible service probe types.
/// Each type maps to exactly one request variant.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Eq, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum ServiceProbeType {
    TestConnectivity,
    KafkaGetTopics,
}

impl TryFrom<String> for ServiceProbeType {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "test_connectivity" => Ok(Self::TestConnectivity),
            "kafka_get_topics" => Ok(Self::KafkaGetTopics),
            _ => Err(DBError::unknown_service_probe_type(value)),
        }
    }
}

impl From<ServiceProbeType> for &'static str {
    fn from(val: ServiceProbeType) -> Self {
        match val {
            ServiceProbeType::TestConnectivity => "test_connectivity",
            ServiceProbeType::KafkaGetTopics => "kafka_get_topics",
        }
    }
}

/// Enumeration of all possible service probe requests.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Eq, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum ServiceProbeRequest {
    /// Tests connectivity to the provided service. For example, for a Kafka service,
    /// this probe will check whether the supplied bootstrap configuration and
    /// credentials are valid by trying to fetch metadata for all topics.
    TestConnectivity,
    /// Retrieves the names of all Kafka topics present.
    KafkaGetTopics,
}

impl ServiceProbeRequest {
    /// Unique service request type used for classification.
    pub fn probe_type(&self) -> ServiceProbeType {
        match self {
            ServiceProbeRequest::TestConnectivity => ServiceProbeType::TestConnectivity,
            ServiceProbeRequest::KafkaGetTopics => ServiceProbeType::KafkaGetTopics,
        }
    }

    /// Deserialize from provided YAML.
    pub fn from_yaml_str(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    /// Serialize to YAML for storage.
    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(&self).unwrap()
    }
}

/// Enumeration of all possible service probe success responses.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Eq, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum ServiceProbeResult {
    /// A connection to the service was established.
    Connected,
    /// The names of all Kafka topics of the service.
    KafkaTopics(Vec<String>),
}

/// Range of possible errors that can occur during a service probe.
/// These are shared across all services.
#[derive(ThisError, Debug, Serialize, Deserialize, ToSchema, Clone, Eq, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum ServiceProbeError {
    #[error("Timeout was exceeded")]
    TimeoutExceeded,
    #[error("Service type ({service_type}) does not support probe type ({probe_type:?})")]
    UnsupportedRequest {
        service_type: String,
        probe_type: String,
    },
    #[error("{0}")]
    Other(String),
}

/// Response being either success or error.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Eq, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum ServiceProbeResponse {
    // Serialization of the enums are done using with::singleton_map
    // because serde_yaml does not support nested enums currently.
    // See also: <https://github.com/dtolnay/serde-yaml/issues/363>
    #[serde(with = "serde_yaml::with::singleton_map")]
    Success(ServiceProbeResult),
    #[serde(with = "serde_yaml::with::singleton_map")]
    Error(ServiceProbeError),
}

impl ServiceProbeResponse {
    /// Deserialize from provided YAML.
    pub fn from_yaml_str(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    /// Serialize to YAML for storage.
    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(&self).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::ServiceProbeType;
    use super::{ServiceProbeRequest, ServiceProbeStatus};
    use crate::db::DBError;

    /// Tests that the status string conversion is the same in both directions.
    #[test]
    fn test_status_string_conversion() {
        for (string_repr, corresponding_status) in [
            ("pending", ServiceProbeStatus::Pending),
            ("running", ServiceProbeStatus::Running),
            ("success", ServiceProbeStatus::Success),
            ("failure", ServiceProbeStatus::Failure),
        ] {
            // Converting status into string (e.g., for storing in database)
            assert_eq!(
                <ServiceProbeStatus as Into<&'static str>>::into(corresponding_status.clone()),
                string_repr
            );

            // Converting string into status (e.g., for reading from database)
            assert_eq!(
                ServiceProbeStatus::try_from(string_repr.to_string()).unwrap(),
                corresponding_status.clone()
            );
        }

        // Converting invalid string into a status fails
        match ServiceProbeStatus::try_from("does_not_exist".to_string()) {
            Ok(_) => {
                panic!("Status converted which does not exist")
            }
            Err(e) => {
                match e {
                    DBError::UnknownServiceProbeStatus { status, .. } => {
                        assert_eq!(status, "does_not_exist".to_string());
                    }
                    _ => {
                        panic!("Incorrect DBError was thrown when converting status that does not exist")
                    }
                }
            }
        }
    }

    /// Tests that the type string conversion and (de-)serialization into JSON is the same.
    #[test]
    fn test_probe_type_string_conversion_and_de_serialization() {
        for (string_repr, corresponding_type) in [
            ("test_connectivity", ServiceProbeType::TestConnectivity),
            ("kafka_get_topics", ServiceProbeType::KafkaGetTopics),
        ] {
            // Converting type into string (e.g., for storing in database)
            assert_eq!(
                <ServiceProbeType as Into<&'static str>>::into(corresponding_type.clone()),
                string_repr
            );

            // Converting string into type (e.g., for reading from database)
            assert_eq!(
                ServiceProbeType::try_from(string_repr.to_string()).unwrap(),
                corresponding_type.clone()
            );

            // Serializing type results in a single JSON string (e.g., in the API output)
            assert_eq!(
                serde_json::to_string(&corresponding_type).unwrap(),
                serde_json::Value::String(string_repr.to_string()).to_string()
            );
            assert_eq!(
                serde_json::to_string(&corresponding_type).unwrap(),
                format!("\"{string_repr}\"")
            );

            // Deserializing a single JSON string results in the type (e.g., from the API input)
            assert_eq!(
                serde_json::from_str::<ServiceProbeType>(format!("\"{string_repr}\"").as_str())
                    .unwrap(),
                corresponding_type
            );
        }

        // Converting invalid string into a type fails
        match ServiceProbeType::try_from("does_not_exist".to_string()) {
            Ok(_) => {
                panic!("Probe type converted which does not exist")
            }
            Err(e) => {
                match e {
                    DBError::UnknownServiceProbeType { probe_type, .. } => {
                        assert_eq!(probe_type, "does_not_exist".to_string());
                    }
                    _ => {
                        panic!("Incorrect DBError was thrown when converting status that does not exist")
                    }
                }
            }
        }
    }

    #[test]
    fn test_request_probe_type_and_de_serialization() {
        let request = ServiceProbeRequest::TestConnectivity;
        assert_eq!(request.probe_type(), ServiceProbeType::TestConnectivity);
        assert_eq!(
            request,
            ServiceProbeRequest::from_yaml_str(&request.to_yaml())
        );

        let request = ServiceProbeRequest::KafkaGetTopics;
        assert_eq!(request.probe_type(), ServiceProbeType::KafkaGetTopics);
        assert_eq!(
            request,
            ServiceProbeRequest::from_yaml_str(&request.to_yaml())
        );
    }
}
