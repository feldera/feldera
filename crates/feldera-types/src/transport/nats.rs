use std::path::PathBuf;
use time::OffsetDateTime;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}

// TODO How does the user choose? Think about what "UI" you would prefer.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum Credentials {
    FromString(String),
    FromFile(PathBuf),
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct UserAndPassword {
    pub user: String,
    pub password: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub struct Auth {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nkey: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_and_password: Option<UserAndPassword>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ConnectOptions {
    pub server_url: String,
    #[serde(default, skip_serializing_if = "is_default")]
    pub auth: Auth,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum DeliverPolicy {
    All,
    Last,
    New,
    ByStartSequence { start_sequence: u64 },
    ByStartTime { start_time: OffsetDateTime },
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ConsumerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub filter_subjects: Vec<String>,
    //pub replay_policy: ReplayPolicy,
    #[serde(default, skip_serializing_if = "is_default")]
    pub rate_limit: u64,
    #[serde(default, skip_serializing_if = "is_default")]
    pub sample_frequency: u8,
    pub deliver_policy: DeliverPolicy,
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_waiting: i64,
    #[serde(default, skip_serializing_if = "is_default")]
    pub metadata: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_expires: Option<std::time::Duration>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct NatsInputConfig {
    pub connection_config: ConnectOptions,
    pub stream_name: String,
    pub consumer_config: ConsumerConfig,
}
