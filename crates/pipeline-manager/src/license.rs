use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use utoipa::ToSchema;

/// The license information lock is held very shortly for both read and write.
pub const LICENSE_INFO_READ_LOCK_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Serialize, Deserialize, ToSchema, Clone, PartialEq)]
#[allow(dead_code)]
pub enum DisplaySchedule {
    /// Display it only once: after dismissal do not show it again
    Once,
    /// Display it again the next session if it is dismissed
    Session,
    /// Display it again after a certain period of time after it is dismissed
    Every { seconds: u64 },
    /// Always display it, do not allow it to be dismissed
    Always,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, PartialEq)]
pub struct LicenseInformation {
    /// Timestamp at which the license expires
    pub expires_at: DateTime<Utc>,
    /// Whether the license is expired
    pub is_expired: bool,
    /// Whether the license is a trial
    pub is_trial: bool,
    /// Optional description of the advantages of extending the license / upgrading from a trial
    pub description_html: String,
    /// URL that navigates the user to extend / upgrade their license
    pub extension_url: Option<String>,
    /// Timestamp from which the user should be reminded of the license expiring soon
    pub remind_starting_at: DateTime<Utc>,
    /// Suggested frequency of reminding the user about the license expiring soon
    pub remind_schedule: DisplaySchedule,
}
