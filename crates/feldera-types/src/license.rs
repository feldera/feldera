//! License related types exchanged between license server and feldera platform.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Request to verify a license.
/// Shared type between client and server.
#[derive(Serialize, Deserialize, Debug)]
pub struct LicenseCheckRequest {
    pub account_id: String,
    pub license_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct LicenseInformation {
    /// Timestamp when the server responded.
    pub current: DateTime<Utc>,
    /// Timestamp at which point the license expires
    pub valid_until: Option<DateTime<Utc>>,
    /// Whether the license is a trial
    pub is_trial: bool,
    /// Optional description of the advantages of extending the license / upgrading from a trial
    pub description_html: String,
    /// URL that navigates the user to extend / upgrade their license
    pub extension_url: Option<String>,
    /// Timestamp from which the user should be reminded of the license expiring soon
    pub remind_starting_at: Option<DateTime<Utc>>,
    /// Suggested frequency of reminding the user about the license expiring soon
    pub remind_schedule: DisplaySchedule,
}
