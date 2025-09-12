use feldera_types::license::LicenseInformation;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use utoipa::ToSchema;

use crate::api::error::ApiError;
use crate::api::main::ServerState;
use crate::error::ManagerError;

/// The license information lock is held very shortly for both read and write.
pub const LICENSE_INFO_READ_LOCK_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum LicenseValidity {
    /// License with that license key exists and thus information was retrieved about it.
    Exists(LicenseInformation),

    /// Either the license key is invalid according to the server, or the request that checks with
    /// the server failed (e.g., if it could not reach the server).
    DoesNotExistOrNotConfirmed(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LicenseCheck {
    pub checked_at: Instant,
    pub check_outcome: LicenseValidity,
}

impl LicenseCheck {
    pub(crate) async fn validate(state: &ServerState) -> Result<Option<Self>, ManagerError> {
        // Acquire and read the license information check. The timeout prevents it from becoming
        // unresponsive if it cannot acquire the lock, which generally should not happen.
        let mut license_check =
            match tokio::time::timeout(LICENSE_INFO_READ_LOCK_TIMEOUT, state.license_check.read())
                .await
            {
                Ok(license_check) => license_check.clone(),
                Err(_elapsed) => {
                    return Err(ManagerError::from(ApiError::LockTimeout {
                        value: "license validity".to_string(),
                        timeout: LICENSE_INFO_READ_LOCK_TIMEOUT,
                    }));
                }
            };

        if let Some(license_check) = &mut license_check {
            if let LicenseValidity::Exists(license_info) = &mut license_check.check_outcome {
                license_info.current += license_check.checked_at.elapsed();
            }
        }

        Ok(license_check)
    }
}
