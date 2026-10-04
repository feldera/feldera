//! Instance and session context shown in the persistent header.

use serde_json::Value;

use super::json;
use super::text::terminal_safe;

/// Facts about the connected Feldera instance from `/v0/config`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InstanceInfo {
    pub edition: String,
    pub version: String,
    pub revision: String,
    pub update_available: Option<String>,
    pub license_message: Option<String>,
}

impl InstanceInfo {
    pub fn from_json(configuration: &Value) -> Self {
        let update_available = configuration
            .get("update_info")
            .and_then(|info| json::string(info, "latest_version"))
            .map(|version| terminal_safe(&version));
        let license_message = configuration
            .get("license_validity")
            .and_then(license_summary);
        Self {
            edition: json::string(configuration, "edition")
                .map(|edition| terminal_safe(&edition))
                .unwrap_or_else(|| "Feldera".to_string()),
            version: json::string(configuration, "version")
                .map(|version| terminal_safe(&version))
                .unwrap_or_else(|| "unknown".to_string()),
            revision: json::string(configuration, "revision")
                .map(|revision| terminal_safe(&revision))
                .unwrap_or_default(),
            update_available,
            license_message,
        }
    }

    /// Short revision suitable for the header.
    pub fn short_revision(&self) -> &str {
        let limit = self.revision.len().min(9);
        &self.revision[..limit]
    }
}

fn license_summary(validity: &Value) -> Option<String> {
    // LicenseValidity is an enum object; surface expiring/expired states only.
    let license = validity.get("Exists").or(Some(validity))?;
    let expired = json::boolean(license, "is_expired");
    if expired {
        return Some("license expired".to_string());
    }
    let expires_at = json::string(license, "valid_until")?;
    json::boolean(license, "is_expiring_soon")
        .then(|| format!("license expires {}", terminal_safe(&expires_at)))
}

/// A tenant this principal may act in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantMembership {
    pub tenant_id: String,
    pub name: String,
    pub role: String,
}

/// The acting identity from `/v0/config/session`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Session {
    pub tenant: Option<String>,
    pub role: Option<String>,
    pub memberships: Vec<TenantMembership>,
}

impl Session {
    pub fn from_json(session: &Value) -> Self {
        let memberships = json::array(session, "memberships")
            .iter()
            .filter_map(|membership| {
                let name = json::string(membership, "name")?;
                Some(TenantMembership {
                    tenant_id: json::string(membership, "tenant_id")
                        .unwrap_or_else(|| name.clone()),
                    name: terminal_safe(&name),
                    role: json::string(membership, "role")
                        .map(|role| terminal_safe(&role))
                        .unwrap_or_else(|| "unknown".to_string()),
                })
            })
            .collect();
        Self {
            tenant: json::string(session, "tenant_name").map(|tenant| terminal_safe(&tenant)),
            role: json::string(session, "role").map(|role| terminal_safe(&role)),
            memberships,
        }
    }

    /// Label for the header's TENANT badge.
    pub fn tenant_label(&self) -> &str {
        self.tenant.as_deref().unwrap_or("default")
    }

    /// Label for the header's ROLE badge.
    pub fn role_label(&self) -> &str {
        self.role.as_deref().unwrap_or("full-access")
    }

    /// Whether the tenant picker is worth opening.
    pub fn has_selectable_tenants(&self) -> bool {
        self.memberships.len() > 1
    }
}

#[cfg(test)]
mod tests {
    use super::{InstanceInfo, Session};
    use serde_json::json;

    #[test]
    fn instance_info_reads_config_and_strips_controls() {
        let info = InstanceInfo::from_json(&json!({
            "edition": "Enterprise",
            "version": "0.338.0\u{1b}[2J",
            "revision": "0123456789abcdef",
            "update_info": {"latest_version": "0.339.0"}
        }));
        assert_eq!(info.edition, "Enterprise");
        assert_eq!(info.version, "0.338.0�[2J");
        assert_eq!(info.short_revision(), "012345678");
        assert_eq!(info.update_available.as_deref(), Some("0.339.0"));
        assert_eq!(info.license_message, None);
    }

    #[test]
    fn instance_info_defaults_when_config_is_foreign() {
        let info = InstanceInfo::from_json(&json!({"something": "else"}));
        assert_eq!(info.edition, "Feldera");
        assert_eq!(info.version, "unknown");
        assert_eq!(info.short_revision(), "");
    }

    #[test]
    fn license_states_surface_expiry_only() {
        let expired = InstanceInfo::from_json(&json!({
            "license_validity": {"Exists": {"is_expired": true}}
        }));
        assert_eq!(expired.license_message.as_deref(), Some("license expired"));

        let expiring = InstanceInfo::from_json(&json!({
            "license_validity": {"is_expiring_soon": true, "valid_until": "2026-09-01"}
        }));
        assert_eq!(
            expiring.license_message.as_deref(),
            Some("license expires 2026-09-01")
        );

        let healthy = InstanceInfo::from_json(&json!({
            "license_validity": {"is_expiring_soon": false, "valid_until": "2027-09-01"}
        }));
        assert_eq!(healthy.license_message, None);
    }

    #[test]
    fn session_reads_memberships_and_labels() {
        let session = Session::from_json(&json!({
            "tenant_name": "acme",
            "role": "admin",
            "memberships": [
                {"tenant_id": "t1", "name": "acme", "role": "admin"},
                {"tenant_id": "t2", "name": "beta", "role": "read"},
                {"role": "read"}
            ]
        }));
        assert_eq!(session.tenant_label(), "acme");
        assert_eq!(session.role_label(), "admin");
        assert_eq!(session.memberships.len(), 2);
        assert!(session.has_selectable_tenants());
    }

    #[test]
    fn anonymous_session_gets_friendly_labels() {
        let session = Session::from_json(&json!({"memberships": []}));
        assert_eq!(session.tenant_label(), "default");
        assert_eq!(session.role_label(), "full-access");
        assert!(!session.has_selectable_tenants());
    }
}
