use log::{debug, warn};
use rustls::crypto::CryptoProvider;
use std::collections::HashSet;
use std::sync::OnceLock;

mod auth;

pub mod api;
pub mod cluster_health;
pub mod common_error;
pub mod compiler;
pub mod config;
pub mod db;
pub mod error;
pub mod license;
pub mod logging;
pub mod runner;

/// Feature gate for new/unstable features that aren't rolled out or will change
/// substantially in the future.
static UNSTABLE_FEATURES: OnceLock<HashSet<&'static str>> = OnceLock::new();

/// Initialization function to set the platform's unstable feature gate.
pub fn platform_enable_unstable(requested_features: &str) {
    let all_features: HashSet<&'static str> = HashSet::from_iter(vec!["runtime_version"]);
    let mut enabled = HashSet::new();
    for requested_feature in requested_features.split(',') {
        if let Some(supported_feature) = all_features.get(requested_feature) {
            enabled.insert(*supported_feature);
        } else {
            warn!("Requested unstable feature '{requested_feature}' is not supported by the platform.");
        }
    }
    UNSTABLE_FEATURES
        .set(enabled)
        .expect("UNSTABLE_FEATURES already initialized");
}

pub fn unstable_features() -> Option<&'static HashSet<&'static str>> {
    UNSTABLE_FEATURES.get()
}

/// To query whether the platform enabled a certain unstable feature.
pub fn has_unstable_feature(feature: &str) -> bool {
    UNSTABLE_FEATURES
        .get()
        .map(|features| features.contains(feature))
        .unwrap_or(false)
}

/// Some dependencies of this crate use the `rustls` library. This library has two features
/// `ring` and `aws-lc-rs`. When both are enabled, the library requires a process-wide default
/// crypto provider to be configured. While no single dependency enables both these features,
/// Rust's feature unification may end up enabling both of them, depending on the exact rustls
/// dependency versions in use.
///
/// Bottom line: this function must be called in the `main` function in this crate, as well as
/// in all tests that exercise libraries that use `rustls` internally.
pub fn ensure_default_crypto_provider() {
    let _ = CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider());
}

/// Try to increase the file descriptor limit to avoid surprises when starting the manager on
/// many cores.
pub fn init_fd_limit() {
    match fdlimit::raise_fd_limit() {
        Ok(_) => {}
        Err(e) => {
            debug!("Failed to raise fd limit: {}", e);
        }
    }
}
