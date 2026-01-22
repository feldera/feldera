use tracing::{info, warn};

fn log_rustls_fips_state(context: &str, enabled: bool, scope: &str) {
    if enabled {
        info!("Rustls {context}: FIPS is enabled for {scope}.");
    } else if cfg!(target_os = "linux") {
        info!("Rustls {context}: FIPS is disabled for {scope}.");
    } else {
        info!("Rustls {context}: FIPS is disabled for {scope} (non-Linux build).");
    }
}

pub fn log_rustls_fips_status(context: &str, config_fips: bool) {
    log_rustls_fips_state(context, config_fips, "connections");
}

pub fn log_rustls_provider_fips_status(context: &str, provider_fips: bool) {
    log_rustls_fips_state(context, provider_fips, "cryptography");
}
