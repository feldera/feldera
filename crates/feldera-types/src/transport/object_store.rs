//! Credential option aliases accepted by object_store 0.13, and by the
//! Unity catalog of delta-rs, which reads the same option map.
//! Keep these with the transport declarations, not in the generic redactor.

use std::collections::HashMap;

pub(crate) fn visit_credentials(
    options: &HashMap<String, String>,
    visit: &mut dyn FnMut(&str, &str),
) {
    for (name, value) in options {
        if let Some(canonical) = credential_key(&name.to_ascii_lowercase()) {
            visit(canonical, value);
        }
    }
}

fn credential_key(name: &str) -> Option<&'static str> {
    Some(match name {
        "aws_secret_access_key" | "secret_access_key" => "aws_secret_access_key",
        "aws_session_token" | "aws_token" | "session_token" => "aws_session_token",
        // `token` is accepted by both AWS and Azure.
        "token" => "token",
        "aws_sse_customer_key_base64" | "sse_customer_key_base64" => "aws_sse_customer_key_base64",
        "azure_storage_account_key"
        | "azure_storage_access_key"
        | "azure_storage_master_key"
        | "master_key"
        | "account_key"
        | "access_key" => "azure_storage_account_key",
        "azure_storage_client_secret" | "azure_client_secret" | "client_secret" => {
            "azure_storage_client_secret"
        }
        "azure_storage_sas_key" | "azure_storage_sas_token" | "sas_key" | "sas_token" => {
            "azure_storage_sas_key"
        }
        "azure_storage_token" => "azure_storage_token",
        // `bearer_token` is accepted by both Azure and Google.
        "bearer_token" => "bearer_token",
        "azure_fabric_session_token" | "fabric_session_token" => "azure_fabric_session_token",
        "azure_storage_encryption_key" | "encryption_key" => "azure_storage_encryption_key",
        "google_service_account_key" | "service_account_key" => "google_service_account_key",
        "google_bearer_token" => "google_bearer_token",
        "access_token" | "unity_access_token" | "databricks_access_token" | "databricks_token" => {
            "unity_access_token"
        }
        "unity_client_secret" | "databricks_client_secret" => "unity_client_secret",
        _ => return None,
    })
}
