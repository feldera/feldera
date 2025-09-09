use std::time::Duration;

use apache_avro::{schema::Name as AvroName, Schema as AvroSchema};
use feldera_adapterlib::catalog::AvroSchemaRefs;
use feldera_types::format::avro::AvroSchemaRegistryConfig;
use schema_registry_converter::blocking::schema_registry::SrSettings;

pub mod deserializer;
pub mod input;
pub mod output;
mod schema;
pub mod serializer;

#[cfg(test)]
mod test;

pub use deserializer::from_avro_value;

/// Convert schema registry config to `struct SrSettings` used by the
/// `schema_registry_converter` crate.
fn schema_registry_settings(
    config: &AvroSchemaRegistryConfig,
) -> Result<Option<SrSettings>, String> {
    if config.registry_urls.is_empty() {
        if config.registry_username.is_some() {
            return Err("'registry_username' requires 'registry_urls' to be set".to_string());
        }
        if config.registry_authorization_token.is_some() {
            return Err(
                "'registry_authorization_token' requires 'registry_urls' to be set".to_string(),
            );
        }
        if config.registry_proxy.is_some() {
            return Err("'registry_proxy' requires 'registry_urls' to be set".to_string());
        }
        if !config.registry_headers.is_empty() {
            return Err("'registry_headers' requires 'registry_urls' to be set".to_string());
        }
    }

    if config.registry_username.is_some() && config.registry_authorization_token.is_some() {
        return Err(
            "'registry_username' and 'registry_authorization_token' options are mutually exclusive"
                .to_string(),
        );
    }

    if config.registry_password.is_some() && config.registry_username.is_none() {
        return Err("'registry_password' option provided without 'registry_username'".to_string());
    }

    if !config.registry_urls.is_empty() {
        let mut sr_settings_builder = SrSettings::new_builder(config.registry_urls[0].clone());
        for url in &config.registry_urls[1..] {
            sr_settings_builder.add_url(url.clone());
        }
        if let Some(username) = &config.registry_username {
            sr_settings_builder
                .set_basic_authorization(username, config.registry_password.as_deref());
        }
        if let Some(token) = &config.registry_authorization_token {
            sr_settings_builder.set_token_authorization(token);
        }

        for (key, val) in config.registry_headers.iter() {
            sr_settings_builder.add_header(key.as_str(), val.as_str());
        }

        if let Some(proxy) = &config.registry_proxy {
            sr_settings_builder.set_proxy(proxy.as_str());
        }

        if let Some(timeout) = config.registry_timeout_secs {
            sr_settings_builder.set_timeout(Duration::from_secs(timeout));
        }

        Ok(Some(sr_settings_builder.build().map_err(|e| {
            format!("invalid schema registry configuration: {e}")
        })?))
    } else {
        Ok(None)
    }
}

fn resolve_ref<'a>(
    schema: &'a AvroSchema,
    refs: &'a AvroSchemaRefs,
) -> Result<&'a AvroSchema, AvroName> {
    match schema {
        AvroSchema::Ref { name } => {
            let name = name.fully_qualified_name(&schema.namespace());
            refs.get(&name).ok_or(name)
        }
        _ => Ok(schema),
    }
}
