//! Semantic checks run only after the complete configuration decodes.
//! Keep their order aligned with the Java validators: it is observable in C0.

use crate::{decode::blank, duration};
use serde_json::{Map, Value};

pub(crate) struct Diagnostic {
    /// None points at the configuration value; Some points at a field key.
    pub field: Option<String>,
    pub message: String,
}

fn warn(out: &mut Vec<Diagnostic>, field: Option<&str>, message: impl Into<String>) {
    out.push(Diagnostic {
        field: field.map(str::to_owned),
        message: message.into(),
    });
}

fn duration(config: &Map<String, Value>, out: &mut Vec<Diagnostic>, current: &str, legacy: &str) {
    // The decoder retains defaults, so field presence is supplied separately by
    // the caller for alias conflicts, including two explicitly written nulls.
    if !config[current].is_null() && !duration::valid(&config[current]) {
        warn(out, Some(current), format!("{current:?}: invalid duration"));
    } else if config[current].is_null()
        && !config[legacy].is_null()
        && !duration::valid(&config[legacy])
    {
        warn(out, Some(legacy), format!("{legacy:?}: invalid duration"));
    }
}

pub(crate) fn validate(
    name: &str,
    config: &Map<String, Value>,
    supplied: &Map<String, Value>,
) -> Vec<Diagnostic> {
    let mut out = Vec::new();
    let duration_fields = match name {
        "KafkaInputConfig" => Some(("group_join_timeout", "group_join_timeout_secs")),
        "AvroParserConfig" | "AvroEncoderConfig" => {
            Some(("registry_timeout", "registry_timeout_secs"))
        }
        _ => None,
    };
    if let Some((current, legacy)) = duration_fields {
        if supplied.contains_key(current) && supplied.contains_key(legacy) {
            warn(
                &mut out,
                Some(legacy),
                format!("{legacy:?} is another spelling of {current:?}; write one of the two"),
            );
        } else {
            duration(config, &mut out, current, legacy);
        }
    }
    match name {
        "PostgresReaderConfig" => {
            // The Java validator short-circuits after the first missing field.
            for field in ["uri", "query"] {
                if blank(&config[field]) {
                    warn(
                        &mut out,
                        Some(field),
                        format!("required field {field:?} is missing or empty"),
                    );
                    break;
                }
            }
        }
        "KafkaInputConfig" => {
            let count = config["topics"].as_array().map_or(0, Vec::len);
            if count == 0 && blank(&config["topic"]) {
                warn(&mut out, None, "required field \"topic\" is missing or empty (or use the legacy \"topics\" form)");
            } else if count > 0 && !blank(&config["topic"]) {
                warn(
                    &mut out,
                    Some("topics"),
                    "\"topic\" and the legacy \"topics\" are mutually exclusive",
                );
            } else if count > 1 {
                warn(
                    &mut out,
                    Some("topics"),
                    format!("the legacy \"topics\" accepts exactly one topic, not {count}"),
                );
            }
        }
        "AvroParserConfig" | "AvroEncoderConfig" => {
            if name == "AvroParserConfig" {
                if config["registry_urls"].is_null() {
                    warn(
                        &mut out,
                        Some("registry_urls"),
                        "\"registry_urls\" cannot be null; remove it to use the default",
                    );
                } else if !config["schema"].is_null()
                    && config["registry_urls"]
                        .as_array()
                        .is_some_and(|v| !v.is_empty())
                {
                    warn(
                        &mut out,
                        Some("schema"),
                        "\"schema\" and \"registry_urls\" are mutually exclusive",
                    );
                }
            } else if !config["cdc_field"].is_null() && config["update_format"] != "raw" {
                warn(
                    &mut out,
                    Some("cdc_field"),
                    "\"cdc_field\" is only valid with \"update_format\": \"raw\"",
                );
            }
            if !config["registry_authorization_token"].is_null()
                && (!config["registry_username"].is_null()
                    || !config["registry_password"].is_null())
            {
                warn(&mut out, Some("registry_authorization_token"), "\"registry_authorization_token\" is mutually exclusive with \"registry_username\" and \"registry_password\"");
            }
        }
        "IcebergReaderConfig" => iceberg(config, &mut out),
        "HttpOutputConfig" => {}
        _ => unreachable!("only the six supported schemas reach validation"),
    }
    out
}

fn iceberg(config: &Map<String, Value>, out: &mut Vec<Diagnostic>) {
    if config["mode"].is_null() {
        warn(out, None, "required field \"mode\" is missing");
        return;
    }
    if !config["snapshot_id"].is_null() && !config["datetime"].is_null() {
        warn(
            out,
            Some("snapshot_id"),
            "\"snapshot_id\" and \"datetime\" are mutually exclusive",
        );
    }
    let catalog = config["catalog_type"].as_str();
    if catalog.is_none() && config["metadata_location"].is_null() {
        warn(out, None, "missing metadata location: specify an Iceberg catalog via \"catalog_type\" or provide a table metadata location via \"metadata_location\"");
    } else if catalog.is_some() && !config["metadata_location"].is_null() {
        warn(
            out,
            Some("metadata_location"),
            "\"metadata_location\" is not supported when \"catalog_type\" is set",
        );
    }
    if catalog.is_none() && !config["table_name"].is_null() {
        warn(
            out,
            Some("table_name"),
            "\"table_name\" is only valid when \"catalog_type\" is set",
        );
    } else if catalog.is_some() && config["table_name"].is_null() {
        warn(
            out,
            None,
            "\"table_name\" must be specified when \"catalog_type\" is set",
        );
    }
    for (kind, fields, missing) in [
        ("glue", "warehouse endpoint access-key-id secret-access-key profile-name region session-token id",
         "missing Iceberg warehouse location—set \"glue.warehouse\" when using \"catalog_type\": \"glue\""),
        ("rest", "uri warehouse oauth2-server-uri credential token scope prefix headers audience resource",
         "missing Iceberg REST catalog URI—set \"rest.uri\" when using \"catalog_type\": \"rest\""),
        ("s3tables", "table-bucket-arn endpoint access-key-id secret-access-key session-token profile-name region",
         "missing S3 table bucket ARN—set \"s3tables.table-bucket-arn\" when using \"catalog_type\": \"s3tables\""),
    ] {
        for (i, field) in fields.split_whitespace().enumerate() {
            let field = format!("{kind}.{field}");
            if catalog == Some(kind) {
                if i == 0 && config[&field].is_null() { warn(out, None, missing); }
            } else if !config[&field].is_null() {
                warn(out, Some(&field), format!("{field:?} is only valid when \"catalog_type\": {kind:?}"));
            }
        }
    }
}
