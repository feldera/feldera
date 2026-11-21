//! Disposable P0 experiment in compiler-local connector decoding. This is not
//! runtime configuration validation and does not connect to external services.

mod decode;
mod duration;
mod locations;
mod source;
mod validate;

use decode::Inventory;
use locations::{Locations, Position};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub id: String,
    pub config_class: String,
    pub json: String,
    pub pointer: String,
    #[serde(default)]
    pub source: Option<source::Source>,
}

#[derive(Serialize)]
pub struct Response {
    pub id: String,
    pub compiler_bug: bool,
    pub warnings: Vec<Warning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted: Option<bool>,
}

#[derive(Serialize)]
pub struct Warning {
    pub warning: bool,
    pub continuation: bool,
    pub error_type: &'static str,
    pub message: String,
    pub position: Position,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
}

fn warning(message: String, position: Position) -> Warning {
    Warning {
        warning: true,
        continuation: false,
        error_type: "Invalid configuration",
        message,
        position,
        snippet: None,
    }
}

pub struct Validator(Inventory);

impl Validator {
    pub fn new() -> Result<Self, String> {
        serde_json::from_str(include_str!("../java-inventory.json"))
            .map(Self)
            .map_err(|e| e.to_string())
    }

    pub fn validate(&self, request: Request) -> Result<Response, String> {
        if request.id.is_empty() {
            return Err("empty probe identity".into());
        }
        if request.json.len() > 1024 * 1024 {
            return Err("configuration exceeds 1 MiB prototype limit".into());
        }
        if request.source.as_ref().is_some_and(|source| {
            source.line == 0
                || source.column == 0
                || source.line > 1_000_000
                || source.column > 1_000_000
                || source.program.len() > 2 * 1024 * 1024
        }) {
            return Err("invalid or oversized SQL source origin".into());
        }
        let schema = self
            .0
            .classes
            .iter()
            .find(|schema| schema.prototype && schema.class == request.config_class)
            .ok_or("unsupported configuration class")?;
        let document: Value = serde_json::from_str(&request.json).map_err(|e| e.to_string())?;
        let config = document
            .pointer(&request.pointer)
            .and_then(Value::as_object)
            .ok_or("pointer must select a configuration object")?;
        let locations = Locations::parse(&request.json)?;
        let mut warnings = match schema.decode(config) {
            Err(error) => {
                let position = locations
                    .key(&format!("{}/{}", request.pointer, error.path))
                    .or_else(|| locations.value(&request.pointer))
                    .unwrap_or_default();
                vec![warning(error.message, position)]
            }
            Ok(decoded) => validate::validate(&request.config_class, &decoded, config)
                .into_iter()
                .map(|diagnostic| {
                    let position = match diagnostic.field {
                        Some(field) => locations.key(&format!("{}/{field}", request.pointer)),
                        None => locations.value(&request.pointer),
                    }
                    .unwrap_or_default();
                    warning(diagnostic.message, position)
                })
                .collect(),
        };
        let accepted = request.source.map(|source| source.apply(&mut warnings));
        Ok(Response {
            id: request.id,
            compiler_bug: false,
            warnings,
            accepted,
        })
    }
}
