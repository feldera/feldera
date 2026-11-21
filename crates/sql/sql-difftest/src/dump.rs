//! The internal observation envelope. This is not the public Gen-2 wire format.

use crate::{
    canonical_plain, compare_circuits, pointer, CanonicalValue, Canonicalizer, ComparisonError,
    Namespace, Result,
};
use serde_json::Value;
use std::collections::BTreeMap;

fn fields(value: &Value, path: &str, names: &[&str]) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| ComparisonError::new(path, "expected an object"))?;
    for name in names {
        if !object.contains_key(*name) {
            return Err(ComparisonError::new(&pointer(path, name), "missing field"));
        }
    }
    if let Some(name) = object.keys().find(|name| !names.contains(&name.as_str())) {
        return Err(ComparisonError::new(&pointer(path, name), "unknown field"));
    }
    Ok(())
}

fn text<'a>(value: &'a Value, path: &str) -> Result<&'a str> {
    value
        .as_str()
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| ComparisonError::new(path, "expected a nonempty string"))
}

fn integer(value: &Value, path: &str) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| ComparisonError::new(path, "expected a nonnegative integer"))
}

fn validate(dump: &Value) -> Result<()> {
    // Check the version before interpreting any version-dependent fields.
    if dump.get("schema_version").and_then(Value::as_u64) != Some(1) {
        return Err(ComparisonError::new(
            "/schema_version",
            "unsupported or missing observation schema version; expected 1",
        ));
    }
    fields(
        dump,
        "",
        &[
            "schema_version",
            "producer_commit",
            "runtime_abi",
            "engine_mode",
            "options",
            "observation",
            "replayable",
            "circuit",
        ],
    )?;
    text(&dump["producer_commit"], "/producer_commit")?;
    text(&dump["runtime_abi"], "/runtime_abi")?;
    if !matches!(dump["engine_mode"].as_str(), Some("classic" | "gen2")) {
        return Err(ComparisonError::new("/engine_mode", "unknown engine mode"));
    }
    if !dump["options"].is_object() {
        return Err(ComparisonError::new("/options", "expected an object"));
    }
    if dump["replayable"] != false {
        return Err(ComparisonError::new(
            "/replayable",
            "version 1 supports observations only; expected false",
        ));
    }
    let observation = &dump["observation"];
    fields(observation, "/observation", &["stage", "sequence", "path"])?;
    let sequence = integer(&observation["sequence"], "/observation/sequence")?;
    let path = observation["path"].as_array().ok_or_else(|| {
        ComparisonError::new("/observation/path", "expected an array of pass frames")
    })?;
    match observation["stage"].as_str() {
        Some("S2") if path.is_empty() && sequence == 0 => {}
        Some("S3") if path.is_empty() => {}
        Some("S4") if !path.is_empty() && sequence > 0 => {}
        _ => {
            return Err(ComparisonError::new(
                "/observation",
                "invalid stage, sequence, or pass path",
            ));
        }
    }
    for (index, frame) in path.iter().enumerate() {
        let location = format!("/observation/path/{index}");
        fields(
            frame,
            &location,
            &["container", "pass", "index", "iteration"],
        )?;
        for name in ["container", "pass"] {
            text(&frame[name], &pointer(&location, name))?;
        }
        for name in ["index", "iteration"] {
            integer(&frame[name], &pointer(&location, name))?;
        }
    }
    Ok(())
}

/// Compare versioned observations after checking their compatibility.
///
/// Both headers must conform to schema version 1. ABI, engine mode, compiler
/// options, and observation identity must match. Producer commits are required
/// provenance but may differ, allowing comparisons across compiler revisions.
/// Circuit ids are compared using [`compare_circuits`]. Version 1 observations
/// cannot be used as checkpoints: they omit compiler and pass side state.
pub fn compare_dumps(expected: &Value, actual: &Value) -> Result<()> {
    compare_observations(expected, actual, false)
}

/// Compare final S3 observations from different entry points. Their local capture
/// sequence numbers may differ; all other headers and circuit contents must match.
pub fn compare_final_s3(expected: &Value, actual: &Value) -> Result<()> {
    compare_observations(expected, actual, true)
}

fn compare_observations(expected: &Value, actual: &Value, final_s3: bool) -> Result<()> {
    validate(expected)?;
    validate(actual)?;
    if final_s3
        && (expected["observation"]["stage"] != "S3" || actual["observation"]["stage"] != "S3")
    {
        return Err(ComparisonError::new(
            "/observation/stage",
            "expected final S3 observations",
        ));
    }
    for field in ["runtime_abi", "engine_mode", "options", "observation"] {
        if final_s3 && field == "observation" {
            continue;
        }
        if expected[field] != actual[field] {
            return Err(ComparisonError::new(
                &pointer("", field),
                "observation headers differ",
            ));
        }
    }
    compare_circuits(&expected["circuit"], &actual["circuit"]).map_err(|mut error| {
        error.path.insert_str(0, "/circuit");
        error
    })
}

/// Encode exactly the fields checked by [`compare_dumps`] for streaming fingerprints.
///
/// This opaque, versioned comparison representation is not replayable circuit IR.
/// Reference tags stay distinct from user JSON. Callers may hash these bytes but
/// must retain run completion/counts and regenerate raw dumps to diagnose differences.
pub fn canonical_dump_bytes(dump: &Value) -> Result<Vec<u8>> {
    validate(dump)?;
    let mut fields = BTreeMap::new();
    for field in ["runtime_abi", "engine_mode", "options", "observation"] {
        fields.insert(field.to_string(), canonical_plain(&dump[field], 0)?);
    }
    fields.insert(
        "circuit".into(),
        Canonicalizer::run(&dump["circuit"], Namespace::Outer)?,
    );
    serde_json::to_vec(&(
        "feldera-canonical-observation-v1",
        CanonicalValue::Object(fields),
    ))
    .map_err(|error| {
        ComparisonError::new("", format!("cannot encode canonical observation: {error}"))
    })
}
