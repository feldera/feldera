//! Validation of the runtime-oracle export, independent of files and engines.

use crate::{compare_circuits, node_id, ComparisonError, Graph, Namespace, NodeId, Result};
use serde_json::Value;
use std::collections::BTreeMap;

mod portable_values;
pub use portable_values::{portable_output_zset, PortableOutputZSet};

/// A case can contain several steps, an expected runtime error, or only a compile check.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CaseSummary {
    pub exported: bool,
    pub steps: usize,
    pub expected_error: bool,
}

fn require(condition: bool, path: &str, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(ComparisonError::new(path, message))
    }
}

fn identifier(value: &Value, path: &str) -> Result<()> {
    require(
        value.as_object().is_some_and(|object| object.len() == 2)
            && value["text"].is_string()
            && value["quoted"].is_boolean(),
        path,
        "expected an identifier with text and quoted fields",
    )
}

struct ZSet<'a> {
    graph: Graph<'a>,
    element_type: NodeId,
    collection_type: NodeId,
    records: Vec<(NodeId, i64)>,
}

fn read_zset(value: &Value) -> Result<ZSet<'_>> {
    require(
        value["class"] == "DBSPZSetExpression",
        "",
        "expected a typed Z-set",
    )?;
    let rows = value["data"]
        .as_array()
        .ok_or_else(|| ComparisonError::new("/data", "expected an array of rows"))?;
    let weights = value["weights"]
        .as_array()
        .ok_or_else(|| ComparisonError::new("/weights", "expected an array of weights"))?;
    require(
        rows.len() == weights.len(),
        "/weights",
        "expected exactly one weight per row",
    )?;
    let graph = Graph::read(value, Namespace::Inner)?;
    let resolve = |value: &Value, path: &str| {
        let id = node_id(value, Namespace::Inner, path)?
            .ok_or_else(|| ComparisonError::new(path, "expected an IR node"))?;
        graph
            .definitions
            .get(&id)
            .map(|node| (id, *node))
            .ok_or_else(|| ComparisonError::new(path, "undefined IR node"))
    };
    let (element_type, element) = resolve(&value["elementType"], "/elementType")?;
    require(
        element["class"]
            .as_str()
            .is_some_and(|name| name.starts_with("DBSPType")),
        "/elementType",
        "expected an element type",
    )?;
    let (collection_type, output) = resolve(&value["type"], "/type")?;
    require(
        output["class"] == "DBSPTypeZSet",
        "/type",
        "expected a Z-set output type",
    )?;
    let (output_element, _) = resolve(&output["elementType"], "/type/elementType")?;
    if output_element != element_type {
        require(
            graph.canonicalize(element_type)? == graph.canonicalize(output_element)?,
            "/type/elementType",
            "Z-set output type and declared element type disagree",
        )?;
    }
    let mut records = Vec::with_capacity(rows.len());
    for (index, (row, weight)) in rows.iter().zip(weights).enumerate() {
        let weight = weight.as_i64().ok_or_else(|| {
            ComparisonError::new(
                &format!("/weights/{index}"),
                "expected a signed 64-bit integer weight",
            )
        })?;
        let path = format!("/data/{index}");
        let (id, row) = resolve(row, &path)?;
        resolve(&row["type"], &format!("{path}/type"))?;
        records.push((id, weight));
    }
    Ok(ZSet {
        graph,
        element_type,
        collection_type,
        records,
    })
}

impl ZSet<'_> {
    fn weights_by_row(&self) -> Result<BTreeMap<Vec<u8>, i128>> {
        let mut weights = BTreeMap::new();
        for (id, weight) in &self.records {
            let row = self.graph.canonicalize(*id)?;
            let bytes = serde_json::to_vec(&row).map_err(|_| {
                ComparisonError::new("/data", "could not serialize a canonical row")
            })?;
            *weights.entry(bytes).or_default() += i128::from(*weight);
        }
        weights.retain(|_, weight| *weight != 0);
        require(
            weights
                .values()
                .all(|weight| i64::try_from(*weight).is_ok()),
            "/weights",
            "consolidated weight exceeds the signed 64-bit range",
        )?;
        Ok(weights)
    }
}

/// Compare two typed Z-set wire values. Row order and allocation IDs may differ.
/// Typed row graphs and values compare exactly, including decimal and float bits.
pub fn compare_zsets(expected: &Value, actual: &Value) -> Result<()> {
    let expected = read_zset(expected)?;
    let actual = read_zset(actual)?;
    require(
        expected.graph.canonicalize(expected.element_type)?
            == actual.graph.canonicalize(actual.element_type)?,
        "/elementType",
        "Z-set element types differ",
    )?;
    require(
        expected.graph.canonicalize(expected.collection_type)?
            == actual.graph.canonicalize(actual.collection_type)?,
        "/type",
        "Z-set output types differ",
    )?;
    require(
        expected.weights_by_row()? == actual.weights_by_row()?,
        "/data",
        "typed rows or consolidated weights differ",
    )
}

/// Check a version-1 case before using its expected values as an oracle.
/// This validates graph references, not SQL types or runtime semantics.
pub fn validate_case(case: &Value) -> Result<CaseSummary> {
    require(
        case["schema_version"].as_u64() == Some(1),
        "/schema_version",
        "expected corpus schema version 1",
    )?;
    for field in [
        "source_test",
        "producer_commit",
        "calcite_commit",
        "runtime_commit",
    ] {
        require(
            case[field].as_str().is_some_and(|s| !s.trim().is_empty()),
            &format!("/{field}"),
            "expected a nonempty string",
        )?;
    }
    require(
        case["case_index"].as_u64().is_some(),
        "/case_index",
        "expected a nonnegative case index",
    )?;
    for field in ["name", "program"] {
        require(
            case[field].is_string(),
            &format!("/{field}"),
            "expected a string",
        )?;
    }
    for field in ["options", "schema"] {
        require(
            case[field].is_object(),
            &format!("/{field}"),
            "expected an object",
        )?;
    }
    // Early v1 exports omitted these boundaries and cannot be recompiled. Keep
    // them readable as runtime-value evidence; replay tools require submissions.
    if let Some(submissions) = case.get("submissions") {
        let submissions = submissions
            .as_array()
            .ok_or_else(|| ComparisonError::new("/submissions", "expected an array"))?;
        let mut previous_batch = 0;
        for (index, submission) in submissions.iter().enumerate() {
            let path = format!("/submissions/{index}");
            require(
                submission.as_object().is_some_and(|s| s.len() == 3)
                    && submission["sql"].is_string()
                    && submission["many"].is_boolean(),
                &path,
                "expected sql, many, and batch fields",
            )?;
            let batch = submission["batch"].as_u64().ok_or_else(|| {
                ComparisonError::new(&format!("{path}/batch"), "expected a nonnegative batch")
            })?;
            require(
                batch >= previous_batch,
                &path,
                "submission batches are out of order",
            )?;
            previous_batch = batch;
        }
    }
    require(
        case["diagnostics"].is_array(),
        "/diagnostics",
        "expected an array",
    )?;
    require(
        case["runtime_verified"] == false,
        "/runtime_verified",
        "export does not certify runtime execution",
    )?;
    let mode = case["engine_mode"].as_str();
    require(
        matches!(mode, Some("classic" | "gen2")),
        "/engine_mode",
        "unknown engine mode",
    )?;
    require(
        case.pointer("/options/ioOptions/gen2")
            .and_then(Value::as_bool)
            == Some(mode == Some("gen2")),
        "/options/ioOptions/gen2",
        "engine mode disagrees with options",
    )?;
    if case["status"] == "skipped" {
        require(
            case["reason"]
                .as_str()
                .is_some_and(|s| !s.trim().is_empty()),
            "/reason",
            "skipped case requires a reason",
        )?;
        require(
            case.get("circuit").is_none() && case.get("commands").is_none(),
            "",
            "skipped case must not contain executable artifacts",
        )?;
        return Ok(CaseSummary::default());
    }
    require(
        case["status"] == "exported",
        "/status",
        "unknown case status",
    )?;
    let incremental = case
        .pointer("/options/languageOptions/incrementalize")
        .and_then(Value::as_bool);
    require(
        incremental.is_some(),
        "/options/languageOptions/incrementalize",
        "missing incremental mode",
    )?;
    require(
        case["step_semantics"]
            == if incremental == Some(true) {
                "delta"
            } else {
                "whole_relation"
            },
        "/step_semantics",
        "step semantics disagree with original test options",
    )?;
    if let Some(effective) = case.get("compilation_options") {
        let effective_incremental = effective
            .pointer("/languageOptions/incrementalize")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                ComparisonError::new(
                    "/compilation_options/languageOptions/incrementalize",
                    "missing effective incremental mode",
                )
            })?;
        require(
            case.pointer("/circuit/incremental")
                .and_then(Value::as_bool)
                == Some(effective_incremental),
            "/compilation_options/languageOptions/incrementalize",
            "effective mode disagrees with the exported circuit",
        )?;
        let mut expected_options = case["options"].clone();
        expected_options["languageOptions"]["incrementalize"] = Value::Bool(effective_incremental);
        require(
            effective == &expected_options,
            "/compilation_options",
            "only incremental mode may differ from the original test options",
        )?;
    }
    require(
        case.get("expected_runtime_error")
            .is_some_and(|v| v.is_null() || v.is_string()),
        "/expected_runtime_error",
        "expected null or an error string",
    )?;
    for field in ["input_order", "output_order"] {
        require(
            case[field]
                .as_array()
                .is_some_and(|a| a.iter().all(Value::is_string)),
            &format!("/{field}"),
            "expected an array of table names",
        )?;
    }
    for field in ["circuit_inputs", "circuit_outputs"] {
        let values = case[field]
            .as_array()
            .ok_or_else(|| ComparisonError::new(&format!("/{field}"), "expected an array"))?;
        for (index, value) in values.iter().enumerate() {
            let path = format!("/{field}/{index}");
            if field == "circuit_outputs" {
                identifier(&value["name"], &format!("{path}/name"))?;
                require(
                    value["system"].is_boolean(),
                    &format!("{path}/system"),
                    "expected a boolean",
                )?;
            } else {
                identifier(value, &path)?;
            }
        }
    }
    compare_circuits(&case["circuit"], &case["circuit"]).map_err(|mut error| {
        error.path = format!("/circuit{}", error.path);
        error
    })?;
    let commands = case["commands"]
        .as_array()
        .ok_or_else(|| ComparisonError::new("/commands", "expected an array"))?;
    let mut summary = CaseSummary {
        exported: true,
        steps: 0,
        expected_error: case["expected_runtime_error"].is_string(),
    };
    for (index, command) in commands.iter().enumerate() {
        let path = format!("/commands/{index}");
        if command["kind"] == "compact" {
            require(
                command.as_object().is_some_and(|o| o.len() == 1),
                &path,
                "compact command has extra fields",
            )?;
            continue;
        }
        require(command["kind"] == "step", &path, "unknown stream command")?;
        summary.steps += 1;
        for field in ["inputs", "outputs"] {
            let sets = command[field].as_array().ok_or_else(|| {
                ComparisonError::new(&format!("{path}/{field}"), "expected an array")
            })?;
            for (index, set) in sets.iter().enumerate() {
                let path = format!("{path}/{field}/{index}");
                identifier(&set["name"], &format!("{path}/name"))?;
                require(
                    set["primary_keys"]
                        .as_array()
                        .is_some_and(|a| a.iter().all(|v| v.as_u64().is_some())),
                    &format!("{path}/primary_keys"),
                    "expected an array of column indices",
                )?;
                read_zset(&set["zset_ir"]).map_err(|mut error| {
                    error.path = format!("{path}/zset_ir{}", error.path);
                    error
                })?;
            }
        }
    }
    Ok(summary)
}
