//! Portable expected output values from the version-1 Java corpus.

use super::{read_zset, require};
use crate::{node_id, ComparisonError, Graph, Namespace, NodeId, Result};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

const MAX_DEPTH: usize = 128;
const MAX_EXPANDED_NODES: usize = 1_000_000;

#[derive(Serialize)]
pub struct PortableOutputZSet {
    schema_version: u8,
    row_type: Value,
    rows: Vec<WeightedRow>,
}

impl PortableOutputZSet {
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

#[derive(Serialize)]
struct WeightedRow {
    row: Value,
    weight: String,
}

struct Values<'a> {
    graph: Graph<'a>,
    visited: usize,
}

impl<'a> Values<'a> {
    fn visit(&mut self, depth: usize) -> Result<()> {
        self.visited += 1;
        require(
            depth <= MAX_DEPTH && self.visited <= MAX_EXPANDED_NODES,
            "",
            "portable value exceeds the expansion limit",
        )
    }

    fn resolve(&self, reference: &Value) -> Result<&'a Value> {
        let id = node_id(reference, Namespace::Inner, "")?
            .ok_or_else(|| invalid("expected a typed value node"))?;
        self.definition(id)
    }

    fn definition(&self, id: NodeId) -> Result<&'a Value> {
        self.graph
            .definitions
            .get(&id)
            .copied()
            .ok_or_else(|| invalid("undefined typed value node"))
    }

    fn types(&mut self, references: &Value, depth: usize) -> Result<Vec<Value>> {
        array(references)?
            .iter()
            .map(|reference| self.sql_type(self.resolve(reference)?, depth))
            .collect()
    }

    fn sql_type(&mut self, node: &Value, depth: usize) -> Result<Value> {
        self.visit(depth)?;
        let nullable = node["mayBeNull"]
            .as_bool()
            .ok_or_else(|| invalid("expected SQL type nullability"))?;
        let kind = match node["class"].as_str().unwrap_or_default() {
            "DBSPTypeNull" => "null",
            "DBSPTypeBool" => "boolean",
            "DBSPTypeInteger" => "integer",
            "DBSPTypeDecimal" => "decimal",
            "DBSPTypeReal" => "float32",
            "DBSPTypeDouble" => "float64",
            "DBSPTypeString" => "string",
            "DBSPTypeBinary" => "binary",
            "DBSPTypeDate" => "date",
            "DBSPTypeTime" => "time",
            "DBSPTypeTimestamp" => "timestamp",
            "DBSPTypeTimestampTz" => "timestamp_tz",
            "DBSPTypeShortInterval" => "day_time_interval",
            "DBSPTypeLongInterval" => "year_month_interval",
            "DBSPTypeUuid" => "uuid",
            "DBSPTypeGeoPoint" => "geo_point",
            "DBSPTypeVariant" => "variant",
            "DBSPTypeTuple" => "tuple",
            "DBSPTypeArray" => "array",
            "DBSPTypeMap" => "map",
            _ => return Err(invalid("unrecognized portable SQL type")),
        };
        let mut result = json!({"kind": kind, "nullable": nullable});
        match kind {
            "integer" => {
                result["width"] = json!(integer(&node["width"])?);
                result["signed"] = json!(boolean(&node["signed"])?);
            }
            "decimal" => {
                result["precision"] = json!(integer(&node["precision"])?);
                result["scale"] = json!(integer(&node["scale"])?);
            }
            "string" | "binary" => {
                result["precision"] = json!(integer(&node["precision"])?);
                result["fixed"] = json!(boolean(&node["fixed"])?);
            }
            "day_time_interval" | "year_month_interval" => {
                result["units"] = json!(string(&node["units"])?);
            }
            "tuple" => result["fields"] = json!(self.types(&node["tupFields"], depth + 1)?),
            "array" | "map" => {
                let args = self.types(&node["typeArgs"], depth + 1)?;
                match (kind, args.as_slice()) {
                    ("array", [element]) => result["element"] = element.clone(),
                    ("map", [key, value]) => {
                        result["key"] = key.clone();
                        result["value"] = value.clone();
                    }
                    _ => return Err(invalid("unexpected composite SQL type arguments")),
                }
            }
            _ => {}
        }
        Ok(result)
    }

    fn members(&mut self, values: &[Value], depth: usize) -> Result<Vec<Value>> {
        values
            .iter()
            .map(|node| self.value(self.resolve(node)?, depth))
            .collect()
    }

    fn value(&mut self, node: &Value, depth: usize) -> Result<Value> {
        self.visit(depth)?;
        let class = node["class"].as_str().unwrap_or_default();
        match class {
            "DBSPNullLiteral" => Ok(Value::Null),
            "DBSPTupleExpression" => self.collection_value(node, "fields", depth),
            "DBSPArrayExpression" => self.collection_value(node, "data", depth),
            "DBSPMapExpression" => self.map_value(node, depth),
            "DBSPSomeExpression" => self.value(self.resolve(&node["expression"])?, depth + 1),
            "DBSPGeoPointConstructor" => Ok(json!([
                self.value(self.resolve(&node["left"])?, depth + 1)?,
                self.value(self.resolve(&node["right"])?, depth + 1)?
            ])),
            "DBSPVariantNullLiteral" => Ok(json!({"kind": "json_null"})),
            "DBSPVariantExpression" => self.variant_value(node, depth),
            _ => scalar(class, node.get("value")),
        }
    }

    fn collection_value(&mut self, node: &Value, field: &str, depth: usize) -> Result<Value> {
        match node.get(field) {
            Some(values) => Ok(json!(self.members(array(values)?, depth + 1)?)),
            None => Ok(Value::Null),
        }
    }

    fn map_value(&mut self, node: &Value, depth: usize) -> Result<Value> {
        match (node.get("keys"), node.get("values")) {
            (None, None) => Ok(Value::Null),
            (Some(keys), Some(values)) => {
                let keys = self.members(array(keys)?, depth + 1)?;
                let values = self.members(array(values)?, depth + 1)?;
                require(
                    keys.len() == values.len(),
                    "",
                    "map keys and values disagree",
                )?;
                let mut entries = BTreeMap::new();
                for (key, value) in keys.into_iter().zip(values) {
                    let previous = entries.insert(canonical_bytes(&key)?, [key, value]);
                    require(previous.is_none(), "", "portable map has a duplicate key")?;
                }
                Ok(json!(entries.into_values().collect::<Vec<_>>()))
            }
            _ => Err(invalid("map requires both keys and values")),
        }
    }

    fn variant_value(&mut self, node: &Value, depth: usize) -> Result<Value> {
        match (boolean(&node["isSqlNull"])?, node.get("value")) {
            (true, None) => Ok(json!({"kind": "sql_null"})),
            (false, None) => Ok(Value::Null),
            (false, Some(value)) => {
                let value = self.resolve(value)?;
                let sql_type = self.sql_type(self.resolve(&value["type"])?, depth + 1)?;
                let value = self.value(value, depth + 1)?;
                Ok(json!({"kind": "value", "type": sql_type, "value": value}))
            }
            (true, Some(_)) => Err(invalid("SQL null variant cannot contain another value")),
        }
    }
}

fn invalid(message: impl Into<String>) -> ComparisonError {
    ComparisonError::new("", message)
}

fn array(value: &Value) -> Result<&[Value]> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| invalid("expected a value array"))
}

fn integer(value: &Value) -> Result<i64> {
    value.as_i64().ok_or_else(|| invalid("expected an integer"))
}

fn boolean(value: &Value) -> Result<bool> {
    value.as_bool().ok_or_else(|| invalid("expected a boolean"))
}

fn string(value: &Value) -> Result<&str> {
    value.as_str().ok_or_else(|| invalid("expected a string"))
}

fn canonical_bytes(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| invalid("cannot serialize portable value"))
}

fn decimal(text: &str) -> Result<Value> {
    let malformed = || invalid("invalid exact decimal literal");
    let (mantissa, exponent) = match text.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>().map_err(|_| malformed())?),
        None => (text, 0),
    };
    let unsigned = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let digits = format!("{whole}{fraction}");
    if digits.is_empty() || !digits.bytes().all(|digit| digit.is_ascii_digit()) {
        return Err(malformed());
    }
    let coefficient = digits.trim_start_matches('0').trim_end_matches('0');
    if coefficient.is_empty() {
        return Ok(json!({"coefficient": "0", "exponent": 0}));
    }
    let exponent = exponent
        .checked_add((digits.len() - digits.trim_end_matches('0').len()) as i64)
        .and_then(|exponent| exponent.checked_sub(fraction.len() as i64))
        .ok_or_else(malformed)?;
    let sign = if mantissa.starts_with('-') { "-" } else { "" };
    Ok(json!({"coefficient": format!("{sign}{coefficient}"), "exponent": exponent}))
}

fn scalar(class: &str, value: Option<&Value>) -> Result<Value> {
    let read: fn(&Value) -> Result<Value> = match class {
        "DBSPBoolLiteral" => |v| Ok(json!(boolean(v)?)),
        "DBSPDateLiteral" => |v| Ok(json!(integer(v)?)),
        "DBSPRealLiteral" => |v| {
            let bits = i32::try_from(integer(v)?).map_err(|_| invalid("invalid float32 bits"))?;
            Ok(json!(format!("{:08x}", bits as u32)))
        },
        "DBSPDoubleLiteral" => |v| Ok(json!(format!("{:016x}", integer(v)? as u64))),
        "DBSPDecimalLiteral" => |v| decimal(string(v)?),
        "DBSPI8Literal"
        | "DBSPI16Literal"
        | "DBSPI32Literal"
        | "DBSPI64Literal"
        | "DBSPShortIntervalLiteral"
        | "DBSPLongIntervalLiteral" => |v| {
            let value = string(v)?
                .parse::<i64>()
                .map_err(|_| invalid("invalid signed literal"))?;
            Ok(json!(value.to_string()))
        },
        "DBSPU8Literal" | "DBSPU16Literal" | "DBSPU32Literal" | "DBSPU64Literal" => |v| {
            let value = string(v)?
                .parse::<u64>()
                .map_err(|_| invalid("invalid unsigned literal"))?;
            Ok(json!(value.to_string()))
        },
        "DBSPStringLiteral"
        | "DBSPBinaryLiteral"
        | "DBSPUuidLiteral"
        | "DBSPTimestampLiteral"
        | "DBSPTimestampTzLiteral" => |v| Ok(json!(string(v)?)),
        "DBSPTimeLiteral" => |v| {
            let time = string(v)?;
            Ok(json!(time.strip_suffix('.').unwrap_or(time)))
        },
        _ => return Err(invalid("expected a literal runtime output value")),
    };
    value.map_or(Ok(Value::Null), read)
}

/// Encode a typed output bag without Java IR class names or allocation identities.
///
/// Input expressions that require evaluation are rejected. Output scalars use
/// lossless JSON encodings, including hexadecimal IEEE bits and exact decimals.
pub fn portable_output_zset(value: &Value) -> Result<PortableOutputZSet> {
    let zset = read_zset(value)?;
    let mut decoder = Values {
        graph: zset.graph,
        visited: 0,
    };
    let row_type = decoder.sql_type(decoder.definition(zset.element_type)?, 0)?;
    let mut rows: BTreeMap<Vec<u8>, (Value, i128)> = BTreeMap::new();
    for (id, weight) in zset.records {
        let row = decoder.definition(id)?;
        let row_value_type = decoder.sql_type(decoder.resolve(&row["type"])?, 0)?;
        require(
            row_type == row_value_type,
            "",
            "row and bag SQL types disagree",
        )?;
        let value = decoder.value(row, 0)?;
        rows.entry(canonical_bytes(&value)?).or_insert((value, 0)).1 += i128::from(weight);
    }
    let rows: Vec<_> = rows
        .into_values()
        .filter(|(_, weight)| *weight != 0)
        .map(|(row, weight)| {
            let weight = i64::try_from(weight)
                .map_err(|_| invalid("consolidated weight exceeds the signed 64-bit range"))?;
            Ok(WeightedRow {
                row,
                weight: weight.to_string(),
            })
        })
        .collect::<Result<_>>()?;
    Ok(PortableOutputZSet {
        schema_version: 1,
        row_type,
        rows,
    })
}
