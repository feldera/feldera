//! Calls the actual nullable runtime point constructors and distance function.

use feldera_sqllib::*;
use serde_json::{Value, json};

pub(super) fn distance(input: &str) -> Result<Value, String> {
    let coordinates: [Option<String>; 4] =
        serde_json::from_str(input).map_err(|e| e.to_string())?;
    let numbers =
        coordinates.map(|value| value.map(|s| s.parse::<f64>().map(F64::from)).transpose());
    let [x1, y1, x2, y2] = numbers;
    let a = make_geopointNN(
        x1.map_err(|e| e.to_string())?,
        y1.map_err(|e| e.to_string())?,
    );
    let b = make_geopointNN(
        x2.map_err(|e| e.to_string())?,
        y2.map_err(|e| e.to_string())?,
    );
    Ok(st_distance_geopointN_geopointN(a, b).map_or_else(
        || json!({"type":"null"}),
        |n| json!({"type":"float", "value":format!("{:016x}", n.into_inner().to_bits())}),
    ))
}
