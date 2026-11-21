//! Finite two-dimensional points only. The Java adapter attests built-in method
//! identities; a SQL function with the same name does not enter this registry.

use crate::SemanticsProfile;
use crate::model::{Expression, Result, Value, unsupported};

pub(crate) fn registered(expression: &Expression) -> bool {
    matches!(
        (
            expression.builtin.as_deref(),
            expression.operator.as_deref()
        ),
        (Some("calcite-point-2d-v1"), Some("ST_POINT"))
            | (Some("calcite-point-distance-v1"), Some("ST_DISTANCE"))
    )
}

fn coordinate(value: &Value) -> Result<f64> {
    let number = match value {
        Value::Int(n) => *n as f64,
        Value::Float(n) => *n,
        Value::Decimal(n) => f64::from(*n),
        _ => return Err(unsupported("point coordinate type")),
    };
    if !number.is_finite() {
        return Err(unsupported("nonfinite point coordinate"));
    }
    Ok(number)
}

pub(crate) fn literal(text: &str) -> Result<Value> {
    let coordinates = text
        .strip_prefix("POINT (")
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| unsupported("geometry literal is not a 2D point"))?;
    let mut parts = coordinates.split_ascii_whitespace();
    let mut next = || {
        parts
            .next()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|n| n.is_finite())
            .ok_or_else(|| unsupported("point literal coordinate"))
    };
    let x = next()?;
    let y = next()?;
    if parts.next().is_some() {
        return Err(unsupported("point literal dimension"));
    }
    Ok(Value::Point(x, y))
}

pub(crate) fn evaluate(
    expression: &Expression,
    values: &[Value],
    profile: SemanticsProfile,
) -> Result<Value> {
    if values.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    match (expression.builtin.as_deref(), values) {
        (Some("calcite-point-2d-v1"), [x, y]) => Ok(Value::Point(coordinate(x)?, coordinate(y)?)),
        (Some("calcite-point-distance-v1"), [Value::Point(x1, y1), Value::Point(x2, y2)]) => {
            let dx = x1 - x2;
            let dy = y1 - y2;
            // JTS Coordinate.distance uses sqrt(dx*dx + dy*dy). geo's point
            // Euclidean distance uses hypot; overflow and last bits can differ.
            let distance = if profile == SemanticsProfile::LegacyJanino {
                (dx * dx + dy * dy).sqrt()
            } else {
                dx.hypot(dy)
            };
            Ok(Value::Float(crate::numeric::canonical_float(distance)))
        }
        _ => Err(unsupported("point function operands")),
    }
}
