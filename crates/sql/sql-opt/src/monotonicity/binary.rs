//! Select a lower bound for binary scalar and temporal expressions.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Operand {
    pub monotone: bool,
    pub constant: bool,
    pub positive_literal: bool,
    pub never_null: bool,
}

#[derive(Clone, Copy, Deserialize)]
pub enum Domain {
    Scalar,
    Temporal,
    TimeOfDay,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub enum BinaryWaterline {
    None,
    Left,
    Right,
    Both,
    /// Apply the operation to both bounds, inheriting this operand's monotonicity.
    InheritLeft,
    InheritRight,
    /// Take the minimum after casting both bounds to the result type.
    Coalesce,
}

pub fn binary_waterline(
    domain: Domain,
    opcode: &str,
    left: Operand,
    right: Operand,
) -> BinaryWaterline {
    use BinaryWaterline::*;
    let both = left.monotone && right.monotone;
    match domain {
        // TIME arithmetic wraps at midnight.
        Domain::TimeOfDay => return None,
        Domain::Temporal => {
            return match opcode {
                "ADD" if both => Both,
                "SUB" if both && right.constant => InheritLeft,
                _ => None,
            }
        }
        Domain::Scalar => {}
    }
    match opcode {
        "ADD" | "U64_TO_BINARY" | "U128_TO_BINARY" | "MIN" | "MIN_IGNORE_NULLS" if both => Both,
        "COALESCE" if both => Coalesce,
        "MAX" | "MAX_NULL_WINS" | "MAX_IGNORE_NULLS" => {
            // MAX_IGNORE_NULLS can return the other operand when a nullable operand is NULL.
            let bounds = |x: Operand| x.monotone && (opcode != "MAX_IGNORE_NULLS" || x.never_null);
            match (bounds(left), bounds(right)) {
                (true, true) => Both,
                (true, false) => Left,
                (false, true) => Right,
                (false, false) => None,
            }
        }
        "SUB" if both && right.constant => InheritLeft,
        "MUL" if right.monotone && left.positive_literal => InheritRight,
        "MUL" | "DIV" | "MUL_INTERVAL" | "DIV_INTERVAL"
            if left.monotone && right.positive_literal =>
        {
            InheritLeft
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nullable_bounds_and_constant_operands() {
        let bound = Operand {
            monotone: true,
            ..Operand::default()
        };
        let non_null = Operand {
            never_null: true,
            ..bound
        };
        let constant = Operand {
            constant: true,
            positive_literal: true,
            ..non_null
        };
        let unknown = Operand::default();
        use BinaryWaterline::*;
        use Domain::*;
        assert_eq!(binary_waterline(Scalar, "MAX", bound, unknown), Left);
        assert_eq!(
            binary_waterline(Scalar, "MAX_IGNORE_NULLS", bound, bound),
            None
        );
        assert_eq!(
            binary_waterline(Scalar, "MAX_IGNORE_NULLS", bound, non_null),
            Right
        );
        assert_eq!(binary_waterline(Scalar, "MIN", non_null, unknown), None);
        assert_eq!(binary_waterline(Scalar, "COALESCE", bound, bound), Coalesce);
        assert_eq!(binary_waterline(Scalar, "DIV", constant, bound), None);
        assert_eq!(
            binary_waterline(Scalar, "MUL", constant, bound),
            InheritRight
        );
        assert_eq!(
            binary_waterline(Scalar, "MUL_INTERVAL", bound, constant),
            InheritLeft
        );
        assert_eq!(binary_waterline(Temporal, "SUB", bound, non_null), None);
        assert_eq!(
            binary_waterline(Temporal, "SUB", bound, constant),
            InheritLeft
        );
        assert_eq!(binary_waterline(TimeOfDay, "ADD", bound, constant), None);
    }
}
