//! Select a lower bound for binary scalar and temporal expressions.
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind, Field, NodeId, TypeId, TypeKind,
};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
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

/// Facts about a binary expression at its point in the monotonicity analysis.
#[derive(Deserialize)]
pub struct BinaryOperation<'a> {
    pub domain: Domain,
    pub opcode: &'a str,
    pub left: Operand,
    pub right: Operand,
}

impl BinaryOperation<'_> {
    /// Construct a bound at the SQL operation's source location. Only the types
    /// and identities of the operand bounds are needed.
    pub fn lower(
        &self,
        edit: &mut CircuitEditor,
        site: NodeId,
        result: TypeId,
        bounds: [Option<ExpressionId>; 2],
    ) -> Result<(BinaryWaterline, Option<ExpressionId>), Error> {
        use BinaryWaterline as Rule;
        let [left, right] = bounds;
        let rule = binary_waterline(self.domain, self.opcode, self.left, self.right);
        if rule == Rule::None {
            return Ok((rule, None));
        }
        let required = edit.with_nullability(result, false)?;
        if matches!(rule, Rule::Left | Rule::Right) {
            let bound = if rule == Rule::Left { left } else { right };
            let bound = bound.ok_or_else(|| Error::new("missing selected operand waterline"))?;
            return Ok((rule, Some(edit.cast(site, bound, required)?)));
        }
        let (Some(mut left), Some(mut right)) = (left, right) else {
            return Err(Error::new("binary waterline requires both operands"));
        };
        let temporal = matches!(self.domain, Domain::Temporal);
        let interval = |kind| matches!(kind, TypeKind::ShortInterval | TypeKind::LongInterval);
        let kind = edit.type_kind(result)?;
        // A date or timestamp difference fits in the result interval.
        let temporal_difference =
            temporal && interval(kind) && !interval(edit.type_kind(edit.expression_type(left)?)?);
        let opcode = match (rule, self.opcode) {
            (Rule::Coalesce, _) | (_, "MIN_IGNORE_NULLS") => "MIN",
            (_, "MAX_IGNORE_NULLS" | "MAX_NULL_WINS") => "MAX",
            (_, "ADD") if !temporal_difference => "ADD_SATURATING",
            (_, "SUB") if !temporal_difference => "SUB_SATURATING",
            (_, "MUL") => "MUL_SATURATING",
            (_, "DIV") => "DIV_SATURATING",
            (_, "MUL_INTERVAL") => "MUL_INTERVAL_SATURATING",
            (_, "DIV_INTERVAL") => "DIV_INTERVAL_SATURATING",
            (_, other) => other,
        };
        if rule == Rule::Coalesce {
            // The parent and each cast have separate result type construction sites.
            let ty = edit.with_nullability(result, false)?;
            left = edit.cast(site, left, ty)?;
            let ty = edit.with_nullability(result, false)?;
            right = edit.cast(site, right, ty)?;
        }
        let mut fields = vec![
            ("left", Field::Reference(left.into())),
            ("right", Field::Reference(right.into())),
            ("opcode", Field::Data(opcode.into())),
        ];
        if temporal && interval(kind) {
            let units = edit
                .node(result.into())?
                .data("units")
                .ok_or_else(|| Error::new("missing interval units for temporal waterline"))?;
            let key = if kind == TypeKind::LongInterval {
                "longUnits"
            } else {
                "shortUnits"
            };
            fields.push((key, Field::Data(units.clone())));
        }
        let expression = if temporal {
            ExpressionKind::TimeAddSub
        } else {
            ExpressionKind::Binary
        };
        Ok((
            rule,
            Some(edit.make_expr(site, expression, required, fields)?),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn temporal_subtraction_preserves_units_and_saturates_interval_arithmetic() -> Result<(), Error>
    {
        let arena = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        let (arena, roots) = arena.with_inner_roots([
            json!({"class":"DBSPTypeTimestamp","id":1,"mayBeNull":false,"code":"TIMESTAMP"}),
            json!({"class":"DBSPTypeShortInterval","id":2,"mayBeNull":false,"code":"INTERVAL_SHORT","units":"SECONDS"}),
            json!({"class":"DBSPTypeLongInterval","id":3,"mayBeNull":false,"code":"INTERVAL_LONG","units":"MONTHS"}),
        ])?;
        let site = arena.root_id();
        let mut edit = CircuitEditor::new(arena);
        let operand = Operand {
            monotone: true,
            constant: true,
            ..Operand::default()
        };
        let subtract = BinaryOperation {
            domain: Domain::Temporal,
            opcode: "SUB",
            left: operand,
            right: operand,
        };
        let timestamp = edit.type_id(roots[0])?;
        for (ty, unit_field, units) in [
            (roots[1], "shortUnits", "SECONDS"),
            (roots[2], "longUnits", "MONTHS"),
        ] {
            let interval = edit.type_id(ty)?;
            for (input, expected) in [(timestamp, "SUB"), (interval, "SUB_SATURATING")] {
                let left = edit.named_variable(input, "lhs")?;
                let right = edit.named_variable(input, "rhs")?;
                let (rule, bound) =
                    subtract.lower(&mut edit, site, interval, [Some(left), Some(right)])?;
                assert_eq!(rule, BinaryWaterline::InheritLeft);
                let node = edit.node(bound.unwrap().into())?;
                assert_eq!(node.class(), "DBSPTimeAddSub");
                assert_eq!(node.data("opcode"), Some(&json!(expected)));
                assert_eq!(node.data(unit_field), Some(&json!(units)));
                assert_eq!(node.edge("left")?, left.into());
                assert_eq!(node.edge("right")?, right.into());
            }
        }
        Ok(())
    }

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
