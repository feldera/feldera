//! Type extrema used for initial waterlines and the contribution of a NULL row.

use super::{CircuitEditor, Error, Expr, ExpressionId, Field, InnerKind, Ty, TypeId};
use serde_json::Value;

#[derive(Clone, Copy)]
enum Extremum {
    Minimum,
    Maximum,
}

impl Extremum {
    fn select<T>(self, minimum: T, maximum: T) -> T {
        match self {
            Self::Minimum => minimum,
            Self::Maximum => maximum,
        }
    }
}

impl CircuitEditor {
    /// The literal synthesized when converting a one-based SQL array index.
    pub fn index_one(&mut self, ty: TypeId) -> Result<ExpressionId, Error> {
        if self.type_kind(ty)? != Ty::Integer {
            return Err(Error::new("SQL array index requires an integer"));
        }
        let node = self.node(ty.0)?;
        let width = self.number(ty, "width")?;
        let signed = node
            .data("signed")
            .and_then(Value::as_bool)
            .ok_or_else(|| Error::new("integer index has no signedness"))?;
        let code = node
            .data("code")
            .cloned()
            .ok_or_else(|| Error::new("integer index has no type code"))?;
        let kind = Expr::from_wire(&format!(
            "DBSP{}{width}Literal",
            if signed { "I" } else { "U" }
        ))
        .ok_or_else(|| Error::new("unsupported integer index width"))?;
        let fresh = self.empty_origin_node(
            ty.0,
            InnerKind::Type(Ty::Integer),
            [
                ("code", Field::Data(code)),
                ("width", Field::Data(width.into())),
                ("signed", Field::Data(signed.into())),
                ("mayBeNull", Field::Data(self.nullable(ty)?.into())),
            ],
        )?;
        self.empty_origin_node(
            ty.0,
            InnerKind::Expression(kind),
            [
                ("type", Field::Reference(fresh)),
                ("value", Field::Data("1".into())),
            ],
        )
        .map(ExpressionId)
    }

    pub fn isize_type(&mut self, sample: TypeId) -> Result<TypeId, Error> {
        self.primitive_singleton(sample.0, Ty::ISize, "ISIZE", self.nullable(sample)?)
    }

    /// A target-sized literal uses the compiler's nonnullable singleton.
    /// The signed literal constructor clears its origin. The unsigned one retains it.
    pub fn required_size_literal(&mut self, literal: ExpressionId) -> Result<ExpressionId, Error> {
        let kind = self.expr_kind(literal)?;
        let (ty, code) = match kind {
            Expr::ISizeLiteral => (Ty::ISize, "ISIZE"),
            Expr::USizeLiteral => (Ty::USize, "USIZE"),
            _ => return Err(Error::new("expected a target-sized literal")),
        };
        let value = self
            .node(literal.into())?
            .data("value")
            .filter(|v| !v.is_null())
            .cloned()
            .ok_or_else(|| Error::new("expected a non-NULL size literal"))?;
        let required = self.primitive_singleton(literal.into(), ty, code, false)?;
        if kind == Expr::ISizeLiteral {
            let id = self.empty_origin_node(
                literal.into(),
                InnerKind::Expression(kind),
                [
                    ("type", Field::Reference(required.into())),
                    ("value", Field::Data(value)),
                ],
            )?;
            return self.expression(id);
        }
        self.make_expr(
            literal.into(),
            kind,
            required,
            [("value", Field::Data(value))],
        )
    }

    pub fn minimum_value(&mut self, ty: TypeId) -> Result<ExpressionId, Error> {
        let kind = self.type_kind(ty)?;
        if matches!(kind, Ty::Tuple | Ty::RawTuple) {
            let fields = self.node(ty.0)?.edges("tupFields")?;
            let values = fields
                .into_iter()
                .map(|t| self.minimum_value(self.type_id(t)?))
                .collect::<Result<Vec<_>, _>>()?;
            return self.tuple_with_type(ty, &values);
        }
        self.scalar_extremum(ty, Extremum::Minimum)
    }

    /// A typed NULL contributes no non-null values, so its lower bound may be
    /// the greatest value of the scalar type.
    pub fn scalar_maximum(&mut self, ty: TypeId) -> Result<ExpressionId, Error> {
        self.scalar_extremum(ty, Extremum::Maximum)
    }

    fn scalar_extremum(&mut self, ty: TypeId, limit: Extremum) -> Result<ExpressionId, Error> {
        let kind = self.type_kind(ty)?;
        let code = self
            .node(ty.0)?
            .data("code")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::new("missing type code"))?
            .to_owned();
        let nullable = self.nullable(ty)?;
        let (literal, output, value) = match kind {
            Ty::Integer => {
                let width = self.number(ty, "width")?;
                let signed = self
                    .node(ty.0)?
                    .data("signed")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| Error::new("missing integer signedness"))?;
                if !matches!(width, 8 | 16 | 32 | 64 | 128) {
                    return Err(Error::new("unsupported integer width"));
                }
                let value = match (limit, signed) {
                    (Extremum::Minimum, false) => "0".to_owned(),
                    (Extremum::Minimum, true) => (i128::MIN >> (128 - width)).to_string(),
                    (Extremum::Maximum, false) => (u128::MAX >> (128 - width)).to_string(),
                    (Extremum::Maximum, true) => (i128::MAX >> (128 - width)).to_string(),
                };
                let name = format!("DBSP{}{width}Literal", if signed { "I" } else { "U" });
                (
                    Expr::from_wire(&name)
                        .ok_or_else(|| Error::new("unrecognized integer literal"))?,
                    ty,
                    value.into(),
                )
            }
            // SQL dates span years 1 through 9999. The wire format counts days
            // since 1970 for DATE and microseconds since 1970 for TIMESTAMP.
            Ty::Date => (
                Expr::DateLiteral,
                ty,
                limit.select(-719_162, 2_932_896).into(),
            ),
            Ty::Time => (
                Expr::TimeLiteral,
                ty,
                limit.select("00:00:00", "23:59:59.999999999").into(),
            ),
            Ty::Timestamp => (
                Expr::TimestampLiteral,
                self.primitive_singleton(ty.0, kind, &code, nullable)?,
                limit
                    .select("-62135596800000000", "253402300799999999")
                    .into(),
            ),
            Ty::TimestampTz => (
                Expr::TimestampTzLiteral,
                ty,
                limit
                    .select("0001-01-01 00:00:00 UTC", "9999-12-31 23:59:59.999999 UTC")
                    .into(),
            ),
            Ty::ShortInterval | Ty::LongInterval => {
                // The interval literal constructor creates a fresh type from the units.
                let units = self
                    .node(ty.0)?
                    .data("units")
                    .cloned()
                    .ok_or_else(|| Error::new("missing interval units"))?;
                let fresh = TypeId(self.empty_origin_node(
                    ty.0,
                    InnerKind::Type(kind),
                    [
                        ("units", Field::Data(units)),
                        ("code", Field::Data(code.into())),
                        ("mayBeNull", Field::Data(nullable.into())),
                    ],
                )?);
                if kind == Ty::ShortInterval {
                    (
                        Expr::ShortIntervalLiteral,
                        fresh,
                        limit.select(i64::MIN, i64::MAX).to_string().into(),
                    )
                } else {
                    (
                        Expr::LongIntervalLiteral,
                        fresh,
                        limit.select(i32::MIN, i32::MAX).to_string().into(),
                    )
                }
            }
            // DBSP's total ordering places the canonical NaN above positive infinity.
            Ty::Real => (
                Expr::RealLiteral,
                self.primitive_singleton(ty.0, kind, &code, nullable)?,
                Value::from(limit.select(f32::NEG_INFINITY.to_bits() as i32, 0x7fc0_0000)),
            ),
            Ty::Double => (
                Expr::DoubleLiteral,
                self.primitive_singleton(ty.0, kind, &code, nullable)?,
                Value::from(
                    limit.select(f64::NEG_INFINITY.to_bits() as i64, 0x7ff8_0000_0000_0000),
                ),
            ),
            Ty::Bool => {
                let fresh = self.empty_origin_node(
                    ty.0,
                    InnerKind::Type(Ty::Bool),
                    [
                        ("code", Field::Data(code.into())),
                        ("mayBeNull", Field::Data(nullable.into())),
                    ],
                )?;
                (
                    Expr::BoolLiteral,
                    TypeId(fresh),
                    limit.select(false, true).into(),
                )
            }
            Ty::Decimal => {
                let precision = self.number(ty, "precision")?;
                let scale = self.number(ty, "scale")?;
                if !(1..=38).contains(&precision) || !(0..=precision).contains(&scale) {
                    return Err(Error::new("invalid decimal bounds"));
                }
                let mut value = String::from(limit.select("-", ""));
                if precision == scale {
                    value.push('0');
                } else {
                    value.push_str(&"9".repeat((precision - scale) as usize));
                }
                if scale > 0 {
                    value.push('.');
                    value.push_str(&"9".repeat(scale as usize));
                }
                (Expr::DecimalLiteral, ty, value.into())
            }
            _ => {
                let name = limit.select("minimum", "maximum");
                return Err(Error::new(format!("{kind:?} has no waterline {name}")));
            }
        };
        let fields = [
            ("type", Field::Reference(output.0)),
            ("value", Field::Data(value)),
        ];
        if matches!(kind, Ty::Date | Ty::Decimal | Ty::Time) {
            self.inner_at(ty.0, InnerKind::Expression(literal), fields)
                .map(ExpressionId)
        } else {
            self.empty_origin_node(ty.0, InnerKind::Expression(literal), fields)
                .map(ExpressionId)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Circuit;
    use serde_json::json;

    #[test]
    fn interval_extrema_use_fresh_unlocated_types() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        for (class, code, units) in [
            ("DBSPTypeShortInterval", "INTERVAL_SHORT", "HOURS"),
            ("DBSPTypeLongInterval", "INTERVAL_LONG", "YEARS"),
        ] {
            let (arena, roots) = circuit.clone().with_inner_roots([json!({
                "class":class,"id":1,"units":units,"code":code,"mayBeNull":false,
                "sourceEmpty":false,"position":{"start_line_number":2,"start_column":5,
                    "end_line_number":2,"end_column":14}
            })])?;
            let mut edit = CircuitEditor::new(arena);
            let input = edit.type_id(roots[0])?;
            for literal in [edit.minimum_value(input)?, edit.scalar_maximum(input)?] {
                let ty = edit.expression_type(literal)?;
                assert_ne!(ty, input);
                let node = edit.node(ty.into())?;
                assert_eq!(node.data("units"), Some(&json!(units)));
                assert_eq!(node.data("sourceEmpty"), Some(&json!(true)));
                assert!(node.field("position").is_none());
            }
        }
        Ok(())
    }

    #[test]
    fn integer_waterlines_cover_the_full_128_bit_range() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        for (signed, code, minimum, maximum) in [
            (true, "INT128", i128::MIN.to_string(), i128::MAX.to_string()),
            (false, "UINT128", "0".to_owned(), u128::MAX.to_string()),
        ] {
            let (arena, roots) = circuit.clone().with_inner_roots([json!({
                "class":"DBSPTypeInteger", "id":1, "width":128, "signed":signed,
                "mayBeNull":false, "code":code
            })])?;
            let mut edit = CircuitEditor::new(arena);
            let ty = edit.type_id(roots[0])?;
            let low = edit.minimum_value(ty)?;
            let high = edit.scalar_maximum(ty)?;
            for (literal, expected) in [(low, minimum), (high, maximum)] {
                assert_eq!(edit.expression_type(literal)?, ty);
                assert_eq!(
                    edit.node(literal.into())?.data("value"),
                    Some(&json!(expected))
                );
            }
        }
        Ok(())
    }
}
