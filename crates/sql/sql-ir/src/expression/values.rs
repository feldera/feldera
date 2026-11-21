//! Type minima used for initial waterlines and the contribution of a NULL row.

use super::{CircuitEditor, Error, Expr, ExpressionId, Field, InnerKind, Ty, TypeId};
use serde_json::Value;

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
                let min = if !signed {
                    0
                } else {
                    i128::MIN >> (128 - width)
                };
                let name = format!("DBSP{}{width}Literal", if signed { "I" } else { "U" });
                (
                    Expr::from_wire(&name)
                        .ok_or_else(|| Error::new("unrecognized integer literal"))?,
                    ty,
                    min.to_string().into(),
                )
            }
            Ty::Date => (Expr::DateLiteral, ty, (-719162).into()),
            Ty::Time => (Expr::TimeLiteral, ty, "0".into()),
            Ty::Timestamp => (
                Expr::TimestampLiteral,
                self.primitive_singleton(ty.0, kind, &code, nullable)?,
                "-62135596800000000".into(),
            ),
            Ty::TimestampTz => (Expr::TimestampTzLiteral, ty, "-62135596800000000".into()),
            Ty::ShortInterval | Ty::LongInterval => {
                let fresh = TypeId(self.derive_inner(ty.0, [])?);
                if kind == Ty::ShortInterval {
                    (
                        Expr::ShortIntervalLiteral,
                        fresh,
                        i64::MIN.to_string().into(),
                    )
                } else {
                    (
                        Expr::LongIntervalLiteral,
                        fresh,
                        i32::MIN.to_string().into(),
                    )
                }
            }
            Ty::Real => (
                Expr::RealLiteral,
                self.primitive_singleton(ty.0, kind, &code, nullable)?,
                Value::from(f32::NEG_INFINITY.to_bits() as i32),
            ),
            Ty::Double => (
                Expr::DoubleLiteral,
                self.primitive_singleton(ty.0, kind, &code, nullable)?,
                Value::from(f64::NEG_INFINITY.to_bits() as i64),
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
                (Expr::BoolLiteral, TypeId(fresh), false.into())
            }
            Ty::Decimal => {
                let precision = self.number(ty, "precision")?;
                let scale = self.number(ty, "scale")?;
                if !(1..=38).contains(&precision) || !(0..=precision).contains(&scale) {
                    return Err(Error::new("invalid decimal bounds"));
                }
                let mut minimum = String::from("-");
                if precision == scale {
                    minimum.push('0');
                } else {
                    minimum.push_str(&"9".repeat((precision - scale) as usize));
                }
                if scale > 0 {
                    minimum.push('.');
                    minimum.push_str(&"9".repeat(scale as usize));
                }
                (Expr::DecimalLiteral, ty, minimum.into())
            }
            _ => return Err(Error::new(format!("{kind:?} has no waterline minimum"))),
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
