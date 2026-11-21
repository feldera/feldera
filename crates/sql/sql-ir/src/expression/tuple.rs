use super::{CircuitEditor, Error, Expr, ExpressionId, Field, InnerKind, NodeId, Ty, TypeId};

const NULL_TUPLE_CAST: &str = "Cast to non-nullable type applied to NULL value";

impl CircuitEditor {
    /// Expand every nested tuple into field reads. Sql-to-DBSP uses this to
    /// give aggregate arguments owned values, including non-Copy columns.
    pub fn expand_tuple(
        &mut self,
        origin: NodeId,
        value: ExpressionId,
    ) -> Result<ExpressionId, Error> {
        let ty = self.expression_type(value)?;
        let tuple = match self.type_kind(ty)? {
            Ty::Tuple => Expr::Tuple,
            Ty::RawTuple => Expr::RawTuple,
            Ty::Bool
            | Ty::Integer
            | Ty::Date
            | Ty::Decimal
            | Ty::Double
            | Ty::Real
            | Ty::GeoPoint
            | Ty::ShortInterval
            | Ty::LongInterval
            | Ty::Time
            | Ty::Timestamp
            | Ty::TimestampTz
            | Ty::String
            | Ty::Binary
            | Ty::Variant
            | Ty::Null
            | Ty::Uuid
            | Ty::Array
            | Ty::Map
            | Ty::ISize
            | Ty::USize
            | Ty::Void
            | Ty::Keyword
            | Ty::Str => return self.clone_if_needed(value),
            other => {
                return Err(Error::new(format!(
                    "cannot expand a value of {other:?} type"
                )))
            }
        };
        let nullable = self.nullable(ty)?;
        let is_constructor = self.expr_kind(value)? == tuple;
        let null_literal = is_constructor && self.node(value.0)?.field("fields").is_none();
        let fields = self.node(ty.0)?.edges("tupFields")?;
        let mut expanded = Vec::with_capacity(fields.len());
        if null_literal {
            for field in fields {
                let field_type = self.type_id(field)?;
                let nullable_field = self.with_nullability(field_type, true)?;
                expanded.push(self.typed_null(nullable_field)?);
            }
        } else {
            let safe = if nullable {
                self.unwrap_tuple(origin, value, ty)?
            } else {
                value
            };
            for index in 0..fields.len() {
                let field = self.tuple_field(safe, index)?;
                expanded.push(self.expand_tuple(origin, field)?);
            }
        }

        let converted = self.tuple_with_type_at(origin, ty, &expanded)?;
        if !nullable {
            return Ok(converted);
        }

        let bool_ty = self.empty_origin_node(
            value.0,
            InnerKind::Type(Ty::Bool),
            [
                ("code", Field::Data("BOOL".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )?;
        let condition = if is_constructor || self.expr_kind(value)? == Expr::Some {
            self.empty_origin_node(
                value.0,
                InnerKind::Expression(Expr::BoolLiteral),
                [
                    ("type", Field::Reference(bool_ty)),
                    ("value", Field::Data(null_literal.into())),
                ],
            )?
        } else {
            self.make_expr(
                value.0,
                Expr::IsNull,
                TypeId(bool_ty),
                [("expression", Field::Reference(value.0))],
            )?
            .0
        };
        let null = self.typed_null(ty)?;
        self.make_expr(
            origin,
            Expr::If,
            ty,
            [
                ("condition", Field::Reference(condition)),
                ("positive", Field::Reference(null.0)),
                ("negative", Field::Reference(converted.0)),
            ],
        )
    }

    /// An unlocated Java constructor has an explicit empty source in replay
    /// checkpoints and omits that field in public observations.
    pub fn empty_origin_node<const N: usize>(
        &mut self,
        sample: NodeId,
        kind: InnerKind,
        fields: [(&str, Field); N],
    ) -> Result<NodeId, Error> {
        let checkpoint = self.has_replay_metadata(sample)?;
        self.inner_with_fields(
            kind,
            fields
                .into_iter()
                .chain(checkpoint.then(|| ("sourceEmpty", Field::Data(true.into())))),
        )
    }

    fn unwrap_tuple(
        &mut self,
        origin: NodeId,
        value: ExpressionId,
        ty: TypeId,
    ) -> Result<ExpressionId, Error> {
        let kind = self.expr_kind(value)?;
        if kind == Expr::Some {
            return self.expression(self.node(value.0)?.edge("expression")?);
        }
        if matches!(kind, Expr::Tuple | Expr::RawTuple) {
            let fields = self.node(value.0)?.edges("fields")?;
            let types = fields
                .iter()
                .map(|&f| {
                    Ok(Field::Reference(
                        self.expression_type(self.expression(f)?)?.0,
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            let (ty_kind, code) = if kind == Expr::Tuple {
                (Ty::Tuple, "TUPLE")
            } else {
                (Ty::RawTuple, "RAW_TUPLE")
            };
            let result = self.empty_origin_node(
                value.0,
                InnerKind::Type(ty_kind),
                [
                    ("tupFields", Field::Array(types)),
                    ("code", Field::Data(code.into())),
                    ("mayBeNull", Field::Data(false.into())),
                ],
            )?;
            return self
                .empty_origin_node(
                    value.0,
                    InnerKind::Expression(kind),
                    [
                        ("type", Field::Reference(result)),
                        (
                            "fields",
                            Field::Array(fields.into_iter().map(Field::Reference).collect()),
                        ),
                    ],
                )
                .map(ExpressionId);
        }
        let result = self.with_nullability(ty, false)?;
        self.make_expr(
            origin,
            Expr::Unwrap,
            result,
            [
                ("expression", Field::Reference(value.0)),
                ("message", Field::Data(NULL_TUPLE_CAST.into())),
            ],
        )
    }

    pub fn typed_null(&mut self, ty: TypeId) -> Result<ExpressionId, Error> {
        let node = self.node(ty.0)?;
        if !self.nullable(ty)? {
            return Err(Error::new("NULL requires a nullable type"));
        }
        if self.type_kind(ty)? == Ty::Struct {
            let types = node
                .edges("fields")?
                .into_iter()
                .map(|f| self.node(f)?.edge("type").map(Field::Reference))
                .collect::<Result<Vec<_>, Error>>()?;
            let mut fields = vec![
                ("code", Field::Data("TUPLE".into())),
                ("mayBeNull", Field::Data(true.into())),
                ("tupFields", Field::Array(types)),
            ];
            if self.has_replay_metadata(ty.0)? {
                fields.push(("originalStruct", Field::Reference(ty.0)));
            }
            let tuple = self.inner_at(ty.0, InnerKind::Type(Ty::Tuple), fields)?;
            return self.make_expr(ty.0, Expr::Tuple, TypeId(tuple), []);
        }
        let code = node.data("code").and_then(|v| v.as_str());
        let kind = match code {
            Some("ISIZE") => Expr::ISizeLiteral,
            Some("INT8") => Expr::I8Literal,
            Some("INT16") => Expr::I16Literal,
            Some("INT32") => Expr::I32Literal,
            Some("INT64") => Expr::I64Literal,
            Some("INT128") => Expr::I128Literal,
            Some("UINT8") => Expr::U8Literal,
            Some("UINT16") => Expr::U16Literal,
            Some("UINT32") => Expr::U32Literal,
            Some("UINT64") => Expr::U64Literal,
            Some("UINT128") => Expr::U128Literal,
            Some("BOOL") => Expr::BoolLiteral,
            Some("DATE") => Expr::DateLiteral,
            Some("DECIMAL") => Expr::DecimalLiteral,
            Some("DOUBLE") => Expr::DoubleLiteral,
            Some("REAL") => Expr::RealLiteral,
            Some("GEOPOINT") => Expr::GeoPointConstructor,
            Some("INTERVAL_SHORT") => Expr::ShortIntervalLiteral,
            Some("INTERVAL_LONG") => Expr::LongIntervalLiteral,
            Some("STRING") => Expr::StringLiteral,
            Some("TIME") => Expr::TimeLiteral,
            Some("ARRAY") => Expr::Array,
            Some("MAP") => Expr::Map,
            Some("TUPLE") => Expr::Tuple,
            Some("RAW_TUPLE") => Expr::RawTuple,
            Some("TIMESTAMP") => Expr::TimestampLiteral,
            Some("TIMESTAMP_TZ") => Expr::TimestampTzLiteral,
            Some("BYTES") => Expr::BinaryLiteral,
            Some("VARIANT") => Expr::Variant,
            Some("UUID") => Expr::UuidLiteral,
            Some("NULL") => Expr::NullLiteral,
            Some("VOID") => Expr::VoidLiteral,
            _ => return Err(Error::new(format!("cannot construct NULL for {code:?}"))),
        };
        let type_kind = self.type_kind(ty)?;
        let code = code
            .ok_or_else(|| Error::new("missing NULL type code"))?
            .to_owned();
        let ty = match type_kind {
            Ty::Integer if kind != Expr::U128Literal => {
                let width = self.number(ty, "width")?;
                let signed = self
                    .node(ty.0)?
                    .data("signed")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| Error::new("missing integer signedness"))?;
                TypeId(self.empty_origin_node(
                    ty.0,
                    InnerKind::Type(Ty::Integer),
                    [
                        ("code", Field::Data(code.into())),
                        ("mayBeNull", Field::Data(true.into())),
                        ("width", Field::Data(width.into())),
                        ("signed", Field::Data(signed.into())),
                    ],
                )?)
            }
            Ty::Bool => TypeId(self.empty_origin_node(
                ty.0,
                InnerKind::Type(Ty::Bool),
                [
                    ("code", Field::Data(code.into())),
                    ("mayBeNull", Field::Data(true.into())),
                ],
            )?),
            Ty::ISize
            | Ty::Date
            | Ty::Time
            | Ty::Timestamp
            | Ty::TimestampTz
            | Ty::Uuid
            | Ty::Double
            | Ty::Real
            | Ty::GeoPoint
            | Ty::Null
            | Ty::Void => self.primitive_singleton(ty.0, type_kind, &code, true)?,
            _ => ty,
        };
        let extra = match kind {
            Expr::StringLiteral => {
                let literal = self.empty_origin_node(
                    ty.into(),
                    InnerKind::Expression(kind),
                    [
                        ("charset", Field::Data("UTF-8".into())),
                        ("type", Field::Reference(ty.into())),
                    ],
                )?;
                return self.expression(literal);
            }
            Expr::Variant => Some(("isSqlNull", Field::Data(false.into()))),
            _ => None,
        };
        self.make_expr(ty.0, kind, ty, extra)
    }
}
