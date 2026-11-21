//! Binary operations constructed during relational lowering and limiter insertion.

use super::{CircuitEditor, Error, Expr, ExpressionId, Field, InnerKind, NodeId, Ty, TypeId};

macro_rules! binary_ops {
    ($($name:ident => $wire:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum BinaryOp { $($name),+ }
        impl BinaryOp {
            pub fn wire_name(self) -> &'static str {
                match self { $(Self::$name => $wire),+ }
            }
            pub fn from_wire(name: &str) -> Option<Self> {
                match name { $($wire => Some(Self::$name),)+ _ => None }
            }
        }
    }
}

binary_ops! {
    Add => "ADD",
    Sub => "SUB",
    Mul => "MUL",
    Div => "DIV",
    DivNull => "DIV_NULL",
    DivIntervalNull => "DIV_INTERVAL_NULL",
    AddSaturating => "ADD_SATURATING",
    SubSaturating => "SUB_SATURATING",
    MulSaturating => "MUL_SATURATING",
    DivSaturating => "DIV_SATURATING",
    Mod => "MOD",
    Eq => "EQ",
    Neq => "NEQ",
    Lt => "LT",
    Gt => "GT",
    Lte => "LTE",
    Gte => "GTE",
    And => "AND",
    BwAnd => "BW_AND",
    MulWeight => "MUL_WEIGHT",
    Or => "OR",
    BwOr => "BW_OR",
    Xor => "XOR",
    Max => "MAX",
    Min => "MIN",
    MaxIgnoreNulls => "MAX_IGNORE_NULLS",
    MinIgnoreNulls => "MIN_IGNORE_NULLS",
    MaxNullWins => "MAX_NULL_WINS",
    Concat => "CONCAT",
    IsDistinct => "IS_DISTINCT",
    Coalesce => "COALESCE",
    SqlIndex => "SQL_INDEX",
    SafeRustIndex => "SAFE_RUST_INDEX",
    MapIndex => "MAP_INDEX",
    VariantIndex => "VARIANT_INDEX",
    RustIndex => "RUST_INDEX",
    MulInterval => "MUL_INTERVAL",
    DivInterval => "DIV_INTERVAL",
    MulIntervalSaturating => "MUL_INTERVAL_SATURATING",
    DivIntervalSaturating => "DIV_INTERVAL_SATURATING",
    AggAnd => "AGG_AND",
    AggOr => "AGG_OR",
    AggXor => "AGG_XOR",
    AggMax => "AGG_MAX",
    AggMin => "AGG_MIN",
    AggMax1 => "AGG_MAX1",
    AggMin1 => "AGG_MIN1",
    AggAdd => "AGG_ADD",
    AggAddNonNull => "AGG_ADD_NON_NULL",
    AggGte => "AGG_GTE",
    AggLte => "AGG_LTE",
    ControlledFilterGte => "CONTROLLED_FILTER_GTE",
    ArrayConvert => "ARRAY_CONVERT",
    ArrayConvertSafe => "ARRAY_CONVERT_SAFE",
    MapConvert => "MAP_CONVERT",
    MapConvertSafe => "MAP_CONVERT_SAFE",
}

impl BinaryOp {
    fn comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Neq | Self::Lt | Self::Lte | Self::Gt | Self::Gte | Self::IsDistinct
        )
    }

    fn strict(self) -> Result<bool, Error> {
        use BinaryOp::*;
        match self {
            MapConvert | ArrayConvert | ControlledFilterGte | AggLte | AggGte | AggAdd | AggMin
            | AggMax | AggXor | AggOr | AggAnd | IsDistinct | Concat | Min | Max | Or | And
            | AggMax1 | AggMin1 | Coalesce => Ok(false),
            DivInterval | DivIntervalNull | MulInterval | RustIndex | VariantIndex | MapIndex
            | SqlIndex | Xor | BwOr | MulWeight | BwAnd | Gte | Lte | Gt | Lt | Neq | Eq | Mod
            | DivNull | Div | Mul | Sub | Add => Ok(true),
            _ => Err(Error::new(format!(
                "binary coercion is undefined for {}",
                self.wire_name()
            ))),
        }
    }

    fn needs_common_type(self, left: Ty, right: Ty) -> bool {
        if self == Self::Concat {
            return false;
        }
        if self == Self::Sub && left.date() && right.date() {
            return true;
        }
        if self.comparison()
            && (left.date()
                || right.date()
                || left == Ty::String && right.interval()
                || left.interval() && right == Ty::String)
        {
            return true;
        }
        !(left.date()
            || left.interval()
            || right.date()
            || right.interval()
            || left == right && matches!(left, Ty::Decimal | Ty::String))
    }
}

impl CircuitEditor {
    /// SQL predicates keep only TRUE. A nullable boolean must first map NULL to false.
    pub fn wrap_bool_if_needed(&mut self, value: ExpressionId) -> Result<ExpressionId, Error> {
        let ty = self.expression_type(value)?;
        if !self.nullable(ty)? {
            return Ok(value);
        }
        let result = self.with_nullability(ty, false)?;
        self.make_expr(
            value.into(),
            Expr::Unary,
            result,
            [
                ("source", Field::Reference(value.into())),
                ("opcode", Field::Data("WRAP_BOOL".into())),
            ],
        )
    }

    /// Build the IS NULL predicate, folding constructors whose presence is known.
    pub fn null_test(&mut self, mut operand: ExpressionId) -> Result<ExpressionId, Error> {
        while self.expr_kind(operand)? == Expr::Clone {
            operand = self.expression(self.node(operand.0)?.edge("expression")?)?;
        }
        let value = self.node(operand.0)?;
        let known = match self.expr_kind(operand)? {
            _ if !self.nullable(self.expression_type(operand)?)? => Some(false),
            Expr::Tuple | Expr::RawTuple => Some(value.field("fields").is_none()),
            Expr::Array => Some(value.field("data").is_none()),
            Expr::Map => Some(value.field("keys").is_none()),
            Expr::Variant => Some(value.field("value").is_none()),
            Expr::GeoPointConstructor => {
                Some(value.field("left").is_none() || value.field("right").is_none())
            }
            Expr::Some | Expr::ZSet | Expr::IndexedZSet => Some(false),
            kind if kind.wire_name().ends_with("Literal") => Some(self.is_null_literal(operand)?),
            _ => None,
        };
        let ty = self.empty_origin_node(
            operand.0,
            InnerKind::Type(Ty::Bool),
            [
                ("code", Field::Data("BOOL".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )?;
        if let Some(is_null) = known {
            return self
                .empty_origin_node(
                    operand.0,
                    InnerKind::Expression(Expr::BoolLiteral),
                    [
                        ("type", Field::Reference(ty)),
                        ("value", Field::Data(is_null.into())),
                    ],
                )
                .map(ExpressionId);
        }
        self.make_expr(
            operand.0,
            Expr::IsNull,
            TypeId(ty),
            [("expression", Field::Reference(operand.0))],
        )
    }

    pub fn is_null_literal(&self, expr: ExpressionId) -> Result<bool, Error> {
        let kind = self.expr_kind(expr)?;
        Ok(match kind {
            Expr::NullLiteral => true,
            Expr::KeywordLiteral
            | Expr::VoidLiteral
            | Expr::StrLiteral
            | Expr::VariantNullLiteral => false,
            _ => {
                kind.wire_name().ends_with("Literal") && self.node(expr.0)?.field("value").is_none()
            }
        })
    }

    pub fn cast(
        &mut self,
        origin: NodeId,
        value: ExpressionId,
        ty: TypeId,
    ) -> Result<ExpressionId, Error> {
        if self.same_type(self.expression_type(value)?, ty)? {
            return Ok(value);
        }
        self.make_expr(
            origin,
            Expr::Cast,
            ty,
            [
                ("source", Field::Reference(value.0)),
                ("safe", Field::Data("SqlUnsafe".into())),
            ],
        )
    }

    fn binary_node(
        &mut self,
        origin: NodeId,
        kind: Expr,
        ty: TypeId,
        op: BinaryOp,
        left: ExpressionId,
        right: ExpressionId,
    ) -> Result<ExpressionId, Error> {
        self.make_expr(
            origin,
            kind,
            ty,
            [
                ("left", Field::Reference(left.0)),
                ("right", Field::Reference(right.0)),
                ("opcode", Field::Data(op.wire_name().into())),
            ],
        )
    }

    /// Resolve operand coercions and construct a possibly-null SQL binary result.
    pub fn binary_expression(
        &mut self,
        origin: NodeId,
        result: TypeId,
        op: BinaryOp,
        mut left: ExpressionId,
        mut right: ExpressionId,
    ) -> Result<ExpressionId, Error> {
        let lt = self.expression_type(left)?;
        let rt = self.expression_type(right)?;
        let a = self.type_kind(lt)?;
        let b = self.type_kind(rt)?;
        if op.strict()? {
            for expr in [left, right] {
                if self.is_null_literal(expr)? {
                    return self.typed_null(result);
                }
            }
        }
        let nullable = self.nullable(lt)? || self.nullable(rt)?;
        let inferred = self.with_nullability(result, nullable)?;
        if op == BinaryOp::DivNull && !self.nullable(result)? {
            return Err(Error::new("DIV_NULL requires a nullable result"));
        }
        let mut ty;
        if op.needs_common_type(a, b) {
            let mut common = self.common_type(lt, rt, op.comparison())?;
            ty = self.with_nullability(common, nullable)?;
            if self.type_kind(common)? == Ty::Decimal {
                ty = self
                    .empty_origin_node(
                        origin,
                        InnerKind::Type(Ty::Decimal),
                        [
                            ("precision", Field::Data(38.into())),
                            ("scale", Field::Data(10.into())),
                            ("code", Field::Data("DECIMAL".into())),
                            ("mayBeNull", Field::Data(nullable.into())),
                        ],
                    )
                    .map(TypeId)?;
            }
            if self.type_kind(common)?.date() && op == BinaryOp::Sub || op == BinaryOp::DivNull {
                ty = result;
            } else if matches!(
                op,
                BinaryOp::BwAnd
                    | BinaryOp::BwOr
                    | BinaryOp::Xor
                    | BinaryOp::Max
                    | BinaryOp::Min
                    | BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
            ) {
                common = inferred;
                ty = common;
            }
            if self.type_kind(common)? == Ty::Null {
                return self.typed_null(result);
            }
            let left_ty = self.with_nullability(common, self.nullable(lt)?)?;
            let right_ty = self.with_nullability(common, self.nullable(rt)?)?;
            left = self.cast(origin, left, left_ty)?;
            right = self.cast(origin, right, right_ty)?;
            if !op.comparison() && (self.type_kind(ty)?.date() || self.type_kind(ty)?.interval()) {
                return self.binary_node(origin, Expr::TimeAddSub, ty, op, left, right);
            }
        } else {
            if a == Ty::Null || b == Ty::Null {
                return self.typed_null(result);
            }
            if matches!(
                op,
                BinaryOp::Mul | BinaryOp::Div | BinaryOp::DivIntervalNull
            ) && (a.interval() || b.interval())
            {
                return self.interval_scale(origin, result, op, left, right);
            }
            if matches!(op, BinaryOp::Sub | BinaryOp::Add)
                && (a.date() || a.interval() || b.date() || b.interval())
            {
                if b.numeric() {
                    return Err(Error::new(
                        "cannot add or subtract a number from a time value",
                    ));
                }
                if a.interval() && !b.interval() {
                    std::mem::swap(&mut left, &mut right);
                }
                let value =
                    self.binary_node(origin, Expr::TimeAddSub, inferred, op, left, right)?;
                return self.cast(origin, value, result);
            }
            if !(a.date()
                || a.interval()
                || b.date()
                || b.interval()
                || a == b && matches!(a, Ty::String | Ty::Decimal | Ty::Binary))
            {
                return Err(Error::new(format!("unsupported {op:?} on {a:?} and {b:?}")));
            }
            ty = inferred;
        }
        if op.comparison() {
            ty = self.primitive_singleton(
                origin,
                Ty::Bool,
                "BOOL",
                op != BinaryOp::IsDistinct && nullable,
            )?;
        }
        let value = self.binary_node(origin, Expr::Binary, ty, op, left, right)?;
        self.cast(origin, value, result)
    }

    fn interval_scale(
        &mut self,
        origin: NodeId,
        result: TypeId,
        op: BinaryOp,
        mut left: ExpressionId,
        mut right: ExpressionId,
    ) -> Result<ExpressionId, Error> {
        let opcode = match op {
            BinaryOp::Mul => BinaryOp::MulInterval,
            BinaryOp::Div => BinaryOp::DivInterval,
            _ => op,
        };
        if opcode == BinaryOp::MulInterval
            && self.type_kind(self.expression_type(right)?)?.interval()
        {
            std::mem::swap(&mut left, &mut right);
        }
        let scale = self.expression_type(right)?;
        let kind = self.type_kind(scale)?;
        let promoted = match kind {
            Ty::Integer => {
                let width = if self.type_kind(self.expression_type(left)?)? == Ty::ShortInterval {
                    64
                } else {
                    32
                };
                self.integer_type(scale, width, true, self.nullable(scale)?)?
            }
            Ty::Real => self.primitive(scale, Ty::Double, "DOUBLE", self.nullable(scale)?)?,
            Ty::Double | Ty::Decimal => scale,
            _ => return Err(Error::new("interval scaling requires a numeric operand")),
        };
        right = self.cast(origin, right, promoted)?;
        let units = self
            .node(result.0)?
            .data("units")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::new("interval needs units"))?;
        let unit = match self.type_kind(result)? {
            Ty::LongInterval if units == "YEARS" => Some("MONTHS"),
            Ty::LongInterval => None,
            Ty::ShortInterval
                if !matches!(
                    units,
                    "SECONDS" | "DAYS_TO_SECONDS" | "HOURS_TO_SECONDS" | "MINUTES_TO_SECONDS"
                ) =>
            {
                Some("SECONDS")
            }
            Ty::ShortInterval => None,
            _ => return Err(Error::new("interval scaling requires an interval result")),
        };
        let finest = match unit {
            Some(units) => {
                TypeId(self.derive_inner(result.0, [("units", Field::Data(units.into()))])?)
            }
            None => result,
        };
        let value = self.binary_node(origin, Expr::Binary, finest, opcode, left, right)?;
        self.cast(origin, value, result)
    }

    /// Build pairwise levels while the operand types agree. Once an implicit
    /// cast is needed, each following operand combines with the left result.
    pub fn binary_expressions(
        &mut self,
        origin: NodeId,
        result: TypeId,
        op: BinaryOp,
        operands: &[ExpressionId],
    ) -> Result<ExpressionId, Error> {
        if operands.len() < 2 {
            return Err(Error::new("a binary reduction needs at least two operands"));
        }
        let mut level = operands.to_vec();
        while level.len() > 1 {
            let mut uniform = true;
            for &expr in &level {
                if !self.same_type(self.expression_type(expr)?, result)? {
                    uniform = false;
                    break;
                }
            }
            if !uniform {
                let mut value = level[0];
                for &next in &level[1..] {
                    value = self.binary_expression(origin, result, op, value, next)?;
                }
                return self.cast(origin, value, result);
            }
            let mut pairs = Vec::with_capacity(level.len().div_ceil(2));
            for chunk in level.chunks(2) {
                pairs.push(if let [a, b] = chunk {
                    self.binary_expression(origin, result, op, *a, *b)?
                } else {
                    chunk[0]
                });
            }
            level = pairs;
        }
        self.cast(origin, level[0], result)
    }
}
