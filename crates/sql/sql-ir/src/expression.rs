//! Typed construction of expressions in the circuit's inner arena.

mod aggregate;
mod binding;
pub use aggregate::OperatorSite;
mod closure;
pub use closure::RefParameter;
mod binary;
pub use binary::BinaryOp;
mod tuple;
mod type_equality;
mod types;
mod values;

use crate::{
    CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeKind as Ty,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExpressionId(NodeId);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TypeId(NodeId);

impl From<ExpressionId> for NodeId {
    fn from(expr: ExpressionId) -> Self {
        expr.0
    }
}

impl From<TypeId> for NodeId {
    fn from(ty: TypeId) -> Self {
        ty.0
    }
}

impl CircuitEditor {
    pub fn expression(&self, id: NodeId) -> Result<ExpressionId, Error> {
        match self.node(id)?.inner_kind() {
            Some(InnerKind::Expression(_)) => Ok(ExpressionId(id)),
            _ => Err(Error::new(format!("expected an expression at {id:?}"))),
        }
    }

    pub fn type_id(&self, id: NodeId) -> Result<TypeId, Error> {
        match self.node(id)?.inner_kind() {
            Some(InnerKind::Type(_)) => Ok(TypeId(id)),
            _ => Err(Error::new(format!("expected a type at {id:?}"))),
        }
    }

    pub fn expression_type(&self, expr: ExpressionId) -> Result<TypeId, Error> {
        self.type_id(self.node(expr.0)?.edge("type")?)
    }

    pub fn type_kind(&self, ty: TypeId) -> Result<Ty, Error> {
        match self.node(ty.0)?.inner_kind() {
            Some(InnerKind::Type(kind)) => Ok(*kind),
            _ => Err(Error::new("expected a type")),
        }
    }

    pub fn expr_kind(&self, expr: ExpressionId) -> Result<Expr, Error> {
        match self.node(expr.0)?.inner_kind() {
            Some(InnerKind::Expression(kind)) => Ok(*kind),
            _ => Err(Error::new("expected an expression")),
        }
    }

    pub fn nullable(&self, ty: TypeId) -> Result<bool, Error> {
        self.node(ty.0)?
            .data("mayBeNull")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| Error::new("type requires a boolean mayBeNull field"))
    }

    pub fn with_nullability(&mut self, ty: TypeId, nullable: bool) -> Result<TypeId, Error> {
        let kind = self.type_kind(ty)?;
        if kind == Ty::Keyword {
            return Err(Error::new(
                "KEYWORD does not support SQL nullability coercion",
            ));
        }
        if self.nullable(ty)? == nullable {
            return Ok(ty);
        }
        // The timestamp factories intern precisely the types with an empty origin.
        if matches!(kind, Ty::Timestamp | Ty::TimestampTz)
            && (self
                .node(ty.0)?
                .data("sourceEmpty")
                .and_then(|v| v.as_bool())
                == Some(true)
                || self.is_registered_singleton(ty)?)
        {
            let code = if kind == Ty::Timestamp {
                "TIMESTAMP"
            } else {
                "TIMESTAMP_TZ"
            };
            return self.primitive_singleton(ty.0, kind, code, nullable);
        }
        let id = self.change_nullability(ty.0, nullable)?;
        Ok(TypeId(id))
    }

    pub fn make_expr(
        &mut self,
        origin: NodeId,
        kind: Expr,
        ty: TypeId,
        fields: impl IntoIterator<Item = (&'static str, Field)>,
    ) -> Result<ExpressionId, Error> {
        // For these constructors Java reads the type before any operands. Emit
        // its definition here if the operands share it with their parent.
        let type_first = matches!(
            kind,
            Expr::Apply
                | Expr::ApplyMethod
                | Expr::Array
                | Expr::Binary
                | Expr::Closure
                | Expr::If
                | Expr::Map
                | Expr::Path
                | Expr::RawTuple
                | Expr::TimeAddSub
                | Expr::Tuple
                | Expr::Unary
                | Expr::Unwrap
        );
        let type_field = || ("type", Field::Reference(ty.0));
        let fields = type_first
            .then(type_field)
            .into_iter()
            .chain(fields)
            .chain((!type_first).then(type_field));
        self.inner_at(origin, InnerKind::Expression(kind), fields)
            .map(ExpressionId)
    }

    /// Project a field of a tuple. A nullable row makes its field nullable.
    /// A runtime function or method with one path segment and no generic arguments.
    pub fn function_path(&mut self, site: NodeId, name: &str) -> Result<ExpressionId, Error> {
        let any = self.standard_type(site, Ty::Any)?;
        self.named_path(any, name)
    }

    /// Reference a named item with a single path segment and its declared type.
    pub fn named_path(&mut self, ty: TypeId, name: &str) -> Result<ExpressionId, Error> {
        let segment = self.empty_origin_node(
            ty.into(),
            InnerKind::Other("DBSPSimplePathSegment".into()),
            [
                ("identifier", Field::Data(name.into())),
                ("genericArgs", Field::Array(vec![])),
            ],
        )?;
        let path = self.empty_origin_node(
            ty.into(),
            InnerKind::Other("DBSPPath".into()),
            [("components", Field::Array(vec![Field::Reference(segment)]))],
        )?;
        self.make_expr(
            ty.into(),
            Expr::Path,
            ty,
            [("path", Field::Reference(path))],
        )
    }

    /// A field access through a nullable row also produces a nullable value.
    pub fn tuple_field_type(&mut self, ty: TypeId, index: usize) -> Result<TypeId, Error> {
        if self.type_kind(ty)? == Ty::Any {
            return Ok(ty);
        }
        let fields = self.node(ty.0)?.edges("tupFields")?;
        let field = *fields
            .get(index)
            .ok_or_else(|| Error::new(format!("tuple has no field {index}")))?;
        let field = self.type_id(field)?;
        if self.nullable(ty)? {
            self.with_nullability(field, true)
        } else {
            Ok(field)
        }
    }

    pub fn tuple_field(&mut self, row: ExpressionId, index: usize) -> Result<ExpressionId, Error> {
        let ty = self.expression_type(row)?;
        let result = self.tuple_field_type(ty, index)?;
        let nullable = self.nullable(ty)?;
        if matches!(self.expr_kind(row)?, Expr::Tuple | Expr::RawTuple) {
            if let Some(Field::Array(values)) = self.node(row.0)?.field("fields") {
                let value = values
                    .get(index)
                    .and_then(Field::node_id)
                    .ok_or_else(|| Error::new("tuple constructor does not match its type"))?;
                let value = self.expression(value)?;
                if !nullable || self.nullable(self.expression_type(value)?)? {
                    return Ok(value);
                }
                return self.lift_nullable(value);
            }
            return self.typed_null(result);
        }
        if self.expr_kind(row)? == Expr::Unwrap
            && self.node(row.0)?.data("message").and_then(|v| v.as_str()) == Some("")
        {
            let inner = self.expression(self.node(row.0)?.edge("expression")?)?;
            let projected = self.tuple_field(inner, index)?;
            if self.same_type(self.expression_type(projected)?, result)? {
                return Ok(projected);
            }
            let output = self.with_nullability(self.expression_type(projected)?, false)?;
            return self.make_expr(
                row.0,
                Expr::Unwrap,
                output,
                [
                    ("expression", Field::Reference(projected.0)),
                    ("message", Field::Data("".into())),
                ],
            );
        }
        self.empty_origin_node(
            row.0,
            InnerKind::Expression(Expr::Field),
            [
                ("expression", Field::Reference(row.0)),
                ("fieldNo", Field::Data(index.into())),
                ("type", Field::Reference(result.0)),
            ],
        )
        .map(ExpressionId)
    }

    fn lift_nullable(&mut self, expr: ExpressionId) -> Result<ExpressionId, Error> {
        self.some_at(expr.0, expr)
    }

    /// Rebuild a Some expression, folding a literal or a tuple into its nullable form.
    pub fn some_at(&mut self, origin: NodeId, expr: ExpressionId) -> Result<ExpressionId, Error> {
        let ty = self.expression_type(expr)?;
        let result = self.with_nullability(ty, true)?;
        if ty == result {
            return Ok(expr);
        }
        let kind = self.expr_kind(expr)?;
        if (kind.wire_name().ends_with("Literal")
            && !matches!(
                kind,
                Expr::KeywordLiteral
                    | Expr::StrLiteral
                    | Expr::VariantNullLiteral
                    | Expr::VoidLiteral
            ))
            || kind == Expr::Tuple
        {
            return self
                .derive_inner(expr.0, [("type", Field::Reference(result.0))])
                .map(ExpressionId);
        }
        self.make_expr(
            origin,
            Expr::Some,
            result,
            [("expression", Field::Reference(expr.0))],
        )
    }

    /// Whether moving a borrowed value of this type into a row requires a clone.
    pub fn requires_clone(&self, ty: TypeId) -> Result<bool, Error> {
        Ok(matches!(
            self.type_kind(ty)?,
            Ty::Tuple
                | Ty::RawTuple
                | Ty::String
                | Ty::Uuid
                | Ty::GeoPoint
                | Ty::Binary
                | Ty::Variant
                | Ty::User
                | Ty::Array
                | Ty::Map
                | Ty::ZSet
                | Ty::IndexedZSet
                | Ty::Vec
                | Ty::TypedBox
                | Ty::BTreeMap
                | Ty::WithCustomOrd
                | Ty::Result
                | Ty::Option
                | Ty::Weight
                | Ty::SqlResult
                | Ty::Lazy
                | Ty::Semigroup
        ))
    }

    /// Acquire an owned field when lowering tuple constructors and join closures.
    pub fn clone_if_needed(&mut self, value: ExpressionId) -> Result<ExpressionId, Error> {
        let ty = self.expression_type(value)?;
        let non_copy = self.requires_clone(ty)?;
        let kind = self.expr_kind(value)?;
        if !non_copy
            || kind.wire_name().ends_with("Literal")
            || matches!(
                kind,
                Expr::Clone
                    | Expr::GeoPointConstructor
                    | Expr::If
                    | Expr::Tuple
                    | Expr::RawTuple
                    | Expr::Variant
                    | Expr::Array
                    | Expr::Map
                    | Expr::ZSet
                    | Expr::IndexedZSet
            )
        {
            return Ok(value);
        }
        let produced = if kind == Expr::Unwrap {
            self.node(value.0)?.edge("expression")?
        } else {
            value.0
        };
        if self.node(produced)?.data("opcode").and_then(|v| v.as_str()) == Some("COALESCE") {
            return Ok(value);
        }
        self.clone_value(value)
    }

    /// Emit an explicit clone, including for copyable types, when the Java
    /// constructor leaves it for a later simplification pass.
    pub fn clone_value(&mut self, value: ExpressionId) -> Result<ExpressionId, Error> {
        let ty = self.expression_type(value)?;
        if self.type_kind(ty)? == Ty::Ref {
            return Err(Error::new("cannot clone a reference"));
        }
        if self.expr_kind(value)? == Expr::Clone {
            return Ok(value);
        }
        self.make_expr(
            value.0,
            Expr::Clone,
            ty,
            [("expression", Field::Reference(value.0))],
        )
    }
}
