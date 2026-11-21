//! A projected tuple holds only monotone fields, in source-column order.

use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, TypeId,
};

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Projection {
    Scalar {
        monotone: bool,
    },
    /// A function or reference can occupy a source slot that the destination skips.
    Other {
        monotone: bool,
    },
    Tuple {
        fields: Vec<Self>,
        raw: bool,
        nullable: bool,
    },
}

// Beta reduction substitutes a fresh read of the parameter at each use.
// The variable itself remains shared by all of the reads.
fn read_field(
    edit: &mut CircuitEditor,
    value: ExpressionId,
    index: usize,
) -> Result<ExpressionId, Error> {
    fn copy_read(edit: &mut CircuitEditor, value: ExpressionId) -> Result<ExpressionId, Error> {
        match edit.expr_kind(value)? {
            Expr::Deref | Expr::Field => {
                let base = edit.expression(edit.node(value.into())?.edge("expression")?)?;
                let base = copy_read(edit, base)?;
                let id = edit.derive_inner(
                    value.into(),
                    [("expression", Field::Reference(base.into()))],
                )?;
                edit.expression(id)
            }
            Expr::VariablePath => Ok(value),
            _ => Err(Error::new("expected a projection of a bound parameter")),
        }
    }
    let read = copy_read(edit, value)?;
    edit.tuple_field(read, index)
}

impl Projection {
    pub fn monotone(&self) -> bool {
        match self {
            Self::Scalar { monotone } | Self::Other { monotone } => *monotone,
            Self::Tuple {
                fields, nullable, ..
            } => fields.iter().any(Self::monotone) || fields.is_empty() && !nullable,
        }
    }

    /// Build the reduced apply closure over an already compacted waterline.
    pub fn project_waterline(
        &self,
        edit: &mut CircuitEditor,
        input: TypeId,
        destination: &Self,
        declared: Option<TypeId>,
        next: &mut u64,
    ) -> Result<ExpressionId, Error> {
        // The unreduced projection reserves one name before createApply rebinds it.
        let mut names = next
            .checked_add(1)
            .ok_or_else(|| Error::new("generated names exhausted"))?;
        let param = edit.ref_parameter(input, &mut names)?;
        let read = edit.deref_parameter(&param)?;
        let value = self.project_with_type(edit, read, destination, declared)?;
        let function = edit.closure(value, &[param])?;
        *next = names;
        Ok(function)
    }

    /// Project an already compacted bound into a subset of its source fields.
    pub fn project(
        &self,
        edit: &mut CircuitEditor,
        value: ExpressionId,
        destination: &Self,
    ) -> Result<ExpressionId, Error> {
        self.project_with_type(edit, value, destination, None)
    }

    fn project_with_type(
        &self,
        edit: &mut CircuitEditor,
        value: ExpressionId,
        destination: &Self,
        declared: Option<TypeId>,
    ) -> Result<ExpressionId, Error> {
        match (self, destination) {
            (Self::Scalar { monotone: a }, Self::Scalar { monotone: b }) if *a || !b => Ok(value),
            (
                Self::Tuple { fields: from, .. },
                Self::Tuple {
                    fields: to, raw, ..
                },
            ) if from.len() == to.len() => {
                let declared_fields = declared
                    .map(|ty| edit.node(ty.into())?.edges("tupFields"))
                    .transpose()?;
                if declared_fields
                    .as_ref()
                    .is_some_and(|fields| fields.len() != to.len())
                {
                    return Err(Error::new(
                        "declared tuple and waterline projection differ in width",
                    ));
                }
                let mut projected = Vec::new();
                let mut index = 0;
                for (column, (src, dst)) in from.iter().zip(to).enumerate() {
                    if dst.monotone() {
                        let ty = declared_fields
                            .as_ref()
                            .map(|f| edit.type_id(f[column]))
                            .transpose()?;
                        if src.monotone() {
                            let field = read_field(edit, value, index)?;
                            projected.push(src.project_with_type(edit, field, dst, ty)?);
                        } else if let Self::Tuple { fields, raw, .. } = dst {
                            if !fields.is_empty() {
                                return Err(Error::new("missing monotone tuple field"));
                            }
                            // An absent input contributes the declared empty tuple. Several
                            // destinations can share its type even across limiter operators.
                            projected.push(match ty {
                                Some(ty) => edit.tuple_with_type(ty, &[])?,
                                None => edit.tuple_expr(value.into(), *raw, &[])?,
                            });
                        } else {
                            return Err(Error::new("missing scalar bound"));
                        }
                    }
                    index += usize::from(src.monotone());
                }
                edit.tuple_expr(value.into(), *raw, &projected)
            }
            _ => Err(Error::new("incompatible monotone projections")),
        }
    }

    /// Merge a declared view waterline with the inherited bound. Columns
    /// present on both sides take their maximum, recursively across tuples.
    pub fn merge(
        &self,
        edit: &mut CircuitEditor,
        left: ExpressionId,
        other: &Self,
        right: ExpressionId,
    ) -> Result<ExpressionId, Error> {
        match (self, other) {
            (Self::Scalar { monotone: false }, Self::Scalar { .. }) => Ok(right),
            (Self::Scalar { .. }, Self::Scalar { monotone: false }) => Ok(left),
            (Self::Scalar { .. }, Self::Scalar { .. }) => edit.binary_expression(
                left.into(),
                edit.expression_type(left)?,
                BinaryOp::AggMax,
                left,
                right,
            ),
            (Self::Tuple { fields: a, .. }, Self::Tuple { fields: b, .. })
                if a.len() == b.len() =>
            {
                let mut fields = Vec::new();
                let mut li = 0;
                let mut ri = 0;
                for (l, r) in a.iter().zip(b) {
                    let lv = l
                        .monotone()
                        .then(|| read_field(edit, left, li))
                        .transpose()?;
                    let rv = r
                        .monotone()
                        .then(|| read_field(edit, right, ri))
                        .transpose()?;
                    li += usize::from(lv.is_some());
                    ri += usize::from(rv.is_some());
                    match (lv, rv) {
                        (Some(x), Some(y)) => fields.push(l.merge(edit, x, r, y)?),
                        (Some(x), None) | (None, Some(x)) => fields.push(x),
                        (None, None) => (),
                    }
                }
                edit.tuple_expr(left.into(), false, &fields)
            }
            _ => Err(Error::new("cannot merge incompatible waterlines")),
        }
    }
}
