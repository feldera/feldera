//! A projected tuple holds only monotone fields, in source-column order.

use feldera_sql_ir::{BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field};

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Projection {
    Scalar {
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
            Self::Scalar { monotone } => *monotone,
            Self::Tuple {
                fields, nullable, ..
            } => fields.iter().any(Self::monotone) || fields.is_empty() && !nullable,
        }
    }

    /// Project an already compacted bound into a subset of its source fields.
    pub fn project(
        &self,
        edit: &mut CircuitEditor,
        value: ExpressionId,
        destination: &Self,
    ) -> Result<ExpressionId, Error> {
        match (self, destination) {
            (Self::Scalar { monotone: a }, Self::Scalar { monotone: b }) if *a || !b => Ok(value),
            (
                Self::Tuple { fields: from, .. },
                Self::Tuple {
                    fields: to, raw, ..
                },
            ) if from.len() == to.len() => {
                let mut projected = Vec::new();
                let mut index = 0;
                for (src, dst) in from.iter().zip(to) {
                    if dst.monotone() {
                        if src.monotone() {
                            let field = read_field(edit, value, index)?;
                            projected.push(src.project(edit, field, dst)?);
                        } else if let Self::Tuple { fields, raw, .. } = dst {
                            if !fields.is_empty() {
                                return Err(Error::new("missing monotone tuple field"));
                            }
                            projected.push(edit.tuple_expr(value.into(), *raw, &[])?);
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
