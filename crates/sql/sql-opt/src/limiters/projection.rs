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
    Reference {
        base: Box<Self>,
    },
    /// An opaque function can occupy a source slot that the destination skips.
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
    /// Project the analyzed row's types. Equal upstream waterline types can
    /// have different identities when a nullable field becomes nonnullable.
    pub(super) fn projected_type(
        &self,
        edit: &mut CircuitEditor,
        ty: TypeId,
    ) -> Result<TypeId, Error> {
        if let Self::Reference { base } = self {
            let referent = edit.type_id(edit.node(ty.into())?.edge("type")?)?;
            let projected = base.projected_type(edit, referent)?;
            return edit.reference_type(projected);
        }
        let Self::Tuple { fields, raw, .. } = self else {
            return edit.with_nullability(ty, false);
        };
        let members = edit.node(ty.into())?.edges("tupFields")?;
        if members.len() != fields.len() {
            return Err(Error::new(
                "analyzed row and waterline projection differ in width",
            ));
        }
        let mut kept = Vec::new();
        for (field, member) in fields.iter().zip(members) {
            if field.monotone() {
                kept.push(field.projected_type(edit, edit.type_id(member)?)?.into());
            }
        }
        edit.tuple_type(ty.into(), *raw, kept)
    }

    pub(super) fn with_unbounded_value_from(&self, right: &Self) -> Result<Self, Error> {
        fn clear_fields(p: &Projection) -> Projection {
            match p {
                Projection::Tuple {
                    fields,
                    raw,
                    nullable,
                } => Projection::Tuple {
                    fields: fields.iter().map(clear_fields).collect(),
                    raw: *raw,
                    nullable: *nullable,
                },
                Projection::Scalar { .. } => Projection::Scalar { monotone: false },
                Projection::Reference { base } => Projection::Reference {
                    base: Box::new(clear_fields(base)),
                },
                Projection::Other { .. } => Projection::Other { monotone: false },
            }
        }
        let (Self::Tuple { fields: output, .. }, Self::Tuple { fields: input, .. }) = (self, right)
        else {
            return Err(Error::new("expected indexed antijoin projections"));
        };
        let ([key, _], [_, value]) = (output.as_slice(), input.as_slice()) else {
            return Err(Error::new("antijoin projection requires a key and value"));
        };
        Ok(Self::Tuple {
            fields: vec![key.clone(), clear_fields(value)],
            raw: true,
            nullable: false,
        })
    }

    /// Select monotone fields from the original data row before comparing it to its bound.
    pub(super) fn project_data(
        &self,
        edit: &mut CircuitEditor,
        value: ExpressionId,
    ) -> Result<ExpressionId, Error> {
        match self {
            Self::Scalar { monotone: true } => Ok(value),
            Self::Reference { base } => {
                let referent = edit.dereference(value)?;
                let projected = base.project_data(edit, referent)?;
                edit.borrow_value(projected)
            }
            Self::Tuple { fields, raw, .. } => {
                let mut selected = Vec::new();
                for (index, projection) in fields.iter().enumerate() {
                    if projection.monotone() {
                        let field = edit.tuple_field(value, index)?;
                        selected.push(projection.project_data(edit, field)?);
                    }
                }
                edit.tuple_expr(value.into(), *raw, &selected)
            }
            _ => Err(Error::new(
                "cannot project a non-monotone or non-data field for key retention",
            )),
        }
    }

    pub fn monotone(&self) -> bool {
        match self {
            Self::Scalar { monotone } | Self::Other { monotone } => *monotone,
            Self::Reference { base } => base.monotone(),
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

    /// Build the reduced apply2 closure for a view that has both an inherited
    /// waterline and its own LATENESS declarations.
    pub fn merge_view_waterline(
        &self,
        edit: &mut CircuitEditor,
        inherited: TypeId,
        declared: &Self,
        declared_type: TypeId,
        next: &mut u64,
    ) -> Result<ExpressionId, Error> {
        // Java builds two parameters before createApply2 rebinds their reads.
        let mut names = next
            .checked_add(2)
            .ok_or_else(|| Error::new("generated names exhausted"))?;
        let left = edit.ref_parameter(inherited, &mut names)?;
        let right = edit.ref_parameter(declared_type, &mut names)?;
        let lhs = edit.deref_parameter(&left)?;
        let rhs = edit.deref_parameter(&right)?;
        let merged = self.merge(edit, lhs, declared, rhs)?;
        let closure = edit.closure(merged, &[left, right])?;
        *next = names;
        Ok(closure)
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
