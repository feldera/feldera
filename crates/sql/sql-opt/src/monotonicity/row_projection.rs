//! Reduce the structural part of a row closure to its projected waterline.
use crate::limiters::Projection;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field};

/// Rebuild the analyzed type using the original input's field objects. Equal SQL
/// types from the closure body can have different source positions and sharing.
#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowType {
    Input,
    Field {
        row: Box<Self>,
        index: usize,
    },
    Tuple {
        fields: Vec<Self>,
        raw: bool,
        nullable: bool,
    },
    WithNullability {
        value: Box<Self>,
        nullable: bool,
    },
}

pub struct ProjectedValue {
    shape: Projection,
    pub row_type: RowType,
    pub reduced: Option<ExpressionId>,
}

struct Row<'a> {
    projection: &'a Projection,
    bound: ExpressionId,
    parameter: &'a str,
}

impl Row<'_> {
    fn visit(
        &self,
        edit: &mut CircuitEditor,
        expression: ExpressionId,
        depth: usize,
    ) -> Result<Option<ProjectedValue>, Error> {
        if depth > 128 {
            return Ok(None);
        }
        let id = expression.into();
        let node = edit.node(id)?;
        let kind = edit.expr_kind(expression)?;
        let value = match kind {
            Expr::Deref => {
                let base = edit.expression(node.edge("expression")?)?;
                if edit.expr_kind(base)? != Expr::VariablePath
                    || edit
                        .node(base.into())?
                        .data("variable")
                        .and_then(|n| n.as_str())
                        != Some(self.parameter)
                {
                    return Ok(None);
                }
                ProjectedValue {
                    shape: self.projection.clone(),
                    row_type: RowType::Input,
                    reduced: self
                        .projection
                        .monotone()
                        .then(|| edit.dereference(self.bound))
                        .transpose()?,
                }
            }
            Expr::Field => {
                let base = edit.expression(node.edge("expression")?)?;
                let index = node
                    .data("fieldNo")
                    .and_then(|n| n.as_u64())
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| Error::new("expected row field index"))?;
                let Some(base) = self.visit(edit, base, depth + 1)? else {
                    return Ok(None);
                };
                let Projection::Tuple {
                    fields, nullable, ..
                } = base.shape
                else {
                    return Ok(None);
                };
                let Some(field) = fields.get(index) else {
                    return Ok(None);
                };
                let mut shape = field.clone();
                if let Projection::Tuple {
                    nullable: inner, ..
                } = &mut shape
                {
                    *inner |= nullable;
                }
                let reduced = if shape.monotone() {
                    let Some(row) = base.reduced else {
                        return Ok(None);
                    };
                    let compressed = fields[..index]
                        .iter()
                        .filter(|field| field.monotone())
                        .count();
                    Some(edit.tuple_field(row, compressed)?)
                } else {
                    None
                };
                ProjectedValue {
                    shape,
                    row_type: RowType::Field {
                        row: Box::new(base.row_type),
                        index,
                    },
                    reduced,
                }
            }
            Expr::RawTuple | Expr::Tuple => {
                if node.field("fields").is_none() {
                    return Ok(None);
                }
                let mut fields = Vec::new();
                let mut types = Vec::new();
                let mut bounds = Vec::new();
                for field in node.edges("fields")? {
                    let Some(value) = self.visit(edit, edit.expression(field)?, depth + 1)? else {
                        return Ok(None);
                    };
                    fields.push(value.shape);
                    types.push(value.row_type);
                    bounds.extend(value.reduced);
                }
                let raw = kind == Expr::RawTuple;
                let nullable = edit.nullable(edit.expression_type(expression)?)?;
                let shape = Projection::Tuple {
                    fields,
                    raw,
                    nullable,
                };
                let reduced = if !shape.monotone() {
                    None
                } else if raw {
                    Some(edit.tuple_expr(id, true, &bounds)?)
                } else {
                    let types = bounds
                        .iter()
                        .map(|&v| Ok(edit.expression_type(v)?.into()))
                        .collect::<Result<Vec<_>, Error>>()?;
                    let ty = edit.tuple_type(id, false, types)?;
                    Some(edit.tuple_with_type_at(id, ty, &bounds)?)
                };
                ProjectedValue {
                    shape,
                    row_type: RowType::Tuple {
                        fields: types,
                        raw,
                        nullable,
                    },
                    reduced,
                }
            }
            Expr::Clone | Expr::Some | Expr::Unwrap => {
                let base = edit.expression(node.edge("expression")?)?;
                let Some(mut value) = self.visit(edit, base, depth + 1)? else {
                    return Ok(None);
                };
                if kind == Expr::Clone {
                    if let Some(bound) = value.reduced {
                        let ty = edit.expression_type(bound)?;
                        value.reduced = Some(edit.make_expr(
                            id,
                            Expr::Clone,
                            ty,
                            [("expression", Field::Reference(bound.into()))],
                        )?);
                    }
                } else {
                    let nullable = kind == Expr::Some;
                    value.row_type = RowType::WithNullability {
                        value: Box::new(value.row_type),
                        nullable,
                    };
                    if let Projection::Tuple {
                        nullable: tuple_nullable,
                        ..
                    } = &mut value.shape
                    {
                        *tuple_nullable = nullable;
                    }
                    // Changing the nullability of an empty tuple changes whether it
                    // has a bound. Let the general analysis handle that transition.
                    if value.shape.monotone() != value.reduced.is_some() {
                        return Ok(None);
                    }
                }
                value
            }
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

pub fn project_row(
    edit: &mut CircuitEditor,
    body: ExpressionId,
    bound: ExpressionId,
    projection: &Projection,
) -> Result<Option<ProjectedValue>, Error> {
    // This visitor handles data rows. References and closure-typed columns need
    // their original binding information in the general monotonicity visitor.
    fn data_row(shape: &Projection) -> bool {
        match shape {
            Projection::Other { .. } => false,
            Projection::Scalar { .. } => true,
            Projection::Tuple { fields, .. } => fields.iter().all(data_row),
        }
    }
    if !data_row(projection) {
        return Ok(None);
    }
    let parameter = edit
        .node(bound.into())?
        .data("variable")
        .and_then(|n| n.as_str())
        .ok_or_else(|| Error::new("expected a projected row variable"))?
        .to_owned();
    Row {
        projection,
        bound,
        parameter: &parameter,
    }
    .visit(edit, body, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn nested_field_reads_use_compressed_row_indexes() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?;
        let mut edit = CircuitEditor::new(circuit);
        let site = edit.circuit().root_id();
        let leaf = edit.bool_type(site, false)?;
        let nested = edit.tuple_type(site, false, [leaf.into(), leaf.into()])?;
        let row = edit.tuple_type(site, false, [leaf.into(), leaf.into(), nested.into()])?;
        let reference = edit.reference_type(row)?;
        let parameter = edit.named_variable(reference, "row")?;
        let row = edit.dereference(parameter)?;
        let first = edit.tuple_field(row, 0)?;
        let second = edit.tuple_field(row, 1)?;
        let third = edit.tuple_field(row, 2)?;
        let nested = edit.tuple_field(third, 1)?;
        let output = edit.tuple_expr(site, false, &[nested, first, second])?;
        let kept = edit.tuple_type(site, false, [leaf.into()])?;
        let compact = edit.tuple_type(site, false, [leaf.into(), kept.into()])?;
        let reference = edit.reference_type(compact)?;
        let bound = edit.named_variable(reference, "row")?;
        let scalar = |monotone| Projection::Scalar { monotone };
        let tuple = |fields| Projection::Tuple {
            fields,
            raw: false,
            nullable: false,
        };
        let shape = tuple(vec![
            scalar(false),
            scalar(true),
            tuple(vec![scalar(false), scalar(true)]),
        ]);
        let result = project_row(&mut edit, output, bound, &shape)?.unwrap();
        let Projection::Tuple { fields, .. } = result.shape else {
            panic!("expected a tuple")
        };
        assert_eq!(
            fields.iter().map(Projection::monotone).collect::<Vec<_>>(),
            [true, false, true]
        );
        let reads = edit.node(result.reduced.unwrap().into())?.edges("fields")?;
        assert_eq!(reads.len(), 2);
        let nested = edit.node(reads[0])?;
        assert_eq!(nested.data("fieldNo"), Some(&json!(0)));
        assert_eq!(
            edit.node(nested.edge("expression")?)?.data("fieldNo"),
            Some(&json!(1))
        );
        assert_eq!(edit.node(reads[1])?.data("fieldNo"), Some(&json!(0)));
        Ok(())
    }
}
