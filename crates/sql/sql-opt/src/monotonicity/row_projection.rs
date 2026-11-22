//! Reduce the structural part of a row closure to its projected waterline.
mod shape;
use self::shape::Shape;
use crate::limiters::Projection;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, Node, NodeId, TypeId,
    TypeKind,
};

/// Rebuild the analyzed type using the original input's field objects. Equal SQL
/// types from the closure body can have different source positions and sharing.
#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowType {
    /// Use the declared SQL type of an unbounded expression in the original closure.
    Unbounded {
        type_id: u64,
    },
    BoundedScalar {
        type_id: u64,
    },
    Local {
        index: usize,
    },
    Scope {
        initializers: Vec<Self>,
        result: Box<Self>,
    },
    Input {
        index: usize,
    },
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
    shape: Shape,
    pub row_type: RowType,
    pub reduced: Option<ExpressionId>,
}

struct Binding {
    name: String,
    variable: ExpressionId,
    shape: Shape,
    /// Indexed closures receive a pair of references. A missing bound moves
    /// the other reference to field zero of the compacted pair.
    field: Option<(usize, usize)>,
}

struct Local {
    name: String,
    ty: TypeId,
    shape: Shape,
    reduced: Option<ExpressionId>,
}

struct Row {
    bindings: Vec<Binding>,
    locals: Vec<Local>,
}

fn field_number(node: &Node) -> Result<usize, Error> {
    node.data("fieldNo")
        .and_then(|n| n.as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| Error::new("expected row field index"))
}

/// Unknown tuple fields can still be read by a subsequent field expression.
/// Retain their layout, including the nullability of each nested tuple.
fn unbounded_shape(edit: &CircuitEditor, ty: TypeId, depth: usize) -> Result<Option<Shape>, Error> {
    if depth > 128 {
        return Ok(None);
    }
    let kind = edit.type_kind(ty)?;
    Ok(Some(match kind {
        TypeKind::RawTuple | TypeKind::Tuple => {
            let mut fields = Vec::new();
            for member in edit.node(ty.into())?.edges("tupFields")? {
                let Some(field) = unbounded_shape(edit, edit.type_id(member)?, depth + 1)? else {
                    return Ok(None);
                };
                fields.push(field);
            }
            Shape::tuple(fields, edit.nullable(ty)?)
        }
        TypeKind::Array
        | TypeKind::Binary
        | TypeKind::Bool
        | TypeKind::Date
        | TypeKind::Decimal
        | TypeKind::Double
        | TypeKind::GeoPoint
        | TypeKind::Integer
        | TypeKind::LongInterval
        | TypeKind::Map
        | TypeKind::Null
        | TypeKind::Real
        | TypeKind::ShortInterval
        | TypeKind::String
        | TypeKind::Time
        | TypeKind::Timestamp
        | TypeKind::TimestampTz
        | TypeKind::Uuid
        | TypeKind::Variant => Shape::Scalar(false),
        _ => return Ok(None),
    }))
}

fn unbounded(
    edit: &CircuitEditor,
    expression: ExpressionId,
    depth: usize,
) -> Result<Option<ProjectedValue>, Error> {
    let ty = edit.expression_type(expression)?;
    let Some(shape) = unbounded_shape(edit, ty, depth)? else {
        return Ok(None);
    };
    // A nonnullable empty tuple has a bound even when it occurs inside an
    // otherwise unknown row. Its construction belongs to the general visitor.
    if shape.monotone() {
        return Ok(None);
    }
    Ok(Some(ProjectedValue {
        shape,
        row_type: RowType::Unbounded {
            type_id: type_number(ty),
        },
        reduced: None,
    }))
}

fn type_number(ty: TypeId) -> u64 {
    let NodeId::Inner(number) = NodeId::from(ty) else {
        unreachable!("type has an inner identity")
    };
    number
}

impl Row {
    fn local(&self, name: &str) -> Option<(usize, &Local)> {
        self.locals
            .iter()
            .enumerate()
            .rev()
            .find(|(_, local)| local.name == name)
    }

    /// Visit each initializer before it shadows the previous binding, just as
    /// the declaration resolver does. A second read reuses the original bound.
    fn scoped(
        &mut self,
        edit: &mut CircuitEditor,
        declarations: &[(String, ExpressionId)],
        body: ExpressionId,
        depth: usize,
    ) -> Result<Option<ProjectedValue>, Error> {
        let outer = self.locals.len();
        let mut initializers = Vec::with_capacity(declarations.len());
        for (name, expression) in declarations {
            let Some(value) = self.visit(edit, *expression, depth + 1)? else {
                self.locals.truncate(outer);
                return Ok(None);
            };
            initializers.push(value.row_type);
            self.locals.push(Local {
                name: name.clone(),
                ty: edit.expression_type(*expression)?,
                shape: value.shape,
                reduced: value.reduced,
            });
        }
        let result = self.visit(edit, body, depth + 1);
        self.locals.truncate(outer);
        let Some(mut value) = result? else {
            return Ok(None);
        };
        if !initializers.is_empty() {
            value.row_type = RowType::Scope {
                initializers,
                result: Box::new(value.row_type),
            };
        }
        Ok(Some(value))
    }

    fn dereference(
        &self,
        edit: &mut CircuitEditor,
        base: ExpressionId,
    ) -> Result<Option<ProjectedValue>, Error> {
        let node = edit.node(base.into())?;
        let (variable, field) = match edit.expr_kind(base)? {
            Expr::VariablePath => (node, None),
            Expr::Field => (
                edit.node(node.edge("expression")?)?,
                Some(field_number(node)?),
            ),
            _ => return Ok(None),
        };
        let Some(name) = variable.data("variable").and_then(|n| n.as_str()) else {
            return Ok(None);
        };
        if self.local(name).is_some() {
            return Ok(None);
        }
        let Some((index, binding)) = self
            .bindings
            .iter()
            .enumerate()
            .find(|(_, b)| b.name == name && b.field.map(|(original, _)| original) == field)
        else {
            return Ok(None);
        };
        let reduced = if binding.shape.monotone() {
            let reference = match binding.field {
                Some((_, compact)) => edit.tuple_field(binding.variable, compact)?,
                None => binding.variable,
            };
            Some(edit.dereference(reference)?)
        } else {
            None
        };
        Ok(Some(ProjectedValue {
            shape: binding.shape.clone(),
            row_type: RowType::Input { index },
            reduced,
        }))
    }

    fn visit(
        &mut self,
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
            Expr::Let => {
                let variable = edit.node(node.edge("variable")?)?;
                let Some(name) = variable.data("variable").and_then(|v| v.as_str()) else {
                    return Ok(None);
                };
                let binding = (name.to_owned(), edit.expression(node.edge("initializer")?)?);
                let body = edit.expression(node.edge("consumer")?)?;
                return self.scoped(edit, &[binding], body, depth);
            }
            Expr::Block => {
                let Some(body) = node.field("lastExpression").and_then(Field::node_id) else {
                    return Ok(None);
                };
                let body = edit.expression(body)?;
                let mut declarations = Vec::new();
                for id in node.edges("contents")? {
                    let statement = edit.node(id)?;
                    if statement.class() != "DBSPLetStatement"
                        || statement.data("mutable").and_then(|v| v.as_bool()) != Some(false)
                    {
                        return Ok(None);
                    }
                    let Some(initializer) = statement.field("initializer").and_then(Field::node_id)
                    else {
                        return Ok(None);
                    };
                    let name = statement
                        .data("variable")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| Error::new("expected a name for a row waterline binding"))?;
                    declarations.push((name.to_owned(), edit.expression(initializer)?));
                }
                return self.scoped(edit, &declarations, body, depth);
            }
            Expr::VariablePath => {
                let Some((index, local)) = node
                    .data("variable")
                    .and_then(|v| v.as_str())
                    .and_then(|name| self.local(name))
                else {
                    return Ok(None);
                };
                if !edit.same_type(edit.expression_type(expression)?, local.ty)? {
                    return Ok(None);
                }
                ProjectedValue {
                    shape: local.shape.clone(),
                    row_type: RowType::Local { index },
                    reduced: local.reduced,
                }
            }
            Expr::NoExpression
            | Expr::StringLiteral
            | Expr::BinaryLiteral
            | Expr::UuidLiteral
            | Expr::VariantNullLiteral => return unbounded(edit, expression, depth),
            Expr::Cast | Expr::Unary | Expr::UnsignedWrap | Expr::UnsignedUnwrap => {
                let source = edit.expression(node.edge("source")?)?;
                let Some(value) = self.visit(edit, source, depth + 1)? else {
                    return Ok(None);
                };
                if !matches!(value.shape, Shape::Scalar(_)) {
                    return Ok(None);
                }
                let reduced = match value.reduced {
                    None => None,
                    Some(bound) if kind == Expr::Cast => {
                        super::casts::cast_bound(edit, expression, bound)?
                    }
                    Some(bound) => super::unary_waterline(edit, expression, bound)?,
                };
                let Some(bound) = reduced else {
                    return unbounded(edit, expression, depth);
                };
                ProjectedValue {
                    shape: Shape::Scalar(true),
                    row_type: RowType::BoundedScalar {
                        type_id: type_number(edit.expression_type(expression)?),
                    },
                    reduced: Some(bound),
                }
            }
            Expr::Deref => {
                let base = edit.expression(node.edge("expression")?)?;
                return self.dereference(edit, base);
            }
            Expr::Field => {
                let base = edit.expression(node.edge("expression")?)?;
                let index = field_number(node)?;
                let Some(base) = self.visit(edit, base, depth + 1)? else {
                    return Ok(None);
                };
                let Shape::Tuple {
                    fields, nullable, ..
                } = base.shape
                else {
                    return Ok(None);
                };
                let Some(field) = fields.get(index) else {
                    return Ok(None);
                };
                let mut shape = field.clone();
                if let Shape::Tuple {
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
                    return unbounded(edit, expression, depth);
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
                let shape = Shape::tuple(fields, nullable);
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
                    if let Shape::Tuple {
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
    project_rows(
        edit,
        body,
        &[bound],
        std::slice::from_ref(projection),
        false,
    )
}

/// Join closures take separate reference parameters. Indexed closures take
/// one pair of references to the key and value.
pub fn project_rows(
    edit: &mut CircuitEditor,
    body: ExpressionId,
    bounds: &[ExpressionId],
    projections: &[Projection],
    indexed: bool,
) -> Result<Option<ProjectedValue>, Error> {
    // This visitor handles data rows. References and closure-typed columns need
    // their original binding information in the general monotonicity visitor.
    if (indexed && (projections.len() != 2 || bounds.len() != 1))
        || (!indexed && projections.len() != bounds.len())
    {
        return Err(Error::new(
            "waterline shapes do not match closure parameters",
        ));
    }
    let mut bindings = Vec::with_capacity(projections.len());
    let mut compact = 0;
    for (index, shape) in projections.iter().enumerate() {
        let Some(shape) = Shape::from_input(shape) else {
            return Ok(None);
        };
        let variable = bounds[if indexed { 0 } else { index }];
        let name = edit
            .node(variable.into())?
            .data("variable")
            .and_then(|n| n.as_str())
            .ok_or_else(|| Error::new("expected a projected row variable"))?
            .to_owned();
        let monotone = shape.monotone();
        bindings.push(Binding {
            name,
            variable,
            shape,
            field: indexed.then_some((index, compact)),
        });
        compact += usize::from(monotone);
    }
    Row {
        bindings,
        locals: Vec::new(),
    }
    .visit(edit, body, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn repeated_row_aliases_keep_a_linear_type_recipe() -> Result<(), Error> {
        let arena = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?;
        let mut edit = CircuitEditor::new(arena);
        let site = edit.circuit().root_id();
        let boolean = edit.bool_type(site, false)?;
        let reference = edit.reference_type(boolean)?;
        let parameter = edit.named_variable(reference, "input")?;
        let mut value = edit.dereference(parameter)?;
        let mut declarations = Vec::new();
        for n in 0..24 {
            let name = format!("v{n}");
            declarations.push(Field::Reference(edit.let_statement(&name, value)?));
            let ty = edit.expression_type(value)?;
            let first = edit.named_variable(ty, &name)?;
            let second = edit.named_variable(ty, &name)?;
            value = edit.tuple_expr(site, false, &[first, second])?;
        }
        let body = edit.make_expr(
            site,
            Expr::Block,
            edit.expression_type(value)?,
            [
                ("contents", Field::Array(declarations)),
                ("lastExpression", Field::Reference(value.into())),
            ],
        )?;
        let shape = Projection::Scalar { monotone: true };
        let projected = project_row(&mut edit, body, parameter, &shape)?.unwrap();
        let recipe = serde_json::to_value(projected.row_type).unwrap();
        assert_eq!(recipe["initializers"].as_array().unwrap().len(), 24);
        for (index, pair) in recipe["initializers"]
            .as_array()
            .unwrap()
            .iter()
            .skip(1)
            .enumerate()
        {
            assert_eq!(
                pair["fields"],
                json!([
                    {"kind": "local", "index": index}, {"kind": "local", "index": index}
                ])
            );
        }
        let mut row = projected.reduced.unwrap();
        for _ in 0..24 {
            let fields = edit.node(row.into())?.edges("fields")?;
            assert_eq!(fields[0], fields[1]);
            row = edit.expression(fields[0])?;
        }
        assert_eq!(edit.expr_kind(row)?, Expr::Deref);
        Ok(())
    }

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
        let Shape::Tuple { fields, .. } = result.shape else {
            panic!("expected a tuple")
        };
        assert_eq!(
            fields.iter().map(Shape::monotone).collect::<Vec<_>>(),
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
