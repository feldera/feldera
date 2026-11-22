//! Reduce the structural part of a row closure to its projected waterline.
mod function;
mod scalar;
mod scope;
mod shape;
use self::{
    scope::Local,
    shape::{Shape, UnorderedRow},
};
use crate::limiters::Projection;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, Node, NodeId,
    TypeId,
};
use std::collections::HashSet;

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
        initializers: Vec<Option<Self>>,
        result: Box<Self>,
    },
    Parameter {
        index: usize,
    },
    Borrow {
        value: Box<Self>,
    },
    Deref {
        value: Box<Self>,
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
    constant: bool,
    pub row_type: RowType,
    pub reduced: Option<ExpressionId>,
}

struct Binding {
    name: String,
    variable: ExpressionId,
    shape: Shape,
}

struct Row {
    bindings: Vec<Binding>,
    locals: Vec<Local>,
    clock_operands: Option<HashSet<ExpressionId>>,
}

fn field_number(node: &Node) -> Result<usize, Error> {
    node.data("fieldNo")
        .and_then(|n| n.as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| Error::new("expected row field index"))
}

fn unbounded(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
) -> Result<Option<ProjectedValue>, Error> {
    let ty = edit.expression_type(expression)?;
    let Some(UnorderedRow { shape, .. }) = UnorderedRow::from_type(edit, ty)? else {
        return Ok(None);
    };
    // A nonnullable empty tuple has a bound even when it occurs inside an
    // otherwise unknown row. Its construction belongs to the general visitor.
    if shape.monotone() {
        return Ok(None);
    }
    Ok(Some(ProjectedValue {
        shape,
        constant: false,
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
    /// Java visits the operands of these expressions, then assigns an unbounded
    /// result regardless of their bounds. Visit them here as well so an
    /// unsupported operand can still take the normal diagnostic path.
    fn opaque(
        &mut self,
        edit: &mut CircuitEditor,
        expression: ExpressionId,
        depth: usize,
    ) -> Result<Option<ProjectedValue>, Error> {
        let mut children = Vec::new();
        for (_, field) in edit.node(expression.into())?.fields() {
            field.collect_inner_edges(&mut children);
        }
        for child in children {
            match edit.node(child)?.inner_kind() {
                Some(InnerKind::Type(_)) => {}
                Some(InnerKind::Expression(_)) => {
                    if self
                        .visit(edit, edit.expression(child)?, depth + 1)?
                        .is_none()
                    {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            }
        }
        unbounded(edit, expression)
    }

    fn visit(
        &mut self,
        edit: &mut CircuitEditor,
        expression: ExpressionId,
        depth: usize,
    ) -> Result<Option<ProjectedValue>, Error> {
        let value = self.project(edit, expression, depth)?;
        if let Some(operands) = &mut self.clock_operands {
            if value.as_ref().is_some_and(|v| v.shape.monotone()) {
                operands.insert(expression);
            }
        }
        Ok(value)
    }

    fn project(
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
            Expr::Let | Expr::Block => return self.scope(edit, expression, depth),
            Expr::VariablePath => return self.variable(edit, expression),
            kind if scalar::literal(kind) => {
                return scalar::literal_bound(edit, expression);
            }
            Expr::Apply => return self.call(edit, expression, depth),
            Expr::Binary | Expr::TimeAddSub => return self.binary(edit, expression, depth),
            // A nested lambda has its own binding scope. The monotonicity
            // visitor assigns it an opaque function type without entering it.
            Expr::Closure
            | Expr::Path
            | Expr::NullLiteral
            | Expr::KeywordLiteral
            | Expr::StrLiteral
            | Expr::VoidLiteral
            | Expr::NoExpression
            | Expr::StringLiteral
            | Expr::BinaryLiteral
            | Expr::UuidLiteral
            | Expr::VariantNullLiteral => return unbounded(edit, expression),
            Expr::If
            | Expr::IsNull
            | Expr::Array
            | Expr::Map
            | Expr::ApplyMethod
            | Expr::Constructor
            | Expr::Lazy
            | Expr::Question
            | Expr::Variant
            | Expr::GeoPointConstructor
            | Expr::UnwrapCustomOrd
            | Expr::CustomOrd
            | Expr::HandleError
            | Expr::Assignment
            | Expr::ConditionalIncrement
            | Expr::Return
            | Expr::QualifyType
            | Expr::CustomOrdField
            | Expr::DirectComparator
            | Expr::FieldComparator
            | Expr::EqualityComparator
            | Expr::AsymmetricFieldComparator
            | Expr::NoComparator
            | Expr::WindowBound
            | Expr::Sort
            | Expr::Static
            | Expr::EnumValue
            | Expr::ZSet
            | Expr::IndexedZSet => {
                return self.opaque(edit, expression, depth);
            }
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
                    let Some(mut unknown) = unbounded(edit, expression)? else {
                        return Ok(None);
                    };
                    unknown.constant = value.constant;
                    return Ok(Some(unknown));
                };
                ProjectedValue::bounded(edit, expression, bound, value.constant)?
            }
            Expr::Borrow | Expr::Deref => {
                let base = edit.expression(node.edge("expression")?)?;
                let Some(value) = self.visit(edit, base, depth + 1)? else {
                    return Ok(None);
                };
                if kind == Expr::Borrow {
                    // For an unbounded borrow, Java constructs the reference
                    // from the declared expression type, including nested fields.
                    if value.reduced.is_none() {
                        let Some(mut result) = unbounded(edit, expression)? else {
                            return Ok(None);
                        };
                        result.constant = value.constant;
                        return Ok(Some(result));
                    }
                    ProjectedValue {
                        shape: Shape::Reference(value.shape.into()),
                        row_type: RowType::Borrow {
                            value: Box::new(value.row_type),
                        },
                        reduced: value.reduced.map(|v| edit.borrow_value(v)).transpose()?,
                        constant: value.constant,
                    }
                } else {
                    let Shape::Reference(base) = value.shape else {
                        return Ok(None);
                    };
                    ProjectedValue {
                        shape: (*base).clone(),
                        row_type: RowType::Deref {
                            value: Box::new(value.row_type),
                        },
                        reduced: value.reduced.map(|v| edit.dereference(v)).transpose()?,
                        constant: value.constant,
                    }
                }
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
                    constant: false,
                    row_type: RowType::Field {
                        row: Box::new(base.row_type),
                        index,
                    },
                    reduced,
                }
            }
            Expr::RawTuple | Expr::Tuple => {
                if node.field("fields").is_none() {
                    return unbounded(edit, expression);
                }
                let mut fields = Vec::new();
                let mut types = Vec::new();
                let mut bounds = Vec::new();
                let mut constant = true;
                for field in node.edges("fields")? {
                    let Some(value) = self.visit(edit, edit.expression(field)?, depth + 1)? else {
                        return Ok(None);
                    };
                    constant &= value.constant;
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
                    constant,
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

/// A temporal filter's input has no declared waterline. Record the original
/// operand IDs as the row visitor encounters them, including inside LETs.
pub(crate) fn clock_operands(
    edit: &mut CircuitEditor,
    parameter: NodeId,
    body: ExpressionId,
) -> Result<Option<HashSet<ExpressionId>>, Error> {
    let node = edit.node(parameter)?;
    let ty = edit.type_id(node.edge("type")?)?;
    let Some(name) = node
        .data("name")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
    else {
        return Ok(None);
    };
    let Some(UnorderedRow {
        shape: shape @ Shape::Reference(_),
        projected,
    }) = UnorderedRow::from_type(edit, ty)?
    else {
        return Ok(None);
    };
    let Some(references) =
        crate::lexical_scope::predicate_references(edit.circuit(), parameter, body.into())?
    else {
        return Ok(None);
    };
    for (read, declaration) in references {
        if declaration == parameter
            && !edit.same_type(edit.expression_type(edit.expression(read)?)?, ty)?
        {
            return Ok(None);
        }
    }
    let variable = edit.named_variable(projected.unwrap_or(ty), &name)?;
    let mut row = Row {
        bindings: vec![Binding {
            name,
            variable,
            shape,
        }],
        locals: Vec::new(),
        clock_operands: Some(HashSet::new()),
    };
    if row.visit(edit, body, 0)?.is_none() {
        return Ok(None);
    }
    Ok(row.clock_operands)
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
    if (indexed && (projections.len() != 2 || bounds.len() != 1))
        || (!indexed && projections.len() != bounds.len())
    {
        return Err(Error::new(
            "waterline shapes do not match closure parameters",
        ));
    }
    let Some(mut shapes) = projections
        .iter()
        .map(|p| Shape::from_input(p).map(|shape| Shape::Reference(shape.into())))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    if indexed {
        // The original pair holds (&key, &value). Ordinary field projection
        // computes the compressed index when either reference lacks a bound.
        shapes = vec![Shape::tuple(shapes, false)];
    }
    let mut bindings = Vec::with_capacity(bounds.len());
    for (&variable, shape) in bounds.iter().zip(shapes) {
        let name = edit
            .node(variable.into())?
            .data("variable")
            .and_then(|n| n.as_str())
            .ok_or_else(|| Error::new("expected a projected row variable"))?
            .to_owned();
        bindings.push(Binding {
            name,
            variable,
            shape,
        });
    }
    Row {
        bindings,
        locals: Vec::new(),
        clock_operands: None,
    }
    .visit(edit, body, 0)
}

/// The bound of a whole closure and the SQL type recipe used by its consumers.
/// A supported closure can have no bounded output fields.
pub struct ProjectedClosure {
    pub row_type: RowType,
    pub closure: Option<ExpressionId>,
}

/// Project the body and bind its reduced reads to the Apply operator's input.
/// Keep both operations in the same arena so repeated reads share the same variable.
pub fn project_closure(
    edit: &mut CircuitEditor,
    body: ExpressionId,
    parameters: &[(NodeId, TypeId)],
    projections: &[Projection],
    indexed: bool,
) -> Result<Option<ProjectedClosure>, Error> {
    // Projection skips the bodies of nested lambdas, but their references
    // and expression occurrences must still be valid before accepting the closure.
    let body_id = body.into();
    if !feldera_sql_expr::is_expression_tree(edit, body_id)? {
        return Ok(None);
    }
    let declared = parameters.iter().map(|&(parameter, _)| parameter);
    let Some(references) =
        crate::lexical_scope::body_references(edit.circuit(), declared, body_id)?
    else {
        return Ok(None);
    };
    for (read, binding) in references {
        let declaration = edit.node(binding)?;
        if matches!(declaration.inner_kind(), Some(InnerKind::Parameter)) {
            let expected = edit.type_id(declaration.edge("type")?)?;
            if !edit.same_type(edit.expression_type(edit.expression(read)?)?, expected)? {
                return Ok(None);
            }
        }
    }
    let mut variables = Vec::with_capacity(parameters.len());
    let mut names = Vec::with_capacity(parameters.len());
    for &(parameter, projected) in parameters {
        let node = edit.node(parameter)?;
        let name = node
            .data("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::new("waterline parameter has no name"))?
            .to_owned();
        let ty = if indexed {
            super::identity::tuple_of_references(edit, projected)?
        } else {
            edit.reference_type(projected)?
        };
        variables.push(edit.named_variable(ty, &name)?);
        names.push(name);
    }
    let Some(value) = project_rows(edit, body, &variables, projections, indexed)? else {
        return Ok(None);
    };
    let closure = match (value.reduced, parameters) {
        (None, _) => None,
        (Some(body), [_]) if !indexed => {
            let path = variables[0];
            let parameter = edit.parameter_at(edit.expression_type(path)?.into(), path)?;
            Some(edit.closure_with_parameters(body, &[parameter])?)
        }
        (Some(body), _) => {
            let inputs: Vec<_> = names
                .iter()
                .zip(parameters)
                .map(|(name, &(_, ty))| (name.as_str(), ty))
                .collect();
            Some(super::bindings::waterline_bindings(edit, &inputs)?.closure(edit, body)?)
        }
    };
    Ok(Some(ProjectedClosure {
        row_type: value.row_type,
        closure,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn clock_operands_keep_nested_unit_fields_in_the_compressed_row() -> Result<(), Error> {
        let arena = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        let site = arena.root_id();
        let mut edit = CircuitEditor::new(arena);
        let flag = edit.bool_type(site, false)?;
        let unit = edit.tuple_type(site, false, [])?;
        let nullable = edit.with_nullability(unit, true)?;
        let nested = edit.tuple_type(site, true, [nullable.into(), unit.into()])?;
        let row = edit.tuple_type(site, false, [flag.into(), nested.into(), nested.into()])?;
        let reference = edit.reference_type(row)?;
        let read = edit.named_variable(reference, "row")?;
        let parameter = edit.parameter_at(site, read)?;
        let input = edit.dereference(read)?;
        let pair = edit.tuple_field(input, 2)?;
        let kept = edit.tuple_field(pair, 1)?;
        let dropped = edit.tuple_field(pair, 0)?;
        let bounds = clock_operands(&mut edit, parameter, kept)?.unwrap();
        assert!(bounds.contains(&kept));
        assert!(!clock_operands(&mut edit, parameter, dropped)?
            .unwrap()
            .contains(&dropped));
        let projected = UnorderedRow::from_type(&mut edit, row)?
            .unwrap()
            .projected
            .unwrap();
        let fields = edit.node(projected.into())?.edges("tupFields")?;
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], fields[1]);
        assert_eq!(edit.node(fields[0])?.class(), "DBSPTypeRawTuple");
        assert_eq!(edit.node(fields[0])?.edges("tupFields")?.len(), 1);
        Ok(())
    }

    #[test]
    fn each_tail_binding_inherits_its_consumers_clock_bound() -> Result<(), Error> {
        let arena = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        let site = arena.root_id();
        let mut edit = CircuitEditor::new(arena);
        let unit = edit.tuple_type(site, false, [])?;
        let row = edit.tuple_type(site, false, [unit.into()])?;
        let reference = edit.reference_type(row)?;
        let input = edit.named_variable(reference, "row")?;
        let parameter = edit.parameter_at(site, input)?;
        let mut tail = edit.named_variable(unit, "v")?;
        let mut scopes = Vec::new();
        for i in 0..256 {
            // Each inner initializer reads the preceding declaration of the same name.
            let initializer = if i == 255 {
                let row = edit.dereference(input)?;
                edit.tuple_field(row, 0)?
            } else {
                edit.named_variable(unit, "v")?
            };
            let variable = edit.named_variable(unit, "v")?;
            tail = edit.let_expression(variable, initializer, tail)?;
            scopes.push(tail);
        }
        let bounds = clock_operands(&mut edit, parameter, tail)?.expect("bound on the unit field");
        assert!(scopes.iter().all(|scope| bounds.contains(scope)));
        Ok(())
    }

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
