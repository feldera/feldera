//! Reduce the structural part of a row closure to its projected waterline.
mod evaluate;
mod function;
mod reads;
pub use reads::RowRead;
mod scalar;
mod scope;
mod shape;
use self::{
    evaluate::Evaluation,
    scope::Local,
    shape::{Shape, UnorderedRow, UnorderedType},
};
use crate::limiters::Projection;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, InnerKind, Node, NodeId, TypeId,
};
use std::collections::{HashMap, HashSet};

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
    /// Type of an earlier expression in the same postorder analysis.
    Prior {
        index: usize,
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

impl ProjectedValue {
    /// Java can describe an empty row as monotone even when the expression
    /// producing it supplies no bound. Only a consumer of that bound needs it.
    fn missing_bound(&self) -> bool {
        self.reduced.is_none() && self.shape.monotone()
    }
}

struct Binding {
    name: String,
    /// Type before waterline projection, when the caller supplies the declaration.
    declared_type: Option<TypeId>,
    variable: ExpressionId,
    shape: Shape,
}

/// One expression as observed by a caller querying intermediate waterlines.
pub enum FactType {
    /// Reads of one parameter share the parameter's original analysis.
    Parameter(usize),
    /// A local read or the result of a block or LET reuses an earlier analysis.
    Alias(usize),
    Computed(RowType),
}

pub struct ExpressionWaterline {
    pub expression: ExpressionId,
    pub origin: FactType,
    pub bound: Option<ExpressionId>,
}

#[derive(Default)]
enum Observations {
    #[default]
    ResultOnly,
    Clocks(HashSet<ExpressionId>),
    Facts(Vec<ExpressionWaterline>),
}

struct Row {
    bindings: Vec<Binding>,
    locals: Vec<Local>,
    observations: Observations,
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
    constant: bool,
) -> Result<Evaluation, Error> {
    unbounded_type(edit, edit.expression_type(expression)?, constant)
}

fn unbounded_type(
    edit: &mut CircuitEditor,
    ty: TypeId,
    constant: bool,
) -> Result<Evaluation, Error> {
    let shape = match UnorderedRow::from_type(edit, ty)? {
        UnorderedType::Row(row) => row.shape,
        UnorderedType::Unsupported(field) => {
            return Ok(Evaluation::Failed(WaterlineError::UnsupportedType(field)));
        }
    };
    Ok(Evaluation::Value(ProjectedValue {
        shape,
        constant,
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

fn project_wrapper(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
    value: ProjectedValue,
) -> Result<Evaluation, Error> {
    let kind = edit.expr_kind(expression)?;
    let read = match kind {
        Expr::Borrow => RowRead::Borrow,
        Expr::Deref => RowRead::Deref,
        Expr::Field => RowRead::Field {
            index: field_number(edit.node(expression.into())?)?,
        },
        Expr::Clone => RowRead::Clone,
        Expr::Some => RowRead::Some,
        Expr::Unwrap => RowRead::Unwrap,
        _ => unreachable!("expected an expression wrapper"),
    };
    read.apply(
        edit,
        expression.into(),
        edit.expression_type(expression)?,
        value,
    )
}

/// Assemble the bounded fields in their original order. SQL rows retain the
/// constructor's source location. Raw tuples use an empty location.
pub fn tuple_waterline(
    edit: &mut CircuitEditor,
    site: NodeId,
    raw: bool,
    fields: &[ExpressionId],
) -> Result<ExpressionId, Error> {
    if raw {
        return edit.tuple_expr(site, true, fields);
    }
    let types = fields
        .iter()
        .map(|&field| Ok(edit.expression_type(field)?.into()))
        .collect::<Result<Vec<_>, Error>>()?;
    let ty = edit.tuple_type(site, false, types)?;
    edit.tuple_with_type_at(site, ty, fields)
}

fn project_tuple(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
    values: impl Iterator<Item = ProjectedValue>,
) -> Result<Option<ProjectedValue>, Error> {
    let id = expression.into();
    let mut fields = Vec::new();
    let mut types = Vec::new();
    let mut bounds = Vec::new();
    let mut constant = true;
    for value in values {
        constant &= value.constant;
        fields.push(value.shape);
        types.push(value.row_type);
        bounds.extend(value.reduced);
    }
    let raw = edit.expr_kind(expression)? == Expr::RawTuple;
    let nullable = edit.nullable(edit.expression_type(expression)?)?;
    let shape = Shape::tuple(fields, nullable);
    let reduced = shape
        .monotone()
        .then(|| tuple_waterline(edit, id, raw, &bounds))
        .transpose()?;
    Ok(Some(ProjectedValue {
        shape,
        constant,
        row_type: RowType::Tuple {
            fields: types,
            raw,
            nullable,
        },
        reduced,
    }))
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
    let UnorderedType::Row(UnorderedRow { shape, projected }) = UnorderedRow::from_type(edit, ty)?
    else {
        return Ok(None);
    };
    if crate::lexical_scope::predicate_references(edit.circuit(), parameter, body.into())?.is_none()
    {
        return Ok(None);
    }
    let variable = edit.named_variable(projected.unwrap_or(ty), &name)?;
    let mut row = Row {
        bindings: vec![Binding {
            name,
            declared_type: Some(ty),
            variable,
            shape,
        }],
        locals: Vec::new(),
        observations: Observations::Clocks(HashSet::new()),
    };
    if !matches!(row.visit(edit, body)?, Evaluation::Value(_)) {
        return Ok(None);
    }
    let Observations::Clocks(clocks) = row.observations else {
        unreachable!()
    };
    Ok(Some(clocks))
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
    let mut row = Row::new(edit, bounds, projections, indexed)?;
    Ok(row.visit(edit, body)?.projected())
}

impl Row {
    fn new(
        edit: &CircuitEditor,
        bounds: &[ExpressionId],
        projections: &[Projection],
        indexed: bool,
    ) -> Result<Self, Error> {
        if (indexed && (projections.len() != 2 || bounds.len() != 1))
            || (!indexed && projections.len() != bounds.len())
        {
            return Err(Error::new(
                "waterline shapes do not match closure parameters",
            ));
        }
        let mut shapes: Vec<_> = projections
            .iter()
            .map(|p| Shape::Reference(Shape::from_input(p).into()))
            .collect();
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
                declared_type: None,
                variable,
                shape,
            });
        }
        Ok(Self {
            bindings,
            locals: Vec::new(),
            observations: Observations::ResultOnly,
        })
    }
}

/// The bound of a whole closure and the SQL type recipe used by its consumers.
/// A supported closure can have no bounded output fields.
pub struct ProjectedClosure {
    pub facts: Vec<ExpressionWaterline>,
    pub row_type: RowType,
    pub closure: Option<ExpressionId>,
}

/// Bindings and diagnostics for the general visitor, all in the original node-ID space.
pub enum WaterlineInspection {
    Free(NodeId),
    Conflict {
        variable: NodeId,
        previous: NodeId,
        current: NodeId,
    },
    Bound {
        references: HashMap<NodeId, NodeId>,
        repeated: Vec<NodeId>,
    },
}

impl WaterlineInspection {
    fn inspect(
        edit: &CircuitEditor,
        resolution: crate::LexicalResolution<'_>,
        body: Option<NodeId>,
    ) -> Result<Self, Error> {
        use crate::LexicalResolution;
        Ok(match resolution {
            LexicalResolution::Free { variable, .. } => Self::Free(variable),
            LexicalResolution::Conflict {
                variable,
                previous,
                current,
            } => Self::Conflict {
                variable,
                previous,
                current,
            },
            LexicalResolution::Bound(references) => {
                let repeated = match body {
                    Some(body) => feldera_sql_expr::first_repeated_expression(edit, body)?,
                    None => None,
                };
                Self::Bound {
                    references,
                    repeated: repeated.unwrap_or_default(),
                }
            }
        })
    }

    pub fn for_root(edit: &CircuitEditor, root: NodeId) -> Result<Self, Error> {
        let node = edit.node(root)?;
        let body = match node.inner_kind() {
            Some(InnerKind::Expression(Expr::Closure)) => Some(node.edge("body")?),
            _ => None,
        };
        Self::inspect(edit, crate::lexical_references(edit.circuit(), root)?, body)
    }
}

pub enum WaterlineError {
    MissingOperand(ExpressionId),
    TargetMaximum(ExpressionId),
    UnsupportedType(TypeId),
    /// Adding nullability to a reference fails at the live reference type's
    /// source location. A nullable tuple also lifts each field as it is read.
    NullableReference {
        source: RowType,
        field: Option<usize>,
    },
}

pub enum ClosureAnalysis {
    Projected(ProjectedClosure),
    /// Earlier facts must be published before the caller reports the error.
    Failed {
        error: WaterlineError,
        facts: Vec<ExpressionWaterline>,
    },
    Visit(WaterlineInspection),
}

/// Bind projected reads to the Apply input. If the general visitor is needed,
/// give it the results of this inspection so it can resume at the closure body.
pub fn project_closure(
    edit: &mut CircuitEditor,
    body: ExpressionId,
    parameters: &[(NodeId, TypeId)],
    projections: &[Projection],
    indexed: bool,
) -> Result<ClosureAnalysis, Error> {
    analyze_closure(
        edit,
        body,
        parameters,
        projections,
        indexed,
        Observations::ResultOnly,
    )
}

/// Include the intermediate expressions queried by callers of the general visitor.
pub fn expression_waterlines(
    edit: &mut CircuitEditor,
    body: ExpressionId,
    parameters: &[(NodeId, TypeId)],
    projections: &[Projection],
    indexed: bool,
) -> Result<ClosureAnalysis, Error> {
    analyze_closure(
        edit,
        body,
        parameters,
        projections,
        indexed,
        Observations::Facts(Vec::new()),
    )
}

fn analyze_closure(
    edit: &mut CircuitEditor,
    body: ExpressionId,
    parameters: &[(NodeId, TypeId)],
    projections: &[Projection],
    indexed: bool,
    observations: Observations,
) -> Result<ClosureAnalysis, Error> {
    let declared = parameters.iter().map(|&(parameter, _)| parameter);
    let resolution = crate::lexical_scope::resolve_body(edit.circuit(), declared, body.into())?;
    let inspection = WaterlineInspection::inspect(edit, resolution, Some(body.into()))?;
    let WaterlineInspection::Bound { repeated, .. } = &inspection else {
        return Ok(ClosureAnalysis::Visit(inspection));
    };
    if !repeated.is_empty() {
        return Ok(ClosureAnalysis::Visit(inspection));
    }
    let result_only = matches!(observations, Observations::ResultOnly);
    let mut result =
        project_validated_closure(edit, body, parameters, projections, indexed, observations)?;
    if result_only && matches!(result, Some(ClosureAnalysis::Failed { .. })) {
        // Errors expose the facts computed before the failing read, even to a
        // caller that originally asked only for the closure result.
        result = project_validated_closure(
            edit,
            body,
            parameters,
            projections,
            indexed,
            Observations::Facts(Vec::new()),
        )?;
    }
    Ok(result.unwrap_or(ClosureAnalysis::Visit(inspection)))
}

fn project_validated_closure(
    edit: &mut CircuitEditor,
    body: ExpressionId,
    parameters: &[(NodeId, TypeId)],
    projections: &[Projection],
    indexed: bool,
    observations: Observations,
) -> Result<Option<ClosureAnalysis>, Error> {
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
    let mut row = Row::new(edit, &variables, projections, indexed)?;
    for (binding, &(parameter, _)) in row.bindings.iter_mut().zip(parameters) {
        binding.declared_type = Some(edit.type_id(edit.node(parameter)?.edge("type")?)?);
    }
    row.observations = observations;
    let result = row.visit(edit, body)?;
    let facts = match row.observations {
        Observations::Facts(facts) => facts,
        _ => Vec::new(),
    };
    let value = match result {
        Evaluation::Value(value) => value,
        Evaluation::Failed(error) => {
            return Ok(Some(ClosureAnalysis::Failed { error, facts }));
        }
        Evaluation::NeedsVisitor => return Ok(None),
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
    Ok(Some(ClosureAnalysis::Projected(ProjectedClosure {
        facts,
        row_type: value.row_type,
        closure,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{Circuit, Field};
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
        assert!(
            !clock_operands(&mut edit, parameter, dropped)?
                .unwrap()
                .contains(&dropped)
        );
        let UnorderedType::Row(shape) = UnorderedRow::from_type(&mut edit, row)? else {
            panic!("expected a projected row");
        };
        let projected = shape.projected.unwrap();
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
