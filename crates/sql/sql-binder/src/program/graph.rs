//! Insert a bound scalar and its relation into the whole-program arenas.

use super::{Failure, USER, unsupported};
use feldera_sql_rel_model::{
    TypeId,
    catalog::{Scan, Table},
    expression_objects::{
        Environment, EnvironmentId, ExpressionObject, ObjectKind, ObjectPosition, RelationUse,
        SourceContext,
    },
    expressions::{ExprId, ExprKind, ExpressionTable, OperatorId, ResolvedOperator},
    input_producers::{InputProducer, RelationForm},
    input_uses::{
        InputEnvironment, InputEnvironmentId, InputOwner, InputReference, InputReferenceId,
        InputUse,
    },
    literals::{Literal, LiteralValue},
    operators::{OperatorObject, OperatorObjectId},
    program::Program,
    relation_metadata::{Coordinates, Hint, RelationMetadata},
    relation_properties::{Aggregate, Properties, RelationProperty},
    source::{Identifier, Origin},
    types::{Field, IntegerWidth, SqlType, StructKind, TypeKind},
};

pub(super) fn append_expression(
    program: &mut Program,
    fragment: ExpressionTable,
) -> Result<(ExprId, TypeId, bool), Failure> {
    let [root] = fragment.roots.as_slice() else {
        return Err(unsupported("expected one bound projection root"));
    };
    let root_expr = &fragment.expressions[root.0 as usize];
    let folded = matches!(root_expr.kind, ExprKind::Literal { .. });
    let type_map: Vec<_> = fragment
        .type_table
        .types
        .into_iter()
        .map(|ty| insert_type(&mut program.table.type_table, ty))
        .collect();
    let ty = |id: TypeId| type_map[id.0 as usize];
    let object_offset = program.expressions.operator_objects.len() as u32;
    let operator_offset = program.expressions.operators.len() as u32;
    let expression_offset = program.expressions.expressions.len() as u32;
    program
        .expressions
        .operator_objects
        .extend(fragment.operator_objects);
    program
        .expressions
        .operators
        .extend(
            fragment
                .operators
                .into_iter()
                .map(|operator| ResolvedOperator {
                    object: OperatorObjectId(object_offset + operator.object.0),
                    result: ty(operator.result),
                    parameters: operator.parameters.into_iter().map(ty).collect(),
                }),
        );
    let scalar = ty(root_expr.ty);
    for expression in fragment.expressions {
        let position = match expression.origin {
            Origin::Absent {} => ObjectPosition::Absent {},
            Origin::Source {
                source: USER,
                start,
                end,
            } => ObjectPosition::Present {
                position: Coordinates {
                    line: start.line,
                    column: start.column,
                    end_line: end.line,
                    end_column: end.column,
                    quoted: false,
                },
                internal: Some(false),
            },
            _ => return Err(unsupported("unexpected constant expression source")),
        };
        let kind = match expression.kind {
            ExprKind::Literal { mut literal } => {
                literal.ty = ty(literal.ty);
                ObjectKind::Literal { literal }
            }
            ExprKind::Call {
                operator,
                arguments,
            } => ObjectKind::Call {
                operator: OperatorId(operator_offset + operator.0),
                arguments: arguments
                    .into_iter()
                    .map(|id| ExprId(expression_offset + id.0))
                    .collect(),
            },
            _ => return Err(unsupported("expected a literal or a checked constant call")),
        };
        program.expressions.expressions.push(ExpressionObject {
            ty: ty(expression.ty),
            position,
            kind,
        });
    }
    Ok((ExprId(expression_offset + root.0), scalar, folded))
}

pub(super) fn insert_row(program: &mut Program, name: &str, scalar: TypeId) -> TypeId {
    insert_row_fields(program, [(name, scalar)])
}

pub(super) fn insert_row_fields<'a>(
    program: &mut Program,
    fields: impl IntoIterator<Item = (&'a str, TypeId)>,
) -> TypeId {
    let fields = fields
        .into_iter()
        .map(|(name, ty)| Field {
            name: Identifier {
                name: name.into(),
                quoted: true,
                origin: Origin::Absent {},
            },
            ty,
        })
        .collect();
    insert_type(
        &mut program.table.type_table,
        SqlType {
            nullable: false,
            origin: Origin::Absent {},
            kind: TypeKind::Row {
                struct_kind: StructKind::PeekFieldsNoExpand,
                fields,
            },
        },
    )
}

#[derive(Clone, Copy)]
pub(super) struct ProjectInput {
    pub(super) field: u32,
    pub(super) ty: TypeId,
}

#[derive(Clone)]
pub(super) enum ProjectValue {
    Column(ProjectInput),
    Integer {
        ty: TypeId,
        value: i64,
    },
    Call {
        registry: &'static str,
        key: &'static str,
        arguments: Vec<TableOperand>,
        ty: TypeId,
        position: Option<Coordinates>,
    },
}

#[derive(Clone)]
pub(super) enum TableOperand {
    Column(ProjectInput),
    Integer { ty: TypeId, value: i64 },
    Computed(Box<ProjectValue>),
}

impl TableOperand {
    fn ty(&self) -> TypeId {
        match self {
            Self::Column(input) => input.ty,
            Self::Integer { ty, .. } => *ty,
            Self::Computed(expression) => expression.ty(),
        }
    }
}

impl ProjectValue {
    pub(super) fn ty(&self) -> TypeId {
        match self {
            Self::Column(input) => input.ty,
            Self::Call { ty, .. } | Self::Integer { ty, .. } => *ty,
        }
    }
}

pub(super) fn append_table_project<'a>(
    program: &mut Program,
    output_row: TypeId,
    scan_row: TypeId,
    projection: impl IntoIterator<Item = &'a ProjectValue>,
) -> u32 {
    let relation = program.table.relations;
    // The whole-program graph numbers a relation before its children.
    let inputs = vec![relation + 1];
    let (input, environment) = table_input(program, scan_row, &inputs);
    let roots = projection
        .into_iter()
        .map(|value| append_table_expression(program, input, value))
        .collect::<Vec<_>>();
    append_relation(
        program,
        InputProducer {
            form: RelationForm::LogicalProject,
            row: output_row,
            inputs,
            constants: Vec::new(),
        },
        Properties::Project {
            expressions: roots.len() as u32,
            variables: Vec::new(),
        },
        environment,
        roots,
        Vec::new(),
    );
    relation
}

pub(super) fn append_table_filter(
    program: &mut Program,
    row: TypeId,
    predicate: &ProjectValue,
) -> u32 {
    let relation = program.table.relations;
    let inputs = vec![relation + 1];
    let (input, environment) = table_input(program, row, &inputs);
    let root = append_table_expression(program, input, predicate);
    append_relation(
        program,
        InputProducer {
            form: RelationForm::LogicalFilter,
            row,
            inputs,
            constants: Vec::new(),
        },
        Properties::Filter {
            variables: Vec::new(),
        },
        environment,
        vec![root],
        Vec::new(),
    );
    relation
}

pub(super) fn append_aggregate(
    program: &mut Program,
    output_row: TypeId,
    input_row: TypeId,
    group: Vec<u32>,
    calls: Vec<Aggregate>,
) -> u32 {
    let relation = program.table.relations;
    let inputs = vec![relation + 1];
    let (_, environment) = table_input(program, input_row, &inputs);
    append_relation(
        program,
        InputProducer {
            form: RelationForm::LogicalAggregate,
            row: output_row,
            inputs,
            constants: Vec::new(),
        },
        Properties::Aggregate {
            groups: vec![group.clone()],
            group,
            calls,
        },
        environment,
        Vec::new(),
        Vec::new(),
    );
    relation
}

fn table_input(
    program: &mut Program,
    row: TypeId,
    inputs: &[u32],
) -> (InputEnvironmentId, EnvironmentId) {
    let input = InputEnvironmentId(program.table.environments.len() as u32);
    program.table.environments.push(InputEnvironment {
        owner: InputOwner::Relations {
            inputs: inputs.to_vec(),
        },
        row,
    });
    (input, add_environment(program, input))
}

fn append_table_expression(
    program: &mut Program,
    input: InputEnvironmentId,
    value: &ProjectValue,
) -> ExprId {
    match value {
        ProjectValue::Column(column) => append_input_ref(program, input, *column),
        ProjectValue::Integer { ty, value } => append_integer(program, *ty, *value),
        ProjectValue::Call {
            registry,
            key,
            arguments,
            ty,
            position,
        } => {
            let object = OperatorObjectId(program.expressions.operator_objects.len() as u32);
            program
                .expressions
                .operator_objects
                .push(OperatorObject::Builtin {
                    registry: (*registry).into(),
                    registry_version: "1".into(),
                    key: (*key).into(),
                });
            let operator = OperatorId(program.expressions.operators.len() as u32);
            program.expressions.operators.push(ResolvedOperator {
                object,
                result: *ty,
                parameters: arguments.iter().map(TableOperand::ty).collect(),
            });
            let root = ExprId(program.expressions.expressions.len() as u32);
            program.expressions.expressions.push(ExpressionObject {
                ty: *ty,
                position: position
                    .as_ref()
                    .map_or(ObjectPosition::Absent {}, |position| {
                        ObjectPosition::Present {
                            position: position.clone(),
                            internal: Some(false),
                        }
                    }),
                kind: ObjectKind::Call {
                    operator,
                    arguments: Vec::new(),
                },
            });
            let mut operands = Vec::with_capacity(arguments.len());
            for argument in arguments {
                operands.push(match argument {
                    TableOperand::Column(column) => append_input_ref(program, input, *column),
                    TableOperand::Integer { ty, value } => append_integer(program, *ty, *value),
                    TableOperand::Computed(expression) => {
                        append_table_expression(program, input, expression)
                    }
                });
            }
            program.expressions.expressions[root.0 as usize].kind = ObjectKind::Call {
                operator,
                arguments: operands,
            };
            root
        }
    }
}

fn append_input_ref(
    program: &mut Program,
    owner: InputEnvironmentId,
    column: ProjectInput,
) -> ExprId {
    let ProjectInput { field, ty } = column;
    let object = InputReferenceId(program.table.objects.len() as u32);
    program.table.objects.push(InputReference { ty, field });
    program.table.uses.push(InputUse {
        object,
        environment: owner,
    });
    let expression = ExprId(program.expressions.expressions.len() as u32);
    program.expressions.expressions.push(ExpressionObject {
        ty,
        position: ObjectPosition::Absent {},
        kind: ObjectKind::InputRef { object },
    });
    expression
}

pub(super) fn append_project(
    program: &mut Program,
    row: TypeId,
    expression: ExprId,
) -> Result<(), Failure> {
    let integer = insert_type(
        &mut program.table.type_table,
        SqlType {
            nullable: false,
            origin: Origin::Absent {},
            kind: TypeKind::Integer {
                width: IntegerWidth::I32,
                signed: true,
            },
        },
    );
    let zero_row = insert_row(program, "ZERO", integer);
    let input = InputEnvironmentId(program.table.environments.len() as u32);
    let values = program.table.relations + 1;
    program.table.environments.push(InputEnvironment {
        owner: InputOwner::Relations {
            inputs: vec![values],
        },
        row: zero_row,
    });
    let environment = add_environment(program, input);
    append_relation(
        program,
        InputProducer {
            form: RelationForm::LogicalProject,
            row,
            inputs: vec![values],
            constants: Vec::new(),
        },
        Properties::Project {
            expressions: 1,
            variables: Vec::new(),
        },
        environment,
        vec![expression],
        Vec::new(),
    );

    let zero = append_integer(program, integer, 0);
    append_values(program, zero_row, Some(zero))?;
    Ok(())
}

pub(super) fn append_integer(program: &mut Program, ty: TypeId, value: i64) -> ExprId {
    let root = ExprId(program.expressions.expressions.len() as u32);
    program.expressions.expressions.push(ExpressionObject {
        ty,
        position: ObjectPosition::Absent {},
        kind: ObjectKind::Literal {
            literal: Literal {
                ty,
                origin: Origin::Absent {},
                value: LiteralValue::Integer {
                    value: value.to_string(),
                },
            },
        },
    });
    root
}

pub(super) fn append_values(
    program: &mut Program,
    row: TypeId,
    expression: Option<ExprId>,
) -> Result<u32, Failure> {
    let relation = program.table.relations;
    let environment = reserve_environment(
        program,
        |owner| matches!(owner, InputOwner::Relations { inputs } if inputs.is_empty()),
    )?;
    append_relation(
        program,
        InputProducer {
            form: RelationForm::LogicalValues,
            row,
            inputs: Vec::new(),
            constants: Vec::new(),
        },
        Properties::Values {
            tuples: u32::from(expression.is_some()),
        },
        environment,
        expression.into_iter().collect(),
        Vec::new(),
    );
    Ok(relation)
}

pub(super) fn append_table_scan(
    program: &mut Program,
    schema: u32,
    row: TypeId,
    table: String,
    alias: Hint,
) -> Result<u32, Failure> {
    let relation = program.table.relations;
    let table_id = program.catalog.tables.len() as u32;
    let name = vec!["schema".into(), table];
    let environment = reserve_environment(
        program,
        |owner| matches!(owner, InputOwner::Relations { inputs } if inputs.is_empty()),
    )?;
    append_relation(
        program,
        InputProducer {
            form: RelationForm::LogicalTableScan,
            row,
            inputs: Vec::new(),
            constants: Vec::new(),
        },
        Properties::Scan { name: name.clone() },
        environment,
        Vec::new(),
        vec![alias],
    );
    program.catalog.tables.push(Table { schema, row, name });
    program.catalog.scans.push(Scan {
        relation,
        table: table_id,
    });
    program.table_scan_order.push(relation);
    Ok(relation)
}

fn append_relation(
    program: &mut Program,
    producer: InputProducer,
    fields: Properties,
    environment: EnvironmentId,
    roots: Vec<ExprId>,
    hints: Vec<Hint>,
) {
    let relation = program.table.relations;
    program.table.relations += 1;
    program.producers.relations.push(producer);
    program.properties.relations.push(RelationProperty {
        metadata: RelationMetadata {
            traits: Vec::new(),
            hints,
        },
        fields,
    });
    program.expressions.relation_uses.push(RelationUse {
        relation,
        environment,
        roots,
    });
}

pub(super) fn insert_type(types: &mut feldera_sql_rel_model::TypeTable, ty: SqlType) -> TypeId {
    if let Some(index) = types.types.iter().position(|known| known == &ty) {
        return TypeId(index as u32);
    }
    let id = TypeId(types.types.len() as u32);
    types.types.push(ty);
    types.roots.push(id);
    id
}

pub(super) fn reserve_environment(
    program: &mut Program,
    select_owner: impl Fn(&InputOwner) -> bool,
) -> Result<EnvironmentId, Failure> {
    let input = program
        .table
        .environments
        .iter()
        .position(|entry| select_owner(&entry.owner))
        .ok_or_else(|| unsupported("system prelude lacks the required input context"))?;
    Ok(add_environment(program, InputEnvironmentId(input as u32)))
}

fn add_environment(program: &mut Program, input: InputEnvironmentId) -> EnvironmentId {
    let result = EnvironmentId(program.expressions.environments.len() as u32);
    program.expressions.environments.push(Environment {
        input,
        source: SourceContext::Present { source: USER },
        correlations: Vec::new(),
    });
    result
}
