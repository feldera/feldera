//! Bind aggregates with direct arguments over one table.

use super::{
    Failure, Projection, StatementText, TableColumn, column_ordinal, column_projection, filter,
    projection_item, render_column,
};
use crate::program::{
    graph::{
        ProjectInput, ProjectValue, TableOperand, append_aggregate, append_integer,
        append_table_project, append_values, insert_row_fields, insert_type,
    },
    source::simple_aggregate_position,
};
use crate::{Expr, SelectItem, Spanned, frel::CALCITE, local_name, unsupported};
use feldera_sql_rel_model::{
    TypeId,
    catalog::Name,
    operators::{OperatorObject, OperatorObjectId},
    program::Program,
    relation_metadata::Coordinates,
    relation_properties::{Aggregate, OptionalField, OptionalFields, OptionalName},
    source::Origin,
    types::{IntegerWidth, SqlType, TypeKind},
};
use sqlparser_064::ast::{DuplicateTreatment, FunctionArg, FunctionArgExpr, FunctionArguments};

pub(super) struct BoundAggregate {
    calls: Vec<Call>,
    inputs: Vec<Input>,
    group_columns: Vec<usize>,
    distinct: bool,
    pub projection: Vec<Projection>,
}

struct Call {
    key: &'static str,
    ty: TypeId,
    name: String,
    arguments: Vec<u32>,
    filter: OptionalField,
    position: Coordinates,
}

enum Input {
    Column(usize),
    Filter {
        name: String,
        sql: String,
        value: ProjectValue,
    },
}

impl Call {
    fn transport(&self, program: &mut Program) -> Aggregate {
        let objects = &mut program.expressions.operator_objects;
        let object = OperatorObject::Builtin {
            registry: "calcite-s1-fixture".into(),
            registry_version: "1".into(),
            key: self.key.into(),
        };
        let operator =
            if let Some(index) = objects.iter().position(|candidate| *candidate == object) {
                OperatorObjectId(index as u32)
            } else {
                let id = OperatorObjectId(objects.len() as u32);
                objects.push(object);
                id
            };
        Aggregate {
            operator,
            ty: self.ty,
            name: OptionalName::Present {
                name: self.name.clone(),
            },
            distinct: false,
            approximate: false,
            ignore_nulls: false,
            arguments: self.arguments.clone(),
            direct_arguments: 0,
            filter: self.filter.clone(),
            distinct_keys: OptionalFields::Absent {},
            collation: Vec::new(),
            position: self.position.clone(),
        }
    }
}

impl BoundAggregate {
    pub(super) fn bind(
        program: &mut Program,
        items: &[SelectItem],
        groups: &[Expr],
        columns: &[TableColumn],
        qualifier: &str,
        sql: &str,
    ) -> Result<Option<Self>, Failure> {
        let aggregate = |item: &SelectItem| {
            projection_item(item)
                .ok()
                .and_then(|(expr, _)| match expr {
                    Expr::Function(call) => Some(call.name.to_string().to_ascii_uppercase()),
                    _ => None,
                })
                .is_some_and(|name| matches!(name.as_str(), "SUM" | "COUNT" | "MIN" | "MAX"))
        };
        if !items.iter().any(aggregate) {
            return Ok(None);
        }
        let mut plan = Self {
            calls: Vec::with_capacity(items.len()),
            inputs: Vec::new(),
            group_columns: Vec::new(),
            distinct: false,
            projection: Vec::with_capacity(items.len()),
        };
        if groups.len() >= items.len() {
            return Err(unsupported(
                "aggregate requires at least one aggregate call",
            ));
        }
        for (group, item) in groups.iter().zip(items) {
            let column = column_ordinal(group, columns, qualifier)?;
            let (selected, alias) = projection_item(item)?;
            if alias.is_some()
                || column_ordinal(selected, columns, qualifier)? != column
                || plan.group_columns.contains(&column)
            {
                return Err(unsupported(
                    "expected projected grouping columns in group order",
                ));
            }
            plan.group_columns.push(column);
            plan.inputs.push(Input::Column(column));
            plan.projection
                .push(column_projection(column, &columns[column], qualifier, None));
        }
        for (index, item) in items.iter().enumerate().skip(groups.len()) {
            let (expression, alias) = projection_item(item)?;
            let Expr::Function(call) = expression else {
                return Err(unsupported(
                    "expected aggregate calls after grouping columns",
                ));
            };
            if !matches!(call.parameters, FunctionArguments::None)
                || call.null_treatment.is_some()
                || call.over.is_some()
                || !call.within_group.is_empty()
                || call.uses_odbc_syntax
            {
                return Err(unsupported(
                    "aggregate modifiers are outside the table slice",
                ));
            }
            let name = local_name(&call.name)?.value.to_ascii_uppercase();
            let FunctionArguments::List(list) = &call.args else {
                return Err(unsupported("expected an aggregate argument list"));
            };
            if !list.clauses.is_empty() || list.duplicate_treatment == Some(DuplicateTreatment::All)
            {
                return Err(unsupported(
                    "aggregate argument modifiers are outside the table slice",
                ));
            }
            let distinct = list.duplicate_treatment == Some(DuplicateTreatment::Distinct);
            if distinct && (name != "COUNT" || items.len() != 1 || call.filter.is_some()) {
                return Err(unsupported("expected COUNT(DISTINCT column) on its own"));
            }
            plan.distinct |= distinct;
            let column = match list.args.as_slice() {
                [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] => {
                    Some(column_ordinal(argument, columns, qualifier)?)
                }
                [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                    if name == "COUNT" && !distinct =>
                {
                    None
                }
                _ => {
                    return Err(unsupported(
                        "expected a direct aggregate column or COUNT(*)",
                    ));
                }
            };
            let key = match name.as_str() {
                "SUM" => "SqlStdOperatorTable.SUM",
                "COUNT" => "SqlStdOperatorTable.COUNT",
                "MIN" => "SqlStdOperatorTable.MIN",
                "MAX" => "SqlStdOperatorTable.MAX",
                _ => return Err(unsupported("aggregate is outside the table slice")),
            };
            let ty = result_type(
                program,
                &name,
                column.map(|index| columns[index].ty),
                groups.is_empty() || call.filter.is_some(),
            )?;
            let (pretty, diagnostic) = column
                .map(|index| render_column(qualifier, &columns[index].name.name))
                .unwrap_or_else(|| ("*".into(), "*".into()));
            let distinct_text = if distinct { "DISTINCT " } else { "" };
            let mut output = Projection {
                name: Name {
                    name: format!("EXPR${index}"),
                    quoted: true,
                },
                value: ProjectValue::Column(ProjectInput {
                    field: index as u32,
                    ty,
                }),
                pretty: format!("{name}({distinct_text}{pretty})"),
                diagnostic: format!("{name}({distinct_text}{diagnostic})"),
            };
            // Calcite rewrites a non-distinct COUNT of a guaranteed non-null field to COUNT(*).
            let arguments = column
                .filter(|&index| {
                    name != "COUNT"
                        || distinct
                        || program.table.type_table.types[columns[index].ty.0 as usize].nullable
                })
                .map(|index| plan.add_column(index))
                .into_iter()
                .collect();
            let filter = if let Some(predicate) = &call.filter {
                let filter::Predicate::Expression {
                    mut value,
                    pretty,
                    diagnostic,
                } = filter::bind(program, predicate, columns, qualifier, sql)?
                else {
                    return Err(unsupported(
                        "literal aggregate FILTER is outside the table slice",
                    ));
                };
                let ProjectValue::Call { .. } = value else {
                    return Err(unsupported(
                        "expected a comparison for the aggregate FILTER",
                    ));
                };
                if program.table.type_table.types[value.ty().0 as usize].nullable {
                    value = ProjectValue::Call {
                        registry: CALCITE,
                        key: "SqlStdOperatorTable.IS_TRUE",
                        arguments: vec![TableOperand::Computed(Box::new(value))],
                        ty: insert_type(&mut program.table.type_table, filter::boolean(false)),
                        position: None,
                    };
                }
                output.pretty.push_str(&format!(" FILTER (WHERE {pretty})"));
                output
                    .diagnostic
                    .push_str(&format!(" FILTER (WHERE {diagnostic})"));
                OptionalField::Present {
                    field: plan.add_filter(pretty, value),
                }
            } else {
                OptionalField::Absent {}
            };
            output.apply_alias(alias);
            if plan.projection.iter().any(|item| item.name == output.name) {
                return Err(unsupported("aggregate output name repeated"));
            }
            plan.calls.push(Call {
                key,
                ty,
                name: output.name.name.clone(),
                arguments,
                filter,
                position: simple_aggregate_position(sql, expression.span())?,
            });
            plan.projection.push(output);
        }
        Ok(Some(plan))
    }

    fn add_column(&mut self, column: usize) -> u32 {
        if let Some(index) = self
            .inputs
            .iter()
            .position(|input| matches!(input, Input::Column(known) if *known == column))
        {
            return index as u32;
        }
        let index = self.inputs.len() as u32;
        self.inputs.push(Input::Column(column));
        index
    }

    fn add_filter(&mut self, sql: String, value: ProjectValue) -> u32 {
        if let Some(index) = self.inputs.iter().position(
            |input| matches!(input, Input::Filter { sql: previous, .. } if *previous == sql),
        ) {
            return index as u32;
        }
        let index = self.inputs.len() as u32;
        self.inputs.push(Input::Filter {
            name: format!("$f{index}"),
            sql,
            value,
        });
        index
    }

    pub(super) fn append(
        &self,
        program: &mut Program,
        scan_row: TypeId,
        view_row: TypeId,
        columns: &[TableColumn],
        predicate: Option<&filter::Predicate>,
    ) -> Result<(u32, bool), Failure> {
        let empty = matches!(predicate, Some(filter::Predicate::Constant(false)));
        if empty && !self.group_columns.is_empty() {
            return Err(unsupported("grouped aggregate with an empty input"));
        }
        if self.inputs.is_empty() {
            let [call] = self.calls.as_slice() else {
                return Err(unsupported("COUNT(*) without a projected argument"));
            };
            if empty {
                let expression = append_integer(program, call.ty, 0);
                return Ok((append_values(program, view_row, Some(expression))?, false));
            }
            // COUNT(*) consumes rows directly, even when no column is referenced.
            let call = call.transport(program);
            let root = append_aggregate(program, view_row, scan_row, Vec::new(), vec![call]);
            return Ok((root, true));
        }
        let input_row = insert_row_fields(
            program,
            self.inputs.iter().map(|input| match input {
                Input::Column(index) => (columns[*index].name.name.as_str(), columns[*index].ty),
                Input::Filter { name, value, .. } => (name.as_str(), value.ty()),
            }),
        );
        let calls = self
            .calls
            .iter()
            .map(|call| call.transport(program))
            .collect();
        let group = (0..self.group_columns.len() as u32).collect();
        let root = append_aggregate(program, view_row, input_row, group, calls);
        if self.distinct {
            if empty {
                return Err(unsupported("COUNT(DISTINCT column) over an empty relation"));
            }
            append_aggregate(program, input_row, input_row, vec![0], Vec::new());
        }
        if empty {
            append_values(program, input_row, None)?;
        } else if self.inputs.len() != columns.len()
            || self
                .inputs
                .iter()
                .enumerate()
                .any(|(index, input)| !matches!(input, Input::Column(column) if *column == index))
        {
            let projection = self
                .inputs
                .iter()
                .map(|input| match input {
                    Input::Column(index) => ProjectValue::Column(ProjectInput {
                        field: *index as u32,
                        ty: columns[*index].ty,
                    }),
                    Input::Filter { value, .. } => value.clone(),
                })
                .collect::<Vec<_>>();
            append_table_project(program, input_row, scan_row, projection.iter());
        }
        Ok((root, !empty))
    }

    pub(super) fn render_grouping(
        &self,
        source: &mut StatementText,
        columns: &[TableColumn],
        qualifier: &str,
    ) {
        if self.group_columns.is_empty() {
            return;
        }
        let group = self
            .group_columns
            .iter()
            .map(|&column| render_column(qualifier, &columns[column].name.name));
        let (pretty, diagnostic): (Vec<_>, Vec<_>) = group.unzip();
        source
            .pretty
            .push_str(&format!("\nGROUP BY {}", pretty.join(", ")));
        source
            .diagnostic
            .push_str(&format!("\nGROUP BY {}", diagnostic.join(", ")));
    }
}

fn result_type(
    program: &mut Program,
    name: &str,
    input: Option<TypeId>,
    global: bool,
) -> Result<TypeId, Failure> {
    let result = if name == "COUNT" {
        SqlType {
            nullable: false,
            origin: Origin::Absent {},
            kind: TypeKind::Integer {
                width: IntegerWidth::I64,
                signed: true,
            },
        }
    } else {
        let Some(input) = input else {
            return Err(unsupported("aggregate requires a numeric column"));
        };
        let mut result = program.table.type_table.types[input.0 as usize].clone();
        if !matches!(
            result.kind,
            TypeKind::Integer {
                width: IntegerWidth::I32 | IntegerWidth::I64,
                signed: true
            }
        ) {
            return Err(unsupported(
                "aggregate requires a signed INT or BIGINT column",
            ));
        }
        result.nullable |= global;
        result
    };
    Ok(insert_type(&mut program.table.type_table, result))
}
