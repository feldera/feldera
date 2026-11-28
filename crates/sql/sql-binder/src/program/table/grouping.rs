//! Bind duplicate elimination and grouping without aggregate calls. The input
//! to the aggregate contains one field per key. A separate projection restores
//! the SELECT order when it drops, reorders or repeats keys.

use super::constants::{Integer, substitute};
use super::{
    DeclarationTokens, Expr, Failure, ProjectInput, ProjectValue, Projection, QueryText,
    SelectItem, TableColumn, TypeId, apply_filtered_projection, bind_projection, filter,
    identity_projection, unsupported,
};
use crate::program::{
    OutputNames,
    graph::{append_aggregate, append_table_project},
    projected_row,
};
use feldera_sql_rel_model::program::Program;
use sqlparser_064::ast::{GroupByExpr, Spanned, Value};
use std::collections::{HashMap, HashSet};

pub(super) struct Grouping {
    keys: Vec<Projection>,
    pub(super) warnings: Vec<feldera_sql_rel_model::diagnostics::FrontendWarning>,
    outputs: Vec<ProjectValue>,
    clause: Vec<(String, String)>,
    distinct_outputs: Option<DistinctOutputs>,
}

impl Grouping {
    pub(super) fn bind(
        program: &mut Program,
        select: &crate::Select,
        projection: &[Projection],
        columns: &[TableColumn],
        qualifier: &str,
        tokens: &DeclarationTokens<'_>,
    ) -> Result<Self, Failure> {
        let GroupByExpr::Expressions(groups, _) = &select.group_by else {
            return Err(unsupported("expected ordinary grouping expressions"));
        };
        let mut warnings = Vec::new();
        let mut keys = Vec::new();
        let mut clause = Vec::new();
        let mut ordinals = HashMap::new();
        if groups.is_empty() {
            for (output, name) in unique_outputs(projection) {
                let mut key = projection[output].clone();
                key.name.name = name;
                ordinals.insert(key.pretty.clone(), keys.len());
                keys.push(key);
            }
        } else {
            let mut assigned = HashSet::new();
            let mut preferred = Vec::new();
            for expr in groups {
                let (mut key, selected) =
                    group_expression(program, expr, projection, columns, qualifier, tokens)?;
                warnings.extend(crate::program::equality::key(
                    &program.table.type_table,
                    key.value.ty(),
                    "GROUP BY",
                    tokens,
                    || {
                        let source = match selected {
                            None => expr,
                            Some(index) if select.projection.len() == projection.len() => {
                                super::projection_item(&select.projection[index])?.0
                            }
                            Some(_) => {
                                return Err(unsupported(
                                    "floating point grouping ordinal over expanded wildcard",
                                ));
                            }
                        };
                        let (inner, enclosing) = crate::program::without_parentheses(source);
                        match inner {
                            Expr::Function(call) => {
                                tokens.call_position(call.name.span(), enclosing)
                            }
                            _ => tokens.literal_position(source),
                        }
                    },
                )?);
                clause.push((key.pretty.clone(), key.diagnostic.clone()));
                if ordinals.contains_key(&key.pretty) {
                    continue;
                }
                // SqlToRelConverter assigns the last matching SELECT alias to
                // an explicit key. Name collisions receive synthetic field names.
                let name = if let Some(output) = projection
                    .iter()
                    .rev()
                    .find(|item| item.pretty == key.pretty)
                {
                    key.name.clone_from(&output.name);
                    Some(&key.name.name)
                } else if matches!(key.value, ProjectValue::Column(_)) {
                    Some(&key.name.name)
                } else {
                    None
                };
                preferred.push(name.is_some_and(|name| assigned.insert(name.clone())));
                ordinals.insert(key.pretty.clone(), keys.len());
                keys.push(key);
            }
            // The converter leaves duplicate and unnamed keys anonymous.
            // Relational row construction assigns their final names in order.
            assigned.clear();
            for (i, (key, named)) in keys.iter_mut().zip(preferred).enumerate() {
                if named && assigned.insert(key.name.name.clone()) {
                    continue;
                }
                let base = if named { key.name.name.as_str() } else { "$f" };
                key.name.name = (i..)
                    .map(|n| format!("{base}{n}"))
                    .find(|name| assigned.insert(name.clone()))
                    .expect("unbounded field names");
            }
        }
        let key_inputs: HashMap<_, _> = keys
            .iter()
            .enumerate()
            .filter_map(|(i, key)| match key.value {
                ProjectValue::Column(input) => Some((input.field, i as u32)),
                _ => None,
            })
            .collect();
        let outputs: Vec<ProjectValue> = projection
            .iter()
            .map(|item| {
                if let Some(&index) = ordinals.get(&item.pretty) {
                    Ok(ProjectValue::Column(ProjectInput {
                        field: index as u32,
                        ty: item.value.ty(),
                    }))
                } else {
                    let mut value = item.value.clone();
                    remap_inputs(&mut value, &key_inputs)?;
                    Ok(value)
                }
            })
            .collect::<Result<_, Failure>>()?;
        let distinct_outputs = if !groups.is_empty()
            && matches!(
                select.distinct,
                Some(sqlparser_064::ast::Distinct::Distinct)
            ) {
            let selected_keys = outputs
                .iter()
                .filter_map(|value| match value {
                    ProjectValue::Column(col) => Some(col.field),
                    _ => None,
                })
                .collect::<HashSet<_>>();
            (selected_keys.len() != keys.len()).then(|| DistinctOutputs::new(program, projection))
        } else {
            None
        };
        Ok(Self {
            keys,
            outputs,
            clause,
            warnings,
            distinct_outputs,
        })
    }

    pub(super) fn append(
        &mut self,
        program: &mut Program,
        scan_row: TypeId,
        view_row: TypeId,
        width: usize,
        predicate: Option<&filter::Predicate>,
    ) -> Result<u32, Failure> {
        let explicit = !self.clause.is_empty();
        let identity_keys = identity_projection(width, &self.keys);
        let output_identity = self.outputs.len() == self.keys.len()
            && self.outputs.iter().enumerate().all(
                |(i, value)| matches!(value, ProjectValue::Column(col) if col.field as usize == i),
            );
        // An explicit group over the whole input drops its identity projection
        // before predicate constants are substituted. DISTINCT substitutes
        // constants in the SELECT list before introducing the aggregate.
        apply_filtered_projection(&mut program.table.type_table, &mut self.keys, predicate);
        let project_keys = !(explicit && identity_keys) && !identity_projection(width, &self.keys);
        let key_row = if project_keys || (!explicit && !output_identity) {
            let fields = self
                .keys
                .iter()
                .map(|key| (key.name.clone(), key.value.ty()))
                .collect::<Vec<_>>();
            projected_row(program, &fields)
        } else {
            scan_row
        };
        let aggregate_input = if project_keys { key_row } else { scan_row };
        let root = program.table.relations;
        let mut values = self.outputs.clone();
        let known = |i: u32| Integer::from_project(&self.keys[i as usize].value);
        substitute(&program.table.type_table, &mut values, known);
        let projected_row = if let Some(distinct) = &self.distinct_outputs {
            distinct.append(program, view_row, &values);
            distinct.row
        } else {
            view_row
        };
        let renamed_constant = explicit
            && identity_keys
            && key_row != view_row
            && self
                .keys
                .iter()
                .any(|key| Integer::from_project(&key.value).is_some());
        if !output_identity || renamed_constant {
            if let Some(distinct) = &self.distinct_outputs {
                append_table_project(
                    program,
                    projected_row,
                    key_row,
                    distinct.unique.iter().map(|&i| &values[i]),
                );
            } else {
                append_table_project(program, view_row, key_row, &values);
            }
        }
        let group = (0..self.keys.len() as u32).collect();
        append_aggregate(program, key_row, aggregate_input, group, vec![]);
        if project_keys {
            append_table_project(
                program,
                key_row,
                scan_row,
                self.keys.iter().map(|p| &p.value),
            );
        }
        Ok(root)
    }

    pub(super) fn render(&self, text: &mut QueryText) {
        if !self.clause.is_empty() {
            let (pretty, diagnostic): (Vec<_>, Vec<_>) = self
                .clause
                .iter()
                .map(|(p, d)| (p.as_str(), d.as_str()))
                .unzip();
            text.pretty
                .push_str(&format!("\nGROUP BY {}", pretty.join(", ")));
            text.diagnostic
                .push_str(&format!("\nGROUP BY {}", diagnostic.join(", ")));
        }
    }
}

/// DISTINCT uses the first occurrence of each expression, after assigning
/// unique field names across the original SELECT list.
fn unique_outputs(projection: &[Projection]) -> Vec<(usize, String)> {
    let mut names = OutputNames::default();
    let mut seen = HashSet::new();
    projection
        .iter()
        .enumerate()
        .filter_map(|(i, item)| {
            let name = names
                .disambiguate(&item.name.name)
                .unwrap_or_else(|| item.name.name.clone());
            seen.insert(&item.pretty).then_some((i, name))
        })
        .collect()
}

/// The SELECT list drops grouping keys, so DISTINCT needs a second
/// aggregate. Restore repeated SELECT expressions above that aggregate.
struct DistinctOutputs {
    unique: Vec<usize>,
    row: TypeId,
    outputs: Vec<ProjectValue>,
}

impl DistinctOutputs {
    fn new(program: &mut Program, projection: &[Projection]) -> Self {
        let unique = unique_outputs(projection);
        let row = crate::program::graph::insert_row_fields(
            program,
            unique
                .iter()
                .map(|(i, name)| (name.as_str(), projection[*i].value.ty())),
        );
        let unique = unique.into_iter().map(|(i, _)| i).collect::<Vec<_>>();
        let ordinals: HashMap<_, _> = unique
            .iter()
            .enumerate()
            .map(|(i, &original)| (&projection[original].pretty, i as u32))
            .collect();
        let outputs: Vec<ProjectValue> = projection
            .iter()
            .map(|p| {
                ProjectValue::Column(ProjectInput {
                    field: ordinals[&p.pretty],
                    ty: p.value.ty(),
                })
            })
            .collect();
        Self {
            unique,
            row,
            outputs,
        }
    }

    fn append(&self, program: &mut Program, view_row: TypeId, values: &[ProjectValue]) {
        if self.unique.len() != self.outputs.len() {
            let mut outputs = self.outputs.clone();
            substitute(&program.table.type_table, &mut outputs, |i| {
                Integer::from_project(&values[self.unique[i as usize]])
            });
            append_table_project(program, view_row, self.row, &outputs);
        }
        append_aggregate(
            program,
            self.row,
            self.row,
            (0..self.unique.len() as u32).collect(),
            vec![],
        );
    }
}

fn group_expression(
    program: &mut Program,
    expr: &Expr,
    projection: &[Projection],
    columns: &[TableColumn],
    qualifier: &str,
    tokens: &DeclarationTokens<'_>,
) -> Result<(Projection, Option<usize>), Failure> {
    if let Expr::Value(value) = expr
        && let Value::Number(number, _) = &value.value
    {
        return number
            .parse::<usize>()
            .ok()
            .and_then(|i| i.checked_sub(1))
            .and_then(|i| projection.get(i).map(|p| (p.clone(), Some(i))))
            .ok_or_else(|| unsupported("GROUP BY ordinal outside the SELECT list"));
    }
    if let Expr::Identifier(id) = expr
        && id.quote_style.is_none()
        && !columns
            .iter()
            .any(|col| col.name.name.eq_ignore_ascii_case(&id.value))
    {
        let alias = id.value.to_lowercase();
        let mut matching = projection
            .iter()
            .enumerate()
            .filter(|(_, col)| col.name.name == alias);
        if let Some((index, item)) = matching.next()
            && matching.next().is_none()
        {
            return Ok((item.clone(), Some(index)));
        }
    }
    let (expr, _) = crate::program::without_parentheses(expr);
    let item = SelectItem::UnnamedExpr(expr.clone());
    let mut fields = bind_projection(program, &[item], columns, qualifier, tokens)?;
    Ok((fields.pop().expect("one scalar grouping expression"), None))
}

/// Resolve SELECT expressions against the *grouped* row. Each column read
/// must have a value fixed by a grouping key.
fn remap_inputs(value: &mut ProjectValue, keys: &HashMap<u32, u32>) -> Result<(), Failure> {
    use crate::program::graph::TableOperand;
    let remap = |input: &mut ProjectInput| {
        input.field = *keys
            .get(&input.field)
            .ok_or_else(|| unsupported("SELECT reads an ungrouped column"))?;
        Ok::<_, Failure>(())
    };
    match value {
        ProjectValue::Column(input) => remap(input),
        ProjectValue::Integer { .. } => Ok(()),
        ProjectValue::Call { arguments, .. } => {
            arguments
                .iter_mut()
                .try_for_each(|argument| match argument {
                    TableOperand::Column(input) => remap(input),
                    TableOperand::Integer { .. } => Ok(()),
                    TableOperand::Computed(value) => remap_inputs(value, keys),
                })
        }
    }
}
