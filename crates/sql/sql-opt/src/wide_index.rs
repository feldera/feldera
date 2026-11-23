//! Project the common key and every narrow value onto one shared row variable.
use crate::index_key_equivalence::{KeyComparisonError, check_bodies, speculative};
use feldera_sql_expr::{SimplifyWarning, reduce_closure_body_with_warnings};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId, TypeId,
    TypeKind as Ty,
};

pub struct WideIndexFields<T = ExpressionId> {
    pub variable: ExpressionId,
    pub key: ExpressionId,
    pub rows: Vec<Vec<T>>,
    pub nullable: bool,
}

fn parameter_type(edit: &CircuitEditor, function: NodeId) -> Result<TypeId, Error> {
    let [parameter] = edit
        .node(function)?
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("a shared index requires one row parameter"))?;
    edit.type_id(edit.node(parameter)?.edge("type")?)
}

fn reduce(
    edit: &mut CircuitEditor,
    value: ExpressionId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let reduced = reduce_closure_body_with_warnings(edit, value.into(), &[], &[], Some(warnings))?;
    edit.expression(reduced)
}

pub fn wide_index_fields(
    edit: &mut CircuitEditor,
    functions: &[NodeId],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<WideIndexFields, Error> {
    project_fields(edit, functions, next, warnings, |_, field, _, _| {
        Ok::<_, Error>(Some(field))
    })?
    .ok_or_else(|| Error::new("missing projected shared-index field"))
}

/// Visit each reduced field before projecting the next one. A live equality
/// error must see exactly the warnings produced up to the failing comparison.
fn project_fields<T, E: From<Error>>(
    edit: &mut CircuitEditor,
    functions: &[NodeId],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
    mut project: impl FnMut(
        &CircuitEditor,
        ExpressionId,
        u64,
        &[SimplifyWarning],
    ) -> Result<Option<T>, E>,
) -> Result<Option<WideIndexFields<T>>, E> {
    let &[first, _, ..] = functions else {
        return Err(Error::new("a wide index needs at least two input indexes").into());
    };
    let input = parameter_type(edit, first)?;
    let variable = edit.variable(input, next)?;
    let call = edit.call_closure(edit.expression(first)?, &[variable])?;
    let value_type = edit.tuple_field_type(edit.expression_type(call)?, 1)?;
    let nullable = edit.nullable(value_type)?;
    let key = edit.tuple_field(call, 0)?;
    let key = reduce(edit, key, warnings)?;
    let mut rows = Vec::with_capacity(functions.len());
    for &function in functions {
        if !edit.same_type(input, parameter_type(edit, function)?)? {
            return Err(Error::new("shared indexes require equivalent row parameter types").into());
        }
        let function = edit.expression(function)?;
        let call = edit.call_closure(function, &[variable])?;
        let row = edit.tuple_field(call, 1)?;
        let row_type = edit.expression_type(row)?;
        if edit.type_kind(row_type)? != Ty::Tuple || edit.nullable(row_type)? != nullable {
            return Err(
                Error::new("shared index values require tuples with equal nullability").into(),
            );
        }
        let types = edit.node(row_type.into())?.edges("tupFields")?;
        let mut fields = Vec::with_capacity(types.len());
        for (index, ty) in types.into_iter().enumerate() {
            let mut field = edit.tuple_field(row, index)?;
            let ty = edit.type_id(ty)?;
            let projected = edit.expression_type(field)?;
            // A nullable containing tuple lifts even a required field. Remove
            // that outer nullability before rebuilding the shared tuple.
            if !edit.nullable(ty)? && edit.nullable(projected)? {
                let target = edit.with_nullability(projected, false)?;
                field = edit.make_expr(
                    field.into(),
                    Expr::Unwrap,
                    target,
                    [
                        ("expression", Field::Reference(field.into())),
                        ("message", Field::Data("".into())),
                    ],
                )?;
            }
            let field = reduce(edit, field, warnings)?;
            let Some(value) = project(edit, field, *next, warnings)? else {
                return Ok(None);
            };
            fields.push(value);
        }
        rows.push(fields);
    }
    Ok(Some(WideIndexFields {
        variable,
        key,
        rows,
        nullable,
    }))
}

/// The first equal expression owns a wide column. Each narrow row retains an
/// entry for every original column, including repeated columns.
pub struct WideIndexLayout {
    pub columns: Vec<ExpressionId>,
    pub positions: Vec<Vec<usize>>,
}

/// Two projected fields whose equality needs the live compiler. Java compares
/// these roots in an empty declaration scope, with the new field on the left.
pub struct IndexComparison<'a> {
    pub graph: &'a Circuit,
    pub fields: [ExpressionId; 2],
    pub next_name: u64,
    pub warnings: &'a [SimplifyWarning],
}

pub type CompareColumn<'a> =
    dyn FnMut(IndexComparison<'_>) -> Result<Option<bool>, Error> + 'a;

fn column_position(
    edit: &CircuitEditor,
    columns: &mut Vec<ExpressionId>,
    field: ExpressionId,
    mut unresolved: impl FnMut([ExpressionId; 2]) -> Result<Option<bool>, Error>,
) -> Result<Option<usize>, KeyComparisonError> {
    for (i, &column) in columns.iter().enumerate() {
        let equal = match check_bodies(edit, field.into(), column.into(), None)? {
            None => unresolved([field, column])?,
            known => known,
        };
        match equal {
            Some(true) => return Ok(Some(i)),
            Some(false) => {}
            None => return Ok(None),
        }
    }
    columns.push(field);
    Ok(Some(columns.len() - 1))
}

impl WideIndexFields {
    pub fn layout(&self, edit: &CircuitEditor) -> Result<Option<WideIndexLayout>, Error> {
        let mut columns = Vec::new();
        let mut positions = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let mut mapping = Vec::with_capacity(row.len());
            for &field in row {
                let Some(i) =
                    speculative(column_position(edit, &mut columns, field, |_| Ok(None)))?
                else {
                    return Ok(None);
                };
                mapping.push(i);
            }
            positions.push(mapping);
        }
        Ok(Some(WideIndexLayout { columns, positions }))
    }
}

pub struct ProjectedIndex {
    pub variable: ExpressionId,
    pub key: ExpressionId,
    pub nullable: bool,
    pub layout: WideIndexLayout,
}

/// Build the layout in field order so each comparison observes only earlier reductions.
pub fn project_index(
    edit: &mut CircuitEditor,
    functions: &[NodeId],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
    compare: &mut CompareColumn,
) -> Result<Option<ProjectedIndex>, KeyComparisonError> {
    let mut columns = Vec::new();
    let fields = project_fields(
        edit,
        functions,
        next,
        warnings,
        |edit, field, next_name, warnings| {
            column_position(edit, &mut columns, field, |fields| {
                compare(IndexComparison {
                    graph: edit.circuit(),
                    fields,
                    next_name,
                    warnings,
                })
            })
        },
    )?;
    Ok(fields.map(|fields| ProjectedIndex {
        variable: fields.variable,
        key: fields.key,
        nullable: fields.nullable,
        layout: WideIndexLayout {
            columns,
            positions: fields.rows,
        },
    }))
}

/// Try fixed-width consumers first, widest first. A trial is viable only if
/// each fixed consumer sees every wide column in its original order.
pub fn split_shared_indexes(
    arena: &feldera_sql_ir::Circuit,
    functions: &[NodeId],
    fixed: &[bool],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<Vec<Vec<usize>>>, Error> {
    let mut generated = *next;
    let mut emitted = Vec::new();
    let groups = speculative(split_with(
        arena,
        functions,
        fixed,
        &mut generated,
        &mut emitted,
        &mut |_| Ok(None),
    ))?;
    if groups.is_some() {
        *next = generated;
        warnings.extend(emitted);
    }
    Ok(groups)
}

/// Plan fixed-width trials in the same order as replacement, using the caller's
/// equality function only for a field that lacks enough serialized information.
pub fn split_with(
    arena: &Circuit,
    functions: &[NodeId],
    fixed: &[bool],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
    compare: &mut CompareColumn,
) -> Result<Option<Vec<Vec<usize>>>, KeyComparisonError> {
    if functions.len() != fixed.len() {
        return Err(Error::new("missing fixed-index requirement").into());
    }
    let edit = CircuitEditor::new(arena.clone());
    let mut remaining = Vec::with_capacity(functions.len());
    for (index, &function) in functions.iter().enumerate() {
        let body = edit.expression(edit.node(function)?.edge("body")?)?;
        let pair = edit
            .node(edit.expression_type(body)?.into())?
            .edges("tupFields")?;
        let [_, value] = pair.as_slice() else {
            return Err(Error::new("shared index requires a key/value pair").into());
        };
        let width = if matches!(
            edit.type_kind(edit.type_id(*value)?)?,
            Ty::RawTuple | Ty::Tuple
        ) {
            edit.node(*value)?.edges("tupFields")?.len()
        } else {
            1
        };
        remaining.push((index, if fixed[index] { Some(width) } else { None }));
    }
    remaining.sort_by_key(|&(_, width)| std::cmp::Reverse(width));
    let mut remaining: Vec<_> = remaining.into_iter().map(|(i, _)| i).collect();
    let mut groups = Vec::new();
    while !remaining.is_empty() {
        let mut group = vec![remaining.remove(0)];
        let mut i = 0;
        while i < remaining.len() {
            let candidate = remaining[i];
            group.push(candidate);
            let functions: Vec<_> = group.iter().map(|&position| functions[position]).collect();
            // Trial projections never escape. Drop their arena before testing
            // another member, retaining just the diagnostics and name counter.
            let mut trial = CircuitEditor::new(arena.clone());
            let Some(index) = project_index(&mut trial, &functions, next, warnings, compare)?
            else {
                return Ok(None);
            };
            let layout = index.layout;
            let compatible = group.iter().zip(&layout.positions).all(|(&member, row)| {
                !fixed[member] || row.iter().copied().eq(0..layout.columns.len())
            });
            if compatible {
                remaining.remove(i);
            } else {
                group.pop();
                i += 1;
            }
        }
        if group.len() > 1 {
            groups.push(group);
        }
    }
    Ok(Some(groups))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn fixed_consumers_keep_their_column_order_when_a_wider_seed_is_tried_first()
    -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPTypeInteger","id":1,"width":32,"signed":true,"mayBeNull":false,"code":"INT32"
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let row_type = edit.tuple_type(roots[0], false, [roots[0]; 3])?;
        let mut next = 20;
        let mut index = |columns: &[usize]| -> Result<NodeId, Error> {
            let parameter = edit.ref_parameter(row_type, &mut next)?;
            let row = edit.deref_parameter(&parameter)?;
            let key = edit.tuple_field(row, 0)?;
            let key = edit.tuple_expr(row.into(), false, &[key])?;
            let columns: Vec<_> = columns
                .iter()
                .map(|&i| edit.tuple_field(row, i))
                .collect::<Result<_, _>>()?;
            let value = edit.tuple_expr(row.into(), false, &columns)?;
            let body = edit.tuple_expr(row.into(), true, &[key, value])?;
            Ok(edit.closure(body, &[parameter])?.into())
        };
        let functions = [index(&[0])?, index(&[1, 0])?, index(&[1, 1])?, index(&[2])?];
        let start = next;
        let mut warnings = Vec::new();
        let groups = split_shared_indexes(
            edit.circuit(),
            &functions,
            &[true, true, false, false],
            &mut next,
            &mut warnings,
        )?;
        assert_eq!(groups, Some(vec![vec![1, 2]]));
        assert_eq!(next - start, 4);
        let fields = wide_index_fields(
            &mut edit,
            &[functions[1], functions[2]],
            &mut next,
            &mut warnings,
        )?;
        assert_eq!(
            fields.layout(&edit)?.unwrap().positions,
            [vec![0, 1], vec![0, 0]]
        );
        assert!(warnings.is_empty());
        Ok(())
    }
}
