//! Move a single reader's value projection before a window. The key and the
//! control stream retain their original values and window bounds.
use crate::rebuild::ProjectionNames;
use crate::row_fields::RowFields;
use crate::stream_rewrites::{body_members, reader_counts};
use feldera_sql_expr::{input_field_paths, parameter_paths, Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field,
    InnerKind, NodeId, OperatorKind as Op, OutputPort, TypeId, TypeKind as Ty,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

fn link(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

/// One component of the borrowed (key, value) pair. Raw tuples keep all
/// positions and represent a discarded scalar with the empty SQL tuple.
struct Component {
    ty: TypeId,
    used: Vec<bool>,
    tuple: bool,
    raw: bool,
}

impl Component {
    fn new(
        edit: &CircuitEditor,
        reference: NodeId,
        side: usize,
        paths: &BTreeSet<Vec<usize>>,
    ) -> Result<Self, Error> {
        let ty = edit.type_id(edit.node(reference)?.edge("type")?)?;
        let kind = edit.type_kind(ty)?;
        let tuple = matches!(kind, Ty::RawTuple | Ty::Tuple);
        let whole = side == 0 || paths.contains(&Vec::new()) || paths.contains([side].as_slice());
        let used = if tuple {
            let mut shape = RowFields::new(edit.circuit());
            let mut used = Vec::new();
            for (i, field) in edit
                .node(ty.into())?
                .edges("tupFields")?
                .into_iter()
                .enumerate()
            {
                used.push(whole || !shape.unused(field, &mut vec![side, i], paths, 0, 0)?);
            }
            used
        } else {
            vec![whole]
        };
        Ok(Self {
            ty,
            used,
            tuple,
            raw: kind == Ty::RawTuple,
        })
    }

    fn compressed_type(&self, edit: &mut CircuitEditor) -> Result<TypeId, Error> {
        if !self.tuple {
            return if self.used[0] {
                Ok(self.ty)
            } else {
                edit.tuple_type(self.ty.into(), false, [])
            };
        }
        let mut kept = Vec::new();
        for (ty, &used) in edit
            .node(self.ty.into())?
            .edges("tupFields")?
            .into_iter()
            .zip(&self.used)
        {
            if used {
                kept.push(ty);
            } else if self.raw {
                if matches!(edit.type_kind(edit.type_id(ty)?)?, Ty::Tuple | Ty::RawTuple) {
                    kept.push(ty);
                } else {
                    kept.push(edit.tuple_type(ty, false, [])?.into());
                }
            }
        }
        let ty = edit.tuple_type(self.ty.into(), self.raw, kept)?;
        edit.with_nullability(ty, edit.nullable(self.ty)?)
    }

    fn project(
        &self,
        edit: &mut CircuitEditor,
        value: ExpressionId,
    ) -> Result<ExpressionId, Error> {
        if !self.tuple {
            return if self.used[0] {
                edit.clone_if_needed(value)
            } else {
                edit.tuple_expr(value.into(), false, &[])
            };
        }
        let nullable = edit.nullable(self.ty)?;
        let mut kept = Vec::new();
        for (i, &used) in self.used.iter().enumerate() {
            if !used && !self.raw {
                continue;
            }
            let source = if nullable {
                let ty = edit.with_nullability(self.ty, false)?;
                edit.make_expr(
                    value.into(),
                    Expr::Unwrap,
                    ty,
                    [
                        ("expression", link(value)),
                        ("message", Field::Data("".into())),
                    ],
                )?
            } else {
                value
            };
            let field = edit.tuple_field(source, i)?;
            let nested_tuple = matches!(
                edit.type_kind(edit.expression_type(field)?)?,
                Ty::Tuple | Ty::RawTuple
            );
            kept.push(if used || nested_tuple {
                edit.clone_if_needed(field)?
            } else {
                edit.tuple_expr(value.into(), false, &[])?
            });
        }
        let tuple = edit.tuple_expr(value.into(), self.raw, &kept)?;
        if !nullable || self.raw {
            return Ok(tuple);
        }
        let tuple = edit.some_at(value.into(), tuple)?;
        let ty = edit.expression_type(tuple)?;
        let condition = edit.null_test(value)?;
        let null = edit.typed_null(ty)?;
        edit.make_expr(
            tuple.into(),
            Expr::If,
            ty,
            [
                ("condition", link(condition)),
                ("positive", link(null)),
                ("negative", link(tuple)),
            ],
        )
    }
}

struct Rebind<'a> {
    bindings: HashSet<NodeId>,
    name: &'a str,
    pair: TypeId,
    value: &'a Component,
}

impl Rebind<'_> {
    // Recognize (*row.1).N before the visitor rebuilds it with the narrower type.
    fn value_field(&self, edit: &CircuitEditor, id: NodeId) -> Result<Option<usize>, Error> {
        let field = edit.node(id)?;
        let deref = edit.node(field.edge("expression")?)?;
        if !matches!(deref.inner_kind(), Some(InnerKind::Expression(Expr::Deref))) {
            return Ok(None);
        }
        let side = edit.node(deref.edge("expression")?)?;
        if !matches!(side.inner_kind(), Some(InnerKind::Expression(Expr::Field)))
            || side.data("fieldNo").and_then(|v| v.as_u64()) != Some(1)
            || !self.bindings.contains(&side.edge("expression")?)
        {
            return Ok(None);
        }
        let index = field
            .data("fieldNo")
            .and_then(|v| v.as_u64())
            .and_then(|i| usize::try_from(i).ok())
            .ok_or_else(|| Error::new("window reader requires a field index"))?;
        if self.value.used.get(index) != Some(&true) {
            return Err(Error::new("window reader references a discarded column"));
        }
        Ok(Some(if self.value.raw {
            index
        } else {
            self.value.used[..index]
                .iter()
                .filter(|&&used| used)
                .count()
        }))
    }
}

impl Transformation for Rebind<'_> {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        if self.bindings.contains(&id) {
            return Ok(Visit::Replace(
                edit.named_variable(self.pair, self.name)?.into(),
            ));
        }
        match edit.node(id)?.inner_kind() {
            Some(InnerKind::Type(_) | InnerKind::Expression(Expr::Closure)) => Ok(Visit::Keep),
            Some(InnerKind::Expression(Expr::Field)) => {
                if let Some(column) = self.value_field(edit, id)? {
                    let row = edit.named_variable(self.pair, self.name)?;
                    let value = edit.tuple_field(row, 1)?;
                    let value = edit.dereference(value)?;
                    return Ok(Visit::Replace(edit.tuple_field(value, column)?.into()));
                }
                Ok(Visit::Children)
            }
            _ => Ok(Visit::Children),
        }
    }
}

fn closures(
    edit: &mut CircuitEditor,
    function: NodeId,
    next: &mut u64,
) -> Result<Option<(ExpressionId, ExpressionId)>, Error> {
    let paths = input_field_paths(edit.circuit(), function)?;
    let closure = edit.node(function)?;
    let [param] = closure
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("window reader must be unary"))?;
    let body = closure.edge("body")?;
    let parameter = edit.node(param)?;
    let name = parameter
        .data("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("missing window reader parameter name"))?
        .to_owned();
    let pair = edit.type_id(parameter.edge("type")?)?;
    let [key, value] = edit
        .node(pair.into())?
        .edges("tupFields")?
        .try_into()
        .map_err(|_| Error::new("expected a window key and value"))?;
    let value = Component::new(edit, value, 1, &paths)?;
    if value.used.iter().all(|&used| used) {
        return Ok(None);
    }
    let key = Component::new(edit, key, 0, &paths)?;
    let row = edit.variable(pair, next)?;
    let mut components = Vec::new();
    let mut references = Vec::new();
    for (i, layout) in [&key, &value].into_iter().enumerate() {
        let field = edit.tuple_field(row, i)?;
        let field = edit.dereference(field)?;
        components.push(layout.project(edit, field)?);
        let compressed = layout.compressed_type(edit)?;
        references.push(edit.reference_type(compressed)?.into());
    }
    let projected = edit.tuple_expr(pair.into(), true, &components)?;
    let pre = edit.closure_from_variables(projected, &[row])?;
    let pair = edit.tuple_type(pair.into(), true, references)?;
    let narrowed = edit.derive_inner(param, [("type", link(pair))])?;
    let bindings = parameter_paths(edit, body, param)?;
    let body = Rewrite::new(Rebind {
        bindings,
        pair,
        name: &name,
        value: &value,
    })
    .apply(edit, body)?;
    let post = edit.closure_with_parameters(edit.expression(body)?, &[narrowed])?;
    Ok(Some((pre, post)))
}

fn add_index(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    input: OutputPort,
    function: ExpressionId,
) -> Result<(NodeId, TypeId), Error> {
    let result = edit.node(function.into())?.edge("body")?;
    let stream = edit.indexed_zset_from_pair(edit.expression_type(edit.expression(result)?)?)?;
    let index = edit.append_operator(
        body,
        site,
        Op::MapIndex,
        &[input],
        [("outputType", link(stream)), ("function", link(function))],
    )?;
    Ok((index, stream))
}

pub fn trim_windows(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let members = body_members(&circuit)?;
    let readers = reader_counts(&circuit, &members);
    let parents: HashMap<_, _> = members.into_iter().map(|(body, id)| (id, body)).collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut projections = ProjectionNames::default();
    let mut names = *next;
    for id in order {
        let reader = edit.node(id)?;
        projections.visit(id, reader, &mut names)?;
        if reader.operator_kind() != Some(Op::MapIndex) {
            continue;
        }
        let window = reader
            .inputs()
            .next()
            .ok_or_else(|| Error::new("window reader requires an input"))?
            .operator;
        if readers.get(&window) != Some(&1)
            || edit.node(window)?.operator_kind() != Some(Op::Window)
        {
            continue;
        }
        let function = reader.edge("function")?;
        let Some((pre, post)) = closures(&mut edit, function, &mut names)? else {
            continue;
        };
        let op = edit.node(window)?;
        let [data, control] = op
            .inputs()
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| Error::new("window requires two inputs"))?;
        let mut options = Vec::new();
        for key in ["lowerInclusive", "upperInclusive", "lowerUnbounded"] {
            options.push((
                key,
                op.field(key)
                    .ok_or_else(|| Error::new("missing window bound option"))?
                    .clone(),
            ));
        }
        let body = parents[&id];
        let (index, stream) = add_index(&mut edit, body, window, data, pre)?;
        options.push(("outputType", link(stream)));
        let narrowed =
            edit.append_operator(body, window, Op::Window, &[output(index), control], options)?;
        let (post, _) = add_index(&mut edit, body, id, output(narrowed), post)?;
        edit.place_before(body, index, id)?;
        replacements.insert(id, output(post));
        projections.replaced(id);
    }
    let circuit = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok(circuit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nullable_window_values_retain_empty_rows_and_rebind_sparse_columns() {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[],
            "type":{"class":"DBSPTypeInteger","id":1,"code":"INT32","mayBeNull":false}
        }))
        .unwrap();
        let mut edit = CircuitEditor::new(graph);
        let integer = edit.type_id(NodeId::Inner(1)).unwrap();
        let empty = edit.tuple_type(integer.into(), false, []).unwrap();
        let value = edit
            .tuple_type(
                integer.into(),
                false,
                [integer.into(), empty.into(), integer.into()],
            )
            .unwrap();
        let value = edit.with_nullability(value, true).unwrap();
        let key = edit.reference_type(integer).unwrap();
        let value = edit.reference_type(value).unwrap();
        let pair = edit
            .tuple_type(value.into(), true, [key.into(), value.into()])
            .unwrap();
        let mut next = 0;
        let row = edit.variable(pair, &mut next).unwrap();
        let key = edit.tuple_field(row, 0).unwrap();
        let key = edit.dereference(key).unwrap();
        let value = edit.tuple_field(row, 1).unwrap();
        let value = edit.dereference(value).unwrap();
        let last = edit.tuple_field(value, 2).unwrap();
        let result = edit.tuple_expr(row.into(), true, &[key, last]).unwrap();
        let reader = edit.closure_from_variables(result, &[row]).unwrap();

        let (pre, post) = closures(&mut edit, reader.into(), &mut next)
            .unwrap()
            .unwrap();
        assert_eq!(next, 2);
        let projected = edit.node(pre.into()).unwrap().edge("body").unwrap();
        let value = edit.node(projected).unwrap().edges("fields").unwrap()[1];
        assert_eq!(
            edit.expr_kind(edit.expression(value).unwrap()).unwrap(),
            Expr::If
        );
        let projected_type = edit
            .expression_type(edit.expression(value).unwrap())
            .unwrap();
        assert!(edit.nullable(projected_type).unwrap());
        assert_eq!(
            edit.node(projected_type.into())
                .unwrap()
                .edges("tupFields")
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            input_field_paths(edit.circuit(), post.into()).unwrap(),
            BTreeSet::from([vec![0], vec![1, 1]])
        );
        assert!(closures(&mut edit, post.into(), &mut next)
            .unwrap()
            .is_none());
        assert_eq!(next, 2);
    }
}
