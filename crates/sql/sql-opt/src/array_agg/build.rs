//! Compose the reader column with the postprocessing and empty-set expressions.
//! Each composition owns its expression tree. Function paths and types remain
//! shared, as they do in the Java expression copy constructors.

use super::reader::{kind, Access, Binding};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, Field, NodeId};

pub(super) fn link(id: NodeId) -> Field {
    Field::Reference(id)
}
pub(super) fn links(ids: &[NodeId]) -> Field {
    Field::Array(ids.iter().copied().map(link).collect())
}

pub(super) fn typed(edit: &CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
    edit.node(id)?.edge("type")
}

pub(super) fn closure(
    edit: &mut CircuitEditor,
    old: NodeId,
    body: NodeId,
    params: &[NodeId],
) -> Result<NodeId, Error> {
    let old_ty = typed(edit, old)?;
    let result = typed(edit, body)?;
    let types = params
        .iter()
        .map(|p| typed(edit, *p))
        .collect::<Result<Vec<_>, _>>()?;
    let ty = edit.derive_inner(
        old_ty,
        [
            ("resultType", link(result)),
            ("parameterTypes", links(&types)),
        ],
    )?;
    edit.derive_inner(
        old,
        [
            ("type", link(ty)),
            ("parameters", links(params)),
            ("body", link(body)),
        ],
    )
}

pub(super) struct Input<'a> {
    pub binding: &'a Binding,
    pub column: usize,
    pub replacement: NodeId,
}

pub(super) fn compose(
    edit: &mut CircuitEditor,
    expr: NodeId,
    input: Option<&Input<'_>>,
) -> Result<NodeId, Error> {
    if let Some(input) = input {
        if let Some(read) = input.binding.access(edit, expr) {
            if read.part == input.binding.value && read.field == input.column {
                return compose(edit, input.replacement, None);
            }
            return Err(Error::new("fused column reads a second aggregate"));
        }
    }
    match kind(edit, expr) {
        Some(Expr::Apply) => {
            let args = edit.node(expr)?.edges("arguments")?;
            let fun = edit.node(expr)?.edge("function")?;
            let fun = compose(edit, fun, input)?;
            let args = args
                .into_iter()
                .map(|arg| compose(edit, arg, input))
                .collect::<Result<Vec<_>, _>>()?;
            edit.derive_inner(expr, [("function", link(fun)), ("arguments", links(&args))])
        }
        Some(Expr::Closure) => {
            let params = edit.node(expr)?.edges("parameters")?;
            let shadows_input = params.iter().any(|p| {
                input.is_some_and(|input| {
                    edit.node(*p)
                        .ok()
                        .and_then(|p| p.data("name"))
                        .and_then(|v| v.as_str())
                        == Some(input.binding.value_name())
                })
            });
            let body = compose(
                edit,
                edit.node(expr)?.edge("body")?,
                if shadows_input { None } else { input },
            )?;
            let params = params
                .into_iter()
                .map(|p| edit.derive_inner(p, []))
                .collect::<Result<Vec<_>, _>>()?;
            closure(edit, expr, body, &params)
        }
        Some(Expr::Binary) => {
            let left = compose(edit, edit.node(expr)?.edge("left")?, input)?;
            let right = compose(edit, edit.node(expr)?.edge("right")?, input)?;
            edit.derive_inner(expr, [("left", link(left)), ("right", link(right))])
        }
        Some(Expr::Deref | Expr::Field | Expr::Unwrap | Expr::Some | Expr::Unary | Expr::Cast) => {
            let slot = if matches!(kind(edit, expr), Some(Expr::Unary | Expr::Cast)) {
                "source"
            } else {
                "expression"
            };
            let child = compose(edit, edit.node(expr)?.edge(slot)?, input)?;
            if kind(edit, expr) == Some(Expr::Unwrap) {
                let ty = edit.derive_inner(typed(edit, expr)?, [])?;
                edit.derive_inner(expr, [(slot, link(child)), ("type", link(ty))])
            } else {
                edit.derive_inner(expr, [(slot, link(child))])
            }
        }
        Some(Expr::Clone) => {
            let child = edit.node(expr)?.edge("expression")?;
            let child = compose(edit, child, input)?;
            // Both a function call and an array constructor produce owned values.
            if matches!(kind(edit, child), Some(Expr::Apply | Expr::Array)) {
                Ok(child)
            } else {
                edit.derive_inner(expr, [("expression", link(child))])
            }
        }
        Some(Expr::NoComparator | Expr::FieldComparator | Expr::DirectComparator) => {
            let ty = edit.derive_inner(typed(edit, expr)?, [])?;
            let mut changes = vec![("type", link(ty))];
            if let Ok(source) = edit.node(expr)?.edge("source") {
                let source = compose(edit, source, input)?;
                changes.push(("source", link(source)));
            }
            edit.derive_inner(expr, changes)
        }
        Some(Expr::Array) => {
            let data = edit.node(expr)?.edges("data")?;
            let data = data
                .into_iter()
                .map(|x| compose(edit, x, input))
                .collect::<Result<Vec<_>, _>>()?;
            edit.derive_inner(expr, [("data", links(&data))])
        }
        Some(Expr::Path | Expr::VariablePath) => edit.derive_inner(expr, []),
        _ if edit.node(expr)?.class().ends_with("Literal") => edit.derive_inner(expr, []),
        _ => Err(Error::new(format!(
            "unsupported expression {} in ARRAY_AGG postprocessing",
            edit.node(expr)?.class()
        ))),
    }
}

pub(super) struct Parameters {
    pub ids: Vec<NodeId>,
    types: Vec<NodeId>,
    names: Vec<String>,
    pair: bool,
    shared_variable: Option<NodeId>,
    arguments: std::collections::BTreeMap<usize, NodeId>,
}

impl Parameters {
    pub fn new(
        edit: &mut CircuitEditor,
        binding: &Binding,
        value: NodeId,
        next: &mut u64,
    ) -> Result<Self, Error> {
        let mut result = Self {
            ids: Vec::new(),
            types: Vec::new(),
            names: Vec::new(),
            pair: binding.pair,
            shared_variable: None,
            arguments: Default::default(),
        };
        for (index, &param) in binding.parameters.iter().enumerate() {
            let old = typed(edit, param)?;
            let ty = if binding.pair {
                let fields = edit.node(old)?.edges("tupFields")?;
                let [key, previous] = fields.as_slice() else {
                    return Err(Error::new("map parameter must be an indexed pair"));
                };
                let value_ref = edit.derive_inner(*previous, [("type", link(value))])?;
                edit.derive_inner(old, [("tupFields", links(&[*key, value_ref]))])?
            } else if index == binding.value {
                edit.derive_inner(old, [("type", link(value))])?
            } else {
                old
            };
            let name = format!("t_{}", *next);
            *next = next
                .checked_add(1)
                .ok_or_else(|| Error::new("generated variable names exhausted"))?;
            result.ids.push(edit.derive_inner(
                param,
                [
                    ("type", link(ty)),
                    ("name", Field::Data(name.clone().into())),
                ],
            )?);
            result.types.push(ty);
            result.names.push(name);
        }
        Ok(result)
    }

    pub fn window(
        edit: &mut CircuitEditor,
        binding: &Binding,
        collection: NodeId,
        old_var: NodeId,
        next: &mut u64,
    ) -> Result<Self, Error> {
        let old_pair = typed(edit, binding.parameters[0])?;
        let old_fields = edit.node(old_pair)?.edges("tupFields")?;
        let [old_key, old_value] = old_fields.as_slice() else {
            return Err(Error::new("invalid window index parameter"));
        };
        let key = edit.derive_inner(
            *old_key,
            [("type", link(edit.node(collection)?.edge("keyType")?))],
        )?;
        let value = edit.derive_inner(
            *old_value,
            [("type", link(edit.node(collection)?.edge("elementType")?))],
        )?;
        let pair = edit.derive_inner(old_pair, [("tupFields", links(&[key, value]))])?;
        let name = format!("t_{}", *next);
        *next = next
            .checked_add(1)
            .ok_or_else(|| Error::new("generated variable names exhausted"))?;
        let param = edit.derive_inner(
            binding.parameters[0],
            [
                ("name", Field::Data(name.clone().into())),
                ("type", link(pair)),
            ],
        )?;
        let var = edit.derive_inner(
            old_var,
            [
                ("variable", Field::Data(name.clone().into())),
                ("type", link(pair)),
            ],
        )?;
        Ok(Self {
            ids: vec![param],
            types: vec![pair],
            names: vec![name],
            pair: true,
            shared_variable: Some(var),
            arguments: Default::default(),
        })
    }

    fn variable(
        &self,
        edit: &mut CircuitEditor,
        old: NodeId,
        param: usize,
    ) -> Result<NodeId, Error> {
        if let Some(var) = self.shared_variable {
            return Ok(var);
        }
        edit.derive_inner(
            old,
            [
                ("type", link(self.types[param])),
                ("variable", Field::Data(self.names[param].clone().into())),
            ],
        )
    }

    fn argument(
        &mut self,
        edit: &mut CircuitEditor,
        old: NodeId,
        param: usize,
    ) -> Result<NodeId, Error> {
        if let Some(&var) = self.arguments.get(&param) {
            return Ok(var);
        }
        let var = self.variable(edit, old, param)?;
        self.arguments.insert(param, var);
        Ok(var)
    }

    pub fn project(
        &mut self,
        edit: &mut CircuitEditor,
        read: Access,
        shared: bool,
    ) -> Result<NodeId, Error> {
        let old_deref = edit.node(read.expression)?.edge("expression")?;
        let old_source = edit.node(old_deref)?.edge("expression")?;
        let (source, reference) = if self.pair {
            let old_var = edit.node(old_source)?.edge("expression")?;
            let part_type = edit
                .node(self.types[0])?
                .edges("tupFields")?
                .get(read.part)
                .copied()
                .ok_or_else(|| Error::new("field outside indexed pair"))?;
            // The beta-reduced reader gets one key argument; every key field
            // refers to that same projection. Value fields are built individually.
            let share_key = shared && read.part == 0;
            let key = if share_key {
                self.arguments.get(&0).copied()
            } else {
                None
            };
            let source = match key {
                Some(key) => key,
                None => {
                    let var = self.variable(edit, old_var, 0)?;
                    let field = edit.derive_inner(
                        old_source,
                        [("expression", link(var)), ("type", link(part_type))],
                    )?;
                    if share_key {
                        self.arguments.insert(0, field);
                    }
                    field
                }
            };
            (source, part_type)
        } else {
            (
                if shared {
                    self.argument(edit, old_source, read.part)?
                } else {
                    self.variable(edit, old_source, read.part)?
                },
                self.types[read.part],
            )
        };
        let row = edit.node(reference)?.edge("type")?;
        let ty = edit
            .node(row)?
            .edges("tupFields")?
            .get(read.field)
            .copied()
            .ok_or_else(|| Error::new("field outside aggregate row"))?;
        let deref = edit.derive_inner(
            old_deref,
            [("expression", link(source)), ("type", link(row))],
        )?;
        edit.derive_inner(
            read.expression,
            [("expression", link(deref)), ("type", link(ty))],
        )
    }
}

/// The beta-reduced reader reuses the projection for each unfused value field.
/// Keep that sharing when two columns call different functions on one array.
pub(super) fn rebind(
    edit: &mut CircuitEditor,
    id: NodeId,
    binding: &Binding,
    params: &mut Parameters,
    values: &mut std::collections::BTreeMap<usize, NodeId>,
) -> Result<NodeId, Error> {
    if let Some(read) = binding.access(edit, id) {
        if read.part == binding.value {
            if let Some(&field) = values.get(&read.field) {
                return Ok(field);
            }
            let field = params.project(edit, read, true)?;
            values.insert(read.field, field);
            return Ok(field);
        }
        return params.project(edit, read, true);
    }
    let changes = match kind(edit, id) {
        Some(Expr::Closure) => {
            let parameters = edit.node(id)?.edges("parameters")?;
            let nested = binding.nested(edit, &parameters)?;
            let old = edit.node(id)?.edge("body")?;
            let new = rebind(edit, old, &nested, params, values)?;
            if new == old {
                return Ok(id);
            }
            for p in &parameters {
                if edit
                    .node(*p)?
                    .data("name")
                    .and_then(|n| n.as_str())
                    .is_some_and(|name| params.names.iter().any(|v| v == name))
                {
                    return Err(Error::new(
                        "generated reader variable would be captured by a nested closure",
                    ));
                }
            }
            return closure(edit, id, new, &parameters);
        }
        Some(Expr::Apply) => {
            let args = edit.node(id)?.edges("arguments")?;
            let new = args
                .iter()
                .map(|&a| rebind(edit, a, binding, params, values))
                .collect::<Result<Vec<_>, _>>()?;
            if args == new {
                return Ok(id);
            }
            vec![("arguments", links(&new))]
        }
        Some(Expr::Tuple | Expr::RawTuple) => {
            let fields = edit
                .node(id)?
                .edges("fields")?
                .into_iter()
                .map(|f| rebind(edit, f, binding, params, values))
                .collect::<Result<Vec<_>, _>>()?;
            vec![("fields", links(&fields))]
        }
        Some(Expr::UnsignedUnwrap | Expr::UnsignedWrap) => {
            let child = rebind(
                edit,
                edit.node(id)?.edge("source")?,
                binding,
                params,
                values,
            )?;
            vec![("source", link(child))]
        }
        Some(Expr::Binary) => {
            let left = rebind(edit, edit.node(id)?.edge("left")?, binding, params, values)?;
            let right = rebind(edit, edit.node(id)?.edge("right")?, binding, params, values)?;
            vec![("left", link(left)), ("right", link(right))]
        }
        Some(
            Expr::Clone
            | Expr::Deref
            | Expr::Field
            | Expr::Unwrap
            | Expr::Some
            | Expr::Unary
            | Expr::Cast,
        ) => {
            let slot = if matches!(kind(edit, id), Some(Expr::Unary | Expr::Cast)) {
                "source"
            } else {
                "expression"
            };
            let old = edit.node(id)?.edge(slot)?;
            let new = rebind(edit, old, binding, params, values)?;
            if new == old {
                return Ok(id);
            }
            vec![(slot, link(new))]
        }
        Some(Expr::VariablePath) if !binding.pair => {
            if let Some(param) = edit
                .node(id)?
                .data("variable")
                .and_then(|v| v.as_str())
                .and_then(|name| binding.parameter(name))
            {
                return params.argument(edit, id, param);
            }
            return Ok(id);
        }
        _ => return Ok(id),
    };
    edit.derive_inner(id, changes)
}

pub(super) fn columns(
    edit: &CircuitEditor,
    id: NodeId,
    out: &mut Vec<NodeId>,
) -> Result<bool, Error> {
    if !matches!(kind(edit, id), Some(Expr::Tuple | Expr::RawTuple))
        || !matches!(edit.node(id)?.field("fields"), Some(Field::Array(_)))
    {
        return Ok(false);
    }
    let fields = edit.node(id)?.edges("fields")?;
    if kind(edit, id) == Some(Expr::Tuple) {
        out.extend(fields);
    } else {
        for field in fields {
            if !columns(edit, field, out)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

pub(super) fn with_columns(
    edit: &mut CircuitEditor,
    old: NodeId,
    columns: &mut impl Iterator<Item = NodeId>,
) -> Result<NodeId, Error> {
    let old_fields = edit.node(old)?.edges("fields")?;
    let fields = if kind(edit, old) == Some(Expr::RawTuple) {
        old_fields
            .into_iter()
            .map(|f| with_columns(edit, f, columns))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        columns.take(old_fields.len()).collect()
    };
    let fields = fields
        .iter()
        .map(|&f| edit.expression(f))
        .collect::<Result<Vec<_>, _>>()?;
    let raw = kind(edit, old) == Some(Expr::RawTuple);
    let tuple = edit.tuple_expr(old, raw, &fields)?;
    if !raw
        && edit
            .node(typed(edit, old)?)?
            .data("mayBeNull")
            .and_then(|v| v.as_bool())
            == Some(true)
    {
        let ty = edit.derive_inner(
            typed(edit, tuple.into())?,
            [("mayBeNull", Field::Data(true.into()))],
        )?;
        return edit.derive_inner(tuple.into(), [("type", link(ty))]);
    }
    Ok(tuple.into())
}
