//! Rebind a closure to parameters whose unused columns have been removed.
use feldera_sql_expr::{Rewrite, Transformation, Visit, parameter_bindings};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
    TypeKind,
};
use serde::Deserialize;
use std::collections::HashMap;

pub enum FieldRewriteError<'a> {
    InvalidIr(Error),
    Binding(crate::BindingDiagnostic<'a>),
}

impl From<Error> for FieldRewriteError<'_> {
    fn from(error: Error) -> Self {
        Self::InvalidIr(error)
    }
}

enum ColumnOffsets {
    /// RewriteFields uses a prefix count for every original position.
    Prefix(Vec<usize>),
    /// A source layout names only the columns available to its reader.
    Retained(Vec<usize>),
}

impl ColumnOffsets {
    fn remap(&self, column: usize) -> Result<usize, Error> {
        match self {
            Self::Prefix(offsets) => offsets
                .get(column)
                .copied()
                .ok_or_else(|| Error::new("reader field exceeds the input arity")),
            Self::Retained(columns) => columns
                .binary_search(&column)
                .map_err(|_| Error::new("reader references a discarded column")),
        }
    }
}

struct Parameter {
    name: String,
    ty: TypeId,
    offsets: HashMap<Vec<usize>, ColumnOffsets>,
}

struct Reader {
    depth: usize,
    bindings: HashMap<NodeId, usize>,
    parameters: HashMap<usize, Parameter>,
}

impl Transformation for Reader {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        if let Some(param) = self.bindings.get(&id).and_then(|i| self.parameters.get(i)) {
            return if edit.expression_type(edit.expression(id)?)? == param.ty {
                Ok(Visit::Keep)
            } else {
                Ok(Visit::Replace(
                    edit.named_variable(param.ty, &param.name)?.into(),
                ))
            };
        }
        match edit.node(id)?.inner_kind() {
            Some(InnerKind::Type(_) | InnerKind::Expression(Expr::Closure)) => Ok(Visit::Keep),
            _ => Ok(Visit::Children),
        }
    }

    fn rebuild_field(
        &mut self,
        edit: &mut CircuitEditor,
        original: NodeId,
        rebuilt: NodeId,
        _translating: bool,
    ) -> Result<NodeId, Error> {
        let field = edit.node(original)?;
        let column = |id| {
            edit.node(id)?
                .data("fieldNo")
                .and_then(|v| v.as_u64())
                .and_then(|v| usize::try_from(v).ok())
                .ok_or_else(|| Error::new("missing projected reader field number"))
        };
        let mut index = column(original)?;
        let source = field.edge("expression")?;
        let source_type = edit.expression_type(edit.expression(source)?)?;
        // Raw tuples preserve positions, even when a field becomes an empty tuple.
        if edit.type_kind(source_type)? != TypeKind::RawTuple {
            let mut receiver = source;
            let mut path = Vec::new();
            loop {
                let node = edit.node(receiver)?;
                match node.inner_kind() {
                    Some(InnerKind::Expression(Expr::Deref | Expr::Unwrap | Expr::Clone)) => (),
                    Some(InnerKind::Expression(Expr::Field)) if path.len() + 1 < self.depth => {
                        path.push(column(receiver)?);
                    }
                    _ => break,
                }
                receiver = node.edge("expression")?;
            }
            if let Some(param) = self
                .bindings
                .get(&receiver)
                .and_then(|i| self.parameters.get(i))
            {
                path.reverse();
                if let Some(offsets) = param.offsets.get(&path) {
                    index = offsets.remap(index)?;
                }
            }
        }
        let value = edit.expression(edit.node(rebuilt)?.edge("expression")?)?;
        Ok(edit.tuple_field(value, index)?.into())
    }
}

/// Usage of one original tuple, identified by its field path from the parameter.
/// Dereferences do not consume a position in the path.
#[derive(Deserialize)]
pub struct TupleUsage {
    path: Vec<usize>,
    used: Vec<bool>,
}

fn offsets(used: &[bool]) -> ColumnOffsets {
    let mut next = 0;
    let offsets = used
        .iter()
        .map(|&keep| {
            let index = next;
            next += usize::from(keep);
            index
        })
        .collect();
    ColumnOffsets::Prefix(offsets)
}

/// Parameter declarations with equivalent types retain their identities. Variable
/// expressions use the replacement type object, including when it is equivalent.
pub fn rebind_reader_with_layout(
    edit: &mut CircuitEditor,
    closure: NodeId,
    replacements: &[NodeId],
    usage: &[Vec<TupleUsage>],
) -> Result<ExpressionId, Error> {
    if edit.node(closure)?.edges("parameters")?.len() != replacements.len()
        || replacements.len() != usage.len()
    {
        return Err(Error::new(
            "reader parameter count differs from its field projections",
        ));
    }
    let projected = replacements
        .iter()
        .zip(usage)
        .enumerate()
        .map(|(slot, (&new, tuples))| {
            let fields = tuples
                .iter()
                .map(|tuple| (tuple.path.clone(), offsets(&tuple.used)))
                .collect();
            (slot, new, fields)
        });
    rewrite_reader(edit, closure, projected)
}

/// Rebind the top-level fields of each parameter using its column mask.
pub fn rebind_projected_reader(
    edit: &mut CircuitEditor,
    closure: NodeId,
    replacements: &[NodeId],
    usage: &[Vec<bool>],
) -> Result<ExpressionId, Error> {
    if edit.node(closure)?.edges("parameters")?.len() != replacements.len()
        || replacements.len() != usage.len()
    {
        return Err(Error::new(
            "reader parameter count differs from its field projections",
        ));
    }
    let projected = replacements
        .iter()
        .zip(usage)
        .enumerate()
        .map(|(slot, (&new, used))| (slot, new, used.as_slice()));
    rebind_parameters(edit, closure, projected)
}

pub(super) fn rebind_parameters<'a>(
    edit: &mut CircuitEditor,
    closure: NodeId,
    projected: impl IntoIterator<Item = (usize, NodeId, &'a [bool])>,
) -> Result<ExpressionId, Error> {
    rewrite_reader(
        edit,
        closure,
        projected
            .into_iter()
            .map(|(slot, new, used)| (slot, new, HashMap::from([(vec![], offsets(used))]))),
    )
}

pub(super) fn rebind_source_reader(
    edit: &mut CircuitEditor,
    closure: NodeId,
    replacement: NodeId,
    columns: &[usize],
) -> Result<ExpressionId, Error> {
    let fields = HashMap::from([(Vec::new(), ColumnOffsets::Retained(columns.to_vec()))]);
    rewrite_reader(edit, closure, [(0, replacement, fields)])
}

fn rewrite_reader(
    edit: &mut CircuitEditor,
    closure: NodeId,
    projected: impl IntoIterator<Item = (usize, NodeId, HashMap<Vec<usize>, ColumnOffsets>)>,
) -> Result<ExpressionId, Error> {
    let function = edit.node(closure)?;
    let original = function.edges("parameters")?;
    let body = function.edge("body")?;
    let bindings = parameter_bindings(edit, body, &original)?;
    let mut declarations = original.clone();
    let mut parameters = HashMap::new();
    let mut depth = 0;
    for (slot, new, offsets) in projected {
        let old = *original
            .get(slot)
            .ok_or_else(|| Error::new("missing projected reader parameter"))?;
        let param = edit.node(new)?;
        let name = param
            .data("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::new("missing projected reader parameter name"))?
            .to_owned();
        let ty = edit.type_id(param.edge("type")?)?;
        let old_type = edit.type_id(edit.node(old)?.edge("type")?)?;
        if !edit.same_type(old_type, ty)? {
            declarations[slot] = new;
        }
        depth = depth.max(offsets.keys().map(|path| path.len() + 1).max().unwrap_or(0));
        parameters.insert(slot, Parameter { name, ty, offsets });
    }
    let result = Rewrite::new(Reader {
        depth,
        bindings,
        parameters,
    })
    .apply(edit, body)?;
    if body == result && declarations == original {
        return edit.expression(closure);
    }
    let types = declarations
        .iter()
        .map(|&p| edit.node(p)?.edge("type"))
        .collect::<Result<Vec<_>, _>>()?;
    let signature = edit.closure_type(edit.expression(result)?, &types)?;
    let function = edit.derive_inner(
        closure,
        [
            (
                "parameters",
                Field::Array(declarations.into_iter().map(Field::Reference).collect()),
            ),
            ("type", Field::Reference(signature)),
            ("body", Field::Reference(result)),
        ],
    )?;
    edit.expression(function)
}

/// Narrow the input row in each aggregate closure that reads it. Accumulators,
/// weights and the final conversion retain their original declarations.
pub fn rebind_projected_aggregate(
    edit: &mut CircuitEditor,
    aggregate: NodeId,
    replacements: &[NodeId],
    used: &[bool],
) -> Result<ExpressionId, Error> {
    let readers = crate::aggregate_groups::row_parameters(edit.node(aggregate)?)?;
    if readers.len() != replacements.len() {
        return Err(Error::new(
            "aggregate row parameter count differs from its replacements",
        ));
    }
    let mut changes = Vec::new();
    for (&(field, slot), &replacement) in readers.iter().zip(replacements) {
        let closure = edit.node(aggregate)?.edge(field)?;
        let narrowed = rebind_parameters(edit, closure, [(slot, replacement, used)])?;
        if NodeId::from(narrowed) != closure {
            changes.push((field, Field::Reference(narrowed.into())));
        }
    }
    if changes.is_empty() {
        return edit.expression(aggregate);
    }
    let rewritten = edit.derive_inner(aggregate, changes)?;
    edit.expression(rewritten)
}
