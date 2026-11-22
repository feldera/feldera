//! Track which parameter columns a closure copies into its result.
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId, TypeKind as Ty,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum Part {
    None,
    Index,
    Value,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct Column {
    pub(crate) part: Part,
    pub(crate) field: usize,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ColumnCopy {
    pub(crate) parameter: usize,
    pub(crate) source: Column,
    pub(crate) output: Column,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ParameterColumn {
    parameter: usize,
    column: Column,
}

#[derive(Clone, Default)]
enum Source {
    #[default]
    Unknown,
    Column(ParameterColumn),
    Ref(Rc<Source>),
    Tuple(Rc<[Source]>),
}

impl Source {
    fn collect(&self, columns: &mut BTreeSet<ParameterColumn>) -> bool {
        let mut complete = true;
        let mut work = vec![self];
        let mut seen = std::collections::HashSet::new();
        while let Some(source) = work.pop() {
            if !seen.insert(std::ptr::from_ref(source)) {
                continue;
            }
            match source {
                Self::Unknown => complete = false,
                Self::Column(column) => {
                    columns.insert(*column);
                }
                Self::Ref(inner) => work.push(inner),
                Self::Tuple(fields) => work.extend(fields.iter()),
            }
        }
        complete
    }

    fn intersect(&self, other: &Self) -> Self {
        // Tuples can reuse the same computed field many times. Compare each
        // pair of source nodes once, preserving sharing in the result.
        fn visit(
            a: &Source,
            b: &Source,
            cache: &mut HashMap<(*const Source, *const Source), Source>,
        ) -> Source {
            let key = (std::ptr::from_ref(a), std::ptr::from_ref(b));
            if let Some(known) = cache.get(&key) {
                return known.clone();
            }
            let value = match (a, b) {
                (Source::Column(left), Source::Column(right)) if left == right => a.clone(),
                (Source::Ref(left), Source::Ref(right)) if Rc::ptr_eq(left, right) => a.clone(),
                (Source::Tuple(left), Source::Tuple(right)) if Rc::ptr_eq(left, right) => a.clone(),
                (Source::Ref(left), Source::Ref(right)) => {
                    Source::Ref(Rc::new(visit(left, right, cache)))
                }
                (Source::Tuple(left), Source::Tuple(right)) if left.len() == right.len() => {
                    Source::Tuple(
                        left.iter()
                            .zip(right.iter())
                            .map(|(l, r)| visit(l, r, cache))
                            .collect(),
                    )
                }
                _ => Source::Unknown,
            };
            cache.insert(key, value.clone());
            value
        }
        visit(self, other, &mut HashMap::new())
    }
}

fn text<'a>(node: &'a Node, key: &str) -> Result<&'a str, Error> {
    node.data(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new(format!("missing {key} on {}", node.class())))
}

enum AnalysisError {
    InvalidIr(Error),
    NestingLimit(&'static str),
}

impl From<Error> for AnalysisError {
    fn from(error: Error) -> Self {
        Self::InvalidIr(error)
    }
}

impl From<AnalysisError> for Error {
    fn from(error: AnalysisError) -> Self {
        match error {
            AnalysisError::InvalidIr(error) => error,
            AnalysisError::NestingLimit(message) => Error::new(message),
        }
    }
}

fn initial(
    edit: &CircuitEditor,
    ty: NodeId,
    parameter: usize,
    part: Part,
    depth: usize,
) -> Result<(Source, bool), AnalysisError> {
    if depth > 128 {
        return Err(AnalysisError::NestingLimit(
            "parameter type exceeds nesting limit",
        ));
    }
    let id = edit.type_id(ty)?;
    let node = edit.node(ty)?;
    let nullable = edit.nullable(id)?;
    let (source, singleton) = match edit.type_kind(id)? {
        Ty::Ref => {
            let (source, singleton) =
                initial(edit, node.edge("type")?, parameter, part, depth + 1)?;
            (Source::Ref(Rc::new(source)), singleton)
        }
        Ty::Tuple => {
            let fields = node.edges("tupFields")?;
            (
                Source::Tuple(
                    (0..fields.len())
                        .map(|field| {
                            Source::Column(ParameterColumn {
                                parameter,
                                column: Column { part, field },
                            })
                        })
                        .collect(),
                ),
                fields.is_empty(),
            )
        }
        Ty::RawTuple => {
            let fields = node.edges("tupFields")?;
            let source = match fields.as_slice() {
                [key, value] if part == Part::None => Source::Tuple(
                    [
                        initial(edit, *key, parameter, Part::Index, depth + 1)?.0,
                        initial(edit, *value, parameter, Part::Value, depth + 1)?.0,
                    ]
                    .into(),
                ),
                _ => Source::Unknown,
            };
            (source, fields.is_empty())
        }
        Ty::Null => return Ok((Source::Unknown, true)),
        _ => (Source::Unknown, false),
    };
    Ok((source, singleton && !nullable))
}

fn preserves_cast(edit: &CircuitEditor, node: &Node, lossless_casts: bool) -> Result<bool, Error> {
    let from = edit.expression_type(edit.expression(node.edge("source")?)?)?;
    let to = edit.type_id(node.edge("type")?)?;
    let (a, b) = (edit.node(from.into())?, edit.node(to.into())?);
    match (edit.type_kind(from)?, edit.type_kind(to)?) {
        (Ty::Integer, Ty::Integer) if lossless_casts => {
            let safe = matches!(text(node, "safe")?, "SqlSafe" | "UnwrapResultSafe");
            let width = |n: &Node| {
                n.data("width")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| Error::new("missing integer width"))
            };
            let (source, target) = (width(a)?, width(b)?);
            if !safe
                || (a.data("signed") == b.data("signed") && target >= source)
                || (b.data("signed").and_then(|v| v.as_bool()) == Some(true) && target > source)
            {
                return Ok(true);
            }
        }
        (Ty::String, Ty::String)
            if lossless_casts
                && [a, b]
                    .iter()
                    .all(|n| n.data("fixed").and_then(|v| v.as_bool()) == Some(false))
                && b.data("precision").and_then(|v| v.as_i64()) == Some(-1) =>
        {
            return Ok(true)
        }
        (Ty::Variant, Ty::Variant) if edit.nullable(from)? && !edit.nullable(to)? => {
            return Ok(false)
        }
        _ => {}
    }
    edit.same_type_ignoring_nullability(from, to)
}

type Scope<'a> = HashMap<&'a str, Source>;

struct Interpreter<'a> {
    edit: &'a CircuitEditor,
    // The Java visitor resolves each shared expression in its declaration scope.
    values: HashMap<NodeId, Source>,
    unhandled: bool,
    lossless_casts: bool,
}

impl<'a> Interpreter<'a> {
    fn new(edit: &'a CircuitEditor, lossless_casts: bool) -> Self {
        Self {
            edit,
            lossless_casts,
            values: HashMap::new(),
            unhandled: false,
        }
    }

    fn closure_result(&mut self, id: NodeId) -> Result<Source, AnalysisError> {
        let closure = self.edit.node(id)?;
        let mut scope = Scope::new();
        for (i, param) in closure.edges("parameters")?.into_iter().enumerate() {
            let parameter = self.edit.node(param)?;
            let (source, _) = initial(self.edit, parameter.edge("type")?, i, Part::None, 0)?;
            scope.insert(text(parameter, "name")?, source);
        }
        self.eval(closure.edge("body")?, &scope, 0)
    }

    fn eval(
        &mut self,
        id: NodeId,
        scope: &Scope<'a>,
        depth: usize,
    ) -> Result<Source, AnalysisError> {
        if depth > 128 {
            return Err(AnalysisError::NestingLimit(
                "lineage expression exceeds nesting limit",
            ));
        }
        if let Some(value) = self.values.get(&id) {
            return Ok(value.clone());
        }
        let node = self.edit.node(id)?;
        let child = |name: &str| node.edge(name);
        let eval = |this: &mut Self, name: &str| this.eval(child(name)?, scope, depth + 1);
        let value = match node.inner_kind() {
            Some(InnerKind::Type(_)) => return Ok(Source::Unknown),
            Some(InnerKind::Expression(Expr::Return | Expr::Assignment)) => {
                self.unhandled = true;
                Source::Unknown
            }
            Some(InnerKind::Expression(Expr::VariablePath)) => scope
                .get(text(node, "variable")?)
                .cloned()
                .unwrap_or_default(),
            Some(InnerKind::Expression(Expr::Borrow)) => {
                Source::Ref(Rc::new(eval(self, "expression")?))
            }
            Some(InnerKind::Expression(Expr::Deref)) => match eval(self, "expression")? {
                Source::Ref(inner) => (*inner).clone(),
                _ => Source::Unknown,
            },
            Some(InnerKind::Expression(
                Expr::Clone | Expr::Some | Expr::Unwrap | Expr::UnwrapCustomOrd,
            )) => eval(self, "expression")?,
            Some(InnerKind::Expression(Expr::HandleError)) => eval(self, "source")?,
            Some(InnerKind::Expression(Expr::Cast)) => {
                let source = eval(self, "source")?;
                if preserves_cast(self.edit, node, self.lossless_casts)? {
                    source
                } else {
                    Source::Unknown
                }
            }
            Some(InnerKind::Expression(Expr::Tuple | Expr::RawTuple)) => {
                if let Some(Field::Array(_)) = node.field("fields") {
                    let fields = node
                        .edges("fields")?
                        .into_iter()
                        .map(|f| self.eval(f, scope, depth + 1))
                        .collect::<Result<Vec<_>, _>>()?;
                    Source::Tuple(fields.into())
                } else {
                    Source::Unknown
                }
            }
            Some(InnerKind::Expression(Expr::Field | Expr::CustomOrdField)) => {
                let field = node
                    .data("fieldNo")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| Error::new("missing field number"))?
                    as usize;
                match eval(self, "expression")? {
                    Source::Tuple(fields) => fields.get(field).cloned().unwrap_or_default(),
                    _ => Source::Unknown,
                }
            }
            Some(InnerKind::Expression(Expr::If)) => {
                eval(self, "condition")?;
                let positive = eval(self, "positive")?;
                if let Some(negative) = node.field("negative").and_then(Field::node_id) {
                    positive.intersect(&self.eval(negative, scope, depth + 1)?)
                } else {
                    Source::Unknown
                }
            }
            Some(InnerKind::Expression(Expr::Binary))
                if node.data("opcode").and_then(|v| v.as_str()) == Some("COALESCE") =>
            {
                eval(self, "left")?.intersect(&eval(self, "right")?)
            }
            Some(InnerKind::Expression(Expr::Closure)) => {
                let mut local = scope.clone();
                for id in node.edges("parameters")? {
                    local.remove(text(self.edit.node(id)?, "name")?);
                }
                self.eval(node.edge("body")?, &local, depth + 1)?
            }
            Some(InnerKind::Expression(Expr::Let)) => {
                let mut local = scope.clone();
                let var = self.edit.node(node.edge("variable")?)?;
                let init = eval(self, "initializer")?;
                // InnerLineage records the initializer on the declaration's variable path.
                self.values.insert(node.edge("variable")?, init);
                local.remove(text(var, "variable")?);
                self.eval(node.edge("consumer")?, &local, depth + 1)?
            }
            Some(InnerKind::Expression(Expr::Block)) => {
                let mut local = scope.clone();
                for stmt in node.edges("contents")? {
                    let s = self.edit.node(stmt)?;
                    if s.class() == "DBSPLetStatement" {
                        let init = match s.field("initializer").and_then(Field::node_id) {
                            Some(id) => self.eval(id, &local, depth + 1)?,
                            None => Source::Unknown,
                        };
                        local.insert(text(s, "variable")?, init);
                    } else {
                        self.eval(stmt, &local, depth + 1)?;
                    }
                }
                match node.field("lastExpression").and_then(Field::node_id) {
                    Some(id) => self.eval(id, &local, depth + 1)?,
                    None => Source::Unknown,
                }
            }
            _ => {
                // Returns and assignments invalidate the proof even in unused subexpressions.
                let mut children = Vec::new();
                for (field, value) in node.fields() {
                    if field != "type" {
                        value.collect_inner_edges(&mut children);
                    }
                }
                for id in children {
                    self.eval(id, scope, depth + 1)?;
                }
                Source::Unknown
            }
        };
        self.values.insert(id, value.clone());
        Ok(value)
    }
}

/// Map flat result positions to the columns of the declared output collection.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutputShape {
    width: usize,
    index_fields: Option<usize>,
}

impl OutputShape {
    fn column(&self, position: usize) -> Column {
        match self.index_fields {
            None => Column {
                part: Part::None,
                field: position,
            },
            Some(n) if position < n => Column {
                part: Part::Index,
                field: position,
            },
            Some(n) => Column {
                part: Part::Value,
                field: position - n,
            },
        }
    }
}

impl Source {
    fn deref_once(&self) -> &Self {
        match self {
            Source::Ref(value) => value,
            value => value,
        }
    }

    fn tuple_fields(&self, width: usize) -> &[Self] {
        match self.deref_once() {
            Source::Tuple(fields) if fields.len() == width => fields,
            _ => &[],
        }
    }
}

/// A raw pair represents an indexed output. Its key and value may be known
/// independently. A plain tuple can also supply just the values of a ranked output.
fn copies(result: &Source, raw_pair: bool, shape: &OutputShape) -> Result<Vec<ColumnCopy>, Error> {
    let mut output = Vec::new();
    let mut append = |fields: &[Source], offset| {
        for (position, value) in fields.iter().enumerate() {
            if let Source::Column(source) = value.deref_once() {
                output.push(ColumnCopy {
                    parameter: source.parameter,
                    source: source.column,
                    output: shape.column(offset + position),
                });
            }
        }
    };
    if raw_pair {
        let index = shape
            .index_fields
            .filter(|&i| i <= shape.width)
            .ok_or_else(|| Error::new("raw tuple result requires an indexed output shape"))?;
        if let [key, value] = result.tuple_fields(2) {
            append(key.tuple_fields(index), 0);
            append(value.tuple_fields(shape.width - index), index);
        }
    } else {
        append(result.tuple_fields(shape.width), 0);
    }
    Ok(output)
}

/// The reference visitor handles unusually deep closures and reports diagnostics for assignments and returns.
pub fn column_copies(
    edit: &CircuitEditor,
    id: NodeId,
    shape: &OutputShape,
    lossless_casts: bool,
) -> Result<Option<Vec<ColumnCopy>>, Error> {
    let mut interpreter = Interpreter::new(edit, lossless_casts);
    let value = match interpreter.closure_result(id) {
        Ok(value) if !interpreter.unhandled => value,
        Ok(_) | Err(AnalysisError::NestingLimit(_)) => return Ok(None),
        Err(AnalysisError::InvalidIr(error)) => return Err(error),
    };
    let body = edit.node(id)?.edge("body")?;
    let ty = edit.expression_type(edit.expression(body)?)?;
    copies(&value, edit.type_kind(ty)? == Ty::RawTuple, shape).map(Some)
}

pub(super) fn injective(edit: &CircuitEditor, id: NodeId, first_only: bool) -> Result<bool, Error> {
    let closure = edit.node(id)?;
    if !matches!(
        closure.inner_kind(),
        Some(InnerKind::Expression(Expr::Closure))
    ) {
        return Ok(false);
    }
    let mut scope = Scope::new();
    let mut required = BTreeSet::new();
    for (i, param) in closure.edges("parameters")?.into_iter().enumerate() {
        let parameter = edit.node(param)?;
        let (source, singleton) = initial(edit, parameter.edge("type")?, i, Part::None, 0)?;
        let needed = if first_only { i == 0 } else { !singleton };
        if needed && !source.collect(&mut required) {
            return Ok(false);
        }
        scope.insert(text(parameter, "name")?, source);
    }
    let mut interpreter = Interpreter::new(edit, true);
    let value = interpreter.eval(closure.edge("body")?, &scope, 0)?;
    let mut copied = BTreeSet::new();
    value.collect(&mut copied);
    Ok(!interpreter.unhandled && required.is_subset(&copied))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn duplicate_fields(mut field: Source, depth: usize) -> Source {
        for _ in 0..depth {
            field = Source::Tuple([field.clone(), field].into());
        }
        field
    }

    #[test]
    fn indexed_result_preserves_a_known_half_and_maps_ranked_columns() {
        let source = Column {
            part: Part::None,
            field: 3,
        };
        let field = Source::Column(ParameterColumn {
            parameter: 1,
            column: source,
        });
        let values = Source::Ref(Rc::new(Source::Tuple([field].into())));
        let output = Column {
            part: Part::Value,
            field: 0,
        };
        let expected = ColumnCopy {
            parameter: 1,
            source,
            output,
        };
        let indexed = OutputShape {
            width: 3,
            index_fields: Some(2),
        };
        let pair = Source::Tuple([Source::Unknown, values.clone()].into());
        assert_eq!(copies(&pair, true, &indexed).unwrap(), [expected]);

        let ranked = OutputShape {
            width: 1,
            index_fields: Some(0),
        };
        let outputs = copies(&values, false, &ranked).unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].output, output);
        assert_eq!(outputs[0].source, source);
        assert!(copies(&values, false, &indexed).unwrap().is_empty());
    }

    #[test]
    fn intersect_shared_tuples_preserves_the_common_column() {
        let column = ParameterColumn {
            parameter: 0,
            column: Column {
                part: Part::Value,
                field: 2,
            },
        };
        let left = Source::Tuple([Source::Column(column), Source::Unknown].into());
        let right = Source::Tuple([Source::Column(column), Source::Column(column)].into());
        let common = duplicate_fields(left, 40).intersect(&duplicate_fields(right, 40));
        let mut columns = BTreeSet::new();
        assert!(!common.collect(&mut columns));
        assert!(columns == BTreeSet::from([column]));
    }
}
