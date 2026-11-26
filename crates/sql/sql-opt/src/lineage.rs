//! Track which parameter columns a closure copies into its result.
mod columns;
mod diagnostic;
mod equivalence;
mod evaluate;
mod report;
pub use columns::{
    checked_column_copies, checked_fixed_columns, column_copies, fixed_columns, OutputShape,
};
pub use equivalence::{checked_equal_computations, equal_computations};
pub use report::{checked_lineage_report, correlated_columns, lineage_report};

use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionKind as Expr, InnerKind, Node, NodeId, TypeKind as Ty,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum Part {
    None,
    Index,
    Value,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Column {
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum Merge {
    CopiedColumns,
    CommonSources,
    JoinSources,
}

#[derive(Clone, Default)]
enum Source {
    #[default]
    Unknown,
    Column(ParameterColumn),
    Tables(Rc<BTreeSet<report::TableColumn>>),
    Constant(NodeId),
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
                Self::Unknown | Self::Constant(_) | Self::Tables(_) => complete = false,
                Self::Column(column) => {
                    columns.insert(*column);
                }
                Self::Ref(inner) => work.push(inner),
                Self::Tuple(fields) => work.extend(fields.iter()),
            }
        }
        complete
    }

    fn combine(
        &self,
        other: &Self,
        edit: &CircuitEditor,
        merge: Merge,
    ) -> Result<Self, AnalysisError<'static>> {
        // Tuples can reuse the same computed field many times. Compare each
        // pair of source nodes once, preserving sharing in the result.
        type Pair = (*const Source, *const Source);
        let key = |a: &Source, b: &Source| -> Pair { (a, b) };
        let mut cache = HashMap::<Pair, Source>::new();
        let mut pending = vec![(self, other, false)];
        while let Some((a, b, expanded)) = pending.pop() {
            if cache.contains_key(&key(a, b)) {
                continue;
            }
            let value = match (a, b) {
                (Source::Unknown, right) if merge == Merge::JoinSources => right.clone(),
                (Source::Tables(left), Source::Tables(right)) => {
                    let columns: BTreeSet<_> = if merge == Merge::JoinSources {
                        left.union(right).cloned().collect()
                    } else {
                        left.intersection(right).cloned().collect()
                    };
                    if columns.is_empty() {
                        Source::Unknown
                    } else {
                        Source::Tables(Rc::new(columns))
                    }
                }
                (Source::Column(left), Source::Column(right)) if left == right => a.clone(),
                (Source::Constant(left), Source::Constant(right)) => {
                    // Constant.intersect and Constant.union ask the other value first.
                    match crate::index_key_equivalence::check_bodies(edit, *right, *left, None)? {
                        Some(true) => a.clone(),
                        Some(false) => Source::Unknown,
                        None => {
                            return Err(AnalysisError::Reference(
                                "unresolved constant equivalence",
                            ));
                        }
                    }
                }
                (Source::Ref(l), Source::Ref(r))
                    if merge == Merge::CopiedColumns && Rc::ptr_eq(l, r) =>
                {
                    a.clone()
                }
                (Source::Tuple(l), Source::Tuple(r))
                    if merge == Merge::CopiedColumns && Rc::ptr_eq(l, r) =>
                {
                    a.clone()
                }
                (Source::Ref(l), Source::Ref(r)) => {
                    if !expanded {
                        pending.push((a, b, true));
                        pending.push((l, r, false));
                        continue;
                    }
                    let inner = &cache[&key(l, r)];
                    if merge != Merge::CopiedColumns && matches!(inner, Source::Unknown) {
                        Source::Unknown
                    } else {
                        Source::Ref(Rc::new(inner.clone()))
                    }
                }
                (Source::Tuple(l), Source::Tuple(r)) if l.len() != r.len() => {
                    return Err(AnalysisError::Diagnostic(LineageDiagnostic::ShapeAssertion));
                }
                (Source::Tuple(l), Source::Tuple(r)) => {
                    if !expanded {
                        pending.push((a, b, true));
                        pending.extend(l.iter().zip(r.iter()).rev().map(|(l, r)| (l, r, false)));
                        continue;
                    }
                    let fields: Rc<[Source]> = l
                        .iter()
                        .zip(r.iter())
                        .map(|(l, r)| cache[&key(l, r)].clone())
                        .collect();
                    if merge != Merge::CopiedColumns
                        && fields.iter().all(|s| matches!(s, Source::Unknown))
                    {
                        Source::Unknown
                    } else {
                        Source::Tuple(fields)
                    }
                }
                _ => Source::Unknown,
            };
            cache.insert(key(a, b), value);
        }
        Ok(cache
            .remove(&key(self, other))
            .expect("completed intersection"))
    }
}

fn text<'a>(node: &'a Node, key: &str) -> Result<&'a str, Error> {
    node.data(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new(format!("missing {key} on {}", node.class())))
}

#[derive(Debug)]
pub enum LineageDiagnostic<'a> {
    Binding(crate::BindingDiagnostic<'a>),
    UnsupportedExpression(NodeId),
    Comparison(crate::ComparisonDiagnostic),
    MissingInput(feldera_sql_ir::OutputPort),
    OutputWidth {
        operator: NodeId,
        fields: usize,
    },
    SourceCast {
        expected: diagnostic::ExpectedSource,
        values: Vec<diagnostic::ReportValue>,
    },
    ShapeAssertion,
    JoinParameterIndex(usize),
    FieldIndex {
        index: usize,
        size: usize,
    },
}

#[derive(Debug)]
pub enum AnalysisError<'a> {
    Diagnostic(LineageDiagnostic<'a>),
    InvalidIr(Error),
    Reference(&'static str),
}

impl From<crate::KeyComparisonError> for AnalysisError<'_> {
    fn from(error: crate::KeyComparisonError) -> Self {
        match error {
            crate::KeyComparisonError::InvalidIr(error) => Self::InvalidIr(error),
            crate::KeyComparisonError::Diagnostic(issue) => {
                Self::Diagnostic(LineageDiagnostic::Comparison(issue))
            }
        }
    }
}

impl From<Error> for AnalysisError<'_> {
    fn from(error: Error) -> Self {
        Self::InvalidIr(error)
    }
}

impl From<AnalysisError<'_>> for Error {
    fn from(error: AnalysisError<'_>) -> Self {
        match error {
            AnalysisError::InvalidIr(error) => error,
            AnalysisError::Reference(message) => Error::new(message),
            AnalysisError::Diagnostic(LineageDiagnostic::SourceCast { expected, .. }) => {
                Error::new(format!("expected {expected:?} lineage"))
            }
            AnalysisError::Diagnostic(LineageDiagnostic::ShapeAssertion) => {
                Error::new("incompatible lineage shape")
            }
            AnalysisError::Diagnostic(LineageDiagnostic::JoinParameterIndex(size)) => {
                Error::new(format!("join lineage needs three parameters, found {size}"))
            }
            AnalysisError::Diagnostic(LineageDiagnostic::FieldIndex { index, size }) => {
                Error::new(format!("lineage field {index} exceeds tuple width {size}"))
            }
            AnalysisError::Diagnostic(LineageDiagnostic::MissingInput(port)) => {
                Error::new(format!("missing lineage for input {port:?}"))
            }
            AnalysisError::Diagnostic(LineageDiagnostic::OutputWidth { operator, fields }) => {
                Error::new(format!(
                    "{fields} lineage fields do not fit the output of {operator:?}"
                ))
            }
            AnalysisError::Diagnostic(LineageDiagnostic::Comparison(issue)) => {
                Error::new(issue.to_string())
            }
            AnalysisError::Diagnostic(LineageDiagnostic::UnsupportedExpression(node)) => {
                Error::new(format!("unsupported lineage expression {node:?}"))
            }
            AnalysisError::Diagnostic(LineageDiagnostic::Binding(
                crate::BindingDiagnostic::Free { name, .. },
            )) => Error::new(format!("unbound variable {name} in lineage closure")),
            AnalysisError::Diagnostic(LineageDiagnostic::Binding(
                crate::BindingDiagnostic::Conflict { variable, .. },
            )) => Error::new(format!(
                "shared variable {variable:?} refers to two declarations"
            )),
        }
    }
}

fn initial(
    edit: &CircuitEditor,
    ty: NodeId,
    parameter: usize,
    part: Part,
) -> Result<(Source, bool), AnalysisError<'static>> {
    let mut id = edit.type_id(ty)?;
    let mut reference_count = 0;
    let mut nullable_reference = false;
    let mut visited = std::collections::HashSet::new();
    while edit.type_kind(id)? == Ty::Ref {
        if !visited.insert(NodeId::from(id)) {
            return Err(Error::new("cycle in lineage parameter references").into());
        }
        reference_count += 1;
        nullable_reference |= edit.nullable(id)?;
        id = edit.type_id(edit.node(id.into())?.edge("type")?)?;
    }
    let node = edit.node(id.into())?;
    let kind = edit.type_kind(id)?;
    let (mut source, singleton) = match kind {
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
                        initial(edit, *key, parameter, Part::Index)?.0,
                        initial(edit, *value, parameter, Part::Value)?.0,
                    ]
                    .into(),
                ),
                _ => Source::Unknown,
            };
            (source, fields.is_empty())
        }
        Ty::Null => (Source::Unknown, true),
        _ => (Source::Unknown, false),
    };
    for _ in 0..reference_count {
        source = Source::Ref(Rc::new(source));
    }
    let singleton = singleton && !nullable_reference && (kind == Ty::Null || !edit.nullable(id)?);
    Ok((source, singleton))
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
            return Ok(true);
        }
        (Ty::Variant, Ty::Variant) if edit.nullable(from)? && !edit.nullable(to)? => {
            return Ok(false);
        }
        _ => {}
    }
    edit.same_type_ignoring_nullability(from, to)
}

type DeclarationValues = HashMap<NodeId, Source>;

enum UnsupportedExpressions {
    Reject,
    Track { found: bool },
}

struct Interpreter<'a> {
    edit: &'a CircuitEditor,
    // The Java visitor resolves each shared expression in its declaration scope.
    values: HashMap<NodeId, Source>,
    // Injectivity uses unknown values for assignments and returns and rejects
    // the closure after traversal. Key and report visitors fail at the node.
    unsupported: UnsupportedExpressions,
    lossless_casts: bool,
    track_constants: bool,
    comparisons: Option<Vec<report::Finding>>,
}

impl<'a> Interpreter<'a> {
    fn new(edit: &'a CircuitEditor, lossless_casts: bool) -> Self {
        Self {
            edit,
            lossless_casts,
            values: HashMap::new(),
            unsupported: UnsupportedExpressions::Reject,
            track_constants: false,
            comparisons: None,
        }
    }

    fn closure_result(&mut self, id: NodeId) -> Result<Source, AnalysisError<'a>> {
        let closure = self.edit.node(id)?;
        let mut declarations = DeclarationValues::new();
        for (i, param) in closure.edges("parameters")?.into_iter().enumerate() {
            let parameter = self.edit.node(param)?;
            let (source, _) = initial(self.edit, parameter.edge("type")?, i, Part::None)?;
            declarations.insert(param, source);
        }
        self.eval(id, declarations)
    }
}

pub(super) fn injective(edit: &CircuitEditor, id: NodeId, first_only: bool) -> Result<bool, Error> {
    let closure = edit.node(id)?;
    if !matches!(
        closure.inner_kind(),
        Some(InnerKind::Expression(Expr::Closure))
    ) {
        return Ok(false);
    }
    let mut declarations = DeclarationValues::new();
    let mut required = BTreeSet::new();
    for (i, param) in closure.edges("parameters")?.into_iter().enumerate() {
        let parameter = edit.node(param)?;
        let (source, singleton) = initial(edit, parameter.edge("type")?, i, Part::None)?;
        let needed = if first_only { i == 0 } else { !singleton };
        if needed && !source.collect(&mut required) {
            return Ok(false);
        }
        declarations.insert(param, source);
    }
    let mut interpreter = Interpreter::new(edit, true);
    interpreter.unsupported = UnsupportedExpressions::Track { found: false };
    let value = interpreter.eval(id, declarations)?;
    let mut copied = BTreeSet::new();
    value.collect(&mut copied);
    Ok(matches!(
        interpreter.unsupported,
        UnsupportedExpressions::Track { found: false }
    ) && required.is_subset(&copied))
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
        let edit = CircuitEditor::new(
            feldera_sql_ir::Circuit::from_json(serde_json::json!({
                "class":"DBSPCircuit", "id":0, "allOperators":[], "declarations":[]
            }))
            .unwrap(),
        );
        let common = duplicate_fields(left, 40)
            .combine(&duplicate_fields(right, 40), &edit, Merge::CopiedColumns)
            .unwrap();
        let mut columns = BTreeSet::new();
        assert!(!common.collect(&mut columns));
        assert!(columns == BTreeSet::from([column]));
    }
}
