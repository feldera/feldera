//! Track recoverable parameter columns through a closure. An expression can
//! add computed columns and still be injective if it also copies every input.
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId, TypeKind as Ty,
};
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Part {
    Row,
    Key,
    Value,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Column {
    parameter: usize,
    part: Part,
    field: usize,
}

#[derive(Clone, Default)]
enum Source {
    #[default]
    Unknown,
    Column(Column),
    Ref(Rc<Source>),
    Tuple(Rc<[Source]>),
}

impl Source {
    fn collect(&self, columns: &mut BTreeSet<Column>) -> bool {
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

fn initial(
    edit: &CircuitEditor,
    ty: NodeId,
    parameter: usize,
    part: Part,
    depth: usize,
) -> Result<(Source, bool), Error> {
    if depth > 128 {
        return Err(Error::new("parameter type exceeds nesting limit"));
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
                            Source::Column(Column {
                                parameter,
                                part,
                                field,
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
                [key, value] if part == Part::Row => Source::Tuple(
                    [
                        initial(edit, *key, parameter, Part::Key, depth + 1)?.0,
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

fn preserves_cast(edit: &CircuitEditor, node: &Node) -> Result<bool, Error> {
    let from = edit.expression_type(edit.expression(node.edge("source")?)?)?;
    let to = edit.type_id(node.edge("type")?)?;
    let (a, b) = (edit.node(from.into())?, edit.node(to.into())?);
    match (edit.type_kind(from)?, edit.type_kind(to)?) {
        (Ty::Integer, Ty::Integer) => {
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
            if [a, b]
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
}

impl<'a> Interpreter<'a> {
    fn eval(&mut self, id: NodeId, scope: &Scope<'a>, depth: usize) -> Result<Source, Error> {
        if depth > 128 {
            return Err(Error::new("lineage expression exceeds nesting limit"));
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
                if preserves_cast(self.edit, node)? {
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
        let (source, singleton) = initial(edit, parameter.edge("type")?, i, Part::Row, 0)?;
        let needed = if first_only { i == 0 } else { !singleton };
        if needed && !source.collect(&mut required) {
            return Ok(false);
        }
        scope.insert(text(parameter, "name")?, source);
    }
    let mut interpreter = Interpreter {
        edit,
        values: HashMap::new(),
        unhandled: false,
    };
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
    fn intersect_shared_tuples_preserves_the_common_column() {
        let column = Column {
            parameter: 0,
            part: Part::Value,
            field: 2,
        };
        let left = Source::Tuple([Source::Column(column), Source::Unknown].into());
        let right = Source::Tuple([Source::Column(column), Source::Column(column)].into());
        let common = duplicate_fields(left, 40).intersect(&duplicate_fields(right, 40));
        let mut columns = BTreeSet::new();
        assert!(!common.collect(&mut columns));
        assert!(columns == BTreeSet::from([column]));
    }
}
