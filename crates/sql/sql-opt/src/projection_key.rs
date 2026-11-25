//! Compare the canonical spelling of a projection over one bound row. The Java
//! window and map fusion passes use printed closures, so cast safety and call result types do
//! not distinguish keys, while operand nullability in a binary expression does.
mod comparators;
mod numbers;
mod types;
use numbers::{decimal_text, floating_text};
use types::{scalar_spelling, type_term};
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, InnerKind, Node, NodeId,
    OutputPort, TypeId,
};
use indexmap::IndexSet;
use std::collections::{HashMap, HashSet};

/// Postorder value numbers preserve operand order and ignore incidental sharing
/// in the input graph. Repeated subexpressions occupy one slot.
#[derive(Eq, Hash, PartialEq)]
pub(crate) struct ProjectionKey(Vec<(Term, Vec<usize>)>);

#[derive(Eq, Hash, PartialEq)]
enum Term {
    Variable(usize),
    Closure(Vec<String>),
    Let(Option<String>),
    Block(Vec<BlockStatement>),
    ReferenceType {
        mutable: bool,
        nullable: bool,
    },
    Array,
    Map,
    NullCollection,
    Tuple,
    RawTuple {
        nullable: bool,
    },
    AbsentTuple,
    NullLiteral,
    VariantNullLiteral,
    NullPoint {
        nullable: bool,
    },
    Field(u64),
    Borrow { mutable: bool },
    Deref,
    Clone,
    IsNull,
    Question,
    Return,
    Lazy,
    Assignment,
    For(String),
    Unwrap(String),
    If,
    Call(String),
    CallClosure,
    Method(String),
    Cast,
    ScalarType(String),
    RawTupleType,
    TupleType { nullable: bool },
    FunctionType,
    NamedType { name: String, nullable: bool },
    Number(String),
    Bool(bool),
    String(String),
    Utf16EscapedString(String),
    Bytes(String),
    Uuid(String),
    TemporalText(String),
    Date(i64),
    Timestamp(i64),
    TypedNull(String),
    Binary {
        opcode: BinaryOp,
        left_nullable: bool,
        right_nullable: bool,
    },
    CollectionConversion(BinaryOp),
    Sort,
    Compare { field: Option<u64>, ascending: bool },
    AsymmetricComparator(Vec<comparators::Collation>),
    Unary {
        opcode: String,
        nullable: bool,
    },
    Interval {
        months: bool,
        units: String,
        value: i64,
        nullable: bool,
    },
}

#[derive(Eq, Hash, PartialEq)]
enum BlockStatement {
    Expression,
    Comment(Option<String>),
    Let { name: Option<String>, mutable: bool, initialized: bool },
}

/// A binding's position within the active parameter list identifies its uses
/// across renaming. Its index in the arena distinguishes sibling scopes when
/// memoizing shared expressions.
struct Binding<'a> {
    variable: RowParameter<'a>,
    parent: Option<usize>,
    position: usize,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct ScopedNode {
    id: NodeId,
    scope: usize,
    in_static: bool,
}

enum Visit {
    Read(ScopedNode),
    Alias(ScopedNode, ScopedNode),
    Intern(ScopedNode, Term, Vec<ScopedNode>),
    OrderedCall(ScopedNode, &'static str, ScopedNode, [bool; 2]),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyForm {
    Original,
    Canonical,
    /// A discarded binding left a gap in the names printed by CanonicalForm.
    PrintedNames,
}

impl KeyForm {
    fn printed_name(self, name: &str) -> Option<String> {
        (self == Self::PrintedNames).then(|| name.to_owned())
    }
}

impl ProjectionKey {
    pub(crate) fn read(
        edit: &CircuitEditor,
        root: ExpressionId,
        parameter: RowParameter<'_>,
    ) -> Result<Option<Self>, Error> {
        Self::from_node(edit, root.into(), parameter)
    }

    /// Fusion compares the whole printed closure, including its parameter type.
    pub(crate) fn parameter_type(
        edit: &CircuitEditor,
        parameter: RowParameter<'_>,
    ) -> Result<Option<Self>, Error> {
        Self::from_node(edit, parameter.ty.into(), parameter)
    }

    fn from_node(
        edit: &CircuitEditor,
        root: NodeId,
        parameter: RowParameter<'_>,
    ) -> Result<Option<Self>, Error> {
        Self::number_terms(edit, root, parameter, KeyForm::Original)
    }

    fn number_terms(
        edit: &CircuitEditor,
        root: NodeId,
        parameter: RowParameter<'_>,
        form: KeyForm,
    ) -> Result<Option<Self>, Error> {
        let mut scopes = vec![Binding {
            variable: parameter,
            parent: None,
            position: 0,
        }];
        let mut terms = IndexSet::new();
        let mut numbers = HashMap::new();
        let mut active = HashSet::new();
        let mut needs_canonical_form = false;
        let mut work = vec![Visit::Read(ScopedNode {
            id: root,
            scope: 0,
            in_static: false,
        })];
        while let Some(visit) = work.pop() {
            match visit {
                Visit::Read(at) => {
                    if numbers.contains_key(&at) {
                        continue;
                    }
                    if !active.insert(at.id) {
                        return Err(Error::new("cycle in a window key expression"));
                    }
                    let node = edit.node(at.id)?;
                    if let Some(InnerKind::Expression(kind)) = node.inner_kind() {
                        if matches!(kind, Expr::Static | Expr::EqualityComparator) {
                            // Both wrappers print only their operand. A static
                            // initializer has additional canonicalization checks.
                            let is_static = *kind == Expr::Static;
                            let value = ScopedNode {
                                id: node.edge(if is_static { "initializer" } else { "comparator" })?,
                                in_static: at.in_static || is_static,
                                ..at
                            };
                            work.extend([Visit::Alias(at, value), Visit::Read(value)]);
                            continue;
                        }
                        let simplifies = feldera_sql_expr::canonical_constructor_simplifies(
                            edit, at.id, *kind,
                        )?;
                        if at.in_static && (simplifies || !preserves_static_children(*kind)) {
                            return Ok(None);
                        }
                        needs_canonical_form |= simplifies;
                        if *kind == Expr::Fail {
                            let message = string(node, "message")
                                .ok_or_else(|| Error::new("missing failure message"))?;
                            // Fail prints a panic! call. Its printer escapes non-ASCII
                            // and control characters as UTF-16 rather than Rust's \u{...}.
                            let text = if message.chars().all(|c| matches!(c, ' '..='~' | '\t' | '\n' | '\r')) {
                                Term::String(message.to_owned())
                            } else {
                                Term::Utf16EscapedString(message.to_owned())
                            };
                            let arg = terms.insert_full((text, vec![])).0;
                            let call = terms.insert_full((Term::Call("panic!".into()), vec![arg])).0;
                            numbers.insert(at, call);
                            active.remove(&at.id);
                            continue;
                        }
                        if matches!(kind, Expr::UnsignedWrap | Expr::UnsignedUnwrap) {
                            let source = edit.expression(node.edge("source")?)?;
                            let Some(method) = comparators::unsigned_method(
                                edit, edit.expression(at.id)?, source, *kind == Expr::UnsignedWrap,
                            )? else {
                                return Ok(None);
                            };
                            let flag = |field| node.data(field).and_then(|v| v.as_bool())
                                .ok_or_else(|| Error::new(format!("missing ordered conversion flag {field}")));
                            let flags = [flag("ascending")?, flag("nullsLast")?];
                            let source = ScopedNode { id: source.into(), ..at };
                            work.extend([Visit::OrderedCall(at, method, source, flags), Visit::Read(source)]);
                            continue;
                        }
                    }
                    let Some((term, children)) = scoped_term(edit, at, &mut scopes, form)? else {
                        return Ok(None);
                    };
                    work.push(Visit::Intern(at, term, children.clone()));
                    work.extend(children.into_iter().rev().map(Visit::Read));
                }
                Visit::Alias(at, child) => {
                    numbers.insert(at, numbers[&child]);
                    active.remove(&at.id);
                }
                Visit::OrderedCall(at, method, source, flags) => {
                    // Intern the implicit arguments in print order so an explicit
                    // conversion call receives the same value number.
                    let mut operands = vec![numbers[&source]];
                    operands.extend(flags.map(|flag| terms.insert_full((Term::Bool(flag), vec![])).0));
                    let call = Term::Call(format!("UnsignedWrappers::{method}"));
                    numbers.insert(at, terms.insert_full((call, operands)).0);
                    active.remove(&at.id);
                }
                Visit::Intern(at, term, children) => {
                    let id = at.id;
                    let operands: Vec<_> = children.into_iter().map(|child| numbers[&child]).collect();
                    // The literal printer and an explicit Some use the same spelling.
                    let wrap = matches!(
                        term,
                        Term::Number(_)
                            | Term::Tuple
                            | Term::Bool(_)
                            | Term::String(_)
                            | Term::Bytes(_)
                            | Term::Uuid(_)
                            | Term::TemporalText(_)
                            | Term::Date(_)
                            | Term::Timestamp(_)
                    ) && edit.nullable(edit.expression_type(edit.expression(id)?)?)?
                        && edit.expr_kind(edit.expression(id)?)? != Expr::StrLiteral;
                    // Nonempty tuple constructors have a trailing separator after
                    // each field. An empty one prints as an ordinary zero-argument call.
                    let term = if matches!(term, Term::Tuple) && operands.is_empty() {
                        Term::Call("Tup0::new".into())
                    } else {
                        term
                    };
                    let (mut number, _) = terms.insert_full((term, operands));
                    if wrap {
                        number = terms
                            .insert_full((Term::Call("Some".into()), vec![number]))
                            .0;
                    }
                    numbers.insert(at, number);
                    active.remove(&id);
                }
            }
        }
        if needs_canonical_form {
            if form != KeyForm::Original {
                return Ok(None);
            }
            let mut scratch = CircuitEditor::new(edit.circuit().clone());
            if scopes.len() == 1 {
                let simplified = feldera_sql_expr::canonicalize_names(&mut scratch, root)?;
                return Self::number_terms(&scratch, simplified, parameter, KeyForm::Canonical);
            }
            let body = scratch.expression(root)?;
            let row = scratch.named_variable(parameter.ty, parameter.name)?;
            let closure = scratch.closure_from_variables(body, &[row])?;
            if !can_rename_scoped_key(&scratch, closure.into())? {
                return Ok(None);
            }
            let closure = feldera_sql_expr::canonicalize_names(&mut scratch, closure.into())?;
            let node = scratch.node(closure)?;
            let row = row_parameter(&scratch, node.edges("parameters")?[0])?;
            return Self::number_terms(&scratch, node.edge("body")?, row, KeyForm::Canonical);
        }
        if form == KeyForm::Canonical && scopes.len() > 1 {
            // The outer row is p0. With no discarded declarations the remaining
            // names are dense, so ordinary lexical binding positions suffice.
            let names: HashSet<_> = scopes[1..].iter().map(|b| b.variable.name).collect();
            if (1..=names.len()).any(|i| !names.contains(format!("p{i}").as_str())) {
                return Self::number_terms(edit, root, parameter, KeyForm::PrintedNames);
            }
        }
        Ok(Some(Self(terms.into_iter().collect())))
    }
}

/// Rewriting each occurrence of a shared leaf costs one visit per input edge.
/// A shared variable must resolve to the same Java declaration at every use.
/// Compound expressions can expand during renaming, and declarations allocate
/// a fresh name on each visit.
fn can_rename_scoped_key(edit: &CircuitEditor, closure: NodeId) -> Result<bool, Error> {
    use feldera_sql_expr::lexical_scope::{lexical_references, push_expression_children, Resolution};

    let mut seen = HashSet::new();
    let mut pending = vec![closure];
    let mut shared_variable = false;
    while let Some(id) = pending.pop() {
        let node = edit.node(id)?;
        if matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
            continue;
        }
        if seen.insert(id) {
            push_expression_children(node, &mut pending);
            continue;
        }
        match node.inner_kind() {
            Some(InnerKind::Expression(Expr::VariablePath)) => shared_variable = true,
            Some(InnerKind::Expression(kind)) if kind.wire_name().ends_with("Literal") => {}
            // Function paths contain identifiers and type arguments.
            _ if matches!(node.class(), "DBSPPath" | "DBSPSimplePathSegment") => {}
            _ => return Ok(false),
        }
    }
    Ok(!shared_variable || matches!(lexical_references(edit.circuit(), closure)?, Resolution::Bound(_)))
}

fn scoped_term<'a>(
    edit: &'a CircuitEditor,
    at: ScopedNode,
    scopes: &mut Vec<Binding<'a>>,
    form: KeyForm,
) -> Result<Option<(Term, Vec<ScopedNode>)>, Error> {
    let node = edit.node(at.id)?;
    let same_scope = |id| ScopedNode { id, ..at };
    Ok(Some(match node.inner_kind() {
        Some(InnerKind::Expression(Expr::Block)) => return block_term(edit, at, scopes, form),
        Some(InnerKind::Expression(Expr::Closure)) => {
            let mut scope = at.scope;
            let mut children = Vec::new();
            let mut names = Vec::new();
            for param in node.edges("parameters")? {
                let variable = row_parameter(edit, param)?;
                names.extend(form.printed_name(variable.name));
                children.push(same_scope(variable.ty.into()));
                scope = bind(scopes, scope, variable);
            }
            children.push(ScopedNode {
                id: node.edge("body")?,
                scope,
                ..at
            });
            (Term::Closure(names), children)
        }
        Some(InnerKind::Expression(Expr::Let)) => {
            let path = edit.node(node.edge("variable")?)?;
            let variable = RowParameter {
                name: string(path, "variable")
                    .ok_or_else(|| Error::new("missing LET binding name"))?,
                ty: edit.type_id(path.edge("type")?)?,
            };
            let scope = bind(scopes, at.scope, variable);
            // Only the consumer sees the new binding.
            (
                Term::Let(form.printed_name(variable.name)),
                vec![
                    same_scope(node.edge("initializer")?),
                    ScopedNode {
                        id: node.edge("consumer")?,
                        scope,
                        ..at
                    },
                ],
            )
        }
        Some(InnerKind::Expression(Expr::VariablePath)) => {
            let name = string(node, "variable")
                .ok_or_else(|| Error::new("missing window key variable name"))?;
            let binding = std::iter::successors(Some(at.scope), |&s| scopes[s].parent)
                .map(|s| &scopes[s])
                .find(|b| b.variable.name == name);
            let Some(binding) = binding else {
                return Ok(None);
            };
            if !edit.same_type(binding.variable.ty, edit.type_id(node.edge("type")?)?)? {
                return Ok(None);
            }
            (Term::Variable(binding.position), Vec::new())
        }
        kind => {
            let term = match kind {
                Some(InnerKind::Type(_)) => type_term(edit, edit.type_id(at.id)?)?,
                _ => key_term(edit, edit.expression(at.id)?)?,
            };
            let Some((term, children)) = term else {
                return Ok(None);
            };
            (term, children.into_iter().map(same_scope).collect())
        }
    }))
}

/// CanonicalForm keeps these constructors when every visited child keeps its
/// identity. A declaration or a simplifying constructor can replace the
/// initializer, which the Java visitor diagnoses at the static expression.
fn preserves_static_children(kind: Expr) -> bool {
    kind.wire_name().ends_with("Literal")
        || matches!(
            kind,
            Expr::Apply | Expr::ApplyMethod | Expr::Constructor | Expr::Block
                | Expr::Tuple | Expr::RawTuple | Expr::Array | Expr::Map
                | Expr::If | Expr::Binary | Expr::TimeAddSub | Expr::Unary
                | Expr::Cast | Expr::IsNull | Expr::Field
                | Expr::WindowBound | Expr::ConditionalIncrement | Expr::Lazy | Expr::Return
                | Expr::NoComparator | Expr::FieldComparator | Expr::DirectComparator
                | Expr::AsymmetricFieldComparator | Expr::UnsignedWrap | Expr::UnsignedUnwrap
                | Expr::For | Expr::Assignment | Expr::Fail | Expr::Sort | Expr::CustomOrd
        )
}

fn block_term<'a>(
    edit: &'a CircuitEditor,
    at: ScopedNode,
    scopes: &mut Vec<Binding<'a>>,
    form: KeyForm,
) -> Result<Option<(Term, Vec<ScopedNode>)>, Error> {
    let block = edit.node(at.id)?;
    let mut scope = at.scope;
    let mut statements = Vec::new();
    let mut children = Vec::new();
    for id in block.edges("contents")? {
        let statement = edit.node(id)?;
        let in_scope = |id| ScopedNode { id, scope, ..at };
        match statement.class() {
            "DBSPComment" => {
                let comment = string(statement, "comment")
                    .ok_or_else(|| Error::new("missing window key comment"))?;
                // Java's split("\n") drops trailing empty lines. An empty comment
                // prints one comment line, while a string of newlines prints none.
                let lines = comment.trim_end_matches('\n');
                statements.push(BlockStatement::Comment(
                    (comment.is_empty() || !lines.is_empty()).then(|| lines.to_owned()),
                ));
            }
            "DBSPExpressionStatement" => {
                statements.push(BlockStatement::Expression);
                children.push(in_scope(statement.edge("expression")?));
            }
            // CanonicalForm renames each declaration, even an unused one.
            "DBSPLetStatement" if !at.in_static => {
                let ty = edit.type_id(statement.edge("type")?)?;
                let name = string(statement, "variable")
                    .ok_or_else(|| Error::new("missing block variable name"))?;
                let mutable = statement
                    .data("mutable")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| Error::new("missing block variable mutability"))?;
                let initializer = statement.field("initializer").and_then(|f| f.node_id());
                statements.push(BlockStatement::Let {
                    name: form.printed_name(name),
                    mutable,
                    initialized: initializer.is_some(),
                });
                children.push(in_scope(ty.into()));
                children.extend(initializer.map(in_scope));
                // The initializer can read a shadowed name. Only subsequent
                // statements and the block result see this declaration.
                scope = bind(scopes, scope, RowParameter { name, ty });
            }
            _ => return Ok(None),
        }
    }
    if let Some(id) = block.field("lastExpression").and_then(|f| f.node_id()) {
        children.push(ScopedNode { id, scope, ..at });
    }
    Ok(Some((Term::Block(statements), children)))
}

fn bind<'a>(scopes: &mut Vec<Binding<'a>>, parent: usize, variable: RowParameter<'a>) -> usize {
    let scope = scopes.len();
    scopes.push(Binding {
        variable,
        parent: Some(parent),
        position: scopes[parent].position + 1,
    });
    scope
}

fn key_term(edit: &CircuitEditor, id: ExpressionId) -> Result<Option<(Term, Vec<NodeId>)>, Error> {
    let node = edit.node(id.into())?;
    let child = |field| edit.expression(node.edge(field)?).map(NodeId::from);
    let children = |field| {
        node.edges(field)?
            .into_iter()
            .map(|id| edit.expression(id).map(NodeId::from))
            .collect::<Result<Vec<_>, Error>>()
    };
    let kind = edit.expr_kind(id)?;
    Ok(Some(match kind {
        Expr::For => {
            // CanonicalForm renames closure and LET declarations, but the
            // iterator's written name appears verbatim in the printed loop.
            let variable = string(node, "variable").ok_or_else(|| Error::new("missing loop variable"))?;
            (Term::For(variable.to_owned()), vec![child("iterated")?, child("block")?])
        }
        Expr::Assignment => {
            let left = edit.expression(node.edge("left")?)?;
            // The printer inserts no parentheses around an assignment. Keep the
            // left-hand side an lvalue so the printed boundary is unambiguous.
            if !matches!(edit.expr_kind(left)?, Expr::VariablePath | Expr::Field | Expr::Deref) {
                return Ok(None);
            }
            (Term::Assignment, vec![left.into(), child("right")?])
        }
        Expr::NoComparator | Expr::DirectComparator | Expr::FieldComparator
        | Expr::AsymmetricFieldComparator => comparators::printed(node, kind)?,
        Expr::Sort => (Term::Sort, vec![child("comparator")?]),
        Expr::CustomOrd => {
            // The wrapper prints WithCustomOrd(source). A computed comparator can
            // still allocate canonical names even though the printer omits it.
            if !comparators::is_ordering_chain(edit, edit.expression(node.edge("comparator")?)?)? {
                return Ok(None);
            }
            (Term::Call("WithCustomOrd".into()), vec![child("source")?])
        }
        Expr::Field => {
            let column = node
                .data("fieldNo")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| Error::new("invalid window key column"))?;
            (Term::Field(column), vec![child("expression")?])
        }
        // Non-null constructors print their contents in order and omit their type.
        Expr::Array if node.field("data").is_some() => (Term::Array, children("data")?),
        // Tuple constructors include their arity and a trailing separator after each field.
        Expr::Tuple if node.field("fields").is_some() => (Term::Tuple, children("fields")?),
        Expr::RawTuple if node.field("fields").is_some() => {
            let fields = children("fields")?;
            let nullable = edit.nullable(edit.expression_type(id)?)?;
            // The raw tuple printer appends a separator to each field. Only
            // an empty nullable tuple spells the same as a plain Some() call.
            let term = if nullable && fields.is_empty() {
                Term::Call("Some".into())
            } else {
                Term::RawTuple { nullable }
            };
            (term, fields)
        }
        Expr::Tuple | Expr::RawTuple => (Term::AbsentTuple, vec![]),
        Expr::VoidLiteral => (Term::RawTuple { nullable: false }, vec![]),
        Expr::StrLiteral => {
            let Some(text) = string(node, "value") else {
                return Ok(None);
            };
            // Both Rust string literal forms use the same quoting as SQL strings here.
            (Term::String(text.to_owned()), vec![])
        }
        Expr::NullLiteral => (Term::NullLiteral, vec![]),
        Expr::VariantNullLiteral => (Term::VariantNullLiteral, vec![]),
        Expr::GeoPointConstructor => {
            let ty = edit.expression_type(id)?;
            match (node.field("left"), node.field("right")) {
                (Some(_), Some(_)) => {
                    let Some(name) = scalar_spelling(edit, ty)? else {
                        return Ok(None);
                    };
                    (Term::Call(name), vec![child("left")?, child("right")?])
                }
                (None, None) => (
                    Term::NullPoint {
                        nullable: edit.nullable(ty)?,
                    },
                    vec![],
                ),
                _ => return Ok(None),
            }
        }
        Expr::Variant => match node.field("value").and_then(|field| field.node_id()) {
            Some(value) => (
                Term::Call("VARIANT".into()),
                vec![edit.expression(value)?.into()],
            ),
            // Both an absent VARIANT and a VARIANT containing SQL NULL print "(VARIANT)null".
            None => (Term::TypedNull("VARIANT".into()), vec![]),
        },
        Expr::Map if node.field("keys").is_some() => {
            let keys = children("keys")?;
            let values = children("values")?;
            if keys.len() != values.len() {
                return Err(Error::new(
                    "window key map has different key and value counts",
                ));
            }
            let entries = keys
                .into_iter()
                .zip(values)
                .flat_map(|(key, value)| [key, value])
                .collect();
            (Term::Map, entries)
        }
        // Both NULL constructors print their complete collection type, including
        // the element type of an array and both type arguments of a map.
        Expr::Array | Expr::Map => (Term::NullCollection, vec![edit.expression_type(id)?.into()]),
        Expr::Borrow => {
            let mutable = node.data("mut").and_then(|value| value.as_bool())
                .ok_or_else(|| Error::new("missing borrow mutability"))?;
            (Term::Borrow { mutable }, vec![child("expression")?])
        }
        Expr::Deref => (Term::Deref, vec![child("expression")?]),
        Expr::Clone => (Term::Clone, vec![child("expression")?]),
        Expr::IsNull => (Term::IsNull, vec![child("expression")?]),
        Expr::Lazy => (Term::Lazy, vec![child("expression")?]),
        Expr::Question => (Term::Question, vec![child("source")?]),
        Expr::Return => (Term::Return, vec![child("argument")?]),
        Expr::WindowBound => {
            let preceding = node.data("isPreceding").and_then(|value| value.as_bool())
                .ok_or_else(|| Error::new("missing window bound direction"))?;
            let name = if preceding { "RelOffset::Before" } else { "RelOffset::After" };
            (Term::Call(name.into()), vec![child("representation")?])
        }
        Expr::ConditionalIncrement => {
            // The aggregate printer emits an ordinary call, including the
            // predicate as its third argument only for a filtered aggregate.
            let name = match string(node, "opcode") {
                Some("AGG_ADD") => "agg_plus",
                Some("AGG_ADD_NON_NULL") => "agg_plus_non_null",
                Some("AGG_MIN") => "agg_min",
                Some("AGG_MAX") => "agg_max",
                Some("AGG_MIN1") => "agg_min1",
                Some("AGG_MAX1") => "agg_max1",
                Some("AGG_AND") => "agg_and",
                Some("AGG_OR") => "agg_or",
                Some("AGG_XOR") => "agg_xor",
                Some("AGG_GTE") => "agg_gte",
                Some("AGG_LTE") => "agg_lte",
                Some("CONTROLLED_FILTER_GTE") => "cf_compare_gte",
                _ => return Ok(None),
            };
            let mut arguments = vec![child("left")?, child("right")?];
            if node.field("condition").is_some() {
                arguments.push(child("condition")?);
            }
            (Term::Call(name.into()), arguments)
        }
        Expr::Unwrap => {
            // The empty message prints .unwrap(); other messages print .expect("…").
            let Some(message) = string(node, "message") else {
                return Ok(None);
            };
            (Term::Unwrap(message.to_owned()), vec![child("expression")?])
        }
        Expr::HandleError => {
            // The printed key is an ordinary call. The debug printer omits the site and index.
            let name = match string(node, "runtimeBehavior") {
                Some("Panic") => "handle_error",
                Some("PanicWithSource") => "handle_error_with_position",
                Some("ReturnNone") => "handle_error_safe",
                _ => return Ok(None),
            };
            (Term::Call(name.into()), vec![child("source")?])
        }
        Expr::If => {
            let mut branches = vec![child("condition")?, child("positive")?];
            if node.field("negative").is_some() {
                branches.push(child("negative")?);
            }
            (Term::If, branches)
        }
        Expr::Unary => {
            let source = child("source")?;
            let Some(opcode) = string(node, "opcode") else {
                return Ok(None);
            };
            // Conversions print as ordinary calls, with no operand nullability marker.
            let term = match opcode {
                "DECIMAL_TO_INTEGER" => Term::Call("decimal_to_integer".into()),
                "INTEGER_TO_DECIMAL" => Term::Call("integer_to_decimal".into()),
                "SHORT_INTERVAL_TO_INTEGER" => Term::Call("short_interval_to_integer".into()),
                "INTEGER_TO_SHORT_INTERVAL" => Term::Call("integer_to_short_interval".into()),
                "LONG_INTERVAL_TO_INTEGER" => Term::Call("long_interval_to_integer".into()),
                "INTEGER_TO_LONG_INTERVAL" => Term::Call("integer_to_long_interval".into()),
                "INTEGER_TO_UUID" => Term::Call("u128_to_uuid".into()),
                "UUID_TO_INTEGER" => Term::Call("uuid_to_u128".into()),
                "BINARY_TO_U64" => Term::Call("bytes_to_u64".into()),
                "BINARY_TO_U128" => Term::Call("bytes_to_u128".into()),
                "INTEGER_TO_BOOL" => Term::Call("i8_to_bool".into()),
                "BOOL_TO_INTEGER" => Term::Call("bool_to_i8".into()),
                "WRAP_BOOL" | "IS_TRUE" | "IS_FALSE" | "IS_NOT_TRUE" | "IS_NOT_FALSE"
                | "NOT" | "NEG" | "UNARY_PLUS" | "INDICATOR" | "TYPEDBOX" => Term::Unary {
                    opcode: opcode.to_owned(),
                    nullable: edit.nullable(edit.expression_type(edit.expression(source)?)?)?,
                },
                _ => return Ok(None),
            };
            (term, vec![source])
        }
        Expr::Some => (Term::Call("Some".into()), vec![child("expression")?]),
        Expr::Cast => (
            Term::Cast,
            vec![edit.expression_type(id)?.into(), child("source")?],
        ),
        Expr::Apply | Expr::ApplyMethod | Expr::Constructor => {
            let function = child("function")?;
            let mut operands = Vec::new();
            let term = match edit.expr_kind(edit.expression(function)?)? {
                Expr::Path => {
                    let path = edit.node(edit.node(function)?.edge("path")?)?;
                    let Some(name) = call_path(edit, path, &mut operands)? else {
                        return Ok(None);
                    };
                    if kind == Expr::ApplyMethod {
                        operands.push(child("self")?);
                        Term::Method(name)
                    } else {
                        Term::Call(name)
                    }
                }
                Expr::Closure if kind != Expr::ApplyMethod => {
                    // The callee prints its parameter types and body before the arguments.
                    operands.push(function);
                    Term::CallClosure
                }
                _ => return Ok(None),
            };
            operands.extend(children("arguments")?);
            (term, operands)
        }
        Expr::Binary | Expr::TimeAddSub => {
            let left = child("left")?;
            let right = child("right")?;
            let Some(op) = string(node, "opcode").and_then(BinaryOp::from_wire) else {
                return Ok(None);
            };
            match op {
                // The multiline conversion call omits the operand nullability
                // markers used by the infix printer.
                BinaryOp::ArrayConvert
                | BinaryOp::ArrayConvertSafe
                | BinaryOp::MapConvert
                | BinaryOp::MapConvertSafe => (Term::CollectionConversion(op), vec![left, right]),
                _ => {
                    // Several distinct opcodes print the same infix operator.
                    let opcode = match op {
                        BinaryOp::MulInterval => BinaryOp::Mul,
                        BinaryOp::DivInterval => BinaryOp::Div,
                        BinaryOp::MulIntervalSaturating => BinaryOp::MulSaturating,
                        BinaryOp::DivIntervalSaturating => BinaryOp::DivSaturating,
                        BinaryOp::Concat => BinaryOp::Or,
                        other => other,
                    };
                    (
                        Term::Binary {
                            opcode,
                            left_nullable: edit
                                .nullable(edit.expression_type(edit.expression(left)?)?)?,
                            right_nullable: edit
                                .nullable(edit.expression_type(edit.expression(right)?)?)?,
                        },
                        vec![left, right],
                    )
                }
            }
        }
        Expr::BinaryLiteral
        | Expr::BoolLiteral
        | Expr::DateLiteral
        | Expr::DecimalLiteral
        | Expr::RealLiteral
        | Expr::DoubleLiteral
        | Expr::StringLiteral
        | Expr::TimeLiteral
        | Expr::TimestampLiteral
        | Expr::TimestampTzLiteral
        | Expr::UuidLiteral
        | Expr::I8Literal
        | Expr::I16Literal
        | Expr::I32Literal
        | Expr::I64Literal
        | Expr::I128Literal
        | Expr::U8Literal
        | Expr::U16Literal
        | Expr::U32Literal
        | Expr::U64Literal
        | Expr::U128Literal
        | Expr::ISizeLiteral
        | Expr::USizeLiteral => {
            let term = match node.data("value").filter(|v| !v.is_null()) {
                Some(value) if kind == Expr::BoolLiteral => {
                    let Some(value) = value.as_bool() else {
                        return Ok(None);
                    };
                    Term::Bool(value)
                }
                Some(_) if kind == Expr::DecimalLiteral => {
                    let (coefficient, scale) = feldera_sql_expr::decimal_literal_parts(node)?;
                    Term::Number(decimal_text(&coefficient, scale))
                }
                Some(value) if matches!(kind, Expr::RealLiteral | Expr::DoubleLiteral) => {
                    let Some(bits) = value.as_i64() else {
                        return Ok(None);
                    };
                    let text = if kind == Expr::RealLiteral {
                        let Ok(bits) = i32::try_from(bits) else {
                            return Ok(None);
                        };
                        floating_text(f32::from_bits(bits as u32))
                    } else {
                        floating_text(f64::from_bits(bits as u64))
                    };
                    Term::Number(text)
                }
                Some(value) if kind == Expr::StringLiteral => {
                    // Quoting and escaping are injective in Java's debug printer.
                    let Some(value) = value.as_str() else {
                        return Ok(None);
                    };
                    Term::String(value.to_owned())
                }
                Some(value) if matches!(kind, Expr::BinaryLiteral | Expr::UuidLiteral) => {
                    // The checkpoint writer uses padded base64 for bytes and the
                    // canonical UUID string. Each uniquely identifies the printed literal.
                    let Some(value) = value.as_str() else {
                        return Ok(None);
                    };
                    if kind == Expr::BinaryLiteral {
                        Term::Bytes(value.to_owned())
                    } else {
                        Term::Uuid(value.to_owned())
                    }
                }
                Some(value) if matches!(kind, Expr::TimeLiteral | Expr::TimestampTzLiteral) => {
                    // Java prints the wire text unquoted, including the original zone.
                    let Some(text) = value.as_str() else {
                        return Ok(None);
                    };
                    Term::TemporalText(text.to_owned())
                }
                Some(value) if matches!(kind, Expr::DateLiteral | Expr::TimestampLiteral) => {
                    let Some(ticks) = value.as_i64().or_else(|| value.as_str()?.parse().ok())
                    else {
                        return Ok(None);
                    };
                    let days = if kind == Expr::DateLiteral {
                        ticks
                    } else {
                        ticks.div_euclid(86_400_000_000)
                    };
                    // Each epoch value has a distinct spelling within Calcite's
                    // four-digit CE years (0001-01-01 through 9999-12-31).
                    if !(-719_162..=2_932_896).contains(&days) {
                        return Ok(None);
                    }
                    if kind == Expr::DateLiteral {
                        Term::Date(ticks)
                    } else {
                        Term::Timestamp(ticks)
                    }
                }
                Some(value) => {
                    // Current writers use decimal strings. Earlier snapshots
                    // also contain JSON integers.
                    let digits = match value.as_str() {
                        Some(s) => s
                            .parse::<i128>()
                            .map(|n| n.to_string())
                            .or_else(|_| s.parse::<u128>().map(|n| n.to_string()))
                            .ok(),
                        None if value.is_i64() || value.is_u64() => Some(value.to_string()),
                        None => None,
                    };
                    let Some(digits) = digits else {
                        return Ok(None);
                    };
                    Term::Number(digits)
                }
                None => {
                    let Some(name) = scalar_spelling(edit, edit.expression_type(id)?)? else {
                        return Ok(None);
                    };
                    Term::TypedNull(name)
                }
            };
            (term, vec![])
        }
        Expr::ShortIntervalLiteral | Expr::LongIntervalLiteral => {
            let ty = edit.expression_type(id)?;
            let Some(units) = string(edit.node(ty.into())?, "units") else {
                return Ok(None);
            };
            let term = match node.data("value").filter(|v| !v.is_null()) {
                Some(value) => {
                    let Some(value) = value.as_str().and_then(|s| s.parse().ok()) else {
                        return Ok(None);
                    };
                    Term::Interval {
                        months: kind == Expr::LongIntervalLiteral,
                        units: units.to_owned(),
                        value,
                        nullable: edit.nullable(ty)?,
                    }
                }
                None => {
                    let Some(name) = scalar_spelling(edit, ty)? else {
                        return Ok(None);
                    };
                    Term::TypedNull(name)
                }
            };
            (term, vec![])
        }
        _ => return Ok(None),
    }))
}

fn string<'a>(node: &'a Node, field: &str) -> Option<&'a str> {
    node.data(field)?.as_str()
}

/// A qualified spelling can arrive as one segment or several. Mark the position
/// of each generic argument in the name and compare its type as a key operand.
fn call_path(
    edit: &CircuitEditor,
    path: &Node,
    type_arguments: &mut Vec<NodeId>,
) -> Result<Option<String>, Error> {
    let mut name = String::new();
    for component in path.edges("components")? {
        let segment = edit.node(component)?;
        if segment.class() != "DBSPSimplePathSegment" {
            return Ok(None);
        }
        let Some(identifier) = string(segment, "identifier") else {
            return Ok(None);
        };
        if identifier != "panic!" && !identifier.split("::").all(|part| {
            !part.is_empty() && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        }) {
            return Ok(None);
        }
        if !name.is_empty() {
            name.push_str("::");
        }
        name.push_str(identifier);
        let arguments = segment.edges("genericArgs")?;
        if !arguments.is_empty() {
            name.push('<');
            for (index, argument) in arguments.into_iter().enumerate() {
                if index != 0 {
                    name.push(',');
                }
                name.push('_');
                type_arguments.push(edit.type_id(argument)?.into());
            }
            name.push('>');
        }
    }
    Ok((!name.is_empty()).then_some(name))
}

/// Canonical closures rename the parameter and print its type, including
/// both references in the (key, value) parameter of an indexed map.
#[derive(Clone, Copy)]
pub(crate) struct RowParameter<'a> {
    pub(crate) ty: TypeId,
    name: &'a str,
}

pub(crate) fn row_parameter(
    edit: &CircuitEditor,
    parameter: NodeId,
) -> Result<RowParameter<'_>, Error> {
    let parameter = edit.node(parameter)?;
    let ty = edit.type_id(parameter.edge("type")?)?;
    let name = parameter
        .data("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("missing timestamp parameter name"))?;
    Ok(RowParameter { ty, name })
}

/// The reference fingerprint includes the port and the printed closure parameter
/// type. A port can occur with several copies of a row type, or with distinct
/// parameter spellings. Keep one representative for each spelling at that port.
#[derive(Default)]
pub(crate) struct SourceRows(HashMap<OutputPort, Vec<TypeId>>);

impl SourceRows {
    pub(crate) fn representative(
        &mut self,
        edit: &CircuitEditor,
        port: OutputPort,
        row: TypeId,
    ) -> Result<Option<TypeId>, Error> {
        let types = self.0.entry(port).or_default();
        for &ty in types.iter() {
            match same_type_spelling(edit, ty, row)? {
                Some(true) => return Ok(Some(ty)),
                Some(false) => {}
                None => return Ok(None),
            }
        }
        types.push(row);
        Ok(Some(row))
    }
}

/// Use the same type printer rules as casts inside the timestamp expression.
/// Comparing shared children once also handles deeply nested row signatures.
fn same_type_spelling(
    edit: &CircuitEditor,
    left: TypeId,
    right: TypeId,
) -> Result<Option<bool>, Error> {
    if left == right {
        return Ok(Some(true));
    }
    let mut pending = vec![(left, right)];
    let mut compared = HashSet::new();
    while let Some((left, right)) = pending.pop() {
        if left == right || !compared.insert((left, right)) {
            continue;
        }
        let (Some((a, fields)), Some((b, other))) =
            (type_term(edit, left)?, type_term(edit, right)?)
        else {
            return Ok(None);
        };
        if a != b || fields.len() != other.len() {
            return Ok(Some(false));
        }
        for (l, r) in fields.into_iter().zip(other) {
            pending.push((edit.type_id(l)?, edit.type_id(r)?));
        }
    }
    Ok(Some(true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{Circuit, Field};
    use serde_json::json;

    #[test]
    fn source_rows_compare_printed_signatures_including_nested_type_arguments() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "allOperators":[], "declarations":[]
        }))?;
        let binary = |id, fixed, precision| json!({
            "class":"DBSPTypeBinary", "code":"BYTES", "id":id, "mayBeNull":false,
            "fixed":fixed, "precision":precision,
        });
        let (circuit, leaves) = circuit.with_inner_roots([
            binary(1, true, 4), binary(2, false, 4), binary(3, false, 8),
        ])?;
        let port = OutputPort { operator: circuit.root_id(), number: 0 };
        let mut edit = CircuitEditor::new(circuit);
        let row = |edit: &mut CircuitEditor, leaf| -> Result<_, Error> {
            let tuple = edit.tuple_type(port.operator, false, [leaf, leaf])?;
            edit.reference_type(tuple)
        };
        let fixed = row(&mut edit, leaves[0])?;
        let varying = row(&mut edit, leaves[1])?;
        let wider = row(&mut edit, leaves[2])?;
        let mut rows = SourceRows::default();
        assert_eq!(rows.representative(&edit, port, fixed)?, Some(fixed));
        assert_eq!(rows.representative(&edit, port, wider)?, Some(wider));
        // BINARY and VARBINARY omit the fixed-width flag in the debug printer.
        assert_eq!(rows.representative(&edit, port, varying)?, Some(fixed));
        assert_eq!(rows.representative(&edit, port, wider)?, Some(wider));
        Ok(())
    }

    #[test]
    fn variant_nulls_and_nullable_tuples_follow_the_printed_constructor() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let (graph, types) = graph.with_inner_roots([json!({
            "class":"DBSPTypeVariant","id":50,"code":"VARIANT","mayBeNull":true
        })])?;
        let mut edit = CircuitEditor::new(graph);
        let bool_ty = edit.bool_type(site, false)?;
        let variant = edit.type_id(types[0])?;
        let mut null = |sql_null: bool| {
            edit.make_expr(
                site,
                Expr::Variant,
                variant,
                [("isSqlNull", Field::Data(sql_null.into()))],
            )
        };
        let absent = null(false)?;
        let sql_null = null(true)?;
        let variant_null = edit.make_expr(site, Expr::VariantNullLiteral, variant, [])?;
        let param = edit.named_variable(bool_ty, "v")?;
        let tuple = edit.tuple_expr(site, false, &[param])?;
        let nullable = edit.with_nullability(edit.expression_type(tuple)?, true)?;
        let fields = [("fields", Field::Array(vec![Field::Reference(param.into())]))];
        let wrapped = edit.make_expr(site, Expr::Tuple, nullable, fields)?;
        let some = edit.make_expr(
            site,
            Expr::Some,
            nullable,
            [("expression", Field::Reference(tuple.into()))],
        )?;
        let absent_tuple = edit.make_expr(site, Expr::Tuple, nullable, [])?;
        let key = |expr| {
            ProjectionKey::read(
                &edit,
                expr,
                RowParameter {
                    name: "v",
                    ty: bool_ty,
                },
            )
            .map(Option::unwrap)
        };
        assert!(key(absent)? == key(sql_null)? && key(absent)? != key(variant_null)?);
        assert!(key(wrapped)? == key(some)? && key(wrapped)? != key(tuple)?);
        assert!(key(absent_tuple)? != key(absent)? && key(absent_tuple)? != key(wrapped)?);
        Ok(())
    }

    #[test]
    fn closure_and_let_keys_preserve_the_declaration_each_use_reads() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.bool_type(site, false)?;
        let outer = edit.named_variable(ty, "row")?;
        let row = RowParameter { name: "row", ty };
        let mut key = |parameter_name, local_name, capture| -> Result<ProjectionKey, Error> {
            let parameter = edit.named_variable(ty, parameter_name)?;
            let local = edit.named_variable(ty, local_name)?;
            // The initializer reads the lambda parameter even when the LET shadows its name.
            let initializer = edit.named_variable(ty, parameter_name)?;
            let body =
                edit.let_expression(local, initializer, if capture { outer } else { local })?;
            let lambda = edit.closure_from_variables(body, &[parameter])?;
            Ok(ProjectionKey::read(&edit, lambda, row)?.expect("bound lambda key"))
        };
        let shadow = key("x", "x", false)?;
        assert!(shadow == key("y", "z", false)?);
        assert!(shadow != key("y", "z", true)?);
        let free = edit.named_variable(ty, "p1")?;
        let lambda = edit.closure_from_variables(free, &[outer])?;
        assert!(ProjectionKey::read(&edit, lambda, row)?.is_none());
        Ok(())
    }

    #[test]
    fn folding_a_scoped_key_preserves_shared_literals_and_captures() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.bool_type(site, false)?;
        let row = RowParameter { name: "row", ty };
        let outer = edit.named_variable(ty, row.name)?;
        let inner = edit.named_variable(ty, "item")?;
        let literal = edit.make_expr(site, Expr::BoolLiteral, ty, [("value", Field::Data(true.into()))])?;
        let cast = edit.make_expr(site, Expr::Cast, ty, [
            ("source", Field::Reference(outer.into())),
            ("safe", Field::Data("SqlUnsafe".into())),
        ])?;
        let mut projection = |value| -> Result<ExpressionId, Error> {
            let body = edit.tuple_expr(site, false, &[value, outer, literal, literal])?;
            edit.closure_from_variables(body, &[inner])
        };
        let folded = projection(cast)?;
        let plain = projection(outer)?;
        let key = ProjectionKey::read(&edit, folded, row)?.expect("shared leaves in a scoped key");
        assert!(Some(key) == ProjectionKey::read(&edit, plain, row)?);

        // The same variable object now names two different declarations.
        let shadow = edit.closure_from_variables(outer, &[outer])?;
        let conflict = edit.tuple_expr(site, false, &[cast, shadow])?;
        assert!(ProjectionKey::read(&edit, conflict, row)?.is_none());
        Ok(())
    }

    #[test]
    fn a_shared_lambda_body_is_numbered_once_per_scope() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.bool_type(site, false)?;
        let row = RowParameter { name: "x", ty };
        let x = edit.named_variable(ty, "x")?;
        let y = edit.named_variable(ty, "y")?;
        let mut body = x;
        for _ in 0..128 {
            body = edit.binary_expression(site, ty, BinaryOp::And, body, body)?;
        }
        let shadow = edit.closure_from_variables(body, &[x])?;
        let capture = edit.closure_from_variables(body, &[y])?;
        let both = edit.tuple_expr(site, false, &[shadow, capture])?;
        let same = edit.tuple_expr(site, false, &[shadow, shadow])?;
        let key = ProjectionKey::read(&edit, both, row)?.expect("two bound lambdas");
        assert!(key.0.len() < 270);
        assert!(Some(key) != ProjectionKey::read(&edit, same, row)?);
        Ok(())
    }

    #[test]
    fn a_cast_numbers_each_shared_field_type_once() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.bool_type(site, false)?;
        let row = RowParameter { name: "v", ty };
        let mut target = ty;
        for _ in 0..160 {
            target = edit.tuple_type(site, false, [target.into(), target.into()])?;
        }
        let variable = edit.named_variable(ty, "v")?;
        let operand = [("source", Field::Reference(variable.into()))];
        let cast = edit.make_expr(site, Expr::Cast, target, operand.clone())?;
        let nullable = edit.with_nullability(target, true)?;
        let other = edit.make_expr(site, Expr::Cast, nullable, operand)?;
        let key = ProjectionKey::read(&edit, cast, row)?.unwrap();
        assert_eq!(key.0.len(), 163);
        assert!(Some(key) != ProjectionKey::read(&edit, other, row)?);
        Ok(())
    }

    #[test]
    fn repeated_operands_share_one_value_number_per_level() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let (graph, types) = graph.with_inner_roots([json!({
            "class":"DBSPTypeTimestamp","id":0,"code":"TIMESTAMP","mayBeNull":false
        })])?;
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.type_id(types[0])?;
        let site = edit.circuit().root_id();
        let row = RowParameter { name: "t", ty };
        let left = edit.named_variable(ty, "t")?;
        let right = edit.named_variable(ty, "t")?;
        let mut shared = edit.binary_expression(site, ty, BinaryOp::Max, left, left)?;
        let mut copied = edit.binary_expression(site, ty, BinaryOp::Max, left, right)?;
        for _ in 0..128 {
            shared = edit.binary_expression(site, ty, BinaryOp::Max, shared, shared)?;
            copied = edit.binary_expression(site, ty, BinaryOp::Max, copied, copied)?;
        }
        let key = ProjectionKey::read(&edit, shared, row)?.unwrap();
        assert_eq!(key.0.len(), 130);
        assert!(Some(key) == ProjectionKey::read(&edit, copied, row)?);
        let wrong_type = edit.bool_type(site, false)?;
        let invalid = edit.named_variable(wrong_type, "t")?;
        assert!(ProjectionKey::read(&edit, invalid, row)?.is_none());
        Ok(())
    }

    #[test]
    fn decimal_keys_preserve_the_scale_printed_by_java() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let (graph, roots) = graph.with_inner_roots([json!({
            "class":"DBSPTypeDecimal","id":0,"code":"DECIMAL",
            "precision":18,"scale":4,"mayBeNull":false
        })])?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.type_id(roots[0])?;
        let mut decimal = |value: &str| {
            edit.make_expr(
                site,
                Expr::DecimalLiteral,
                ty,
                [("value", Field::Data(value.into()))],
            )
        };
        let whole = decimal("1")?;
        let tenth = decimal("1.0")?;
        let hundredth = decimal("1.00")?;
        let exponent = decimal("100E-2")?;
        let i32_type = edit.integer_type(ty, 32, true, false)?;
        let integer = edit.make_expr(
            site,
            Expr::I32Literal,
            i32_type,
            [("value", Field::Data(1.into()))],
        )?;
        let key =
            |id| ProjectionKey::read(&edit, id, RowParameter { name: "r", ty }).map(Option::unwrap);
        assert!(key(whole)? == key(integer)?);
        assert!(key(tenth)? != key(integer)? && key(tenth)? != key(hundredth)?);
        assert!(key(hundredth)? == key(exponent)?);
        Ok(())
    }

    #[test]
    fn method_keys_distinguish_the_receiver_and_argument_order() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.bool_type(site, false)?;
        let row = RowParameter { name: "row", ty };
        let input = edit.named_variable(ty, row.name)?;
        let other = edit.make_expr(
            site,
            Expr::BoolLiteral,
            ty,
            [("value", Field::Data(true.into()))],
        )?;
        let mut keys = HashSet::new();
        for (name, receiver, args) in [
            ("select", input, [input, other]),
            ("select", other, [input, other]),
            ("select", input, [other, input]),
            ("replace", input, [input, other]),
        ] {
            let call = edit.iterator_method(site, name, receiver, &args)?;
            let key = ProjectionKey::read(&edit, call, row)?.expect("method key");
            assert!(keys.insert(key));
        }
        Ok(())
    }

    #[test]
    fn collection_conversions_use_the_opcode_and_printed_operands() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.bool_type(site, false)?;
        let opt = edit.with_nullability(ty, true)?;
        // Function application prints its name and arguments. The result
        // type can differ while the printed operand stays the same.
        let function = edit.function_path(site, "values")?;
        let call = [
            ("function", Field::Reference(function.into())),
            ("arguments", Field::Array(vec![])),
        ];
        let left = edit.make_expr(site, Expr::Apply, ty, call.clone())?;
        let nullable = edit.make_expr(site, Expr::Apply, opt, call)?;
        let var = edit.named_variable(ty, "item")?;
        let mapper = edit.closure_from_variables(var, &[var])?;
        let row = RowParameter { name: "row", ty };
        let mut keys = HashSet::new();
        for op in [
            BinaryOp::ArrayConvert,
            BinaryOp::ArrayConvertSafe,
            BinaryOp::MapConvert,
            BinaryOp::MapConvertSafe,
        ] {
            let mut conversion = |ty, input: ExpressionId| {
                edit.make_expr(
                    site,
                    Expr::Binary,
                    ty,
                    [
                        ("opcode", Field::Data(op.wire_name().into())),
                        ("left", Field::Reference(input.into())),
                        ("right", Field::Reference(mapper.into())),
                    ],
                )
            };
            let first = conversion(ty, left)?;
            let second = conversion(opt, nullable)?;
            let key = ProjectionKey::read(&edit, first, row)?.expect("conversion key");
            assert!(Some(&key) == ProjectionKey::read(&edit, second, row)?.as_ref());
            assert!(keys.insert(key));
        }
        Ok(())
    }

    #[test]
    fn integer_keys_follow_printed_width_nullability_and_operators() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let origin = edit.bool_type(site, false)?;
        let i16 = edit.integer_type(origin, 16, true, false)?;
        let u64 = edit.integer_type(origin, 64, false, false)?;
        let opt_i16 = edit.with_nullability(i16, true)?;
        let opt_u64 = edit.with_nullability(u64, true)?;
        let one = edit.make_expr(
            site,
            Expr::I16Literal,
            i16,
            [("value", Field::Data(json!(1)))],
        )?;
        let wide = edit.make_expr(
            site,
            Expr::U64Literal,
            u64,
            [("value", Field::Data(json!("1")))],
        )?;
        let nullable = edit.make_expr(
            site,
            Expr::U64Literal,
            opt_u64,
            [("value", Field::Data(json!("1")))],
        )?;
        let some = edit.make_expr(
            site,
            Expr::Some,
            opt_i16,
            [("expression", Field::Reference(one.into()))],
        )?;
        let function = edit.function_path(site, "Some")?;
        let call = edit.make_expr(
            site,
            Expr::Apply,
            opt_u64,
            [
                ("function", Field::Reference(function.into())),
                (
                    "arguments",
                    Field::Array(vec![Field::Reference(wide.into())]),
                ),
            ],
        )?;
        let null16 = edit.make_expr(site, Expr::I16Literal, opt_i16, [])?;
        let null64 = edit.make_expr(site, Expr::U64Literal, opt_u64, [])?;
        let binary = |edit: &mut CircuitEditor, op: BinaryOp, right: ExpressionId| {
            edit.make_expr(
                site,
                Expr::Binary,
                u64,
                [
                    ("left", Field::Reference(wide.into())),
                    ("opcode", Field::Data(json!(op.wire_name()))),
                    ("right", Field::Reference(right.into())),
                ],
            )
        };
        let product = binary(&mut edit, BinaryOp::Mul, one)?;
        let scaled_interval = binary(&mut edit, BinaryOp::MulInterval, wide)?;
        let nullable_product = binary(&mut edit, BinaryOp::Mul, some)?;
        let quotient = binary(&mut edit, BinaryOp::Div, one)?;
        let key = |id| {
            ProjectionKey::read(&edit, id, RowParameter { name: "r", ty: i16 })
                .map(|k| k.expect("numeric window key"))
        };
        assert!(key(one)? == key(wide)?);
        assert!(key(nullable)? == key(some)? && key(some)? == key(call)?);
        assert!(key(one)? != key(nullable)? && key(null16)? != key(null64)?);
        assert!(key(product)? == key(scaled_interval)?);
        assert!(key(product)? != key(nullable_product)? && key(product)? != key(quotient)?);
        Ok(())
    }
}
