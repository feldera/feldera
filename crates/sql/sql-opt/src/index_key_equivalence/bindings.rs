//! Pair local declarations while comparing expressions under alpha-renaming.
use super::{ComparisonDiagnostic, KeyComparisonError, optional_child, same_type};
use feldera_sql_ir::{CircuitEditor, Error, Node, NodeId};

/// Each pending comparison sees the bindings in scope at that expression.
/// Two branches may use the same variable names for different declarations.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(super) enum Scope {
    /// The initial EquivalenceContext has no declaration scope.
    Root,
    Empty,
    Bound(usize),
}

impl Scope {
    fn enter(self) -> Self {
        if self == Self::Root {
            Self::Empty
        } else {
            self
        }
    }
}
pub(super) enum Work {
    Compare(NodeId, NodeId, Scope),
    FlatmapOutput(NodeId, NodeId),
    /// Stop after earlier comparisons have had a chance to report their result.
    Answer(Option<bool>),
    EmptyContext,
    Leave(NodeId, NodeId),
}

#[derive(Default)]
pub(super) struct Bindings<'a> {
    pairs: Vec<(&'a str, &'a str, Scope)>,
}

pub(super) fn name<'a>(node: &'a Node, field: &str) -> Result<&'a str, Error> {
    node.data(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new(format!("missing {field} on {}", node.class())))
}

impl<'a> Bindings<'a> {
    pub fn bind(&mut self, left: &'a str, right: &'a str, parent: Scope) -> Scope {
        let index = self.pairs.len();
        self.pairs.push((left, right, parent));
        Scope::Bound(index)
    }

    pub fn variables(
        &self,
        mut scope: Scope,
        left: &Node,
        right: &Node,
    ) -> Result<bool, KeyComparisonError> {
        let left = name(left, "variable")?;
        let right = name(right, "variable")?;
        let mut declarations = (None, None);
        while let Scope::Bound(index) = scope {
            let (l, r, parent) = self.pairs[index];
            if l == left && declarations.0.is_none() {
                declarations.0 = Some(index);
            }
            if r == right && declarations.1.is_none() {
                declarations.1 = Some(index);
            }
            if let (Some(l), Some(r)) = declarations {
                return Ok(l == r);
            }
            scope = parent;
        }
        // DBSPVariablePath.equivalent looks up the left declaration first.
        let missing = if declarations.0.is_none() {
            left
        } else {
            right
        };
        Err(ComparisonDiagnostic::MissingDeclaration(missing.to_owned()).into())
    }

    pub fn let_expression(
        &mut self,
        edit: &'a CircuitEditor,
        left: &Node,
        right: &Node,
        scope: Scope,
        pending: &mut Vec<Work>,
    ) -> Result<(), Error> {
        let local = self.bind(
            name(edit.node(left.edge("variable")?)?, "variable")?,
            name(edit.node(right.edge("variable")?)?, "variable")?,
            scope,
        );
        pending.push(Work::Compare(
            left.edge("consumer")?,
            right.edge("consumer")?,
            local,
        ));
        pending.push(Work::Compare(
            left.edge("initializer")?,
            right.edge("initializer")?,
            scope,
        ));
        Ok(())
    }

    pub fn closure(
        &mut self,
        edit: &'a CircuitEditor,
        left: &Node,
        right: &Node,
        mut scope: Scope,
        pending: &mut Vec<Work>,
    ) -> Result<bool, Error> {
        let left_params = left.edges("parameters")?;
        let right_params = right.edges("parameters")?;
        if left_params.len() != right_params.len() {
            return Ok(false);
        }
        scope = scope.enter();
        for (l, r) in left_params.into_iter().zip(right_params) {
            let l = edit.node(l)?;
            let r = edit.node(r)?;
            if !same_type(edit, l, r)? {
                return Ok(false);
            }
            scope = self.bind(name(l, "name")?, name(r, "name")?, scope);
        }
        pending.push(Work::Compare(
            left.edge("body")?,
            right.edge("body")?,
            scope,
        ));
        Ok(true)
    }

    pub fn block(
        &mut self,
        edit: &'a CircuitEditor,
        left: &Node,
        right: &Node,
        mut scope: Scope,
        pending: &mut Vec<Work>,
    ) -> Result<bool, Error> {
        let statements = |block: &Node| -> Result<Vec<&Node>, Error> {
            let mut result = Vec::new();
            for id in block.edges("contents")? {
                let node = edit.node(id)?;
                if node.class() != "DBSPComment" {
                    result.push(node);
                }
            }
            Ok(result)
        };
        let left_statements = statements(left)?;
        let right_statements = statements(right)?;
        if left_statements.len() != right_statements.len() {
            return Ok(false);
        }
        // Process initializers before consumers, even if their names shadow.
        let start = pending.len();
        scope = scope.enter();
        for (l, r) in left_statements.into_iter().zip(right_statements) {
            if l.class() != r.class() {
                pending.push(Work::Answer(Some(false)));
                break;
            }
            match l.class() {
                "DBSPLetStatement" => {
                    if !optional_child(l, r, "initializer", scope, pending)?
                        || l.data("mutable") != r.data("mutable")
                    {
                        pending.push(Work::Answer(Some(false)));
                        break;
                    }
                    scope = self.bind(name(l, "variable")?, name(r, "variable")?, scope);
                }
                "DBSPExpressionStatement" => pending.push(Work::Compare(
                    l.edge("expression")?,
                    r.edge("expression")?,
                    scope,
                )),
                // Java's item declarations never compare equivalent, even to themselves.
                "DBSPComparatorItem" | "DBSPFunctionItem" | "DBSPStaticItem" | "DBSPStructItem" => {
                    pending.push(Work::Answer(Some(false)));
                    break;
                }
                _ => {
                    pending.push(Work::Answer(None));
                    break;
                }
            }
        }
        if !matches!(pending[start..].last(), Some(Work::Answer(_)))
            && !optional_child(left, right, "lastExpression", scope, pending)?
        {
            pending.push(Work::Answer(Some(false)));
        }
        pending[start..].reverse();
        Ok(true)
    }
}
