//! Collect value-numbering events in the order the CSE visitor observes the live graph.
use super::key::{Form, Operator};
use super::{Definition, Event, LiveNumbering, NumberingInput, ResolvedNumbers, UseSite};
use crate::lexical_scope::{References, expression_children};
use feldera_sql_ir::{Circuit, Error, ExpressionKind as Expr, InnerKind, Node, NodeId, TypeKind};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// CSE keys include the live compiler's rendering of types, literals and operators.
#[derive(Deserialize)]
pub struct NumberingSpellings {
    types: HashMap<u64, String>,
    literals: HashMap<u64, Option<String>>,
    paths: HashMap<u64, String>,
    symbols: HashMap<String, String>,
    absent_tuples: HashMap<u64, String>,
    rewrite: bool,
}

#[derive(Serialize)]
pub struct CollectedNumbers {
    #[serde(flatten)]
    numbers: ResolvedNumbers,
    sites: Vec<UseSite>,
}

fn inner(id: NodeId) -> u64 {
    let NodeId::Inner(id) = id else {
        unreachable!("inner visitor follows inner edges")
    };
    id
}

fn text<'a>(node: &'a Node, field: &str) -> Result<&'a str, Error> {
    node.data(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new(format!("{} has no {field}", node.class())))
}

struct DefinitionSite {
    form: Form,
    operands: Vec<NodeId>,
}

impl NumberingSpellings {
    fn definition(
        &self,
        circuit: &Circuit,
        id: u64,
        node: &Node,
        kind: Expr,
    ) -> Result<Option<DefinitionSite>, Error> {
        let ty = || {
            let id = inner(node.edge("type")?);
            self.types
                .get(&id)
                .cloned()
                .ok_or_else(|| Error::new(format!("missing CSE spelling for type {id}")))
        };
        let unary = || Ok::<_, Error>(vec![node.edge("expression")?]);
        let (form, operands) = match kind {
            Expr::Path => {
                let path = self
                    .paths
                    .get(&id)
                    .ok_or_else(|| Error::new("missing CSE path spelling"))?;
                (Form::Path { text: path.clone() }, vec![])
            }
            Expr::VariablePath => (Form::Read, vec![]),
            Expr::Let => {
                let binder = circuit
                    .node(node.edge("variable")?)
                    .expect("resolved let binder");
                (
                    Form::Let {
                        name: text(binder, "variable")?.into(),
                        declaration: id,
                    },
                    vec![node.edge("initializer")?, node.edge("consumer")?],
                )
            }
            Expr::Binary | Expr::Unary => {
                let opcode = text(node, "opcode")?.to_owned();
                let symbol = self
                    .symbols
                    .get(&opcode)
                    .cloned()
                    .ok_or_else(|| Error::new(format!("missing CSE spelling for {opcode}")))?;
                if kind == Expr::Binary {
                    (
                        Form::Binary {
                            r#type: ty()?,
                            opcode,
                            symbol,
                        },
                        vec![node.edge("left")?, node.edge("right")?],
                    )
                } else {
                    (
                        Form::Unary {
                            r#type: ty()?,
                            opcode,
                            symbol,
                        },
                        vec![node.edge("source")?],
                    )
                }
            }
            Expr::Apply | Expr::ApplyMethod => {
                let method = kind == Expr::ApplyMethod;
                let mut operands = Vec::new();
                if method {
                    operands.push(node.edge("self")?);
                }
                operands.push(node.edge("function")?);
                operands.extend(node.edges("arguments")?);
                (
                    Form::Call {
                        r#type: ty()?,
                        method,
                    },
                    operands,
                )
            }
            Expr::Cast => {
                let source = node.edge("source")?;
                let string_type = |n: &Node| -> Result<bool, Error> {
                    Ok(matches!(
                        circuit.node(n.edge("type")?).and_then(Node::inner_kind),
                        Some(InnerKind::Type(TypeKind::String))
                    ))
                };
                let source_string =
                    string_type(circuit.node(source).expect("resolved cast source"))?;
                (
                    Form::Cast {
                        r#type: ty()?,
                        safety: text(node, "safe")?.into(),
                        source_string,
                        target_string: string_type(node)?,
                    },
                    vec![source],
                )
            }
            Expr::Tuple | Expr::RawTuple => {
                if let Some(value) = self.absent_tuples.get(&id) {
                    (
                        Form::AbsentTuple {
                            r#type: ty()?,
                            value: value.clone(),
                        },
                        vec![],
                    )
                } else {
                    let form = if kind == Expr::RawTuple {
                        Form::RawTuple
                    } else {
                        Form::Tuple { r#type: ty()? }
                    };
                    (form, node.edges("fields")?)
                }
            }
            Expr::Field => {
                let index = node
                    .data("fieldNo")
                    .and_then(|v| v.as_u64())
                    .and_then(|i| usize::try_from(i).ok())
                    .ok_or_else(|| Error::new("invalid CSE field number"))?;
                (Form::Field { index }, unary()?)
            }
            Expr::Borrow if node.data("mut").and_then(|v| v.as_bool()) == Some(false) => (
                Form::Simple {
                    operator: Operator::Borrow,
                },
                unary()?,
            ),
            Expr::Clone => (
                Form::Simple {
                    operator: Operator::Clone,
                },
                unary()?,
            ),
            Expr::Deref => (
                Form::Simple {
                    operator: Operator::Deref,
                },
                unary()?,
            ),
            Expr::IsNull => (
                Form::Simple {
                    operator: Operator::IsNull,
                },
                unary()?,
            ),
            Expr::Some => (
                Form::Simple {
                    operator: Operator::WrapSome,
                },
                unary()?,
            ),
            Expr::If => {
                let mut operands = vec![node.edge("condition")?, node.edge("positive")?];
                operands.extend(node.field("negative").and_then(|f| f.node_id()));
                (
                    Form::Simple {
                        operator: Operator::If,
                    },
                    operands,
                )
            }
            _ => match self.literals.get(&id) {
                Some(value) => (
                    Form::Literal {
                        r#type: ty()?,
                        value: value.clone(),
                    },
                    vec![],
                ),
                None => return Ok(None),
            },
        };
        Ok(Some(DefinitionSite { form, operands }))
    }

    pub fn number(
        self,
        circuit: &Circuit,
        root: NodeId,
        references: &References,
    ) -> Result<CollectedNumbers, Error> {
        let mut input = LiveNumbering {
            numbers: NumberingInput {
                events: Vec::new(),
                mutable: HashSet::new(),
                sites: Vec::new(),
                rewrite: self.rewrite,
            },
            mutable_places: Vec::new(),
            mutable_reads: Vec::new(),
        };
        let mut assignment = false;
        let mut pending = vec![(root, None, false)];
        while let Some((id, closure, postorder)) = pending.pop() {
            let node = circuit
                .node(id)
                .ok_or_else(|| Error::new("missing CSE expression"))?;
            if !postorder {
                pending.push((id, closure, true));
                let owner = match node.inner_kind() {
                    Some(InnerKind::Expression(Expr::Closure)) => Some(inner(id)),
                    _ => closure,
                };
                pending.extend(
                    expression_children(node)
                        .into_iter()
                        .rev()
                        .map(|child| (child, owner, false)),
                );
                continue;
            }
            let Some(&InnerKind::Expression(kind)) = node.inner_kind() else {
                continue;
            };
            let parent = inner(id);
            let mut record_use = |operand: usize, child| {
                let site = input.numbers.sites.len();
                input.numbers.sites.push(UseSite {
                    parent,
                    operand,
                    closure,
                });
                input.numbers.events.push(Event::Use {
                    input: inner(child),
                    site,
                });
            };
            match kind {
                Expr::Assignment => assignment = true,
                Expr::Borrow if node.data("mut").and_then(|v| v.as_bool()) == Some(true) => {
                    input.mutable_places.push(inner(node.edge("expression")?));
                }
                Expr::VariablePath => {
                    let ty = circuit
                        .node(node.edge("type")?)
                        .expect("resolved variable type");
                    if matches!(ty.inner_kind(), Some(InnerKind::Type(TypeKind::Ref)))
                        && ty.data("mutable").and_then(|v| v.as_bool()) == Some(true)
                    {
                        input.mutable_reads.push(parent);
                    }
                }
                Expr::Block => {
                    let statements = node.edges("contents")?;
                    for (i, statement) in statements.iter().enumerate() {
                        let statement = circuit.node(*statement).expect("resolved block statement");
                        if statement.class() == "DBSPLetStatement" {
                            if let Some(init) =
                                statement.field("initializer").and_then(|f| f.node_id())
                            {
                                record_use(i, init);
                            }
                        }
                    }
                    if let Some(last) = node.field("lastExpression").and_then(|f| f.node_id()) {
                        record_use(statements.len(), last);
                    }
                }
                _ => {}
            }
            if let Some(definition) = self.definition(circuit, parent, node, kind)? {
                for (i, &child) in definition.operands.iter().enumerate() {
                    record_use(i, child);
                }
                if closure.is_some() {
                    input.numbers.events.push(Event::Define {
                        expression: parent,
                        operands: definition.operands.into_iter().map(inner).collect(),
                        key: Definition::Typed {
                            form: definition.form,
                        },
                    });
                }
            }
        }
        let sites = if self.rewrite {
            Vec::new()
        } else {
            std::mem::take(&mut input.numbers.sites)
        };
        let mut numbers = input.number(circuit, references)?;
        numbers.unknown_write |= assignment;
        Ok(CollectedNumbers { numbers, sites })
    }
}
