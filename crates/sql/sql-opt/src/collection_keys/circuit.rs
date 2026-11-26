//! Derive keys in circuit order so every operator sees the keys and equalities
//! of its original input ports. A nested circuit boundary carries no key facts.
use super::{
    copies_by_source, equivalence::ColumnEquivalence, filter_keys, minimal, Copies, JoinInputs,
    KeyColumns, KeyProjection,
};
use crate::lineage::{
    column_copies, equal_computations, fixed_columns, Column, ColumnCopy, OutputShape, Part,
};
use feldera_sql_ir::{
    BodyOrder, CircuitEditor, Error, Field, Node, NodeId, OperatorKind as Op, OutputPort,
};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Default)]
struct Facts {
    keys: Vec<KeyColumns>,
    equal: ColumnEquivalence,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "columns")]
enum Transform {
    Identity,
    Projection(Vec<ColumnCopy>),
}

#[derive(Serialize)]
pub struct PortKeys {
    operator: u64,
    keys: Vec<KeyColumns>,
    transform: Option<Transform>,
}

fn column(part: Part, field: usize) -> Column {
    Column { part, field }
}

fn width(node: &Node, name: &str) -> Option<usize> {
    node.data(name)?.as_u64()?.try_into().ok()
}

fn layout(node: &Node, fields: &str, index: &str) -> Option<OutputShape> {
    Some(OutputShape {
        width: width(node, fields)?,
        index_fields: width(node, index),
    })
}

fn primary_fields(node: &Node) -> Result<Vec<usize>, Error> {
    let Some(Field::Array(fields)) = node.field("keyFields") else {
        return Err(Error::new("missing primary key fields"));
    };
    fields
        .iter()
        .map(|field| {
            match field {
                Field::Data(number) => number.as_u64().and_then(|n| n.try_into().ok()),
                _ => None,
            }
            .ok_or_else(|| Error::new("invalid primary key field"))
        })
        .collect()
}

fn source(node: &Node, shape: OutputShape, indexed: bool) -> Result<Facts, Error> {
    let fields = primary_fields(node)?;
    let mut facts = Facts::default();
    if indexed {
        let Some(index) = shape.index_fields else {
            return Ok(facts);
        };
        let rows = width(node, "sourceWidth").ok_or_else(|| Error::new("missing table width"))?;
        let values = shape.width - index;
        if rows == values {
            for (i, field) in fields.iter().enumerate() {
                facts
                    .equal
                    .include([column(Part::Index, i), column(Part::Value, *field)]);
            }
        } else if values.checked_add(fields.len()) != Some(rows) {
            return Err(Error::new(
                "indexed table value width disagrees with its primary key",
            ));
        }
        facts
            .keys
            .push(facts.equal.key((0..index).map(|i| column(Part::Index, i))));
    } else if !fields.is_empty() {
        facts.keys.push(
            facts
                .equal
                .key(fields.into_iter().map(|i| column(Part::None, i))),
        );
    }
    Ok(facts)
}

struct Projection {
    copies: Vec<ColumnCopy>,
    by_source: Copies,
    equal: ColumnEquivalence,
}

impl Projection {
    fn read(
        edit: &CircuitEditor,
        closure: NodeId,
        shape: OutputShape,
        lossless: bool,
    ) -> Result<Option<Self>, Error> {
        let Some(copies) = column_copies(edit, closure, &shape, lossless)? else {
            return Ok(None);
        };
        let Some(computations) = equal_computations(edit, closure, &shape)? else {
            return Ok(None);
        };
        let by_source = copies_by_source(&copies);
        let mut equal = ColumnEquivalence::default();
        for columns in by_source.values() {
            equal.include(columns.iter().copied());
        }
        for columns in computations {
            equal.include(columns);
        }
        Ok(Some(Self {
            copies,
            by_source,
            equal,
        }))
    }

    fn input_equal(&self, input: &Facts, projection: KeyProjection) -> ColumnEquivalence {
        input
            .equal
            .after(|c, out| projection.copy_to(c, &self.by_source, out))
    }
}

fn deindex(column: Column, out: &mut BTreeSet<Column>) {
    if column.part == Part::Value {
        out.insert(Column {
            part: Part::None,
            ..column
        });
    }
}

impl Facts {
    fn add_key(&mut self, columns: impl IntoIterator<Item = Column>) {
        let mut keys = std::mem::take(&mut self.keys);
        keys.push(self.equal.key(columns));
        self.keys = minimal(keys);
    }

    fn grouped(&self, input: Option<OutputShape>, output: OutputShape) -> Self {
        let mut result = Self::default();
        if let Some(index) = output.index_fields {
            if input.and_then(|s| s.index_fields) == Some(index) {
                result.equal = self.equal.after(|c, out| {
                    if c.part == Part::Index {
                        out.insert(c);
                    }
                });
            }
            result.add_key((0..index).map(|i| column(Part::Index, i)));
        }
        result
    }

    fn values_only(&self) -> Self {
        Self {
            keys: minimal(
                self.keys
                    .iter()
                    .filter_map(|k| k.project(deindex))
                    .collect(),
            ),
            equal: self.equal.after(deindex),
        }
    }
}

#[derive(Default)]
struct Derived {
    facts: Facts,
    transform: Option<Transform>,
}

impl Derived {
    fn project(
        edit: &CircuitEditor,
        node: &Node,
        output: OutputShape,
        first: &Facts,
        projection: KeyProjection,
        lossless: bool,
    ) -> Result<Option<Self>, Error> {
        let (shape, index) = match projection {
            KeyProjection::Ranked => {
                let Some(index) = output.index_fields else {
                    return Ok(Some(Self::default()));
                };
                (
                    OutputShape {
                        width: output.width - index,
                        index_fields: Some(0),
                    },
                    Some(index),
                )
            }
            KeyProjection::Row if layout(node, "inputWidth", "inputIndexWidth").is_none() => {
                return Ok(Some(Self::default()));
            }
            KeyProjection::Row => (output, None),
            KeyProjection::Join { .. } => {
                return Err(Error::new("expected a single-input key projection"));
            }
        };
        let Some(closure) = node.field("function").and_then(Field::node_id) else {
            // A ranked output with no producer must take the reference diagnostic path.
            return Ok(index.is_none().then(Self::default));
        };
        let Some(columns) = Projection::read(edit, closure, shape, lossless)? else {
            return Ok(None);
        };
        let mut equal = columns.input_equal(first, projection);
        equal.extend(columns.equal);
        let mut facts = Facts {
            keys: projection.map(&first.keys, &columns.by_source),
            equal,
        };
        let transform = if let Some(index) = index {
            if node.data("singleRank").and_then(|n| n.as_bool()) == Some(true) {
                facts.add_key((0..index).map(|i| column(Part::Index, i)));
            }
            None
        } else {
            Some(Transform::Projection(columns.copies))
        };
        Ok(Some(Self { facts, transform }))
    }
}

#[derive(Default)]
struct Analysis {
    facts: HashMap<OutputPort, Facts>,
    results: Vec<PortKeys>,
    unknown: Facts,
}

impl Analysis {
    fn input(&self, port: OutputPort) -> &Facts {
        self.facts.get(&port).unwrap_or(&self.unknown)
    }

    fn union(&self, inputs: &[OutputPort]) -> Facts {
        let mut facts = inputs.iter().map(|&port| self.input(port));
        let Some(first) = facts.next() else {
            return Facts::default();
        };
        if inputs.len() == 1 {
            return first.clone();
        }
        let equal = facts.fold(first.equal.clone(), |common, next| {
            common.intersect(&next.equal)
        });
        Facts {
            keys: Vec::new(),
            equal,
        }
    }

    fn join(&self, columns: Projection, inputs: &[OutputPort], left_join: bool) -> Facts {
        let join = JoinInputs {
            inputs: inputs.iter().map(|&p| self.input(p).keys.clone()).collect(),
            left_join,
        };
        let mut result = Facts {
            keys: join.derive(&columns.copies),
            equal: ColumnEquivalence::default(),
        };
        for (i, &port) in inputs.iter().enumerate() {
            result.equal.extend(columns.input_equal(
                self.input(port),
                KeyProjection::Join {
                    value_parameter: i + 1,
                },
            ));
        }
        result.equal.extend(columns.equal);
        result
    }

    fn transfer(
        &self,
        edit: &CircuitEditor,
        node: &Node,
        kind: Op,
        lossless: bool,
    ) -> Result<Option<Derived>, Error> {
        let mut transform = match kind {
            Op::Filter | Op::Noop | Op::Integrate | Op::Differentiate => Some(Transform::Identity),
            _ => None,
        };
        let Some(output) = layout(node, "width", "indexWidth") else {
            return Ok(Some(Derived {
                facts: Facts::default(),
                transform,
            }));
        };
        let inputs: Vec<_> = node.inputs().collect();
        let first = inputs
            .first()
            .map(|&p| self.input(p))
            .unwrap_or(&self.unknown);
        let input_layout = layout(node, "inputWidth", "inputIndexWidth");
        let function = node.field("function").and_then(Field::node_id);
        let facts = match kind {
            Op::SourceMultiset | Op::SourceMap | Op::SourceLazyMap => {
                source(node, output, kind != Op::SourceMultiset)?
            }
            Op::Noop
            | Op::Integrate
            | Op::Differentiate
            | Op::Delay
            | Op::DelayedIntegral
            | Op::Negate
            | Op::Positive
            | Op::Sink
            | Op::View
            | Op::AntiJoin
            | Op::StreamAntiJoin => first.clone(),
            Op::AggregateZero => Facts {
                keys: vec![KeyColumns::default()],
                ..Facts::default()
            },
            Op::Distinct | Op::StreamDistinct => {
                let mut result = first.clone();
                result.add_key((0..output.width).map(|i| output.column(i)));
                transform = Some(Transform::Identity);
                result
            }
            Op::Filter => {
                let mut result = first.clone();
                if let (Some(input), Some(closure)) = (input_layout, function) {
                    let Some(fixed) = fixed_columns(edit, closure, &input, lossless)? else {
                        return Ok(None);
                    };
                    result.keys = filter_keys(result.keys, &fixed);
                }
                result
            }
            Op::Sum | Op::AtomicSum | Op::Subtract => self.union(&inputs),
            Op::Deindex => first.values_only(),
            Op::Aggregate
            | Op::StreamAggregate
            | Op::PartitionedRollingAggregate
            | Op::ChainAggregate
            | Op::PrimitiveAggregate
            | Op::AggregateLinearPostprocess
            | Op::AggregateLinearPostprocessRetainKeys => first.grouped(input_layout, output),
            Op::Map | Op::MapIndex | Op::IndexedTopK => {
                let projection = if kind == Op::IndexedTopK {
                    KeyProjection::Ranked
                } else {
                    KeyProjection::Row
                };
                return Derived::project(edit, node, output, first, projection, lossless);
            }
            Op::Join
            | Op::JoinIndex
            | Op::StreamJoin
            | Op::StreamJoinIndex
            | Op::LeftJoin
            | Op::LeftJoinIndex
            | Op::StarJoin
            | Op::StarJoinIndex => {
                let Some(closure) = function else {
                    return Ok(Some(Derived::default()));
                };
                let Some(columns) = Projection::read(edit, closure, output, lossless)? else {
                    return Ok(None);
                };
                self.join(
                    columns,
                    &inputs,
                    matches!(kind, Op::LeftJoin | Op::LeftJoinIndex),
                )
            }
            _ => Facts::default(),
        };
        Ok(Some(Derived { facts, transform }))
    }
}

/// Returns no partial facts when a closure needs the reference interpreter.
pub fn circuit_keys(edit: &CircuitEditor, lossless: bool) -> Result<Option<Vec<PortKeys>>, Error> {
    let mut analysis = Analysis::default();
    for id in edit.circuit().operator_order(BodyOrder::Postorder)? {
        let node = edit.node(id)?;
        let Some(kind) = node.operator_kind() else {
            continue;
        };
        let Some(Derived { facts, transform }) = analysis.transfer(edit, node, kind, lossless)?
        else {
            return Ok(None);
        };
        if !facts.keys.is_empty() || transform.is_some() {
            let NodeId::Outer(operator) = id else {
                return Err(Error::new("expected an operator"));
            };
            analysis.results.push(PortKeys {
                operator,
                keys: facts.keys.clone(),
                transform,
            });
        }
        analysis.facts.insert(
            OutputPort {
                operator: id,
                number: 0,
            },
            facts,
        );
    }
    Ok(Some(analysis.results))
}
