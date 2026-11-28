//! Lower positional set operations from typed FRel to collection operators.

use super::{
    source_table::{range, Ids},
    view::{scan_stream, stream_schema, Stream},
    Error,
};
use feldera_sql_ir::{Circuit, NodeId, OperatorKind as Op, OutputPort};
use feldera_sql_opt::{stream_multiplicities, Multiplicities};
use feldera_sql_rel_model::{
    catalog::CatalogSchema, input_producers::SetKind, program::Program,
    relation_properties::Properties, statements::StatementSource, types::TypeKind,
};
use serde_json::{json, Value};
use std::collections::HashSet;

pub(super) fn lower(
    checkpoint: &mut Value,
    program: &Program,
    ids: &mut Ids,
    schema: &CatalogSchema,
    relation: u32,
    source: &StatementSource,
) -> Result<Stream, Error> {
    let mut query = SetQuery {
        program,
        source,
        seen: HashSet::new(),
    };
    query.stream(checkpoint, ids, schema, relation)
}

struct SetQuery<'a> {
    program: &'a Program,
    source: &'a StatementSource,
    seen: HashSet<u32>,
}

impl SetQuery<'_> {
    fn stream(
        &mut self,
        checkpoint: &mut Value,
        ids: &mut Ids,
        schema: &CatalogSchema,
        relation: u32,
    ) -> Result<Stream, Error> {
        if !self.seen.insert(relation) {
            return Err(Error(
                "shared input inside a set operation requires stream reuse".into(),
            ));
        }
        let program = self.program;
        let properties = &program.properties.relations[relation as usize];
        let Properties::Set { all } = properties.fields else {
            return scan_stream(checkpoint, program, ids, schema, relation, self.source);
        };
        let producer = &program.producers.relations[relation as usize];
        let kind = producer
            .form
            .set_kind()
            .ok_or_else(|| Error("invalid set relation".into()))?;
        if producer.inputs.len() < 2
            || !properties.metadata.traits.is_empty()
            || !properties.metadata.hints.is_empty()
        {
            return Err(Error(
                "expected a positional set operation of at least two inputs".into(),
            ));
        }
        let mut inputs = Vec::with_capacity(producer.inputs.len());
        let mut first_names = Vec::new();
        for &input in &producer.inputs {
            let row = program.producers.relations[input as usize].row;
            let TypeKind::Row { fields, .. } = &program.table.type_table.types[row.0 as usize].kind
            else {
                return Err(Error("set input requires a row type".into()));
            };
            // Input rows retain their own names. The SQL result inherits its
            // names from the left operand, and the view can rename them again.
            let child = stream_schema(schema, fields)?;
            let names: Vec<_> = fields.iter().map(|f| f.name.name.as_str()).collect();
            if inputs.is_empty() {
                first_names = names;
            } else if names != first_names
                && names.iter().collect::<HashSet<_>>() == first_names.iter().collect()
            {
                return Err(Error(
                    "set operands require a reordered-fields warning".into(),
                ));
            }
            inputs.push(self.stream(checkpoint, ids, &child, input)?);
        }
        let id = ids.relation(relation);
        let origin = set_origin(kind, &inputs, all, id, ids.plan(relation)?)?;
        let circuit = &mut checkpoint["circuit"];
        let multiplicities = if all || kind == SetKind::Union {
            Multiplicities::new()
        } else {
            let graph = Circuit::from_json(circuit.clone()).map_err(|e| Error(e.to_string()))?;
            stream_multiplicities(&graph).map_err(|e| Error(e.to_string()))?
        };
        let mut writer = SetWriter {
            operators: circuit["allOperators"]
                .as_array_mut()
                .expect("system operators"),
            ids,
            relation: origin.clone(),
            intermediate: None,
        };
        let output = writer.combine(kind, all, &inputs, &multiplicities)?;
        Ok(Stream {
            operator: output.id.into(),
            output_type: output.ty.clone(),
            relation: json!({"ref":id}),
            origin,
        })
    }
}

#[derive(Clone, Copy)]
struct Port<'a> {
    id: u64,
    ty: &'a Value,
}

struct SetWriter<'a> {
    operators: &'a mut Vec<Value>,
    ids: &'a mut Ids,
    /// The first use emits the full relation. Subsequent uses emit a reference.
    relation: Value,
    intermediate: Option<u64>,
}

impl SetWriter<'_> {
    fn provenance(&mut self, last: bool) -> Value {
        if let Some(id) = self.intermediate.filter(|_| !last) {
            return json!({"ref":id});
        }
        let id = self.ids.next_origin();
        if !last {
            self.intermediate = Some(id);
        }
        let relation_id = self
            .relation
            .get("id")
            .unwrap_or(&self.relation["ref"])
            .clone();
        let relation = std::mem::replace(&mut self.relation, json!({"ref":relation_id}));
        provenance(id, last, relation)
    }

    fn emit<'a>(&mut self, op: Op, inputs: &[Port<'a>], provenance: Value) -> Port<'a> {
        let id = self.ids.next_operator();
        let ty = inputs[0].ty;
        let edges: Vec<_> = inputs
            .iter()
            .map(|input| json!({"operator":{"node":input.id},"outputNumber":0}))
            .collect();
        self.operators
            .push(json!({"class":op.wire_name(),"id":id,"inputs":edges,
            "outputType":ty,"annotations":[],"provenance":provenance}));
        Port { id, ty }
    }

    fn operator<'a>(&mut self, op: Op, inputs: &[Port<'a>], last: bool) -> Port<'a> {
        let provenance = self.provenance(last);
        self.emit(op, inputs, provenance)
    }

    fn input<'a>(&mut self, stream: &'a Stream, facts: &Multiplicities) -> Result<Port<'a>, Error> {
        let id = stream
            .operator
            .as_u64()
            .ok_or_else(|| Error("missing set input operator".into()))?;
        let port = Port {
            id,
            ty: &stream.output_type,
        };
        if facts.is_empty() {
            return Ok(port);
        }
        let key = OutputPort {
            operator: NodeId::Outer(id),
            number: 0,
        };
        match facts.get(&key) {
            Some(true) => {
                let source = provenance(self.ids.next_origin(), false, stream.relation.clone());
                Ok(self.emit(Op::StreamDistinct, &[port], source))
            }
            Some(false) => Ok(port),
            None => Err(Error(format!("missing multiplicity of set input {id}"))),
        }
    }

    /// Retain positive weights for EXCEPT ALL, or emit one copy per positive
    /// weight for EXCEPT DISTINCT. Positive operates on a delta stream.
    fn difference<'a>(&mut self, inputs: &[Port<'a>], all: bool) -> Port<'a> {
        let sum = self.operator(Op::Sum, inputs, false);
        if all {
            let delta = self.operator(Op::Differentiate, &[sum], false);
            let positive = self.operator(Op::Positive, &[delta], false);
            self.operator(Op::Integrate, &[positive], true)
        } else {
            self.operator(Op::StreamDistinct, &[sum], true)
        }
    }

    fn combine<'a>(
        &mut self,
        kind: SetKind,
        all: bool,
        inputs: &'a [Stream],
        facts: &Multiplicities,
    ) -> Result<Port<'a>, Error> {
        let first = self.input(&inputs[0], facts)?;
        if kind == SetKind::Intersect {
            let mut current = first;
            for stream in &inputs[1..] {
                // A intersect B = A minus (A minus B), for sets and multisets.
                let next = self.input(stream, facts)?;
                let negative = self.operator(Op::Negate, &[next], false);
                let difference = self.difference(&[current, negative], all);
                let negative_difference = self.operator(Op::Negate, &[difference], false);
                current = self.operator(Op::Sum, &[current, negative_difference], false);
            }
            return Ok(current);
        }
        let mut ports = vec![first];
        for stream in &inputs[1..] {
            let port = self.input(stream, facts)?;
            ports.push(if kind == SetKind::Except {
                self.operator(Op::Negate, &[port], false)
            } else {
                port
            });
        }
        if kind == SetKind::Except {
            return Ok(self.difference(&ports, all));
        }
        let sum = self.operator(Op::Sum, &ports, all);
        Ok(if all {
            sum
        } else {
            self.operator(Op::StreamDistinct, &[sum], true)
        })
    }
}

fn provenance(id: u64, last: bool, relation: Value) -> Value {
    json!({"id":id,"kind":if last {"LastRel"} else {"IntermediateRel"},
           "position":range(0,0,0,0),"positions":[],"relation":relation})
}

fn set_origin(
    kind: SetKind,
    inputs: &[Stream],
    all: bool,
    id: usize,
    plan: u64,
) -> Result<Value, Error> {
    let form = kind.form();
    let mut digest = Vec::new();
    let mut text = format!("{form:?}(all=[{all}])");
    let mut sql = Vec::new();
    for (ordinal, stream) in inputs.iter().enumerate() {
        let child = &stream.origin;
        let child_text = child["text"]
            .as_str()
            .ok_or_else(|| Error("missing input plan text".into()))?;
        let (kind, _) = child_text
            .split_once('(')
            .ok_or_else(|| Error("missing input relation kind".into()))?;
        let child_id = child["relId"]
            .as_u64()
            .ok_or_else(|| Error("missing input relation ID".into()))?;
        digest.push(format!("input#{ordinal}={kind}#{child_id}"));
        text.push_str("\n  ");
        text.push_str(&child_text.replace('\n', "\n  "));
        let query = child["sql"]
            .as_str()
            .ok_or_else(|| Error("missing input SQL".into()))?;
        sql.push(match kind {
            "LogicalUnion" | "LogicalMinus" | "LogicalIntersect" => {
                format!("SELECT *\nFROM ({query})")
            }
            _ => query.into(),
        });
    }
    digest.push(format!("all={all}"));
    let operation = format!("{}{}", kind.keyword(), if all { " ALL" } else { "" });
    let sql = sql.join(&format!("\n{operation}\n"));
    let stateful = match kind {
        SetKind::Union => (!all).then(|| "a UNION".to_owned()),
        SetKind::Intersect | SetKind::Except => Some(format!("an {operation}")),
    };
    Ok(
        json!({"id":id,"relId":id,"planId":plan,"tableScan":false,"digest":format!("{form:?}.({})",digest.join(",")),
              "text":text,"sql":sql,"statefulSqlName":stateful,"origin":inputs[0].origin["origin"]}),
    )
}
