//! Lower snapshot aggregates to the accumulator and stream operations used by DBSP.
mod chain;
mod linear;
mod min_max;

use crate::{
    aggregate_groups,
    rebuild::{input_type, ProjectionNames},
    stream_rewrites::body_members,
    AggregateKind,
};
use feldera_sql_expr::{copy_expression, reduce_closure_body_with_warnings, SimplifyWarning};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind as Op,
    OperatorSite, OutputPort, TypeId, TypeKind as Ty,
};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};

fn link(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}
fn port(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

/// Input and construction context of one stream aggregate. Every generated operator
/// inherits its relational origin and is scheduled before the original reader.
struct Expansion<'a> {
    edit: &'a mut CircuitEditor,
    next: &'a mut u64,
    warnings: &'a mut Vec<SimplifyWarning>,
    body: NodeId,
    site: NodeId,
    input: OutputPort,
    key: TypeId,
    value: TypeId,
}

impl Expansion<'_> {
    fn operator<const N: usize>(
        &mut self,
        kind: Op,
        input: OutputPort,
        fields: [(&'static str, Field); N],
    ) -> Result<NodeId, Error> {
        self.edit
            .append_operator(self.body, self.site, kind, &[input], fields)
    }

    fn call(&mut self, closure: NodeId, args: &[ExpressionId]) -> Result<ExpressionId, Error> {
        let c = self.edit.node(closure)?;
        let result = reduce_closure_body_with_warnings(
            self.edit,
            c.edge("body")?,
            &c.edges("parameters")?,
            &args.iter().copied().map(Into::into).collect::<Vec<_>>(),
            Some(self.warnings),
        )?;
        self.edit.expression(result)
    }

    fn pair_variable(&mut self, value: TypeId) -> Result<ExpressionId, Error> {
        let k = self.edit.reference_type(self.key)?;
        let v = self.edit.reference_type(value)?;
        let ty = self.edit.tuple_type(k.into(), true, [k.into(), v.into()])?;
        self.edit.variable(ty, self.next)
    }

    fn half(&mut self, pair: ExpressionId, index: usize) -> Result<ExpressionId, Error> {
        let field = self.edit.tuple_field(pair, index)?;
        self.edit.dereference(field)
    }

    fn flatten(&mut self, value: ExpressionId) -> Result<Vec<ExpressionId>, Error> {
        let fields = self
            .edit
            .node(self.edit.expression_type(value)?.into())?
            .edges("tupFields")?;
        (0..fields.len())
            .map(|i| {
                let copy = copy_expression(self.edit, value.into())?;
                let field = self.edit.tuple_field(self.edit.expression(copy)?, i)?;
                self.edit.clone_if_needed(field)
            })
            .collect()
    }

    fn map_index(
        &mut self,
        input: OutputPort,
        row: ExpressionId,
        var: ExpressionId,
    ) -> Result<NodeId, Error> {
        let closure = self.edit.closure_from_variables(row, &[var])?;
        let ty = self
            .edit
            .indexed_zset_from_pair(self.edit.expression_type(row)?)?;
        self.operator(
            Op::MapIndex,
            input,
            [("outputType", link(ty)), ("function", link(closure))],
        )
    }

    fn restore_order(&mut self, input: NodeId, permutation: &[usize]) -> Result<NodeId, Error> {
        if permutation.iter().copied().eq(0..permutation.len()) {
            return Ok(input);
        }
        let collection = input_type(self.edit, port(input))?;
        let value = self
            .edit
            .type_id(self.edit.node(collection.into())?.edge("elementType")?)?;
        let var = self.pair_variable(value)?;
        let key = self.half(var, 0)?;
        let key = self.edit.clone_if_needed(key)?;
        let keys = self.flatten(key)?;
        let key = self.edit.tuple_expr(var.into(), false, &keys)?;
        let row = self.half(var, 1)?;
        let fields = self.flatten(row)?;
        if fields.len() != permutation.len() {
            return Err(Error::new(
                "aggregate result width differs from its column permutation",
            ));
        }
        let mut ordered = fields.clone();
        for (&destination, field) in permutation.iter().zip(fields) {
            ordered[destination] = field;
        }
        let value = self.edit.tuple_expr(var.into(), false, &ordered)?;
        let row = self.edit.tuple_expr(var.into(), true, &[key, value])?;
        self.map_index(port(input), row, var)
    }

    /// A one-entry list already owns its cached empty-result tuple. Regrouping
    /// a longer list constructs a fresh tuple and type for each group.
    fn group(
        &mut self,
        list: NodeId,
        parts: &[NodeId],
        regroup: bool,
    ) -> Result<(NodeId, TypeId), Error> {
        let cached = self
            .edit
            .node(list)?
            .field("cachedEmptySetResult")
            .and_then(Field::node_id);
        if !regroup {
            if let Some(cached) = cached {
                return Ok((
                    list,
                    self.edit.expression_type(self.edit.expression(cached)?)?,
                ));
            }
        }
        let values = parts
            .iter()
            .map(|&p| {
                self.edit
                    .expression(self.edit.node(p)?.edge("emptySetResult")?)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let types = values
            .iter()
            .map(|&v| self.edit.expression_type(v).map(Into::into))
            .collect::<Result<Vec<_>, _>>()?;
        let ty = self.edit.tuple_type(list, false, types)?;
        if !regroup {
            return Ok((list, ty));
        }
        let entries = Field::Array(parts.iter().copied().map(link).collect());
        let mut edits = vec![("aggregates", entries)];
        if cached.is_some() {
            let tuple = self.edit.tuple_with_type_at(list, ty, &values)?;
            edits.push(("cachedEmptySetResult", link(tuple)));
        }
        Ok((self.edit.derive_inner(list, edits)?, ty))
    }

    fn expand(
        &mut self,
        list: NodeId,
        append_only: bool,
        weight: ExpressionId,
    ) -> Result<NodeId, Error> {
        let parts = self.edit.node(list)?.edges("aggregates")?;
        if parts.is_empty() {
            let ty = self
                .edit
                .type_id(self.edit.node(self.site)?.edge("outputType")?)?;
            return self.linear(list, &[], ty);
        }
        let kinds = parts
            .iter()
            .map(|&part| match self.edit.node(part)?.class() {
                "LinearAggregate" => Ok(AggregateKind::Linear),
                "NonLinearAggregate" => Ok(AggregateKind::NonLinear),
                "MinMaxAggregate" => Ok(AggregateKind::MinMax),
                kind => Err(Error::new(format!(
                    "unknown aggregate implementation {kind}"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let groups = aggregate_groups(&kinds, append_only);
        let mut operators = Vec::with_capacity(groups.len());
        let mut permutation = Vec::with_capacity(parts.len());
        for indices in groups {
            let members = indices.iter().map(|&i| parts[i]).collect::<Vec<_>>();
            let (group, value) = self.group(list, &members, parts.len() > 1)?;
            let ty = self.edit.indexed_zset_type(value.into(), self.key, value)?;
            let operator = match kinds[indices[0]] {
                AggregateKind::NonLinear => self.operator(
                    Op::StreamAggregate,
                    self.input,
                    [("aggregate", link(group)), ("outputType", link(ty))],
                )?,
                _ if append_only => self.chain(&members, weight)?,
                AggregateKind::Linear => self.linear(group, &members, ty)?,
                AggregateKind::MinMax => self.min_max(members[0], value)?,
            };
            operators.push(operator);
            permutation.extend(indices);
        }
        let combined = self.edit.combine_aggregate_list(
            self.body,
            OperatorSite {
                operator: self.site,
                position: None,
            },
            self.key,
            &operators,
            self.next,
        )?;
        self.restore_order(combined, &permutation)
    }
}

/// Compiler state retained by the aggregate pass when its schedule is built.
#[derive(Deserialize)]
pub struct AggregateContext {
    pub weight_id: u64,
    pub weight_name: String,
    pub append_only_inputs: BTreeSet<u64>,
}

pub struct ExpandedAggregates {
    pub circuit: Circuit,
    pub warnings: Vec<SimplifyWarning>,
    /// Weight path first introduced by this expansion and reachable in its output.
    pub new_weight: Option<u64>,
}

/// Expand each aggregate list in postorder, retaining the compiler's shared
/// row-weight expression wherever a chain initializer or step references it.
pub fn expand_stream_aggregates(
    circuit: Circuit,
    context: &AggregateContext,
    next: &mut u64,
) -> Result<ExpandedAggregates, Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let Some(site) = order.iter().copied().find(|&id| {
        circuit.node(id).is_some_and(|n| {
            n.operator_kind() == Some(Op::StreamAggregate)
                && n.field("aggregate").and_then(Field::node_id).is_some()
        })
    }) else {
        return Ok(ExpandedAggregates {
            circuit,
            warnings: Vec::new(),
            new_weight: None,
        });
    };
    let bodies = body_members(&circuit)?
        .into_iter()
        .map(|(b, id)| (id, b))
        .collect::<HashMap<_, _>>();
    let mut edit = CircuitEditor::new(circuit);
    let weight_id = NodeId::Inner(context.weight_id);
    let already_bound = edit.circuit().node(weight_id).is_some();
    let weight = if already_bound {
        if edit
            .node(weight_id)?
            .data("variable")
            .and_then(|v| v.as_str())
            != Some(context.weight_name.as_str())
            || edit.type_kind(edit.expression_type(edit.expression(weight_id)?)?)? != Ty::Weight
        {
            return Err(Error::new("invalid aggregate weight binding"));
        }
        edit.expression(weight_id)?
    } else {
        let ty = edit.standard_type(site, Ty::Weight)?;
        edit.named_variable(ty, &context.weight_name)?
    };
    let mut names = ProjectionNames::default();
    let mut counter = *next;
    let mut warnings = Vec::new();
    let mut replacements = BTreeMap::new();
    for id in order {
        let n = edit.node(id)?;
        names.visit(id, n, &mut counter)?;
        if n.operator_kind() != Some(Op::StreamAggregate) {
            continue;
        }
        let Some(list) = n.field("aggregate").and_then(Field::node_id) else {
            continue;
        };
        let original = n
            .inputs()
            .next()
            .ok_or_else(|| Error::new("stream aggregate requires an input"))?;
        let input = *replacements.get(&original.operator).unwrap_or(&original);
        let ty = input_type(&edit, input)?;
        let ix = edit.node(ty.into())?;
        let key = edit.type_id(ix.edge("keyType")?)?;
        let value = edit.type_id(ix.edge("elementType")?)?;
        let body = bodies[&id];
        let before = edit.node(body)?.edges("allOperators")?.len();
        let mut expansion = Expansion {
            edit: &mut edit,
            next: &mut counter,
            warnings: &mut warnings,
            body,
            site: id,
            input,
            key,
            value,
        };
        let result = expansion.expand(
            list,
            original.number == 0 && matches!(original.operator, NodeId::Outer(id) if context.append_only_inputs.contains(&id)),
            weight,
        )?;
        let first = edit.node(body)?.edges("allOperators")?[before];
        // The next expansion can refer to this relational origin before finish_forwarding.
        let origin = edit.node(id)?.data("provenance").cloned();
        let reference = edit.node(first)?.data("provenance").cloned();
        edit.set_intermediate_origin(first, origin)?;
        edit.set_intermediate_origin(id, reference)?;
        edit.place_before(body, first, id)?;
        names.replaced(id);
        replacements.insert(id, port(result));
    }
    let circuit = names.finish(edit.finish_forwarding(&replacements)?)?;
    *next = counter;
    let new_weight = match NodeId::from(weight) {
        NodeId::Inner(id) if !already_bound && circuit.node(weight.into()).is_some() => Some(id),
        _ => None,
    };
    Ok(ExpandedAggregates {
        circuit,
        warnings,
        new_weight,
    })
}
