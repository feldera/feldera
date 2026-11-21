//! Fetch the payload carried through a chain of left joins once, at its end.
//! The chain analysis supplies the identifying key and column correspondence.
mod keys;
pub use keys::{ChainKeyChoice, ChainKeys, choose_chain_key};

use crate::rebuild::{ProjectionNames, input_type};
use crate::stream_rewrites::{append_map, body_members};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind as Op,
    OutputPort, TypeId, TypeKind,
};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Part {
    None,
    Index,
    Value,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
struct Column {
    part: Part,
    field: usize,
}

#[derive(Deserialize)]
struct ColumnCopy {
    source: Column,
    result: Column,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChainLayout {
    start: u64,
    port: usize,
    reuse_index: bool,
    key: Vec<ColumnCopy>,
    deferred: Vec<ColumnCopy>,
    copies: Vec<ColumnCopy>,
}

fn port(id: NodeId) -> OutputPort {
    OutputPort {
        operator: id,
        number: 0,
    }
}

fn borrowed(edit: &mut CircuitEditor, ty: TypeId, next: &mut u64) -> Result<ExpressionId, Error> {
    let reference = edit.reference_type(ty)?;
    edit.variable(reference, next)
}

fn field(
    edit: &mut CircuitEditor,
    reference: ExpressionId,
    index: usize,
) -> Result<ExpressionId, Error> {
    let value = edit.dereference(reference)?;
    let field = edit.tuple_field(value, index)?;
    edit.clone_if_needed(field)
}

impl Column {
    fn read(self, edit: &mut CircuitEditor, row: ExpressionId) -> Result<ExpressionId, Error> {
        let reference = match self.part {
            Part::None => row,
            Part::Index => edit.tuple_field(row, 0)?,
            Part::Value => edit.tuple_field(row, 1)?,
        };
        field(edit, reference, self.field)
    }
}

/// The ordered columns of a stream element, with a borrowed parameter for
/// expressions that read them. Indexed streams expose their key first.
struct Row {
    variable: ExpressionId,
    indexed: bool,
    columns: Vec<Column>,
}

impl Row {
    fn new(edit: &mut CircuitEditor, stream: TypeId, next: &mut u64) -> Result<Self, Error> {
        let value = edit.type_id(edit.node(stream.into())?.edge("elementType")?)?;
        let key = match edit.type_kind(stream)? {
            TypeKind::IndexedZSet => {
                Some(edit.type_id(edit.node(stream.into())?.edge("keyType")?)?)
            }
            TypeKind::ZSet => None,
            _ => return Err(Error::new("left-join chain requires a collection")),
        };
        let mut columns = Vec::new();
        let variable = if let Some(key) = key {
            let width = edit.node(key.into())?.edges("tupFields")?.len();
            columns.extend((0..width).map(|field| Column {
                part: Part::Index,
                field,
            }));
            let k = edit.reference_type(key)?;
            let v = edit.reference_type(value)?;
            let pair = edit.tuple_type(stream.into(), true, [k.into(), v.into()])?;
            edit.variable(pair, next)?
        } else {
            borrowed(edit, value, next)?
        };
        let width = edit.node(value.into())?.edges("tupFields")?.len();
        let part = if key.is_some() {
            Part::Value
        } else {
            Part::None
        };
        columns.extend((0..width).map(|field| Column { part, field }));
        Ok(Self {
            variable,
            indexed: key.is_some(),
            columns,
        })
    }
}

struct CarriedRow {
    row: Row,
    fields: Vec<ExpressionId>,
    types: HashMap<Column, TypeId>,
}

impl CarriedRow {
    fn new(edit: &mut CircuitEditor, stream: TypeId, next: &mut u64) -> Result<Self, Error> {
        let row = Row::new(edit, stream, next)?;
        let mut fields = Vec::with_capacity(row.columns.len());
        let mut types = HashMap::with_capacity(row.columns.len());
        for &column in &row.columns {
            let value = column.read(edit, row.variable)?;
            fields.push(value);
            types.insert(column, edit.expression_type(value)?);
        }
        Ok(Self { row, fields, types })
    }

    fn column_type(&self, column: Column) -> Result<TypeId, Error> {
        self.types
            .get(&column)
            .copied()
            .ok_or_else(|| Error::new("carried column is absent from the chain output"))
    }
}

fn key_value(
    edit: &mut CircuitEditor,
    row: ExpressionId,
    key: &[ExpressionId],
    value: &[ExpressionId],
) -> Result<ExpressionId, Error> {
    let k = edit.tuple_expr(row.into(), false, key)?;
    let v = edit.tuple_expr(row.into(), false, value)?;
    edit.tuple_expr(row.into(), true, &[k, v])
}

struct Lookup<'a> {
    edit: &'a mut CircuitEditor,
    body: NodeId,
    end: NodeId,
    next: &'a mut u64,
}

impl Lookup<'_> {
    fn index(
        &mut self,
        input: OutputPort,
        row: ExpressionId,
        key: &[ExpressionId],
        value: &[ExpressionId],
    ) -> Result<OutputPort, Error> {
        let pair = key_value(self.edit, row, key, value)?;
        let function = self.edit.closure_from_variables(pair, &[row])?;
        let (id, _) = append_map(
            self.edit,
            self.body,
            self.end,
            Op::MapIndex,
            input,
            function,
        )?;
        Ok(port(id))
    }

    fn build(&mut self, plan: &ChainLayout) -> Result<OutputPort, Error> {
        let integrate = self.edit.node(self.end)?;
        if integrate.operator_kind() != Some(Op::Integrate)
            || plan.deferred.is_empty()
            || plan.key.is_empty()
        {
            return Err(Error::new(
                "expected a keyed left-join chain ending at an integrator",
            ));
        }
        let delta = integrate
            .inputs()
            .next()
            .ok_or_else(|| Error::new("missing integrator input"))?;
        let stream = self.edit.type_id(integrate.edge("outputType")?)?;
        let carried = CarriedRow::new(self.edit, stream, self.next)?;
        let integral = self
            .edit
            .insert_integral(self.body, self.end, delta, stream, None, false)?;
        self.edit.copy_annotations(self.end, integral.operator)?;
        let (right, key) = self.index_start(plan, &carried)?;
        let left = self.index(integral, carried.row.variable, &key, &carried.fields)?;
        let join = self.restore_columns(plan, &carried, stream, left, right)?;
        self.edit
            .place_before(self.body, integral.operator, self.end)?;
        Ok(join)
    }

    fn index_start(
        &mut self,
        plan: &ChainLayout,
        carried: &CarriedRow,
    ) -> Result<(OutputPort, Vec<ExpressionId>), Error> {
        let start = OutputPort {
            operator: NodeId::Outer(plan.start),
            number: plan.port,
        };
        let start_type = input_type(self.edit, start)?;
        let mut row_key = Vec::new();
        let right = if plan.reuse_index {
            let key = self.edit.node(start_type.into())?.edge("keyType")?;
            let types = self.edit.node(key)?.edges("tupFields")?;
            if types.len() != plan.key.len() {
                return Err(Error::new(
                    "carried key and reused index have different widths",
                ));
            }
            for (copy, ty) in plan.key.iter().zip(types) {
                let value = copy.result.read(self.edit, carried.row.variable)?;
                row_key.push(
                    self.edit
                        .cast(value.into(), value, self.edit.type_id(ty)?)?,
                );
            }
            start
        } else {
            let source = Row::new(self.edit, start_type, self.next)?;
            let mut keys = Vec::new();
            for copy in &plan.key {
                let output = copy.result.read(self.edit, carried.row.variable)?;
                let input = copy.source.read(self.edit, source.variable)?;
                let ty = self.edit.expression_type(output)?;
                let nullable = self.edit.nullable(self.edit.expression_type(input)?)?;
                let common = self.edit.with_nullability(ty, nullable)?;
                row_key.push(self.edit.cast(output.into(), output, common)?);
                keys.push(self.edit.cast(input.into(), input, common)?);
            }
            let mut payload = Vec::new();
            for copy in &plan.deferred {
                let input = copy.source.read(self.edit, source.variable)?;
                payload.push(self.edit.cast(
                    input.into(),
                    input,
                    carried.column_type(copy.result)?,
                )?);
            }
            self.index(start, source.variable, &keys, &payload)?
        };
        Ok((right, row_key))
    }

    fn restore_columns(
        &mut self,
        plan: &ChainLayout,
        carried: &CarriedRow,
        stream: TypeId,
        left: OutputPort,
        right: OutputPort,
    ) -> Result<OutputPort, Error> {
        let left_type = input_type(self.edit, left)?;
        let right_type = input_type(self.edit, right)?;
        let mut vars = Vec::with_capacity(3);
        for (ty, property) in [
            (left_type, "keyType"),
            (left_type, "elementType"),
            (right_type, "elementType"),
        ] {
            let ty = self
                .edit
                .type_id(self.edit.node(ty.into())?.edge(property)?)?;
            vars.push(borrowed(self.edit, ty, self.next)?);
        }
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let payload_positions: HashMap<_, _> = plan
            .deferred
            .iter()
            .enumerate()
            .map(|(i, copy)| (copy.source, i))
            .collect();
        let copies: HashMap<_, _> = plan
            .copies
            .iter()
            .filter_map(|copy| {
                Some((
                    copy.result,
                    (*payload_positions.get(&copy.source)?, copy.source),
                ))
            })
            .collect();
        for (i, &column) in carried.row.columns.iter().enumerate() {
            let deferred = copies.get(&column).copied();
            let value = match deferred {
                None => field(self.edit, vars[1], i)?,
                Some((_, source)) if plan.reuse_index => {
                    let value = field(self.edit, vars[2], source.field)?;
                    self.edit
                        .cast(value.into(), value, carried.column_type(column)?)?
                }
                Some((j, _)) => field(self.edit, vars[2], j)?,
            };
            match column.part {
                Part::Index => keys.push(value),
                _ => values.push(value),
            }
        }
        let (kind, result) = if carried.row.indexed {
            (
                Op::StreamJoinIndex,
                key_value(self.edit, vars[0], &keys, &values)?,
            )
        } else {
            (
                Op::StreamJoin,
                self.edit.tuple_expr(vars[0].into(), false, &values)?,
            )
        };
        let function = self.edit.closure_from_variables(result, &vars)?;
        let join = self.edit.append_operator(
            self.body,
            self.end,
            kind,
            &[left, right],
            [
                ("balanced", Field::Data(false.into())),
                ("outputType", Field::Reference(stream.into())),
                ("function", Field::Reference(function.into())),
            ],
        )?;
        Ok(port(join))
    }
}

pub fn defer_carried_columns(
    circuit: Circuit,
    mut layouts: BTreeMap<u64, ChainLayout>,
    next: &mut u64,
) -> Result<Circuit, Error> {
    if layouts.is_empty() {
        return Ok(circuit);
    }
    let parents: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut projections = ProjectionNames::default();
    let mut replacements = BTreeMap::new();
    let mut names = *next;
    for end in order {
        projections.visit(end, edit.node(end)?, &mut names)?;
        let NodeId::Outer(id) = end else { continue };
        if let Some(plan) = layouts.remove(&id) {
            let output = Lookup {
                edit: &mut edit,
                body: parents[&end],
                end,
                next: &mut names,
            }
            .build(&plan)?;
            replacements.insert(end, output);
            projections.replaced(end);
        }
    }
    if !layouts.is_empty() {
        return Err(Error::new("carried columns refer to an unknown integrator"));
    }
    let circuit = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok(circuit)
}
