//! Trim unused columns from a non-materialized input and rebind its sole reader.
//! The preceding field-use analysis supplies the ordered columns. The struct's
//! generated name comes from the compiler that owns the SQL type printer.
use crate::rebuild::rebuild_input_closures;
use feldera_sql_expr::{Rewrite, Transformation, Visit, parameter_paths};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, NodeId,
    OperatorKind as Op, TypeId, TypeKind,
};
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InputLayout {
    reader: u64,
    columns: Vec<usize>,
    hash_name: String,
}

fn select<T: Clone>(items: &[T], columns: &[usize]) -> Result<Vec<T>, Error> {
    columns
        .iter()
        .map(|&i| {
            items
                .get(i)
                .cloned()
                .ok_or_else(|| Error::new("trimmed column outside input row"))
        })
        .collect()
}

fn links(ids: &[NodeId]) -> Field {
    Field::Array(ids.iter().copied().map(Field::Reference).collect())
}

fn trim_table(edit: &mut CircuitEditor, table: NodeId, layout: &InputLayout) -> Result<(), Error> {
    let source = edit.node(table)?;
    let record = source.edge("originalRowType")?;
    let fields = select(&edit.node(record)?.edges("fields")?, &layout.columns)?;
    let output = edit.node(source.edge("outputType")?)?;
    let input_row = output.edge("elementType")?;
    let types = select(&edit.node(input_row)?.edges("tupFields")?, &layout.columns)?;
    let row = edit.derive_inner(
        record,
        [
            ("fields", links(&fields)),
            ("hashName", Field::Data(layout.hash_name.clone().into())),
        ],
    )?;
    let tuple = edit.inner_at(
        input_row,
        InnerKind::Type(TypeKind::Tuple),
        [
            ("tupFields", links(&types)),
            ("originalStruct", Field::Reference(row)),
            ("code", Field::Data("TUPLE".into())),
            (
                "mayBeNull",
                edit.node(input_row)?
                    .field("mayBeNull")
                    .ok_or_else(|| Error::new("missing row nullability"))?
                    .clone(),
            ),
        ],
    )?;
    let output = edit.zset_type(edit.type_id(tuple)?)?;
    edit.trim_source_columns(table, row, output, &layout.columns)
}

struct RebindFields<'a> {
    bindings: HashSet<NodeId>,
    columns: &'a [usize],
    name: String,
    reference: TypeId,
}

impl Transformation for RebindFields<'_> {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        let node = edit.node(id)?;
        if matches!(
            node.inner_kind(),
            Some(InnerKind::Type(_) | InnerKind::Expression(Expr::Closure))
        ) {
            return Ok(Visit::Keep);
        }
        if self.bindings.contains(&id) {
            return Ok(Visit::Replace(
                edit.named_variable(self.reference, &self.name)?.into(),
            ));
        }
        if matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::Field))) {
            let operand = edit.node(node.edge("expression")?)?;
            if matches!(
                operand.inner_kind(),
                Some(InnerKind::Expression(Expr::Deref))
            ) && self.bindings.contains(&operand.edge("expression")?)
            {
                let old = node
                    .data("fieldNo")
                    .and_then(|v| v.as_u64())
                    .and_then(|i| usize::try_from(i).ok())
                    .ok_or_else(|| Error::new("invalid projected field number"))?;
                let index = self
                    .columns
                    .binary_search(&old)
                    .map_err(|_| Error::new("reader references a discarded column"))?;
                let variable = edit.named_variable(self.reference, &self.name)?;
                let row = edit.dereference(variable)?;
                return Ok(Visit::Replace(edit.tuple_field(row, index)?.into()));
            }
        }
        Ok(Visit::Children)
    }
}

fn trim_reader(edit: &mut CircuitEditor, reader: NodeId, columns: &[usize]) -> Result<(), Error> {
    let node = edit.node(reader)?;
    let kind = node.operator_kind();
    let original = node.edge("function")?;
    let closure = edit.node(original)?;
    let [parameter] = closure
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("expected unary input projection"))?;
    let body = closure.edge("body")?;
    let param = edit.node(parameter)?;
    let name = param
        .data("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("missing input parameter name"))?
        .to_owned();
    let reference = param.edge("type")?;
    let row = edit.node(reference)?.edge("type")?;
    let fields = select(&edit.node(row)?.edges("tupFields")?, columns)?;
    let compressed = edit.tuple_type(row, false, fields)?;
    let nullable = edit.nullable(edit.type_id(row)?)?;
    let compressed = edit.with_nullability(compressed, nullable)?;
    let reference = edit.reference_type(compressed)?;
    let declaration =
        edit.derive_inner(parameter, [("type", Field::Reference(reference.into()))])?;
    let bindings = parameter_paths(edit, body, parameter)?;
    let body = Rewrite::new(RebindFields {
        bindings,
        columns,
        name,
        reference,
    })
    .apply(edit, body)?;
    let body = edit.expression(body)?;
    let signature = edit.closure_type(body, &[reference.into()])?;
    let function = edit.derive_inner(
        original,
        [
            ("parameters", links(&[declaration])),
            ("type", Field::Reference(signature)),
            ("body", Field::Reference(body.into())),
        ],
    )?;
    let ty = edit.expression_type(body)?;
    let output = match kind {
        Some(Op::Map) => edit.zset_type(ty)?,
        Some(Op::MapIndex) => edit.indexed_zset_from_pair(ty)?,
        _ => return Err(Error::new("invalid input projection reader")),
    };
    edit.set_operator_field(reader, "function", Field::Reference(function))?;
    edit.set_operator_field(reader, "outputType", Field::Reference(output.into()))?;
    edit.clear_annotations(reader)
}

pub fn trim_inputs(
    circuit: Circuit,
    layouts: &BTreeMap<u64, InputLayout>,
    next: &mut u64,
) -> Result<Circuit, Error> {
    if layouts.is_empty() {
        return Ok(circuit);
    }
    let mut edit = CircuitEditor::new(circuit);
    for (&source, layout) in layouts {
        let table = NodeId::Outer(source);
        let reader = NodeId::Outer(layout.reader);
        if layout.columns.windows(2).any(|pair| pair[0] >= pair[1])
            || edit.node(reader)?.inputs().next().map(|p| p.operator) != Some(table)
        {
            return Err(Error::new("invalid input trimming layout"));
        }
        trim_table(&mut edit, table, layout)?;
        trim_reader(&mut edit, reader, &layout.columns)?;
    }
    rebuild_input_closures(edit.finish_with_rewrites()?, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trimming_preserves_both_representations_of_a_nested_row() {
        let integer = json!({"class":"DBSPTypeInteger","id":1,"code":"INT32","mayBeNull":false});
        let named = json!({"class":"DBSPTypeStruct","id":2,"mayBeNull":true,
            "fields":[{"class":"Field","id":3,"type":{"node":1}}]});
        let tuple = json!({"class":"DBSPTypeTuple","id":4,"mayBeNull":true,
            "tupFields":[{"node":1}],"originalStruct":{"node":2}});
        let source = json!({"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],"annotations":[],
            "originalRowType":{"class":"DBSPTypeStruct","id":5,"mayBeNull":false,"fields":[
                {"class":"Field","id":6,"type":integer},{"class":"Field","id":7,"type":named}]},
            "outputType":{"class":"DBSPTypeZSet","id":8,"elementType":{
                "class":"DBSPTypeTuple","id":9,"mayBeNull":false,"tupFields":[{"node":1},tuple]}},
            "metadata":{"columnMetadata":[{"name":"discarded"},{"name":"retained"}]}});
        let circuit = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,
            "declarations":[],"allOperators":[source]}))
        .unwrap();
        let mut edit = CircuitEditor::new(circuit);
        let table = NodeId::Outer(1);
        let layout = InputLayout {
            reader: 2,
            columns: vec![1],
            hash_name: "narrowed".into(),
        };
        trim_table(&mut edit, table, &layout).unwrap();

        let source = edit.node(table).unwrap();
        let record = source.edge("originalRowType").unwrap();
        let fields = edit.node(record).unwrap().edges("fields").unwrap();
        assert_eq!(fields, [NodeId::Inner(7)]);
        let zset = edit.node(source.edge("outputType").unwrap()).unwrap();
        let row = edit.node(zset.edge("elementType").unwrap()).unwrap();
        assert_eq!(row.edges("tupFields").unwrap(), [NodeId::Inner(4)]);
        assert_eq!(row.edge("originalStruct").unwrap(), record);
    }
}
