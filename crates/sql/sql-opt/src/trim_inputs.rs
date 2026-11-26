//! Trim unused columns from a non-materialized input and rebind its sole reader.
//! The preceding field-use analysis supplies the ordered columns. The struct's
//! generated name comes from the compiler that owns the SQL type printer.
use crate::{
    inspect_references, rebuild::rebuild_input_closures, BindingDiagnostic, FieldRewriteError,
};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, Field, InnerKind, NodeId, OperatorKind as Op,
    TypeKind,
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

fn trim_reader(edit: &mut CircuitEditor, reader: NodeId, columns: &[usize]) -> Result<(), Error> {
    let node = edit.node(reader)?;
    let kind = node.operator_kind();
    let original = node.edge("function")?;
    let closure = edit.node(original)?;
    let [parameter] = closure
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("expected unary input projection"))?;
    let reference = edit.node(parameter)?.edge("type")?;
    let row = edit.node(reference)?.edge("type")?;
    let fields = select(&edit.node(row)?.edges("tupFields")?, columns)?;
    let compressed = edit.tuple_type(row, false, fields)?;
    let nullable = edit.nullable(edit.type_id(row)?)?;
    let compressed = edit.with_nullability(compressed, nullable)?;
    let reference = edit.reference_type(compressed)?;
    let declaration =
        edit.derive_inner(parameter, [("type", Field::Reference(reference.into()))])?;
    let function =
        crate::projected_reader::rebind_source_reader(edit, original, declaration, columns)?;
    let signature = edit.expression_type(function)?;
    let ty = edit.type_id(edit.node(signature.into())?.edge("resultType")?)?;
    let output = match kind {
        Some(Op::Map) => edit.zset_type(ty)?,
        Some(Op::MapIndex) => edit.indexed_zset_from_pair(ty)?,
        _ => return Err(Error::new("invalid input projection reader")),
    };
    edit.set_operator_field(reader, "function", Field::Reference(function.into()))?;
    edit.set_operator_field(reader, "outputType", Field::Reference(output.into()))?;
    edit.clear_annotations(reader)
}

pub fn trim_inputs(
    circuit: Circuit,
    layouts: &BTreeMap<u64, InputLayout>,
    next: &mut u64,
) -> Result<Circuit, Error> {
    checked_trim_inputs(&circuit, layouts, next).map_err(|error| match error {
        FieldRewriteError::InvalidIr(error) => error,
        FieldRewriteError::Binding(BindingDiagnostic::Free { name, .. }) => {
            Error::new(format!("unbound variable {name} in source reader"))
        }
        FieldRewriteError::Binding(BindingDiagnostic::Conflict { variable, .. }) => Error::new(
            format!("shared variable {variable:?} refers to two declarations"),
        ),
    })
}

/// Inspect the original readers in circuit order. Field discovery can accept
/// a copied reader whose original variable refers to two different declarations.
pub fn checked_trim_inputs<'a>(
    circuit: &'a Circuit,
    layouts: &BTreeMap<u64, InputLayout>,
    next: &mut u64,
) -> Result<Circuit, FieldRewriteError<'a>> {
    if layouts.is_empty() {
        return Ok(circuit.clone());
    }
    let mut edit = CircuitEditor::new(circuit.clone());
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut trimmed = 0;
    for (position, &reader) in order.iter().enumerate() {
        let node = edit.node(reader)?;
        let Some(NodeId::Outer(source)) = node.inputs().next().map(|port| port.operator) else {
            continue;
        };
        let Some(layout) = layouts
            .get(&source)
            .filter(|l| NodeId::Outer(l.reader) == reader)
        else {
            continue;
        };
        if layout.columns.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::new("invalid input trimming layout").into());
        }
        let function = node.edge("function")?;
        if let Some(issue) = inspect_references(circuit, function, false)?.diagnostic {
            // Earlier Noop and Deindex rebuilds consumed names before this diagnostic.
            let earlier: HashSet<_> = order[..position].iter().copied().collect();
            let mut prefix = edit.finish_with_rewrites()?;
            prefix.rebuilt_inputs.retain(|id| earlier.contains(id));
            rebuild_input_closures(prefix, next)?;
            return Err(FieldRewriteError::Binding(issue));
        }
        trim_table(&mut edit, NodeId::Outer(source), layout)?;
        trim_reader(&mut edit, reader, &layout.columns)?;
        trimmed += 1;
    }
    if trimmed != layouts.len() {
        return Err(Error::new("invalid input trimming layout").into());
    }
    Ok(rebuild_input_closures(edit.finish_with_rewrites()?, next)?)
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
