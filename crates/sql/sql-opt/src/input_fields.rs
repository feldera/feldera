//! Identify the source rows that a single consumer can trim and the fields it reads.
use crate::row_fields::RowFields;
use crate::stream_rewrites::{body_members, reader_counts};
use feldera_sql_expr::{live_field_dependencies, live_parameter_field_paths};
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind as Op};
use std::collections::{BTreeMap, BTreeSet};

pub type ParameterPaths = Vec<BTreeSet<Vec<usize>>>;

pub fn input_projections(circuit: &Circuit) -> Result<Vec<NodeId>, Error> {
    let readers = reader_counts(circuit, &body_members(circuit)?);
    Ok(circuit
        .operator_order(BodyOrder::Postorder)?
        .into_iter()
        .filter(|&id| {
            let reader = circuit.node(id).expect("validated operator");
            matches!(reader.operator_kind(), Some(Op::Map | Op::MapIndex))
                && reader.inputs().next().is_some_and(|input| {
                    readers.get(&input.operator) == Some(&1)
                        && circuit.node(input.operator).and_then(|n| n.operator_kind())
                            == Some(Op::SourceMultiset)
                })
        })
        .collect())
}

#[derive(serde::Serialize)]
pub struct InputFieldUse {
    pub reader: u64,
    pub paths: Option<BTreeSet<Vec<usize>>>,
}

/// The live compiler applies the mask to its source metadata. An undecided
/// reader is inspected at its turn, after earlier warnings have been emitted.
#[derive(serde::Serialize)]
#[serde(tag = "kind", content = "used")]
pub enum SourceColumns {
    Keep,
    Trim(Vec<bool>),
    Inspect,
}

#[derive(serde::Serialize)]
pub struct SourceColumnDecision {
    reader: u64,
    #[serde(flatten)]
    columns: SourceColumns,
}

impl InputFieldUse {
    /// A read of a nested field keeps the entire source column. Empty tuples
    /// count as used even in a reader that does not reference its parameter.
    pub fn column_mask(&self, circuit: &Circuit) -> Result<SourceColumns, Error> {
        let Some(paths) = &self.paths else {
            return Ok(SourceColumns::Inspect);
        };
        let reader = circuit
            .node(NodeId::Outer(self.reader))
            .ok_or_else(|| Error::new("missing source reader"))?;
        let function = circuit
            .node(reader.edge("function")?)
            .expect("validated reader function");
        let [parameter] = function
            .edges("parameters")?
            .try_into()
            .map_err(|_| Error::new("expected one source reader parameter"))?;
        let ty = circuit
            .node(parameter)
            .expect("validated parameter")
            .edge("type")?;
        source_columns(circuit, ty, paths)
    }
}

pub fn source_columns(
    circuit: &Circuit,
    ty: NodeId,
    paths: &BTreeSet<Vec<usize>>,
) -> Result<SourceColumns, Error> {
    Ok(match RowFields::new(circuit).projection_mask(ty, paths)? {
        None => SourceColumns::Keep,
        Some(used) => SourceColumns::Trim(used),
    })
}

pub fn source_column_decisions(circuit: &Circuit) -> Result<Vec<SourceColumnDecision>, Error> {
    input_field_uses(circuit)?
        .into_iter()
        .map(|usage| {
            Ok(SourceColumnDecision {
                reader: usage.reader,
                columns: usage.column_mask(circuit)?,
            })
        })
        .collect()
}

/// A discarded statement can fail even when the result uses no input fields.
/// Keep an unanswered reader distinct from a reader that returns a constant.
fn unary_paths(paths: Option<ParameterPaths>) -> Result<Option<BTreeSet<Vec<usize>>>, Error> {
    let Some(paths) = paths else {
        return Ok(None);
    };
    let [paths] = paths
        .try_into()
        .map_err(|_| Error::new("expected a unary map"))?;
    Ok(Some(paths))
}

pub fn input_field_uses(circuit: &Circuit) -> Result<Vec<InputFieldUse>, Error> {
    input_projections(circuit)?
        .into_iter()
        .map(|reader| {
            let function = circuit
                .node(reader)
                .expect("validated projection")
                .edge("function")?;
            let NodeId::Outer(reader) = reader else {
                unreachable!("operator ID")
            };
            Ok(InputFieldUse {
                reader,
                paths: unary_paths(live_field_dependencies(circuit, function)?)?,
            })
        })
        .collect()
}

#[derive(serde::Serialize)]
#[serde(tag = "kind", content = "paths")]
pub enum MapInputLayout {
    Keep,
    Project(BTreeSet<Vec<usize>>),
    /// Resume at this reader's turn so earlier diagnostics and edits survive.
    Inspect,
}

pub fn map_field_dependencies(circuit: &Circuit) -> Result<BTreeMap<u64, MapInputLayout>, Error> {
    let mut fields = RowFields::new(circuit);
    circuit
        .root()
        .edges("allOperators")?
        .into_iter()
        .map(|id| {
            let function = circuit
                .node(id)
                .expect("validated map reader")
                .edge("function")?;
            let NodeId::Inner(number) = function else {
                return Err(Error::new("expected an inner map closure"));
            };
            let Some(paths) = unary_paths(live_parameter_field_paths(circuit, function)?)? else {
                return Ok((number, MapInputLayout::Inspect));
            };
            let unused = fields.map_has_unused_fields(function, &paths)?;
            Ok((
                number,
                if unused {
                    MapInputLayout::Project(paths)
                } else {
                    MapInputLayout::Keep
                },
            ))
        })
        .collect()
}
