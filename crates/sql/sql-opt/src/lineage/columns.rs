//! Interpret a closure result using the row layout of its collection.
use super::{AnalysisError, Column, ColumnCopy, Interpreter, Part, Source, text};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, NodeId, TypeKind as Ty};
use serde::Deserialize;
use std::collections::BTreeSet;

/// Map flat result positions to the columns of the declared output collection.
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutputShape {
    pub(crate) width: usize,
    pub(crate) index_fields: Option<usize>,
}

impl OutputShape {
    fn contains(&self, column: Column) -> bool {
        match (self.index_fields, column.part) {
            (None, Part::None) => column.field < self.width,
            (Some(n), Part::Index) => column.field < n,
            (Some(n), Part::Value) => column.field < self.width.saturating_sub(n),
            _ => false,
        }
    }

    pub(crate) fn column(&self, position: usize) -> Column {
        match self.index_fields {
            None => Column {
                part: Part::None,
                field: position,
            },
            Some(n) if position < n => Column {
                part: Part::Index,
                field: position,
            },
            Some(n) => Column {
                part: Part::Value,
                field: position - n,
            },
        }
    }
}

impl Source {
    fn deref_once(&self) -> &Self {
        match self {
            Source::Ref(value) => value,
            value => value,
        }
    }

    fn tuple_fields(&self, width: usize) -> &[Self] {
        match self.deref_once() {
            Source::Tuple(fields) if fields.len() == width => fields,
            _ => &[],
        }
    }
}

/// A raw pair represents an indexed output. Its key and value may be known
/// independently. A plain tuple can also supply just the values of a ranked output.
fn copies(result: &Source, raw_pair: bool, shape: &OutputShape) -> Result<Vec<ColumnCopy>, Error> {
    let mut output = Vec::new();
    let mut append = |fields: &[Source], offset| {
        for (position, value) in fields.iter().enumerate() {
            if let Source::Column(source) = value.deref_once() {
                output.push(ColumnCopy {
                    parameter: source.parameter,
                    source: source.column,
                    output: shape.column(offset + position),
                });
            }
        }
    };
    if raw_pair {
        let index = shape
            .index_fields
            .filter(|&i| i <= shape.width)
            .ok_or_else(|| Error::new("raw tuple result requires an indexed output shape"))?;
        if let [key, value] = result.tuple_fields(2) {
            append(key.tuple_fields(index), 0);
            append(value.tuple_fields(shape.width - index), index);
        }
    } else {
        append(result.tuple_fields(shape.width), 0);
    }
    Ok(output)
}

/// Detached callers can retry the analysis when it requires a live diagnostic.
fn speculative<T>(result: Result<T, AnalysisError<'_>>) -> Result<Option<T>, Error> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(AnalysisError::InvalidIr(error)) => Err(error),
        Err(AnalysisError::Diagnostic(_) | AnalysisError::Reference(_)) => Ok(None),
    }
}

pub fn column_copies(
    edit: &CircuitEditor,
    id: NodeId,
    shape: &OutputShape,
    lossless_casts: bool,
) -> Result<Option<Vec<ColumnCopy>>, Error> {
    speculative(checked_column_copies(edit, id, shape, lossless_casts))
}

pub fn checked_column_copies<'a>(
    edit: &'a CircuitEditor,
    id: NodeId,
    shape: &OutputShape,
    lossless_casts: bool,
) -> Result<Vec<ColumnCopy>, AnalysisError<'a>> {
    let mut interpreter = Interpreter::new(edit, lossless_casts);
    interpreter.track_constants = true;
    let value = interpreter.closure_result(id)?;
    let body = edit.node(id)?.edge("body")?;
    let ty = edit.expression_type(edit.expression(body)?)?;
    Ok(copies(&value, edit.type_kind(ty)? == Ty::RawTuple, shape)?)
}

/// Columns equated with a constant by a top-level conjunct no longer contribute to a key.
pub fn fixed_columns(
    edit: &CircuitEditor,
    closure: NodeId,
    shape: &OutputShape,
    lossless_casts: bool,
) -> Result<Option<BTreeSet<Column>>, Error> {
    speculative(checked_fixed_columns(edit, closure, shape, lossless_casts))
}

pub fn checked_fixed_columns<'a>(
    edit: &'a CircuitEditor,
    closure: NodeId,
    shape: &OutputShape,
    lossless_casts: bool,
) -> Result<BTreeSet<Column>, AnalysisError<'a>> {
    let mut interpreter = Interpreter::new(edit, lossless_casts);
    interpreter.track_constants = true;
    interpreter.closure_result(closure)?;
    let body = edit.expression(edit.node(closure)?.edge("body")?)?;
    let mut fixed = BTreeSet::new();
    for term in crate::filters::conjuncts(edit, body)? {
        let comparison = crate::filters::strip_wrap_bool(edit, term)?;
        let node = edit.node(comparison.into())?;
        if edit.expr_kind(comparison)? != Expr::Binary || text(node, "opcode")? != "EQ" {
            continue;
        }
        let left = interpreter.values.get(&node.edge("left")?);
        let right = interpreter.values.get(&node.edge("right")?);
        let column = match (left, right) {
            (Some(source), Some(Source::Constant(_)))
            | (Some(Source::Constant(_)), Some(source)) => source,
            _ => continue,
        };
        if let Source::Column(source) = column.deref_once() {
            if source.parameter == 0 && shape.contains(source.column) {
                fixed.insert(source.column);
            }
        }
    }
    Ok(fixed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lineage::ParameterColumn;
    use std::rc::Rc;

    #[test]
    fn shared_reads_require_the_same_declaration_at_each_use() -> Result<(), Error> {
        let graph = feldera_sql_ir::Circuit::from_json(serde_json::json!({
            "class": "DBSPCircuit", "id": 0, "allOperators": [], "declarations": []
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.bool_type(site, false)?;
        let input = edit.named_variable(ty, "x")?;
        let shared = edit.named_variable(ty, "x")?;
        let mut repeated = shared;
        for _ in 0..64 {
            repeated = edit.binary_expression(
                site,
                ty,
                feldera_sql_ir::BinaryOp::And,
                repeated,
                repeated,
            )?;
        }
        let local = edit.named_variable(ty, "x")?;
        let other = edit.named_variable(ty, "x")?;
        let shape = OutputShape {
            width: 2,
            index_fields: None,
        };
        // A shadowed read is valid if it has its own node. Reusing the outer
        // read in that position makes the closure's reference map ambiguous.
        for (read, valid) in [(other, true), (shared, false)] {
            let shadowed = edit.let_expression(local, input, read)?;
            let pair = edit.tuple_expr(site, false, &[repeated, shadowed])?;
            let closure = edit.closure_from_variables(pair, &[input])?;
            assert_eq!(
                column_copies(&edit, closure.into(), &shape, false)?.is_some(),
                valid
            );
        }
        Ok(())
    }

    #[test]
    fn deeply_nested_bindings_preserve_the_input_column() -> Result<(), Error> {
        let graph = feldera_sql_ir::Circuit::from_json(serde_json::json!({
            "class": "DBSPCircuit", "id": 0, "allOperators": [], "declarations": []
        }))?;
        let root = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.bool_type(root, false)?;
        let tuple = edit.tuple_type(root, false, [ty.into()])?;
        let row = edit.ref_parameter(tuple, &mut 0)?;
        let read = edit.deref_parameter(&row)?;
        let mut value = edit.tuple_field(read, 0)?;
        for _ in 0..512 {
            let local = edit.named_variable(ty, "x")?;
            value = edit.let_expression(local, value, local)?;
        }
        let result = edit.tuple_expr(root, false, &[value])?;
        let closure = edit.closure(result, &[row])?;
        let shape = OutputShape {
            width: 1,
            index_fields: None,
        };
        assert_eq!(
            column_copies(&edit, closure.into(), &shape, false)?,
            Some(vec![ColumnCopy {
                parameter: 0,
                source: shape.column(0),
                output: shape.column(0)
            }])
        );
        Ok(())
    }

    #[test]
    fn indexed_result_preserves_a_known_half_and_maps_ranked_columns() {
        let source = Column {
            part: Part::None,
            field: 3,
        };
        let field = Source::Column(ParameterColumn {
            parameter: 1,
            column: source,
        });
        let values = Source::Ref(Rc::new(Source::Tuple([field].into())));
        let output = Column {
            part: Part::Value,
            field: 0,
        };
        let expected = ColumnCopy {
            parameter: 1,
            source,
            output,
        };
        let indexed = OutputShape {
            width: 3,
            index_fields: Some(2),
        };
        let pair = Source::Tuple([Source::Unknown, values.clone()].into());
        assert_eq!(copies(&pair, true, &indexed).unwrap(), [expected]);

        let ranked = OutputShape {
            width: 1,
            index_fields: Some(0),
        };
        let outputs = copies(&values, false, &ranked).unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].output, output);
        assert_eq!(outputs[0].source, source);
        assert!(copies(&values, false, &indexed).unwrap().is_empty());
    }

    #[test]
    fn filter_fixes_only_columns_equated_with_a_constant_in_every_row() -> Result<(), Error> {
        use feldera_sql_ir::{BinaryOp, Circuit, Field};
        use serde_json::json;

        let circuit = Circuit::from_json(
            json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]}),
        )?;
        let (circuit, types) = circuit.with_inner_roots([
            json!({"class":"DBSPTypeBool","id":1,"code":"BOOL","mayBeNull":false}),
        ])?;
        let mut edit = CircuitEditor::new(circuit);
        let site = types[0];
        let boolean = edit.type_id(site)?;
        let ty = edit.tuple_type(site, false, [site, site])?;
        let reference = edit.reference_type(ty)?;
        let row = edit.named_variable(reference, "row")?;
        let value = edit.dereference(row)?;
        let x = edit.tuple_field(value, 0)?;
        let y = edit.tuple_field(value, 1)?;
        let literal = |edit: &mut CircuitEditor, b: bool| {
            edit.make_expr(
                site,
                Expr::BoolLiteral,
                boolean,
                [("value", Field::Data(b.into()))],
            )
        };
        let a = literal(&mut edit, true)?;
        let b = literal(&mut edit, true)?;
        let other = literal(&mut edit, false)?;
        let equal = edit.binary_expression(site, boolean, BinaryOp::Coalesce, a, b)?;
        let different = edit.binary_expression(site, boolean, BinaryOp::Coalesce, a, other)?;
        let fixed_x = edit.binary_expression(site, boolean, BinaryOp::Eq, equal, x)?;
        let uncertain_y = edit.binary_expression(site, boolean, BinaryOp::Eq, y, different)?;
        let fixed_y = edit.binary_expression(site, boolean, BinaryOp::Eq, y, a)?;
        let shape = OutputShape {
            width: 2,
            index_fields: None,
        };
        for (op, right, expected) in [
            (BinaryOp::And, uncertain_y, vec![0]),
            (BinaryOp::And, fixed_y, vec![0, 1]),
            (BinaryOp::Or, fixed_y, vec![]),
        ] {
            let test = edit.binary_expression(site, boolean, op, fixed_x, right)?;
            let closure = edit.closure_from_variables(test, &[row])?;
            let fixed = fixed_columns(&edit, closure.into(), &shape, false)?.unwrap();
            assert_eq!(
                fixed,
                expected
                    .into_iter()
                    .map(|field| Column {
                        part: Part::None,
                        field
                    })
                    .collect()
            );
        }
        Ok(())
    }
}
