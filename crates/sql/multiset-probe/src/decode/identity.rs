//! Recognize the exact identity closures constructed by the Java pass.

use super::{array, field, inner_identity, input_type, text, uint, Definitions};
use crate::bypass::{self, Retention};
use crate::model::{Circuit, Edit, OperatorKind, Scalar, Type, TypeKind};
use crate::{Error, Result};
use serde_json::Value;

pub(crate) fn identity_maps(wire: &Value, circuit: &Circuit) -> Result<(Vec<Edit>, u64)> {
    let definitions = Definitions::read(wire)?;
    let original = array(wire, "allOperators")?;
    let mut output_types = Vec::with_capacity(original.len());
    let mut identity = Vec::with_capacity(original.len());
    let mut retention = Vec::with_capacity(original.len());
    let mut names = 0;
    for (node, operator) in original.iter().zip(&circuit.operators) {
        let ty = match node.get("outputType") {
            Some(ty) => Some(inner_identity(ty)?),
            None if matches!(
                operator.kind,
                OperatorKind::View { .. } | OperatorKind::Sink
            ) =>
            {
                Some(input_type(operator.inputs[0], original, &output_types)?)
            }
            None => None,
        };
        let class = text(node, "class")?;
        identity.push(if matches!(operator.kind, OperatorKind::Map(_)) {
            let input = input_type(operator.inputs[0], original, &output_types)?;
            let output = ty.ok_or_else(|| Error("missing identity map output type".into()))?;
            if same_collection_type(&definitions, input, output)? {
                let (matches, allocated_name) =
                    closure_is_identity(&definitions, field(node, "function")?)?;
                names += u64::from(allocated_name);
                matches
            } else {
                false
            }
        } else {
            false
        });
        output_types.push(ty);
        retention.push(match class {
            "DBSPIntegrateTraceRetainKeysOperator" => Some(Retention::Keys),
            "DBSPIntegrateTraceRetainValuesOperator"
            | "DBSPIntegrateTraceRetainNValuesOperator" => Some(Retention::Values),
            _ => None,
        });
    }
    Ok((
        bypass::remove_identity_maps(circuit, &identity, &retention),
        names,
    ))
}

fn same_collection_type(definitions: &Definitions<'_>, left: u64, right: u64) -> Result<bool> {
    let lookup = |id| {
        definitions
            .inner
            .get(&id)
            .copied()
            .ok_or_else(|| Error("undefined collection type".into()))
    };
    let (left, right) = (lookup(left)?, lookup(right)?);
    let class = text(left, "class")?;
    if class != text(right, "class")? || left["mayBeNull"] != right["mayBeNull"] {
        return Ok(false);
    }
    if class == "DBSPTypeIndexedZSet"
        && definitions.ty(field(left, "keyType")?, 0)?
            != definitions.ty(field(right, "keyType")?, 0)?
    {
        return Ok(false);
    }
    if !matches!(class, "DBSPTypeIndexedZSet" | "DBSPTypeZSet") {
        return Err(Error(format!(
            "unsupported identity map collection type {class}"
        )));
    }
    Ok(definitions.ty(field(left, "elementType")?, 0)?
        == definitions.ty(field(right, "elementType")?, 0)?)
}

#[derive(Clone, Copy)]
enum Access {
    Deref,
    Field(usize),
}

fn matches_access<'a>(
    definitions: &Definitions<'a>,
    mut node: &'a Value,
    parameter: &str,
    path: &[Access],
    needs_clone: bool,
) -> Result<bool> {
    node = definitions.resolve(node)?;
    if needs_clone {
        if node["class"] != "DBSPCloneExpression" {
            return Ok(false);
        }
        node = definitions.resolve(field(node, "expression")?)?;
    }
    for access in path.iter().rev() {
        let matches = match access {
            Access::Deref => node["class"] == "DBSPDerefExpression",
            Access::Field(index) => {
                node["class"] == "DBSPFieldExpression" && uint(node, "fieldNo")? == *index as u64
            }
        };
        if !matches {
            return Ok(false);
        }
        node = definitions.resolve(field(node, "expression")?)?;
    }
    Ok(node["class"] == "DBSPVariablePath" && node["variable"] == parameter)
}

fn needs_clone(ty: &Type) -> bool {
    matches!(
        ty.kind,
        TypeKind::Tuple(_)
            | TypeKind::RawTuple(_)
            | TypeKind::String { .. }
            | TypeKind::Result(_)
            | TypeKind::Scalar(Scalar::Uuid | Scalar::Variant)
    )
}

fn tuple_is_identity(
    definitions: &Definitions<'_>,
    node: &Value,
    parameter: &str,
    row: &Type,
    prefix: &[Access],
) -> Result<bool> {
    let TypeKind::Tuple(fields) = &row.kind else {
        return Ok(false);
    };
    if node["class"] != "DBSPTupleExpression"
        || node.get("fields").is_none_or(Value::is_null)
        || definitions.ty(field(node, "type")?, 0)? != *row
        || (row.nullable && fields.iter().any(|field| !field.nullable))
    {
        return Ok(false);
    }
    let elements = array(node, "fields")?;
    if elements.len() != fields.len() {
        return Ok(false);
    }
    let mut path = prefix.to_vec();
    for (index, (element, ty)) in elements.iter().zip(fields).enumerate() {
        path.push(Access::Field(index));
        if !matches_access(definitions, element, parameter, &path, needs_clone(ty))? {
            return Ok(false);
        }
        path.pop();
    }
    Ok(true)
}

fn closure_is_identity(definitions: &Definitions<'_>, closure: &Value) -> Result<(bool, bool)> {
    let closure = definitions.resolve(closure)?;
    let [parameter] = array(closure, "parameters")? else {
        return Ok((false, false));
    };
    let parameter = definitions.resolve(parameter)?;
    let name = text(parameter, "name")?;
    let ty = definitions.ty(field(parameter, "type")?, 0)?;
    let body = definitions.resolve(field(closure, "body")?)?;
    let result = definitions.ty(field(body, "type")?, 0)?;
    match ty.kind {
        TypeKind::Ref(row) if *row == result => {
            if matches!(row.kind, TypeKind::Tuple(_)) {
                Ok((
                    tuple_is_identity(definitions, body, name, &row, &[Access::Deref])?,
                    true,
                ))
            } else {
                Ok((
                    !matches!(row.kind, TypeKind::RawTuple(_))
                        && matches_access(
                            definitions,
                            body,
                            name,
                            &[Access::Deref],
                            needs_clone(&row),
                        )?,
                    true,
                ))
            }
        }
        TypeKind::RawTuple(parts) => {
            let TypeKind::RawTuple(result) = result.kind else {
                return Ok((false, false));
            };
            let ([key_type, value_type], [result_key, result_value]) =
                (parts.as_slice(), result.as_slice())
            else {
                return Ok((false, false));
            };
            let (TypeKind::Ref(key), TypeKind::Ref(value)) = (&key_type.kind, &value_type.kind)
            else {
                return Ok((false, false));
            };
            if **key != *result_key || **value != *result_value {
                return Ok((false, false));
            }
            if body["class"] != "DBSPRawTupleExpression" || !body["fields"].is_array() {
                return Ok((false, true));
            }
            let [key_expression, value_expression] = array(body, "fields")? else {
                return Ok((false, true));
            };
            for (index, (expression, row)) in [(key_expression, key), (value_expression, value)]
                .into_iter()
                .enumerate()
            {
                if row.nullable
                    || !tuple_is_identity(
                        definitions,
                        definitions.resolve(expression)?,
                        name,
                        row,
                        &[Access::Field(index), Access::Deref],
                    )?
                {
                    return Ok((false, true));
                }
            }
            Ok((true, true))
        }
        _ => Ok((false, false)),
    }
}
