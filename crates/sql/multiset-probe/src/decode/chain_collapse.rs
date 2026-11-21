//! Compose two bounded projections over an indexed input.

use super::{array, encode_rewrite, field, inner_reference, text, uint, Definitions};
use crate::{Error, Result};
use serde_json::{json, Value};

pub(crate) fn collapse(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let mut next = definitions.inner.keys().copied().max().unwrap_or_default();
    let mut fresh = || {
        next = next
            .checked_add(1)
            .ok_or_else(|| Error("chain identities exhausted".into()))?;
        Ok(next)
    };
    let mut rewritten = circuit.clone();
    collapse_operators(circuit, &mut rewritten, &definitions, &mut fresh)?;
    encode_rewrite(circuit, &rewritten)
}

fn collapse_operators(
    original: &Value,
    rewritten: &mut Value,
    definitions: &Definitions<'_>,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<()> {
    let operators = rewritten["allOperators"]
        .as_array_mut()
        .ok_or_else(|| Error("missing operator array".into()))?;
    for (before, after) in array(original, "allOperators")?.iter().zip(operators) {
        match text(before, "class")? {
            "DBSPChainOperator" => {
                after["function"] = compose_chain(before, definitions, fresh)?;
                after["class"] = "DBSPMapOperator".into();
                let fields = after.as_object_mut().expect("validated operator");
                for name in ["inputType", "chain", "kinds"] {
                    fields.remove(name);
                }
            }
            "DBSPNestedOperator" => collapse_operators(before, after, definitions, fresh)?,
            _ => {}
        }
    }
    Ok(())
}

fn compose_chain(
    chain: &Value,
    definitions: &Definitions<'_>,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Value> {
    let [extract, project] = array(chain, "chain")? else {
        return Err(outside_scope());
    };
    if chain["kinds"] != json!(["Map", "Map"]) {
        return Err(outside_scope());
    }
    let extract = definitions.resolve(extract)?;
    let project = definitions.resolve(project)?;
    let parameter = only_parameter(definitions, extract)?;
    let output_parameter = only_parameter(definitions, project)?;
    let source = projection_source(
        definitions,
        field(extract, "body")?,
        text(parameter, "name")?,
    )?;

    let tuple = expect_class(definitions, field(project, "body")?, "DBSPTupleExpression")?;
    let mut body = tuple.clone();
    let mut fields = Vec::new();
    for value in array(tuple, "fields")? {
        let value = definitions.resolve(value)?;
        let expression = if value["class"] == "DBSPCastExpression" {
            field(value, "source")?
        } else {
            value
        };
        let access = expect_class(definitions, expression, "DBSPFieldExpression")?;
        let deref = expect_class(
            definitions,
            field(access, "expression")?,
            "DBSPDerefExpression",
        )?;
        let variable = expect_class(definitions, field(deref, "expression")?, "DBSPVariablePath")?;
        if variable["variable"] != output_parameter["name"] {
            return Err(outside_scope());
        }
        let index = usize::try_from(uint(access, "fieldNo")?).map_err(|_| outside_scope())?;
        let projected = source.access(definitions, access, index, fresh)?;
        fields.push(if value["class"] == "DBSPCastExpression" {
            let mut cast = value.clone();
            cast["id"] = fresh()?.into();
            cast["source"] = projected;
            cast
        } else {
            projected
        });
    }
    body["fields"] = fields.into();
    body["id"] = fresh()?.into();
    let function_type = json!({
        "class": "DBSPTypeFunction",
        "id": fresh()?,
        "sourceEmpty": true,
        "resultType": inner_reference(field(tuple, "type")?)?,
        "parameterTypes": [inner_reference(field(parameter, "type")?)?],
        "code": "FUNCTION",
        "mayBeNull": false,
    });
    Ok(json!({
        "class": "DBSPClosureExpression",
        "id": fresh()?,
        "sourceEmpty": true,
        "type": function_type,
        "parameters": [inner_reference(parameter)?],
        "body": body,
    }))
}

enum ProjectionSource<'a> {
    Value { row: &'a Value, fields: &'a [Value] },
    Fields(Vec<&'a Value>),
}

impl ProjectionSource<'_> {
    fn access(
        &self,
        definitions: &Definitions<'_>,
        access: &Value,
        index: usize,
        fresh: &mut impl FnMut() -> Result<u64>,
    ) -> Result<Value> {
        let (ty, existing) = match self {
            Self::Value { fields, .. } => (fields.get(index).ok_or_else(outside_scope)?, None),
            Self::Fields(fields) => {
                let field_expr = *fields.get(index).ok_or_else(outside_scope)?;
                (field(field_expr, "type")?, Some(field_expr))
            }
        };
        let ty = expect_class(definitions, ty, "DBSPTypeInteger")?;
        if definitions.ty(field(access, "type")?, 0)? != definitions.ty(ty, 0)? {
            return Err(outside_scope());
        }
        if let Some(existing) = existing {
            return inner_reference(existing);
        }
        let Self::Value { row, .. } = self else {
            unreachable!("field projections returned their existing expression");
        };
        let mut result = access.clone();
        result["id"] = fresh()?.into();
        result["type"] = inner_reference(ty)?;
        result["expression"] = inner_reference(row)?;
        Ok(result)
    }
}

fn projection_source<'a>(
    definitions: &Definitions<'a>,
    body: &'a Value,
    parameter: &str,
) -> Result<ProjectionSource<'a>> {
    let body = definitions.resolve(body)?;
    match text(body, "class")? {
        "DBSPCloneExpression" => {
            let (row, index) = indexed_field(definitions, field(body, "expression")?, parameter)?;
            if index != 1 {
                return Err(outside_scope());
            }
            let row_type = expect_class(definitions, field(row, "type")?, "DBSPTypeTuple")?;
            Ok(ProjectionSource::Value {
                row,
                fields: array(row_type, "tupFields")?,
            })
        }
        "DBSPTupleExpression" => {
            let mut fields = Vec::new();
            for entry in array(body, "fields")? {
                let entry = expect_class(definitions, entry, "DBSPFieldExpression")?;
                indexed_field(definitions, field(entry, "expression")?, parameter)?;
                expect_class(definitions, field(entry, "type")?, "DBSPTypeInteger")?;
                fields.push(entry);
            }
            Ok(ProjectionSource::Fields(fields))
        }
        _ => Err(outside_scope()),
    }
}

fn indexed_field<'a>(
    definitions: &Definitions<'a>,
    deref: &'a Value,
    parameter: &str,
) -> Result<(&'a Value, u64)> {
    let row = expect_class(definitions, deref, "DBSPDerefExpression")?;
    let entry = expect_class(
        definitions,
        field(row, "expression")?,
        "DBSPFieldExpression",
    )?;
    let pair = expect_class(definitions, field(entry, "expression")?, "DBSPVariablePath")?;
    expect_class(definitions, field(pair, "type")?, "DBSPTypeRawTuple")?;
    let row_type = expect_class(definitions, field(row, "type")?, "DBSPTypeTuple")?;
    let index = uint(entry, "fieldNo")?;
    if index > 1 || pair["variable"] != parameter || row_type["mayBeNull"] != false {
        return Err(outside_scope());
    }
    Ok((row, index))
}

fn only_parameter<'a>(definitions: &Definitions<'a>, closure: &'a Value) -> Result<&'a Value> {
    let closure = expect_class(definitions, closure, "DBSPClosureExpression")?;
    let [parameter] = array(closure, "parameters")? else {
        return Err(outside_scope());
    };
    expect_class(definitions, parameter, "DBSPParameter")
}

fn expect_class<'a>(
    definitions: &Definitions<'a>,
    value: &'a Value,
    class: &str,
) -> Result<&'a Value> {
    let value = definitions.resolve(value)?;
    if value["class"] == class {
        Ok(value)
    } else {
        Err(outside_scope())
    }
}

fn outside_scope() -> Error {
    Error("chain collapse requires two supported indexed integer projections".into())
}
