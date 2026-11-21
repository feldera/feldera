//! Simplify the measured declaration and retention-predicate fragments.

use super::{array, boolean, encode_rewrite, field, inner_reference, text, uint, Definitions};
use crate::{Error, Result};
use serde_json::Value;

pub(crate) fn rewrite(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let mut next = definitions.inner.keys().copied().max().unwrap_or_default();
    let mut fresh = || {
        next = next
            .checked_add(1)
            .ok_or_else(|| Error("simplifier expression identities exhausted".into()))?;
        Ok(next)
    };
    let mut result = circuit.clone();
    for (original, updated) in array(circuit, "declarations")?.iter().zip(
        result["declarations"]
            .as_array_mut()
            .ok_or_else(outside_scope)?,
    ) {
        if let Some(item) = simplify_declaration(original, &definitions, &mut fresh)? {
            updated["item"] = item;
        }
    }
    for (original, updated) in array(circuit, "allOperators")?.iter().zip(
        result["allOperators"]
            .as_array_mut()
            .ok_or_else(outside_scope)?,
    ) {
        match text(original, "class")? {
            "DBSPControlledKeyFilterOperator" | "DBSPIntegrateTraceRetainKeysOperator" => {
                if let Some(function) = simplify_retention(original, &definitions, &mut fresh)? {
                    updated["function"] = function;
                }
            }
            "DBSPNestedOperator" => return Err(outside_scope()),
            _ => {}
        }
    }
    encode_rewrite(circuit, &result)
}

fn simplify_declaration<'a>(
    declaration: &'a Value,
    defs: &Definitions<'a>,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Option<Value>> {
    let item = defs.resolve(field(declaration, "item")?)?;
    if item["class"] != "DBSPFunctionItem" {
        return Ok(None);
    }
    let function = defs.resolve(field(item, "function")?)?;
    let Some(body) = function.get("body").filter(|body| !body.is_null()) else {
        return Ok(None);
    };
    let cast = defs.resolve(body)?;
    if cast["class"] != "DBSPCastExpression" {
        return Ok(None);
    }
    let literal = defs.resolve(field(cast, "source")?)?;
    let from = defs.resolve(field(literal, "type")?)?;
    let to = defs.resolve(field(cast, "type")?)?;
    if literal["class"] != "DBSPI32Literal"
        || from["class"] != "DBSPTypeInteger"
        || to["class"] != "DBSPTypeInteger"
        || from["code"] != "INT32"
        || from["code"] != to["code"]
        || boolean(from, "mayBeNull")?
        || !boolean(to, "mayBeNull")?
        || cast["safe"] != "SqlUnsafe"
    {
        return Err(outside_scope());
    }
    let mut nullable = reidentify(from, fresh)?;
    nullable["mayBeNull"] = true.into();
    let mut literal = reidentify(literal, fresh)?;
    literal["type"] = nullable;
    let mut function = reidentify(function, fresh)?;
    function["body"] = literal;
    let mut item = reidentify(item, fresh)?;
    item["function"] = function;
    Ok(Some(item))
}

fn simplify_retention<'a>(
    operator: &'a Value,
    defs: &Definitions<'a>,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Option<Value>> {
    let function = defs.resolve(field(operator, "function")?)?;
    let body = defs.resolve(field(function, "body")?)?;
    if body["class"] != "DBSPBlockExpression" {
        return Ok(None);
    }
    let last = defs.resolve(field(body, "lastExpression")?)?;
    let mut updated = body.clone();
    let mut changed = false;
    for (index, statement) in array(body, "contents")?.iter().enumerate() {
        let statement = defs.resolve(statement)?;
        let Some(initializer) = statement
            .get("initializer")
            .filter(|value| !value.is_null())
        else {
            continue;
        };
        let borrow = defs.resolve(initializer)?;
        if borrow["class"] != "DBSPBorrowExpression" {
            continue;
        }
        let deref = defs.resolve(field(borrow, "expression")?)?;
        if deref["class"] != "DBSPDerefExpression" {
            continue;
        }
        let reference = defs.resolve(field(deref, "expression")?)?;
        if boolean(borrow, "mut")?
            || defs.ty(field(borrow, "type")?, 0)? != defs.ty(field(reference, "type")?, 0)?
        {
            return Err(outside_scope());
        }
        let mut statement = reidentify(statement, fresh)?;
        statement["initializer"] = inner_reference(reference)?;
        statement["type"] = inner_reference(field(reference, "type")?)?;
        updated["contents"][index] = statement;
        changed = true;
    }
    if last["class"] == "DBSPBinaryExpression" && last["opcode"] == "AND" {
        let left = defs.resolve(field(last, "left")?)?;
        let right = defs.resolve(field(last, "right")?)?;
        if left["class"] != "DBSPBoolLiteral"
            || left["value"] != true
            || defs.ty(field(last, "type")?, 0)? != defs.ty(field(right, "type")?, 0)?
        {
            return Err(outside_scope());
        }
        updated["lastExpression"] = inner_reference(right)?;
        updated["type"] = inner_reference(field(right, "type")?)?;
        changed = true;
    }
    if !changed {
        return Ok(None);
    }
    updated["id"] = fresh()?.into();
    let mut function_type = reidentify(defs.resolve(field(function, "type")?)?, fresh)?;
    function_type["resultType"] = inner_reference(field(&updated, "type")?)?;
    let mut function = reidentify(function, fresh)?;
    function["type"] = function_type;
    function["body"] = updated;
    Ok(Some(function))
}

fn reidentify(node: &Value, fresh: &mut impl FnMut() -> Result<u64>) -> Result<Value> {
    uint(node, "id")?;
    let mut rewritten = node.clone();
    rewritten["id"] = fresh()?.into();
    Ok(rewritten)
}

fn outside_scope() -> Error {
    Error("simplification outside the measured declaration and retention slice".into())
}
