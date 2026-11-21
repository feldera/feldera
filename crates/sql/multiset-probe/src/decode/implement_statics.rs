//! Hoist bounded classic literals and derive their names from their Rust text.

use super::{array, encode_rewrite, field, inner_reference, max_outer_id, text, Definitions};
use crate::{Error, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

pub(crate) fn rewrite(circuit: &Value) -> Result<Value> {
    let defs = Definitions::read(circuit)?;
    let mut statics = Statics {
        next_inner: defs.inner.keys().copied().max().unwrap_or_default(),
        next_outer: max_outer_id(circuit)?,
        defs,
        declared: HashSet::new(),
        declarations: Vec::new(),
    };
    let mut result = circuit.clone();
    let output = result["allOperators"]
        .as_array_mut()
        .ok_or_else(outside_scope)?;
    for (before, after) in array(circuit, "allOperators")?.iter().zip(output) {
        let root = match text(before, "class")? {
            "DBSPMapIndexOperator" => "function",
            "DBSPWaterlineOperator" => "init",
            "DBSPControlledKeyFilterOperator" => "error",
            "DBSPNestedOperator" => return Err(outside_scope()),
            _ => continue,
        };
        if let Some(closure) = statics.rewrite_closure(field(before, root)?, root)? {
            after[root] = closure;
        }
    }
    result["declarations"]
        .as_array_mut()
        .ok_or_else(outside_scope)?
        .extend(statics.declarations);
    encode_rewrite(circuit, &result)
}

struct Statics<'a> {
    defs: Definitions<'a>,
    next_inner: u64,
    next_outer: u64,
    declared: HashSet<String>,
    declarations: Vec<Value>,
}

impl<'a> Statics<'a> {
    fn rewrite_closure(&mut self, function: &'a Value, root: &str) -> Result<Option<Value>> {
        let function = self.defs.resolve(function)?;
        let body = self.defs.resolve(field(function, "body")?)?;
        let body = match root {
            "init" => self.reference(body)?,
            "function" if body["class"] == "DBSPRawTupleExpression" => {
                let [key, _] = array(body, "fields")? else {
                    return Err(outside_scope());
                };
                if let Some(key) = self.reference(self.defs.resolve(key)?)? {
                    let mut body = self.duplicate(body)?;
                    body["fields"][0] = key;
                    Some(body)
                } else {
                    None
                }
            }
            "error" if body["class"] == "DBSPTupleExpression" => {
                let mut updated = None;
                for (index, field) in array(body, "fields")?.iter().enumerate() {
                    let field = self.defs.resolve(field)?;
                    if field["class"] == "DBSPStringLiteral" {
                        let field = self.reference(field)?.ok_or_else(outside_scope)?;
                        if updated.is_none() {
                            updated = Some(self.duplicate(body)?);
                        }
                        updated.as_mut().ok_or_else(outside_scope)?["fields"][index] = field;
                    }
                }
                updated
            }
            _ => None,
        };
        let Some(body) = body else {
            return Ok(None);
        };
        let mut closure = self.duplicate(function)?;
        closure["type"] = self.duplicate(self.defs.resolve(field(function, "type")?)?)?;
        closure["body"] = body;
        Ok(Some(closure))
    }

    fn reference(&mut self, expression: &'a Value) -> Result<Option<Value>> {
        let Some(rust) = self.print_initializer(expression)? else {
            return Ok(None);
        };
        let digest = Sha256::digest(rust.as_bytes());
        let name = format!(
            "STATIC_{:016x}",
            u64::from_be_bytes(digest[..8].try_into().unwrap())
        );
        let ty = inner_reference(field(expression, "type")?)?;
        if self.declared.insert(name.clone()) {
            let static_expression = json!({
                "class": "DBSPStaticExpression", "id": self.fresh()?, "sourceEmpty": true,
                "initializer": inner_reference(expression)?, "type": ty, "name": name,
            });
            let declaration = json!({
                "class": "DBSPDeclaration", "id": next_id(&mut self.next_outer)?,
                "item": {"class": "DBSPStaticItem", "id": self.fresh()?, "sourceEmpty": true,
                    "expression": static_expression},
            });
            self.declarations.push(declaration);
        }
        Ok(Some(json!({
            "class": "DBSPCloneExpression", "id": self.fresh()?, "sourceEmpty": true, "type": ty,
            "expression": {
                "class": "DBSPPathExpression", "id": self.fresh()?, "sourceEmpty": true, "type": ty,
                "path": {
                    "class": "DBSPPath", "id": self.fresh()?, "sourceEmpty": true,
                    "components": [{
                        "class": "DBSPSimplePathSegment", "id": self.fresh()?, "sourceEmpty": true,
                        "genericArgs": [], "identifier": name,
                    }],
                },
            },
        })))
    }

    fn print_initializer(&self, expression: &'a Value) -> Result<Option<String>> {
        let ty = self.defs.resolve(field(expression, "type")?)?;
        if expression["sourceEmpty"] != true || ty["mayBeNull"] != false {
            return Ok(None);
        }
        match text(expression, "class")? {
            "DBSPStringLiteral" if ty["class"] == "DBSPTypeString" => {
                let value = text(expression, "value")?;
                if expression["charset"] != "UTF-8"
                    || !value.bytes().all(|b| (32..127).contains(&b))
                {
                    return Err(outside_scope());
                }
                Ok(Some(format!(
                    "SqlString::from(arcstr::literal!({value:?})):SqlString"
                )))
            }
            "DBSPTupleExpression" if ty["class"] == "DBSPTypeTuple" => {
                let expected = match array(expression, "fields")? {
                    [] => Some("Tup0::new():Tup0"),
                    [element] => {
                        let element = self.defs.resolve(element)?;
                        // The retention lower bound uses SQL's earliest sentinel.
                        if element["class"] == "DBSPTimestampLiteral"
                            && element["value"] == "-62135596800000000"
                        {
                            Some(concat!(
                                "Tup1::new(Timestamp::from_microseconds(",
                                "-62135596800000000/*0001-01-01 00:00:00*/)):Tup1<Timestamp>"
                            ))
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                Ok(expected.map(str::to_owned))
            }
            _ => Ok(None),
        }
    }

    fn duplicate(&mut self, original: &Value) -> Result<Value> {
        let mut result = original.clone();
        result["id"] = self.fresh()?.into();
        Ok(result)
    }

    fn fresh(&mut self) -> Result<u64> {
        next_id(&mut self.next_inner)
    }
}

fn next_id(next: &mut u64) -> Result<u64> {
    *next = next.checked_add(1).ok_or_else(outside_scope)?;
    Ok(*next)
}

fn outside_scope() -> Error {
    Error("static value is outside the measured classic slice".into())
}
