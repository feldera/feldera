// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: MIT

//! Wire constructors for one-parameter optimizer closures.

pub(super) mod clock;
pub(super) mod lateness;

use super::{array, boolean, field, inner_reference, text, Definitions};
use crate::{Error, Result};
use serde_json::{json, Value};

pub(crate) const GENERATED_NAME: &str = "next_generated_name";

pub(crate) fn advance_name(next: u64, count: u64) -> Result<u64> {
    next.checked_add(count)
        .filter(|end| *end < i64::MAX as u64)
        .ok_or_else(|| Error("generated variable name counter exhausted".into()))
}

pub(super) fn function(
    definitions: &Definitions<'_>,
    collection: &Value,
    name: u64,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Value> {
    let element = field(collection, "elementType")?;
    let mut closure = ClosureWriter {
        name: format!("t_{name}"),
        fresh,
        source: None,
    };
    let element_type = inner_reference(element)?;
    let element_ref = closure.reference(definitions.resolve(element)?)?;
    let (parameter, result, body) = match text(collection, "class")? {
        "DBSPTypeZSet" => {
            let value = closure.read_parameter(&element_ref)?;
            let body = closure.clone_deref(value, &element_type)?;
            (element_ref, element_type, body)
        }
        "DBSPTypeIndexedZSet" => {
            let key = field(collection, "keyType")?;
            let key_type = inner_reference(key)?;
            let key_ref = closure.reference(definitions.resolve(key)?)?;
            let key_ref_id = inner_reference(&key_ref)?;
            let element_ref_id = inner_reference(&element_ref)?;
            let parameter = closure.raw_tuple_type(json!([key_ref, element_ref]))?;
            let value = closure.read_parameter(&parameter)?;
            let value_ref = inner_reference(&value)?;
            let key = closure.field(value, key_ref_id, 0)?;
            let element = closure.field(value_ref, element_ref_id, 1)?;
            let key = closure.clone_deref(key, &key_type)?;
            let element = closure.clone_deref(element, &element_type)?;
            let result = closure.raw_tuple_type(json!([key_type, element_type]))?;
            let body = closure.node(
                "DBSPRawTupleExpression",
                json!({
                    "type": inner_reference(&result)?,
                    "fields": [key, element]
                }),
            )?;
            (parameter, result, body)
        }
        class => return Err(Error(format!("unsupported noop collection type {class}"))),
    };
    closure.closure(parameter, result, body)
}

pub(super) fn constant_function(
    row: &Value,
    body: Value,
    name: u64,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Value> {
    let mut closure = ClosureWriter {
        name: format!("t_{name}"),
        fresh,
        source: None,
    };
    let parameter = closure.reference(row)?;
    closure.closure(parameter, inner_reference(row)?, body)
}

pub(super) fn unmatched_left(
    definitions: &Definitions<'_>,
    left_collection: &Value,
    join_function: &Value,
    name: u64,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Value> {
    let join = definitions.resolve(join_function)?;
    let [_, _, right] = array(join, "parameters")? else {
        return Err(Error("expected a three-parameter left join closure".into()));
    };
    let right = definitions.resolve(right)?;
    let right_ref = definitions.resolve(field(right, "type")?)?;
    let right_row = definitions.resolve(field(right_ref, "type")?)?;
    let return_type = definitions.resolve(field(join, "type")?)?;
    let result = inner_reference(field(return_type, "resultType")?)?;
    let key = definitions.resolve(field(left_collection, "keyType")?)?;
    let left_row = definitions.resolve(field(left_collection, "elementType")?)?;
    if text(left_collection, "class")? != "DBSPTypeIndexedZSet"
        || [key, left_row]
            .iter()
            .any(|ty| ty["class"] != "DBSPTypeTuple" && ty["class"] != "DBSPTypeRawTuple")
        || text(right_ref, "class")? != "DBSPTypeRef"
        || !boolean(right_row, "mayBeNull")?
    {
        return Err(Error("unsupported left join key or row type".into()));
    }
    let absent_class = match text(right_row, "class")? {
        "DBSPTypeTuple" => "DBSPTupleExpression",
        "DBSPTypeRawTuple" => "DBSPRawTupleExpression",
        class => {
            return Err(Error(format!(
                "unsupported nullable left join input {class}"
            )))
        }
    };

    let mut closure = ClosureWriter {
        name: format!("t_{name}"),
        fresh,
        source: None,
    };
    let key_ref = closure.reference(key)?;
    let left_ref = closure.reference(left_row)?;
    let key_ref_id = inner_reference(&key_ref)?;
    let left_ref_id = inner_reference(&left_ref)?;
    let parameter = closure.raw_tuple_type(json!([key_ref, left_ref]))?;
    let value = closure.read_parameter(&parameter)?;
    let value_ref = inner_reference(&value)?;
    let key_field = closure.field(value, key_ref_id, 0)?;
    let row_field = closure.field(value_ref, left_ref_id, 1)?;
    let key_value = closure.clone_deref(key_field, &inner_reference(key)?)?;
    let row_value = closure.clone_deref(row_field, &inner_reference(left_row)?)?;
    let key_argument = closure.borrow(key_value, key)?;
    let left_argument = closure.borrow(row_value, left_row)?;
    let mut absent = closure.node(absent_class, json!({"type": inner_reference(right_row)?}))?;
    copy_source(right_row, &mut absent);
    let right_argument = closure.borrow(absent, right_row)?;
    let body = closure.node(
        "DBSPApplyExpression",
        json!({"type": result, "function": inner_reference(join_function)?,
               "arguments": [key_argument, left_argument, right_argument]}),
    )?;
    closure.closure(parameter, result, body)
}

pub(super) fn deindex(
    definitions: &Definitions<'_>,
    collection: &Value,
    original_function: &Value,
    name: u64,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<(Value, Value)> {
    if text(collection, "class")? != "DBSPTypeIndexedZSet" {
        return Err(Error("deindex requires an indexed collection".into()));
    }
    let key = definitions.resolve(field(collection, "keyType")?)?;
    let row = definitions.resolve(field(collection, "elementType")?)?;
    initial_deindex(key, row, Some(original_function), name, fresh)
}

pub(super) fn initial_deindex(
    key: &Value,
    row: &Value,
    source: Option<&Value>,
    name: u64,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<(Value, Value)> {
    let mut closure = ClosureWriter {
        name: format!("t_{name}"),
        fresh,
        source,
    };
    let key_ref = closure.reference(key)?;
    let row_ref = closure.reference(row)?;
    let row_ref_id = inner_reference(&row_ref)?;
    let parameter = closure.raw_tuple_type(json!([key_ref, row_ref]))?;
    let value = closure.read_parameter(&parameter)?;
    let value = closure.field(value, row_ref_id, 1)?;
    let row_id = inner_reference(row)?;
    let body = closure.clone_deref(value, &row_id)?;
    let function = closure.closure(parameter, row_id.clone(), body)?;
    let mut collection = closure.node(
        "DBSPTypeZSet",
        json!({"elementType": row_id, "name": "WSet", "code": "ZSET", "mayBeNull": false}),
    )?;
    copy_source(row, &mut collection);
    Ok((function, collection))
}

/// Reconstruct the original field order from Gen-2's disjoint key and value tuples.
pub(super) fn reconstruct_indexed_row(
    key: &Value,
    value: &Value,
    fields: &[(usize, usize, &Value)],
    name: u64,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<(Value, Value)> {
    let mut closure = ClosureWriter {
        name: format!("t_{name}"),
        fresh,
        source: None,
    };
    let key_ref = closure.reference(key)?;
    let value_ref = closure.reference(value)?;
    let tuples = [
        (inner_reference(&key_ref)?, inner_reference(key)?),
        (inner_reference(&value_ref)?, inner_reference(value)?),
    ];
    let parameter = closure.raw_tuple_type(json!([key_ref, value_ref]))?;
    let parameter_ref = closure.read_parameter(&parameter)?;
    let mut output_fields = Vec::new();
    let mut output_types = Vec::new();
    for (pair, field, ty) in fields {
        let (reference_type, tuple_type) = &tuples[*pair];
        let tuple_ref = closure.field(parameter_ref.clone(), reference_type.clone(), *pair)?;
        let tuple = closure.node(
            "DBSPDerefExpression",
            json!({"expression": tuple_ref, "type": tuple_type}),
        )?;
        let ty = inner_reference(ty)?;
        output_fields.push(closure.field(tuple, ty.clone(), *field)?);
        output_types.push(ty);
    }
    let output_type = closure.node(
        "DBSPTypeTuple",
        json!({"tupFields": output_types, "code": "TUPLE", "mayBeNull": false}),
    )?;
    let output_type_ref = inner_reference(&output_type)?;
    let body = closure.node(
        "DBSPTupleExpression",
        json!({"type": output_type_ref, "fields": output_fields}),
    )?;
    let function = closure.closure(parameter, output_type.clone(), body)?;
    let collection = closure.node(
        "DBSPTypeZSet",
        json!({"elementType": output_type, "name": "WSet", "code": "ZSET", "mayBeNull": false}),
    )?;
    Ok((function, collection))
}

fn copy_source(from: &Value, to: &mut Value) {
    for field in ["sourceEmpty", "position"] {
        if let Some(value) = from.get(field) {
            to[field] = value.clone();
        }
    }
}

struct ClosureWriter<'a, F> {
    name: String,
    fresh: &'a mut F,
    source: Option<&'a Value>,
}

impl<F: FnMut() -> Result<u64>> ClosureWriter<'_, F> {
    fn closure(&mut self, parameter: Value, result: Value, body: Value) -> Result<Value> {
        let binding = self.node(
            "DBSPParameter",
            json!({"type": inner_reference(&parameter)?, "name": self.name}),
        )?;
        let ty = self.node(
            "DBSPTypeFunction",
            json!({"resultType": result, "parameterTypes": [parameter], "code": "FUNCTION", "mayBeNull": false}),
        )?;
        self.node(
            "DBSPClosureExpression",
            json!({"type": ty, "parameters": [binding], "body": body}),
        )
    }

    fn node(&mut self, class: &str, fields: Value) -> Result<Value> {
        let mut node = json!({"class": class, "id": (self.fresh)()?, "sourceEmpty": true});
        if let Some(source) = self.source.filter(|_| !class.starts_with("DBSPType")) {
            copy_source(source, &mut node);
        }
        node.as_object_mut()
            .expect("node object")
            .extend(fields.as_object().expect("fields object").clone());
        Ok(node)
    }

    fn reference(&mut self, ty: &Value) -> Result<Value> {
        let mut node = self.node(
            "DBSPTypeRef",
            json!({
                "type": inner_reference(ty)?,
                "mutable": false,
                "code": "REF",
                "mayBeNull": false
            }),
        )?;
        copy_source(ty, &mut node);
        Ok(node)
    }

    fn raw_tuple_type(&mut self, fields: Value) -> Result<Value> {
        self.node(
            "DBSPTypeRawTuple",
            json!({
                "tupFields": fields, "code": "RAW_TUPLE", "mayBeNull": false
            }),
        )
    }

    fn read_parameter(&mut self, ty: &Value) -> Result<Value> {
        self.node(
            "DBSPVariablePath",
            json!({"type": inner_reference(ty)?, "variable": self.name}),
        )
    }

    fn field(&mut self, expression: Value, ty: Value, index: usize) -> Result<Value> {
        self.node(
            "DBSPFieldExpression",
            json!({"expression": expression, "type": ty, "fieldNo": index}),
        )
    }

    fn borrow(&mut self, expression: Value, ty: &Value) -> Result<Value> {
        let reference = self.reference(ty)?;
        let mut node = self.node(
            "DBSPBorrowExpression",
            json!({"type": reference, "mut": false}),
        )?;
        copy_source(&expression, &mut node);
        node["expression"] = expression;
        Ok(node)
    }

    fn clone_deref(&mut self, value: Value, ty: &Value) -> Result<Value> {
        let deref = self.node(
            "DBSPDerefExpression",
            json!({"expression": value, "type": ty}),
        )?;
        self.node(
            "DBSPCloneExpression",
            json!({"expression": deref, "type": ty}),
        )
    }
}
