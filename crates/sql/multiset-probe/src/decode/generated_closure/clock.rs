//! Closure builders for the synthetic clock trace and projection join.

use super::{copy_source, ClosureWriter};
use crate::decode::{array, field, inner_reference};
use crate::Result;
use serde_json::{json, Value};

pub(in crate::decode) struct Index {
    pub function: Value,
    pub collection: Value,
    pub key: Value,
    pub row: Value,
}

pub(in crate::decode) enum IndexInput<'a> {
    Clock(&'a Value),
    Relation(&'a Value),
}

pub(in crate::decode) fn index(
    input: IndexInput<'_>,
    name: u64,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Index> {
    let mut writer = writer(name, fresh);
    let clock_row;
    let (input, clock) = match input {
        IndexInput::Clock(timestamp) => {
            clock_row = writer.tuple_type(vec![timestamp.clone()])?;
            (&clock_row, true)
        }
        IndexInput::Relation(row) => (row, false),
    };
    let parameter = writer.typed_ref(input)?;
    let deref = writer.read_row(&parameter, input, &writer.name.clone())?;
    let values = writer.fields(&deref, input)?;
    let key = writer.tuple_type(Vec::new())?;
    let value_type = writer.tuple_type(array(input, "tupFields")?.to_vec())?;
    let empty = writer.tuple_expression(&key, Vec::new())?;
    let values = writer.tuple_expression(&value_type, values)?;
    let result = writer.raw_tuple_type(json!([key, value_type]))?;
    let body = writer.node(
        "DBSPRawTupleExpression",
        json!({"type": inner_reference(&result)?, "fields": [empty, values]}),
    )?;
    let function = writer.closure(parameter, result, body)?;
    let (key, row) = if clock {
        (key, value_type)
    } else {
        (writer.tuple_type(Vec::new())?, input.clone())
    };
    let mut collection = writer.node(
        "DBSPTypeIndexedZSet",
        json!({"keyType": key, "elementType": row, "name": "IndexedWSet",
            "code": "INDEXED_ZSET", "mayBeNull": false}),
    )?;
    if !clock {
        copy_source(input, &mut collection);
    }
    Ok(Index {
        function,
        collection,
        key,
        row,
    })
}

pub(in crate::decode) struct ClockAggregate {
    pub init: Value,
    pub update: Value,
}

pub(in crate::decode) fn aggregate(
    timestamp: &Value,
    weight_name: &str,
    timestamp_name: u64,
    accumulator_name: u64,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<ClockAggregate> {
    let mut writer = writer(timestamp_name, fresh);
    let ts_row = writer.tuple_type(vec![timestamp.clone()])?;
    let accumulator = writer.tuple_type(vec![timestamp.clone()])?;
    let ts_ref = writer.typed_ref(&ts_row)?;
    let weight = writer.node(
        "DBSPTypeWeight",
        json!({"typeArgs": [], "name": "Weight", "code": "USER", "mayBeNull": false}),
    )?;
    let timestamp_name = writer.name.clone();
    let accumulator_name = format!("t_{accumulator_name}");
    let incoming = writer.named_value(&timestamp_name, &ts_ref)?;
    let value = writer.dereference(incoming.clone(), &ts_row)?;
    let value = writer.field(value, inner_reference(timestamp)?, 0)?;
    let result = writer.tuple_type(vec![inner_reference(timestamp)?])?;
    let init_body = writer.tuple_expression(&result, vec![value])?;
    let init = writer.with_parameters(
        &[
            (timestamp_name.as_str(), ts_ref.clone()),
            (weight_name, weight.clone()),
        ],
        result,
        init_body,
    )?;

    let current = writer.named_value(&accumulator_name, &accumulator)?;
    let current = writer.field(current, inner_reference(timestamp)?, 0)?;
    let incoming = writer.dereference(incoming, &ts_row)?;
    let incoming = writer.field(incoming, inner_reference(timestamp)?, 0)?;
    let max = writer.node(
        "DBSPConditionalIncrementExpression",
        json!({"type": inner_reference(timestamp)?, "left": current, "right": incoming,
                "opcode": "AGG_MAX"}),
    )?;
    let result = writer.tuple_type(vec![inner_reference(timestamp)?])?;
    let update_body = writer.tuple_expression(&result, vec![max])?;
    let update = writer.with_parameters(
        &[
            (accumulator_name.as_str(), accumulator),
            (timestamp_name.as_str(), ts_ref),
            (weight_name, weight),
        ],
        result,
        update_body,
    )?;
    Ok(ClockAggregate { init, update })
}

pub(in crate::decode) fn join(
    key: &Value,
    left: &Value,
    right: &Value,
    names: [u64; 3],
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<(Value, Value)> {
    let mut writer = writer(names[0], fresh);
    let names = names.map(|name| format!("t_{name}"));
    let right = writer.tuple_type(array(right, "tupFields")?.to_vec())?;
    let [key_ref, left_ref, right_ref] = [
        writer.typed_ref(key)?,
        writer.typed_ref(left)?,
        writer.typed_ref(&right)?,
    ];
    let left_value = writer.read_row(&left_ref, left, &names[1])?;
    let right_value = writer.read_row(&right_ref, &right, &names[2])?;
    let mut values = writer.fields(&left_value, left)?;
    values.extend(writer.fields(&right_value, &right)?);
    let mut types = array(left, "tupFields")?.to_vec();
    types.extend_from_slice(array(&right, "tupFields")?);
    let row = writer.tuple_type(types.clone())?;
    let body = writer.tuple_expression(&row, values)?;
    let function = writer.with_parameters(
        &[
            (&names[0], key_ref),
            (&names[1], left_ref),
            (&names[2], right_ref),
        ],
        row,
        body,
    )?;
    let row = writer.tuple_type(types)?;
    let output = writer.node(
        "DBSPTypeZSet",
        json!({"elementType": row, "name": "WSet", "code": "ZSET", "mayBeNull": false}),
    )?;
    Ok((function, output))
}

pub(in crate::decode) fn projection(
    body: &Value,
    input_row: &Value,
    timestamp: &Value,
    name: &str,
    fields: &[usize],
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Value> {
    let mut writer = ClosureWriter {
        name: name.to_owned(),
        fresh,
        source: None,
    };
    let mut types = array(input_row, "tupFields")?.to_vec();
    types.push(inner_reference(timestamp)?);
    let row = writer.tuple_type(types)?;
    let parameter = writer.typed_ref(&row)?;
    let mut projected = Vec::new();
    let row_types = array(&row, "tupFields")?;
    for position in fields {
        let deref = writer.read_row(&parameter, &row, name)?;
        let ty = row_types.get(*position).ok_or_else(|| {
            crate::Error("clock projection field is outside the parameter row".into())
        })?;
        projected.push(writer.field(deref, inner_reference(ty)?, *position)?);
    }
    let result_type = inner_reference(field(body, "type")?)?;
    let mut result = writer.tuple_expression(&result_type, projected)?;
    copy_source(body, &mut result);
    writer.closure(parameter, result_type, result)
}

fn writer<F: FnMut() -> Result<u64>>(name: u64, fresh: &mut F) -> ClosureWriter<'_, F> {
    ClosureWriter {
        name: format!("t_{name}"),
        fresh,
        source: None,
    }
}

impl<F: FnMut() -> Result<u64>> ClosureWriter<'_, F> {
    pub(super) fn typed_ref(&mut self, ty: &Value) -> Result<Value> {
        let mut reference = self.reference(ty)?;
        reference["type"] = ty.clone();
        Ok(reference)
    }

    pub(super) fn tuple_type(&mut self, fields: Vec<Value>) -> Result<Value> {
        self.node(
            "DBSPTypeTuple",
            json!({"tupFields": fields, "code": "TUPLE", "mayBeNull": false}),
        )
    }

    pub(super) fn tuple_expression(&mut self, ty: &Value, fields: Vec<Value>) -> Result<Value> {
        self.node(
            "DBSPTupleExpression",
            json!({"type": inner_reference(ty)?, "fields": fields}),
        )
    }

    pub(super) fn named_value(&mut self, name: &str, ty: &Value) -> Result<Value> {
        self.node(
            "DBSPVariablePath",
            json!({"type": inner_reference(ty)?, "variable": name}),
        )
    }

    pub(super) fn read_row(&mut self, ty: &Value, row: &Value, name: &str) -> Result<Value> {
        let value = self.named_value(name, ty)?;
        self.dereference(value, row)
    }

    pub(super) fn dereference(&mut self, value: Value, row: &Value) -> Result<Value> {
        self.node(
            "DBSPDerefExpression",
            json!({"expression": value, "type": inner_reference(row)?}),
        )
    }

    fn fields(&mut self, value: &Value, row: &Value) -> Result<Vec<Value>> {
        array(row, "tupFields")?
            .iter()
            .enumerate()
            .map(|(position, ty)| self.field(value.clone(), inner_reference(ty)?, position))
            .collect()
    }

    pub(super) fn with_parameters(
        &mut self,
        parameters: &[(&str, Value)],
        result: Value,
        body: Value,
    ) -> Result<Value> {
        let bindings = parameters
            .iter()
            .map(|(name, ty)| {
                self.node(
                    "DBSPParameter",
                    json!({"name": name, "type": inner_reference(ty)?}),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let parameter_types = parameters.iter().map(|(_, ty)| ty).collect::<Vec<_>>();
        let ty = self.node(
            "DBSPTypeFunction",
            json!({"resultType": result, "parameterTypes": parameter_types,
                "code": "FUNCTION", "mayBeNull": false}),
        )?;
        self.node(
            "DBSPClosureExpression",
            json!({"type": ty, "parameters": bindings, "body": body}),
        )
    }
}
