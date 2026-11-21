//! Typed expressions for a nullable timestamp's lateness and retention checks.

use super::{copy_source, ClosureWriter};
use crate::decode::{field, inner_reference, Definitions};
use crate::Result;
use serde_json::{json, Value};

pub(in crate::decode) struct Functions {
    pub waterline_type: Value,
    pub waterline_init: Value,
    pub waterline_extract: Value,
    pub waterline_max: Value,
    pub minimum: Value,
    pub clone_waterline: Value,
    pub filter: Value,
    pub error: Value,
    pub error_type: Value,
    pub retain_minimum: Value,
    pub retain: Value,
}

pub(in crate::decode) const NAME_COUNT: usize = 12;

pub(in crate::decode) struct TimestampSource<'a> {
    pub table: &'a str,
    pub row: &'a Value,
    pub index: usize,
    pub column: &'a Value,
    pub interval: &'a Value,
}

pub(in crate::decode) fn build(
    definitions: &Definitions<'_>,
    source: TimestampSource<'_>,
    first_name: u64,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Functions> {
    let mut writer = ClosureWriter {
        name: String::new(),
        fresh,
        source: None,
    };
    let TimestampSource {
        table,
        row,
        index,
        column,
        interval,
    } = source;
    let names: [String; NAME_COUNT] =
        std::array::from_fn(|offset| format!("t_{}", first_name + offset as u64));
    let [time, max_left, max_right, data, control, error_data, error_key, error_weight, unused, clone, retain_control, retain_data] =
        names.each_ref().map(String::as_str);

    let timestamp =
        writer.default_type(definitions, "DBSPTypeTimestamp", "TIMESTAMP", json!({}))?;
    let boolean = writer.default_type(definitions, "DBSPTypeBool", "BOOL", json!({}))?;
    let empty_tuple = writer.default_type(
        definitions,
        "DBSPTypeRawTuple",
        "RAW_TUPLE",
        json!({"tupFields": []}),
    )?;
    let bound = writer.tuple_type(vec![timestamp.clone()])?;
    let waterline_type = writer.tuple_type(vec![timestamp.clone()])?;
    let minimum = writer.minimum_timestamp_tuple(&bound, &timestamp)?;
    let init = writer.with_parameters(&[], bound.clone(), minimum.clone())?;

    let left_ref = writer.typed_ref(&bound)?;
    let right_ref = writer.typed_ref(&bound)?;
    let left = writer.read_row(&left_ref, &bound, max_left)?;
    let left = writer.field(left, inner_reference(&timestamp)?, 0)?;
    let right = writer.read_row(&right_ref, &bound, max_right)?;
    let right = writer.field(right, inner_reference(&timestamp)?, 0)?;
    let mut max = writer.binary("DBSPBinaryExpression", "AGG_MAX", &timestamp, left, right)?;
    copy_source(interval, &mut max);
    let max = writer.tuple_expression(&waterline_type, vec![max])?;
    let max = writer.with_parameters(
        &[(max_left, left_ref), (max_right, right_ref)],
        waterline_type.clone(),
        max,
    )?;

    let time_ref = writer.typed_ref(row)?;
    let value = writer.read_row(&time_ref, row, time)?;
    let value = writer.field(value, inner_reference(column)?, index)?;
    let bool_type = writer.boolean_type()?;
    let absent = writer.node(
        "DBSPIsNullExpression",
        json!({"type": bool_type, "expression": value}),
    )?;
    let min = writer.minimum_timestamp(&timestamp)?;
    let value = writer.read_row(&time_ref, row, time)?;
    let value = writer.field(value, inner_reference(column)?, index)?;
    let mut unwrapped_type = column.clone();
    unwrapped_type["id"] = json!((writer.fresh)()?);
    unwrapped_type["mayBeNull"] = json!(false);
    let mut unwrap = writer.node(
        "DBSPUnwrapExpression",
        json!({"type": unwrapped_type, "expression": value, "message": ""}),
    )?;
    copy_source(column, &mut unwrap);
    let mut select = writer.node("DBSPIfExpression",
        json!({"type": inner_reference(&timestamp)?, "condition": absent, "positive": min, "negative": unwrap}))?;
    copy_source(interval, &mut select);
    let mut extract = writer.binary(
        "DBSPTimeAddSub",
        "SUB_SATURATING",
        &timestamp,
        select,
        inner_reference(interval)?,
    )?;
    copy_source(interval, &mut extract);
    let extract = writer.tuple_expression(&bound, vec![extract])?;
    let empty_ref = writer.typed_ref(&empty_tuple)?;
    let extract = writer.with_parameters(
        &[(time, time_ref), (unused, empty_ref)],
        bound.clone(),
        extract,
    )?;

    let clone_ref = writer.typed_ref(&waterline_type)?;
    let value = writer.named_value(clone, &clone_ref)?;
    let value = writer.clone_deref(value, &inner_reference(&waterline_type)?)?;
    let clone = writer.with_parameters(&[(clone, clone_ref)], waterline_type.clone(), value)?;

    let row_ref = writer.typed_ref(row)?;
    let bound_ref = writer.typed_ref(&bound)?;
    let body = writer.comparison(
        FieldRead {
            name: data,
            ty: row,
            reference: &row_ref,
            field: column,
            index,
        },
        FieldRead {
            name: control,
            ty: &bound,
            reference: &bound_ref,
            field: &timestamp,
            index: 0,
        },
        &boolean,
    )?;
    let filter = writer.with_parameters(
        &[(control, bound_ref.clone()), (data, row_ref.clone())],
        field(&body, "type")?.clone(),
        body,
    )?;

    let (error, error_type) = writer.late_row(
        definitions,
        table,
        [(control, bound_ref), (error_data, row_ref)],
        [error_key, error_weight],
        &empty_tuple,
    )?;

    let retain_ref = writer.typed_ref(row)?;
    let retain_bound_ref = writer.typed_ref(&waterline_type)?;
    let body = writer.comparison(
        FieldRead {
            name: retain_data,
            ty: row,
            reference: &retain_ref,
            field: column,
            index,
        },
        FieldRead {
            name: retain_control,
            ty: &waterline_type,
            reference: &retain_bound_ref,
            field: &timestamp,
            index: 0,
        },
        &boolean,
    )?;
    let result_type = field(&body, "type")?.clone();
    let mut retain = writer.with_parameters(
        &[
            (retain_data, retain_ref),
            (retain_control, retain_bound_ref),
        ],
        result_type,
        body,
    )?;
    copy_source(row, &mut retain["parameters"][0]);
    let retain_minimum = writer.minimum_timestamp_tuple(&waterline_type, &timestamp)?;

    Ok(Functions {
        waterline_type,
        waterline_init: init,
        waterline_extract: extract,
        waterline_max: max,
        minimum,
        clone_waterline: clone,
        filter,
        error,
        error_type,
        retain_minimum,
        retain,
    })
}

struct FieldRead<'a> {
    name: &'a str,
    ty: &'a Value,
    reference: &'a Value,
    field: &'a Value,
    index: usize,
}

impl<F: FnMut() -> Result<u64>> ClosureWriter<'_, F> {
    fn default_type(
        &mut self,
        definitions: &Definitions<'_>,
        class: &str,
        code: &str,
        extras: Value,
    ) -> Result<Value> {
        if let Some(value) = definitions
            .inner
            .values()
            .find(|ty| ty["class"] == class && ty["singleton"] == true)
        {
            return Ok((*value).clone());
        }
        let mut fields = extras;
        fields["singleton"] = json!(true);
        fields["code"] = json!(code);
        fields["mayBeNull"] = json!(false);
        self.node(class, fields)
    }

    fn minimum_timestamp(&mut self, timestamp: &Value) -> Result<Value> {
        self.node(
            "DBSPTimestampLiteral",
            json!({"type": timestamp, "value": "-62135596800000000"}),
        )
    }

    fn minimum_timestamp_tuple(&mut self, ty: &Value, timestamp: &Value) -> Result<Value> {
        let minimum = self.minimum_timestamp(timestamp)?;
        self.tuple_expression(ty, vec![minimum])
    }

    fn binary(
        &mut self,
        class: &str,
        opcode: &str,
        ty: &Value,
        left: Value,
        right: Value,
    ) -> Result<Value> {
        self.node(
            class,
            json!({"type": ty, "opcode": opcode, "left": left, "right": right}),
        )
    }

    fn boolean_type(&mut self) -> Result<Value> {
        self.node("DBSPTypeBool", json!({"code": "BOOL", "mayBeNull": false}))
    }

    fn comparison(
        &mut self,
        data: FieldRead<'_>,
        control: FieldRead<'_>,
        boolean: &Value,
    ) -> Result<Value> {
        let row = self.read_row(data.reference, data.ty, data.name)?;
        let value = self.field(row, inner_reference(data.field)?, data.index)?;
        let left_tuple = self.tuple_type(vec![inner_reference(data.field)?])?;
        let left = self.tuple_expression(&left_tuple, vec![value])?;
        let bound = self.read_row(control.reference, control.ty, control.name)?;

        let left_ref = self.typed_ref(&left_tuple)?;
        let right_ref = self.typed_ref(control.ty)?;
        let left_binding = self.borrowed_binding("left", &left_ref, left)?;
        let right_binding = self.borrowed_binding("right", &right_ref, bound)?;
        let left = self.read_row(&left_ref, &left_tuple, "left")?;
        let left = self.field(left, inner_reference(data.field)?, 0)?;
        let right = self.read_row(&right_ref, control.ty, "right")?;
        let right = self.field(right, inner_reference(control.field)?, 0)?;
        let ge = self.binary(
            "DBSPBinaryExpression",
            "CONTROLLED_FILTER_GTE",
            boolean,
            left,
            right,
        )?;

        let literal_type = self.boolean_type()?;
        let yes = self.node(
            "DBSPBoolLiteral",
            json!({"type": literal_type, "value": true}),
        )?;
        let ty = self.boolean_type()?;
        let predicate = self.binary("DBSPBinaryExpression", "AND", &ty, yes, ge)?;
        self.node("DBSPBlockExpression",
            json!({"type": ty, "contents": [left_binding, right_binding], "lastExpression": predicate}))
    }

    fn borrowed_binding(&mut self, name: &str, reference: &Value, value: Value) -> Result<Value> {
        let borrowed = self.node(
            "DBSPBorrowExpression",
            json!({"type": inner_reference(reference)?, "expression": value, "mut": false}),
        )?;
        self.node(
            "DBSPLetStatement",
            json!({"variable": name, "type": reference, "initializer": borrowed, "mutable": false}),
        )
    }

    fn late_row(
        &mut self,
        definitions: &Definitions<'_>,
        table: &str,
        bindings: [(&str, Value); 2],
        [key, weight]: [&str; 2],
        empty_tuple: &Value,
    ) -> Result<(Value, Value)> {
        let string = self.default_type(
            definitions,
            "DBSPTypeString",
            "STRING",
            json!({"fixed": false, "precision": -1}),
        )?;
        let str_ty = self.default_type(definitions, "DBSPTypeStr", "STR", json!({}))?;
        let any = self.default_type(definitions, "DBSPTypeAny", "ANY", json!({}))?;
        let table = self.node(
            "DBSPStringLiteral",
            json!({"type": string, "value": table, "charset": "UTF-8"}),
        )?;
        let message = self.node(
            "DBSPStringLiteral",
            json!({"type": string, "value": "Late row", "charset": "UTF-8"}),
        )?;
        let format = self.rust_path("format!", &any)?;
        let debug = self.node(
            "DBSPStrLiteral",
            json!({"type": str_ty, "value": "{:?}", "raw": false}),
        )?;
        let value = self.named_value(bindings[1].0, &bindings[1].1)?;
        let formatted = self.node(
            "DBSPApplyExpression",
            json!({"type": any, "function": format, "arguments": [debug, value]}),
        )?;
        let into = self.rust_path("into", &any)?;
        let string_value = self.node(
            "DBSPApplyMethodExpression",
            json!({"type": string, "self": formatted, "function": into, "arguments": []}),
        )?;
        let result = self.tuple_type(vec![string.clone(), string.clone(), string])?;
        let output = self.node(
            "DBSPTypeZSet",
            json!({"elementType": result, "code": "ZSET", "name": "WSet", "mayBeNull": false}),
        )?;
        let body = self.tuple_expression(&result, vec![table, message, string_value])?;
        let weight_type =
            definitions
                .inner
                .values()
                .find(|ty| ty["class"] == "DBSPTypeWeight")
                .map(|ty| Ok((*ty).clone()))
                .unwrap_or_else(|| {
                    self.node("DBSPTypeWeight",
                json!({"typeArgs": [], "name": "Weight", "code": "USER", "mayBeNull": false}))
                })?;
        let [control, row] = bindings;
        let key_type = self.typed_ref(empty_tuple)?;
        let error = self.with_parameters(
            &[control, row, (key, key_type), (weight, weight_type)],
            result,
            body,
        )?;
        Ok((error, output))
    }

    fn rust_path(&mut self, name: &str, ty: &Value) -> Result<Value> {
        let name = self.node(
            "DBSPSimplePathSegment",
            json!({"identifier": name, "genericArgs": []}),
        )?;
        let path = self.node("DBSPPath", json!({"components": [name]}))?;
        self.node("DBSPPathExpression", json!({"type": ty, "path": path}))
    }
}
