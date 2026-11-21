//! Small maps retain insertion order for the Java adapter. SQL lookup uses
//! value equality; duplicate keys retain the last value, as sqllib does.

use crate::model::{Expression, Refusal, Result, SqlType, Value, unsupported};

const MAX_ENTRIES: usize = 256;

/// Janino concatenates maps before converting their keys to the result SQL
/// type. Integer 1 and decimal 1.0 are distinct Java keys, even if both become
/// decimal 1.0 in the resulting Rex constructor. Preserve this observable stage.
pub(crate) fn legacy_concat(expression: &Expression, values: &[Value]) -> Result<Value> {
    if values.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    let (Some(target_key), Some(target_value)) = (&expression.ty.key, &expression.ty.value) else {
        return Err(unsupported("legacy map concat requires component types"));
    };
    if values.len() < 2 {
        return Err(unsupported("map concat arity"));
    }
    struct Entry<'a> {
        raw_key: &'a Value,
        key_type: &'a SqlType,
        key: Value,
        value: Value,
    }
    let mut entries: Vec<Entry<'_>> = Vec::new();
    let mut remaining_entries = MAX_ENTRIES;
    let mut remaining_bytes: usize = 1024 * 1024;
    for (argument, value) in expression.arguments.iter().zip(values) {
        let (Some(key_type), Some(value_type), Value::Map(map)) =
            (&argument.ty.key, &argument.ty.value, value)
        else {
            return Err(unsupported("legacy map concat operand types"));
        };
        remaining_entries = remaining_entries
            .checked_sub(map.len())
            .ok_or(Refusal::Budget)?;
        for (raw_key, raw_value) in map {
            let key = crate::casts::cast(
                raw_key.clone(),
                key_type,
                target_key,
                crate::SemanticsProfile::LegacyJanino,
            )?;
            let value = crate::casts::cast(
                raw_value.clone(),
                value_type,
                target_value,
                crate::SemanticsProfile::LegacyJanino,
            )?;
            let key_bytes = key.retained_bytes()?;
            let value_bytes = value.retained_bytes()?;
            remaining_bytes = remaining_bytes
                .checked_sub(key_bytes)
                .and_then(|n| n.checked_sub(value_bytes))
                .ok_or(Refusal::Budget)?;
            let mut previous = None;
            for (index, entry) in entries.iter().enumerate() {
                if legacy_key_equal(entry.raw_key, entry.key_type, raw_key, key_type)? {
                    previous = Some(index);
                    break;
                }
            }
            if let Some(index) = previous {
                entries[index].value = value;
            } else {
                entries.push(Entry {
                    raw_key,
                    key_type,
                    key,
                    value,
                });
            }
        }
    }
    Ok(Value::Map(
        entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect(),
    ))
}

fn legacy_key_equal(a: &Value, a_type: &SqlType, b: &Value, b_type: &SqlType) -> Result<bool> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(a == b);
    }
    if a_type.name != b_type.name || (a_type.name == "DECIMAL" && a_type.scale != b_type.scale) {
        return Ok(false);
    }
    if let (Value::Array(a), Value::Array(b)) = (a, b) {
        let (Some(a_type), Some(b_type)) = (&a_type.component, &b_type.component) else {
            return Err(unsupported("legacy array key component types"));
        };
        if a.len() != b.len() {
            return Ok(false);
        }
        for (a, b) in a.iter().zip(b) {
            if !legacy_key_equal(a, a_type, b, b_type)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    if let (Value::Float(a), Value::Float(b)) = (a, b) {
        return Ok((a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits());
    }
    crate::comparison::equal(a, b)
}

fn insert(entries: &mut Vec<(Value, Value)>, key: &Value, value: &Value) -> Result<()> {
    for (previous, stored) in entries.iter_mut() {
        if crate::comparison::equal(previous, key)? {
            *stored = value.clone();
            return Ok(());
        }
    }
    if entries.len() >= MAX_ENTRIES {
        return Err(Refusal::Budget);
    }
    entries.push((key.clone(), value.clone()));
    Ok(())
}

pub(crate) fn construct(values: &[Value]) -> Result<Value> {
    if !values.len().is_multiple_of(2) {
        return Err(unsupported("odd MAP constructor arity"));
    }
    if values.len() / 2 > MAX_ENTRIES {
        return Err(Refusal::Budget);
    }
    let mut entries = Vec::with_capacity(values.len() / 2);
    for [key, value] in values.as_chunks::<2>().0 {
        insert(&mut entries, key, value)?;
    }
    Ok(Value::Map(entries))
}

pub(crate) fn lookup<'a>(entries: &'a [(Value, Value)], key: &Value) -> Result<Option<&'a Value>> {
    for (candidate, value) in entries {
        if crate::comparison::equal(candidate, key)? {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

pub(crate) fn evaluate(name: &str, values: &[Value]) -> Result<Value> {
    if values.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    match (name, values) {
        ("MAP_CONTAINS_KEY", [Value::Map(entries), key]) => {
            Ok(Value::Bool(lookup(entries, key)?.is_some()))
        }
        ("MAP_CONCAT", maps) if maps.len() >= 2 => {
            // Bound both retained entries and quadratic equality work, even
            // when many input maps repeatedly replace the same keys.
            let mut count = 0;
            for map in maps {
                let Value::Map(entries) = map else {
                    return Err(unsupported("MAP_CONCAT operand"));
                };
                count += entries.len();
                if count > MAX_ENTRIES {
                    return Err(Refusal::Budget);
                }
            }
            let mut result = Vec::with_capacity(count);
            for map in maps {
                if let Value::Map(entries) = map {
                    for (key, value) in entries {
                        insert(&mut result, key, value)?;
                    }
                }
            }
            Ok(Value::Map(result))
        }
        _ => Err(unsupported("map function operands")),
    }
}
