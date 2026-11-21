use crate::{
    model::{Expression, Refusal, Result, Value, unsupported},
    numeric,
};
use chrono::NaiveDateTime;
use feldera_fxp::DynamicDecimal;

const MAX_STRING_BYTES: usize = 1024 * 1024;

pub(crate) struct Evaluator {
    semantics_profile: crate::SemanticsProfile,
    remaining: usize,
    remaining_value_bytes: usize,
}

impl Evaluator {
    pub fn new(semantics_profile: crate::SemanticsProfile) -> Self {
        Self {
            semantics_profile,
            remaining: 4096,
            remaining_value_bytes: 1024 * 1024,
        }
    }

    pub fn evaluate(&mut self, expression: &Expression, depth: usize) -> Result<Value> {
        let value = self.evaluate_inner(expression, depth)?;
        self.remaining_value_bytes = self
            .remaining_value_bytes
            .checked_sub(value.retained_bytes()?)
            .ok_or(Refusal::Budget)?;
        Ok(value)
    }

    fn evaluate_inner(&mut self, expression: &Expression, depth: usize) -> Result<Value> {
        if depth > 64 || self.remaining == 0 {
            return Err(Refusal::Budget);
        }
        self.remaining -= 1;
        if expression.kind == "LITERAL" {
            return literal(expression);
        }
        // OFFSET/ORDINAL variants share SqlKind.ITEM but have different index
        // bases and out-of-bounds rules. They are not the runtime ITEM kernel.
        if expression.kind == "ITEM" && expression.operator.as_deref() != Some("ITEM") {
            return Err(unsupported("array subscript variant"));
        }
        if expression.deterministic != Some(true)
            || expression.dynamic
            || expression.operator_class.as_deref().is_some_and(|class| {
                class.contains("UserDefined") && !crate::spatial::registered(expression)
            })
        {
            return Err(unsupported(
                "dynamic, nondeterministic, or user-defined call",
            ));
        }
        match expression.kind.as_str() {
            "DATE_TRUNC" | "FLOOR" | "CEIL"
                if matches!(expression.ty.name.as_str(), "DATE" | "TIMESTAMP") =>
            {
                let [value, unit] = expression.arguments.as_slice() else {
                    return Err(unsupported("temporal rounding arity"));
                };
                let unit = self.flag(unit, depth + 1)?;
                let value = self.evaluate(value, depth + 1)?;
                return crate::temporal_rounding::evaluate(&expression.kind, &unit, &value);
            }
            "EXTRACT" => {
                let [unit, value] = expression.arguments.as_slice() else {
                    return Err(unsupported("EXTRACT arity"));
                };
                let unit = self.flag(unit, depth + 1)?;
                let value = self.evaluate(value, depth + 1)?;
                return crate::extract::evaluate(&unit, &value, &expression.ty);
            }
            "TRIM" => {
                let [mode, remove, text] = expression.arguments.as_slice() else {
                    return Err(unsupported("TRIM arity"));
                };
                let mode = self.flag(mode, depth + 1)?;
                let remove = self.evaluate(remove, depth + 1)?;
                let text = self.evaluate(text, depth + 1)?;
                return crate::strings::trim(&mode, &remove, &text);
            }
            "CASE" => {
                let args = &expression.arguments;
                if args.is_empty() || args.len().is_multiple_of(2) {
                    return Err(unsupported("malformed CASE"));
                }
                for pair in args[..args.len() - 1].as_chunks::<2>().0 {
                    match self.evaluate(&pair[0], depth + 1)? {
                        Value::Bool(true) => return self.evaluate(&pair[1], depth + 1),
                        Value::Bool(false) | Value::Null => {}
                        _ => return Err(unsupported("non-boolean CASE condition")),
                    }
                }
                return self.evaluate(&args[args.len() - 1], depth + 1);
            }
            "COALESCE" => {
                for arg in &expression.arguments {
                    let value = self.evaluate(arg, depth + 1)?;
                    if value != Value::Null {
                        return Ok(value);
                    }
                }
                return Ok(Value::Null);
            }
            "AND" | "OR" => {
                let mut saw_null = false;
                let deciding = expression.kind == "OR";
                for arg in &expression.arguments {
                    match self.evaluate(arg, depth + 1)? {
                        Value::Bool(b) if b == deciding => return Ok(Value::Bool(b)),
                        Value::Bool(_) => {}
                        Value::Null => saw_null = true,
                        _ => return Err(unsupported("non-boolean AND/OR")),
                    }
                }
                return Ok(if saw_null {
                    Value::Null
                } else {
                    Value::Bool(!deciding)
                });
            }
            _ => {}
        }
        // Registration precedes null propagation: an unknown function can have
        // non-strict null semantics and must never be guessed to return NULL.
        let array_function = expression
            .operator
            .as_deref()
            .filter(|name| crate::arrays::registered(name));
        let math_function = expression
            .operator
            .as_deref()
            .filter(|name| expression.kind == "OTHER_FUNCTION" && crate::math::registered(name));
        let registered = array_function.is_some()
            || math_function.is_some()
            || crate::spatial::registered(expression)
            || crate::selection::registered(&expression.kind)
            || match expression.kind.as_str() {
                "ARRAY_VALUE_CONSTRUCTOR"
                | "MAP_VALUE_CONSTRUCTOR"
                | "MAP_CONTAINS_KEY"
                | "MAP_CONCAT"
                | "ARRAY_COMPACT"
                | "FLOOR"
                | "CEIL"
                | "IS_NULL"
                | "IS_NOT_NULL"
                | "CAST"
                | "SAFE_CAST"
                | "PLUS"
                | "CHECKED_PLUS"
                | "MINUS"
                | "CHECKED_MINUS"
                | "TIMES"
                | "CHECKED_TIMES"
                | "DIVIDE"
                | "CHECKED_DIVIDE"
                | "MOD"
                | "MINUS_PREFIX"
                | "CHECKED_MINUS_PREFIX"
                | "NOT"
                | "ITEM"
                | "EQUALS"
                | "NOT_EQUALS"
                | "LESS_THAN"
                | "LESS_THAN_OR_EQUAL"
                | "GREATER_THAN"
                | "GREATER_THAN_OR_EQUAL"
                | "IS_DISTINCT_FROM"
                | "IS_NOT_DISTINCT_FROM"
                | "IS_TRUE"
                | "IS_NOT_TRUE"
                | "IS_FALSE"
                | "IS_NOT_FALSE" => true,
                "LIKE" => matches!(expression.operator.as_deref(), Some("LIKE" | "ILIKE")),
                "OTHER" => expression.operator.as_deref() == Some("||"),
                "CHAR_LENGTH" | "POSITION" | "REVERSE" => true,
                "OTHER_FUNCTION" | "STARTS_WITH" | "ENDS_WITH" => matches!(
                    expression.operator.as_deref(),
                    Some(
                        "PI" | "CHAR_LENGTH"
                            | "ROUND"
                            | "TRUNCATE"
                            | "TRUNC"
                            | "CHARACTER_LENGTH"
                            | "UPPER"
                            | "LOWER"
                            | "INITCAP"
                            | "ARRAY"
                            | "CONCAT"
                            | "STARTS_WITH"
                            | "ENDS_WITH"
                            | "SUBSTRING"
                            | "LEFT"
                            | "RIGHT"
                            | "POSITION"
                            | "REVERSE"
                            | "REPLACE"
                            | "REPEAT"
                            | "OCTET_LENGTH"
                            | "ASCII"
                            | "MD5"
                            | "TO_HEX"
                            | "OVERLAY"
                            | "SPLIT"
                            | "CONCAT_WS"
                            | "MAP_CONTAINS_KEY"
                            | "MAP_CONCAT"
                    )
                ),
                _ => false,
            };
        if !registered {
            return Err(unsupported(format!(
                "operator {} / {:?}",
                expression.kind, expression.operator
            )));
        }
        let values = expression
            .arguments
            .iter()
            .map(|arg| self.evaluate(arg, depth + 1))
            .collect::<Result<Vec<_>>>()?;
        if crate::selection::registered(&expression.kind) {
            return crate::selection::evaluate(&expression.kind, &values, &expression.ty);
        }
        if crate::spatial::registered(expression) {
            return crate::spatial::evaluate(expression, &values, self.semantics_profile);
        }
        if expression.kind == "MAP_VALUE_CONSTRUCTOR" {
            return crate::maps::construct(&values);
        }
        if let Some(name @ ("MAP_CONTAINS_KEY" | "MAP_CONCAT")) = expression.operator.as_deref() {
            if name == "MAP_CONCAT"
                && self.semantics_profile == crate::SemanticsProfile::LegacyJanino
            {
                return crate::maps::legacy_concat(expression, &values);
            }
            return crate::maps::evaluate(name, &values);
        }
        if array_function == Some("CARDINALITY")
            && let [Value::Map(entries)] = values.as_slice()
        {
            return Ok(Value::Int(entries.len() as i128));
        }
        // Append/prepend/repeat preserve NULL elements; they are not strict in
        // every argument and must run before ordinary NULL propagation.
        if let Some(name) = array_function {
            return crate::arrays::evaluate(name, &values);
        }
        if expression.kind == "OTHER_FUNCTION"
            && expression.operator.as_deref() == Some("CONCAT_WS")
        {
            return crate::strings::evaluate("CONCAT_WS", &values, self.semantics_profile);
        }
        if expression.kind == "ARRAY_VALUE_CONSTRUCTOR"
            || (expression.kind == "OTHER_FUNCTION"
                && expression.operator.as_deref() == Some("ARRAY"))
        {
            return Ok(Value::Array(values));
        }
        if expression.kind == "IS_NULL" {
            return unary(&values).map(|v| Value::Bool(*v == Value::Null));
        }
        if expression.kind == "IS_NOT_NULL" {
            return unary(&values).map(|v| Value::Bool(*v != Value::Null));
        }
        match expression.kind.as_str() {
            "IS_DISTINCT_FROM" | "IS_NOT_DISTINCT_FROM" => {
                let (a, b) = binary(&values)?;
                let equal = crate::comparison::equal(a, b)?;
                return Ok(Value::Bool(
                    equal == (expression.kind == "IS_NOT_DISTINCT_FROM"),
                ));
            }
            "IS_TRUE" | "IS_NOT_TRUE" | "IS_FALSE" | "IS_NOT_FALSE" => {
                let value = unary(&values)?;
                if !matches!(value, Value::Bool(_) | Value::Null) {
                    return Err(unsupported("boolean truth predicate operand"));
                }
                let expected = expression.kind.ends_with("TRUE");
                let equal = *value == Value::Bool(expected);
                return Ok(Value::Bool(equal != expression.kind.starts_with("IS_NOT_")));
            }
            _ => {}
        }
        if values.contains(&Value::Null) {
            return Ok(Value::Null);
        }
        if let Some(name) = math_function {
            return crate::math::evaluate(name, &values, self.semantics_profile);
        }
        let ty = &expression.ty;
        match expression.kind.as_str() {
            "OTHER" if expression.operator.as_deref() == Some("||") => {
                if values.iter().any(|v| matches!(v, Value::Binary(_))) {
                    crate::binary::evaluate("||", &values)
                } else {
                    crate::strings::evaluate("CONCAT", &values, self.semantics_profile)
                }
            }
            "LIKE" => {
                crate::pattern::evaluate(expression.operator.as_deref().unwrap_or(""), &values)
            }
            "ARRAY_COMPACT" => match unary(&values)? {
                Value::Array(v) => Ok(Value::Array(
                    v.iter().filter(|v| **v != Value::Null).cloned().collect(),
                )),
                _ => Err(unsupported("ARRAY_COMPACT type")),
            },
            "CEIL" | "FLOOR" => crate::rounding::evaluate(&expression.kind, &values, ty),
            "CAST" | "SAFE_CAST" => {
                match crate::casts::cast(
                    unary(&values)?.clone(),
                    &expression.arguments[0].ty,
                    ty,
                    self.semantics_profile,
                ) {
                    Err(Refusal::RuntimeError(_)) if expression.kind == "SAFE_CAST" => {
                        Ok(Value::Null)
                    }
                    result => result,
                }
            }
            "PLUS" | "CHECKED_PLUS" | "MINUS" | "CHECKED_MINUS" | "TIMES" | "CHECKED_TIMES"
            | "DIVIDE" | "CHECKED_DIVIDE" | "MOD" => {
                let (a, b) = binary(&values)?;
                let op = match expression.kind.as_str() {
                    "PLUS" | "CHECKED_PLUS" => "+",
                    "MINUS" | "CHECKED_MINUS" => "-",
                    "TIMES" | "CHECKED_TIMES" => "*",
                    "MOD" => "%",
                    _ => "/",
                };
                if matches!(
                    a,
                    Value::Months(_) | Value::Micros(_) | Value::Date(_) | Value::Timestamp(_)
                ) || matches!(b, Value::Months(_) | Value::Micros(_))
                {
                    crate::temporal::arithmetic(op, a, b, ty)
                } else {
                    numeric::arithmetic(op, a.clone(), b.clone(), ty)
                }
            }
            "MINUS_PREFIX" | "CHECKED_MINUS_PREFIX" => match unary(&values)? {
                Value::Int(v) => numeric::integer(
                    v.checked_neg()
                        .ok_or(Refusal::RuntimeError("negation overflow"))?,
                    ty,
                ),
                Value::Float(v) => Ok(numeric::floating(-v, ty)),
                Value::Decimal(v) => numeric::decimal(
                    DynamicDecimal::new(
                        v.significand()
                            .checked_neg()
                            .ok_or(Refusal::RuntimeError("decimal negation overflow"))?,
                        v.exponent(),
                    ),
                    ty,
                ),
                _ => Err(unsupported("negation type")),
            },
            "NOT" => match unary(&values)? {
                Value::Bool(v) => Ok(Value::Bool(!v)),
                _ => Err(unsupported("NOT type")),
            },
            "ITEM" => match binary(&values)? {
                (Value::Map(entries), key) => Ok(crate::maps::lookup(entries, key)?
                    .cloned()
                    .unwrap_or(Value::Null)),
                (Value::Array(array), Value::Int(index)) => Ok(index
                    .checked_sub(1)
                    .and_then(|i| usize::try_from(i).ok())
                    .and_then(|i| array.get(i))
                    .cloned()
                    .unwrap_or(Value::Null)),
                _ => Err(unsupported("ITEM type")),
            },
            "EQUALS" | "NOT_EQUALS" => {
                let (a, b) = binary(&values)?;
                let equal = crate::comparison::equal(a, b)?;
                Ok(Value::Bool(equal == (expression.kind == "EQUALS")))
            }
            "LESS_THAN" | "LESS_THAN_OR_EQUAL" | "GREATER_THAN" | "GREATER_THAN_OR_EQUAL" => {
                let (a, b) = binary(&values)?;
                let order = crate::comparison::compare(a, b)?;
                Ok(Value::Bool(match expression.kind.as_str() {
                    "LESS_THAN" => order.is_lt(),
                    "LESS_THAN_OR_EQUAL" => order.is_le(),
                    "GREATER_THAN" => order.is_gt(),
                    _ => order.is_ge(),
                }))
            }
            "OTHER_FUNCTION" | "STARTS_WITH" | "ENDS_WITH" | "CHAR_LENGTH" | "POSITION"
            | "REVERSE" => {
                let name = expression.operator.as_deref().unwrap_or("");
                if matches!(name, "ROUND" | "TRUNCATE" | "TRUNC") {
                    crate::rounding::evaluate(name, &values, ty)
                } else if values.iter().any(|v| matches!(v, Value::Binary(_))) {
                    crate::binary::evaluate(name, &values)
                } else {
                    crate::strings::evaluate(name, &values, self.semantics_profile)
                }
            }
            _ => Err(unsupported(format!(
                "operator {} / {:?}",
                expression.kind, expression.operator
            ))),
        }
    }

    /// Flags are typed control operands, not string values or callable expressions.
    fn flag(&mut self, expression: &Expression, depth: usize) -> Result<String> {
        if depth > 64 || self.remaining == 0 {
            return Err(Refusal::Budget);
        }
        self.remaining -= 1;
        if expression.kind != "LITERAL" || expression.ty.name != "SYMBOL" {
            return Err(unsupported("expected a literal symbol"));
        }
        expression
            .value
            .as_ref()
            .filter(|s| s.len() <= 32)
            .cloned()
            .ok_or_else(|| unsupported("missing or oversized symbol"))
    }
}

fn unary(values: &[Value]) -> Result<&Value> {
    if let [value] = values {
        Ok(value)
    } else {
        Err(unsupported("unary arity"))
    }
}

fn binary(values: &[Value]) -> Result<(&Value, &Value)> {
    if let [a, b] = values {
        Ok((a, b))
    } else {
        Err(unsupported("binary arity"))
    }
}

fn literal(expression: &Expression) -> Result<Value> {
    let Some(text) = &expression.value else {
        return Ok(Value::Null);
    };
    if text.len() > MAX_STRING_BYTES {
        return Err(Refusal::Budget);
    }
    let ty = &expression.ty;
    let malformed = || unsupported(format!("malformed {} literal", ty.name));
    match ty.name.as_str() {
        "GEOMETRY" => crate::spatial::literal(text),
        "BOOLEAN" => text.parse().map(Value::Bool).map_err(|_| malformed()),
        "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "UTINYINT" | "USMALLINT" | "UINTEGER"
        | "UBIGINT" => numeric::integer(text.parse().map_err(|_| malformed())?, ty),
        "REAL" | "FLOAT" | "DOUBLE" => {
            let value: f64 = text.parse().map_err(|_| malformed())?;
            Ok(Value::Float(if ty.name == "REAL" {
                f64::from(value as f32)
            } else {
                value
            }))
        }
        "DECIMAL" => text.parse().map(Value::Decimal).map_err(|_| malformed()),
        "CHAR" | "VARCHAR" => Ok(Value::String(text.clone())),
        "BINARY" | "VARBINARY" => crate::binary::parse(text).map(Value::Binary),
        "UUID" => crate::uuid::parse(text).map(Value::Uuid),
        "DATE" => text.parse().map(Value::Date).map_err(|_| malformed()),
        "TIME" => text.parse().map(Value::Time).map_err(|_| malformed()),
        "TIMESTAMP" => NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
            .map(Value::Timestamp)
            .map_err(|_| malformed()),
        name if name.starts_with("INTERVAL_") => {
            let value: DynamicDecimal = text.parse().map_err(|_| malformed())?;
            if matches!(
                name,
                "INTERVAL_YEAR" | "INTERVAL_MONTH" | "INTERVAL_YEAR_MONTH"
            ) {
                i32::try_from(value)
                    .map(Value::Months)
                    .map_err(|_| malformed())
            } else {
                // Calcite stores milliseconds, Feldera stores microseconds.
                let micros = value
                    .significand()
                    .checked_mul(1000)
                    .ok_or_else(malformed)?
                    / 10i128
                        .checked_pow(value.exponent().into())
                        .ok_or_else(malformed)?;
                i64::try_from(micros)
                    .map(Value::Micros)
                    .map_err(|_| malformed())
            }
        }
        _ => Err(unsupported(format!("literal type {}", ty.name))),
    }
}
