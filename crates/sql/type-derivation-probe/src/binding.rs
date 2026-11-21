//! The sampled validated-SQL call boundary, before SQL-to-Rel rewriting.
//!
//! This shared encoder turns each strategy's independently inferred operands
//! into explicit cast chains. It is not an independent lowering strategy and
//! does not model S2 runtime calls or constant folding.
use crate::model::{DerivedType, Kind, Outcome, SqlType};
use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OperatorIdentity {
    registry: &'static str,
    registry_version: &'static str,
    key: String,
}

/// Resolve only the pinned, unambiguous signatures in this experiment.
/// Registry keys follow S1OperatorRegistry and S1BuiltinOperatorRegistry;
/// the ABS clone is selected by CustomFunctions, not by display-name matching.
fn operator(name: &str) -> OperatorIdentity {
    let (registry, key) = if name == "ABS" {
        ("feldera-s1-builtins", "org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.CustomFunctions$AbsFunction#abs".to_owned())
    } else if name == "CONCAT" {
        (
            "calcite-s1-fixture",
            "SqlLibraryOperators.CONCAT_FUNCTION".to_owned(),
        )
    } else if matches!(
        name,
        "LENGTH" | "LOG" | "MD5" | "REPEAT" | "REVERSE" | "LEFT" | "RIGHT"
    ) {
        ("calcite-s1-fixture", format!("SqlLibraryOperators.{name}"))
    } else {
        ("calcite-s1-fixture", format!("SqlStdOperatorTable.{name}"))
    };
    OperatorIdentity {
        registry,
        registry_version: "1",
        key,
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TypeDescriptor {
    sql_type: &'static str,
    nullable: bool,
    precision: i32,
    scale: i32,
    full: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    charset: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collation: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    coercibility: Option<&'static str>,
}

impl TypeDescriptor {
    /// The declared experiment fixes timestamp precision, UTF-8 and implicit
    /// collation. A production binder must carry these as input attributes.
    fn scalar(ty: SqlType) -> Self {
        let (name, precision, scale, parameters) = match ty.kind {
            Kind::Boolean => ("BOOLEAN", 1, i32::MIN, None),
            Kind::Integer { bits } => match bits {
                8 => ("TINYINT", 3, 0, None),
                16 => ("SMALLINT", 5, 0, None),
                32 => ("INTEGER", 10, 0, None),
                64 => ("BIGINT", 19, 0, None),
                _ => unreachable!("inference validates integer widths"),
            },
            Kind::Real => ("REAL", 7, i32::MIN, None),
            Kind::Double => ("DOUBLE", 15, i32::MIN, None),
            Kind::Decimal { precision, scale } => (
                "DECIMAL",
                i32::from(precision),
                i32::from(scale),
                Some(format!("({precision}, {scale})")),
            ),
            Kind::Character { fixed, length } | Kind::Binary { fixed, length } => {
                let character = matches!(ty.kind, Kind::Character { .. });
                let name = match (character, fixed) {
                    (true, true) => "CHAR",
                    (true, false) => "VARCHAR",
                    (false, true) => "BINARY",
                    (false, false) => "VARBINARY",
                };
                (
                    name,
                    length.map_or(-1, |n| n as i32),
                    i32::MIN,
                    length.map(|n| format!("({n})")),
                )
            }
            Kind::Date => ("DATE", 0, i32::MIN, None),
            Kind::Timestamp { precision } => (
                "TIMESTAMP",
                i32::from(precision),
                i32::MIN,
                Some(format!("({precision})")),
            ),
        };
        let mut full = name.to_owned();
        if let Some(parameters) = parameters {
            full.push_str(&parameters);
        }
        let character = ty.character();
        if character {
            full.push_str(" CHARACTER SET \"UTF-8\"");
        }
        if !ty.nullable {
            full.push_str(" NOT NULL");
        }
        Self {
            sql_type: name,
            nullable: ty.nullable,
            precision,
            scale,
            full,
            charset: character.then_some("UTF-8"),
            collation: character.then_some("ISO-8859-1$en_US$primary"),
            coercibility: character.then_some("IMPLICIT"),
        }
    }

    fn trim_symbol() -> Self {
        Self {
            sql_type: "SYMBOL",
            nullable: false,
            precision: -1,
            scale: i32::MIN,
            full: "SYMBOL NOT NULL".to_owned(),
            charset: None,
            collation: None,
            coercibility: None,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ArgumentBinding {
    argument: usize,
    operand: usize,
    original_type: TypeDescriptor,
    coerced_type: TypeDescriptor,
    inferred_casts: Vec<TypeDescriptor>,
}

#[derive(Debug, Serialize)]
pub struct DefaultOperand {
    operand: usize,
    sql: &'static str,
    #[serde(rename = "type")]
    ty: TypeDescriptor,
}

#[derive(Debug, Serialize)]
pub struct CallBinding {
    operator: OperatorIdentity,
    result_type: TypeDescriptor,
    arguments: Vec<ArgumentBinding>,
    defaults: Vec<DefaultOperand>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Binding {
    Bound(Box<CallBinding>),
    Rejected,
    Unsupported,
}

/// Encode casts around original arguments, preserving their ordinal positions.
/// The original argument subtree remains in the independent capture; this
/// projection compares added casts, not literal parsing or source positions.
pub fn bind(name: &str, arguments: &[SqlType], outcome: &Outcome) -> Binding {
    let Outcome::Bound { result, operands } = outcome else {
        return match outcome {
            Outcome::Rejected { .. } => Binding::Rejected,
            _ => Binding::Unsupported,
        };
    };
    let DerivedType::Scalar(result) = result else {
        return Binding::Unsupported;
    };
    if !result.is_valid() || arguments.iter().chain(operands).any(|t| !t.is_valid()) {
        return Binding::Unsupported;
    }
    if crate::sampled_arity(name) != Some(arguments.len()) || arguments.len() != operands.len() {
        return Binding::Unsupported;
    }
    let offset = if name == "TRIM" { 2 } else { 0 };
    let bindings = arguments
        .iter()
        .zip(operands)
        .enumerate()
        .map(|(index, (original, coerced))| {
            let cast = TypeDescriptor::scalar(*coerced);
            ArgumentBinding {
                argument: index,
                operand: index + offset,
                original_type: TypeDescriptor::scalar(*original),
                coerced_type: cast.clone(),
                inferred_casts: if original.kind == coerced.kind {
                    Vec::new()
                } else {
                    vec![cast]
                },
            }
        })
        .collect();
    let defaults = if name == "TRIM" {
        vec![
            DefaultOperand {
                operand: 0,
                sql: "BOTH",
                ty: TypeDescriptor::trim_symbol(),
            },
            DefaultOperand {
                operand: 1,
                sql: "' '",
                ty: TypeDescriptor::scalar(SqlType {
                    kind: Kind::Character {
                        fixed: true,
                        length: Some(1),
                    },
                    nullable: false,
                }),
            },
        ]
    } else {
        Vec::new()
    };
    Binding::Bound(Box::new(CallBinding {
        operator: operator(name),
        result_type: TypeDescriptor::scalar(*result),
        arguments: bindings,
        defaults,
    }))
}
