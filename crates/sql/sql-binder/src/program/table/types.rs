//! Resolve column declarations and preserve Calcite's statement and diagnostic spellings.

use super::super::graph::insert_type;
use crate::{DataType, Failure, Kind, checked_type, unsupported};
use feldera_sql_rel_model::{
    TypeId, TypeTable,
    source::Origin,
    types::{
        Cardinality, CharacterMetadata, FloatFlavor, IntegerWidth, Precision, SqlType, TimeZone,
        TypeKind,
    },
};
use sqlparser_064::ast::{ArrayElemTypeDef, ExactNumberInfo, MapBracketKind};

/// The statement printer quotes type aliases. The diagnostic printer uses
/// the same resolved names without quotes, including inside collections.
pub(super) struct TypeSpelling {
    pub(super) quoted: String,
    pub(super) diagnostic: String,
}

impl TypeSpelling {
    pub(super) fn render(f: impl Fn(bool) -> String) -> Self {
        Self {
            quoted: f(true),
            diagnostic: f(false),
        }
    }

    pub(super) fn spelling(&self, quoted: bool) -> &str {
        if quoted {
            &self.quoted
        } else {
            &self.diagnostic
        }
    }
}

pub(super) fn declare_type(
    types: &mut TypeTable,
    aliases: &super::declarations::Aliases,
    data_type: &DataType,
    nullable: bool,
    depth: usize,
) -> Result<(TypeId, TypeSpelling), Failure> {
    let child_depth = depth
        .checked_sub(1)
        .ok_or_else(|| unsupported("table type nesting exceeds the validation limit"))?;
    let (kind, spelling) = match data_type {
        DataType::Array(ArrayElemTypeDef::Qualified(element, None)) => {
            let (element, spelling) = declare_type(types, aliases, element, true, child_depth)?;
            (
                TypeKind::Array {
                    element,
                    cardinality: Cardinality::Unbounded,
                },
                TypeSpelling::render(|quoted| format!("{} ARRAY", spelling.spelling(quoted))),
            )
        }
        DataType::Map(key, value, MapBracketKind::AngleBrackets) => {
            let (key, key_spelling) = declare_type(types, aliases, key, false, child_depth)?;
            if !types.types[key.0 as usize].kind.can_be_map_key() {
                return Err(unsupported("MAP keys cannot have type MAP or NULL"));
            }
            let (value, value_spelling) = declare_type(types, aliases, value, true, child_depth)?;
            (
                TypeKind::Map { key, value },
                TypeSpelling::render(|quoted| {
                    format!(
                        "MAP< {}, {} >",
                        key_spelling.spelling(quoted),
                        value_spelling.spelling(quoted)
                    )
                }),
            )
        }
        _ => {
            if let Some((kind, alias)) = default_type_alias(data_type) {
                (
                    kind,
                    TypeSpelling {
                        quoted: format!("`{alias}`"),
                        diagnostic: alias,
                    },
                )
            } else if let DataType::Custom(name, modifiers) = data_type
                && modifiers.is_empty()
            {
                let (_, name, spelling) = super::declarations::type_name(name)?;
                let id = aliases
                    .get(&name.name)
                    .ok_or_else(|| unsupported(format!("unknown type {}", spelling.diagnostic)))?;
                (types.types[id.0 as usize].kind.clone(), spelling)
            } else {
                let (kind, spelling) = scalar_type(data_type, nullable)?;
                (
                    kind,
                    TypeSpelling {
                        quoted: spelling.clone(),
                        diagnostic: spelling,
                    },
                )
            }
        }
    };
    let ty = insert_type(
        types,
        SqlType {
            kind,
            nullable,
            origin: Origin::Absent {},
        },
    );
    Ok((ty, spelling))
}

pub(super) fn warn_decimal(
    program: &mut feldera_sql_rel_model::program::Program,
    position: feldera_sql_rel_model::relation_metadata::Coordinates,
) {
    program.diagnostics.before[0].messages.push(feldera_sql_rel_model::diagnostics::FrontendWarning {
        context: Origin::Absent {},
        range: super::super::source_origin(&position),
        continuation: false,
        category: "DECIMAL precision and scale unspecified".into(),
        message: "DECIMAL/NUMERIC type used without specifying precision and scale.\nThis is interpreted as DECIMAL(38, 0)".into(),
    });
}

pub(super) fn warns_on_unspecified_precision(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Numeric(ExactNumberInfo::None)
            | DataType::Decimal(ExactNumberInfo::None)
            | DataType::Dec(ExactNumberInfo::None)
    )
}

// Calcite registers these names as catalog aliases. A type declaration
// retains the alias in its source even though the type arena holds its target.
fn default_type_alias(data_type: &DataType) -> Option<(TypeKind, String)> {
    let alias = data_type.to_string().to_ascii_lowercase();
    let int = |width| TypeKind::Integer {
        width,
        signed: true,
    };
    let kind = match alias.as_str() {
        "string" | "text" => TypeKind::Character {
            fixed: false,
            length: Precision::Unspecified,
            metadata: CharacterMetadata::implicit(),
        },
        "bytea" => TypeKind::Binary {
            fixed: false,
            length: Precision::Unspecified,
        },
        "bool" => TypeKind::Boolean {},
        "int2" => int(IntegerWidth::I16),
        "int4" | "signed" => int(IntegerWidth::I32),
        "int8" | "int64" => int(IntegerWidth::I64),
        "float4" | "float32" => TypeKind::Float {
            flavor: FloatFlavor::Real,
        },
        "float8" | "float64" => TypeKind::Float {
            flavor: FloatFlavor::Double,
        },
        "number" => TypeKind::Decimal {
            precision: 38,
            scale: 0,
            explicit_scale: true,
        },
        _ => return None,
    };
    Some((kind, alias))
}

fn scalar_type(data_type: &DataType, nullable: bool) -> Result<(TypeKind, String), Failure> {
    use sqlparser_064::ast::{BinaryLength, TimezoneInfo};

    let integer =
        |width, signed, spelling: &str| (TypeKind::Integer { width, signed }, spelling.into());
    let kind = match data_type {
        DataType::Numeric(info) | DataType::Decimal(info) | DataType::Dec(info) => {
            let (precision, scale, spelling) = match *info {
                ExactNumberInfo::None => (38, 0, "DECIMAL".into()),
                ExactNumberInfo::Precision(p) if (1..=38).contains(&p) => {
                    (p, 0, format!("DECIMAL({p})"))
                }
                ExactNumberInfo::PrecisionAndScale(p, s)
                    if (1..=38).contains(&p) && (0..=p as i64).contains(&s) =>
                {
                    (p, s, format!("DECIMAL({p}, {s})"))
                }
                _ => {
                    return Err(unsupported(
                        "decimal declaration requires precision from 1 to 38 and scale from 0 to precision",
                    ));
                }
            };
            (
                TypeKind::Decimal {
                    precision: precision as u32,
                    scale: scale as u32,
                    explicit_scale: true,
                },
                spelling,
            )
        }
        DataType::TinyIntUnsigned(None) => integer(IntegerWidth::I8, false, "TINYINT UNSIGNED"),
        DataType::SmallIntUnsigned(None) => integer(IntegerWidth::I16, false, "SMALLINT UNSIGNED"),
        DataType::BigIntUnsigned(None) => integer(IntegerWidth::I64, false, "BIGINT UNSIGNED"),
        DataType::Unsigned
        | DataType::UnsignedInteger
        | DataType::IntUnsigned(None)
        | DataType::IntegerUnsigned(None) => integer(IntegerWidth::I32, false, "INTEGER UNSIGNED"),
        DataType::Date => (TypeKind::Date {}, "DATE".into()),
        DataType::Real => (
            TypeKind::Float {
                flavor: FloatFlavor::Real,
            },
            "REAL".into(),
        ),
        DataType::Binary(_) | DataType::Varbinary(_) => {
            let (fixed, width) = match data_type {
                DataType::Binary(width) => (true, *width),
                DataType::Varbinary(None) => (false, None),
                DataType::Varbinary(Some(BinaryLength::IntegerLength { length })) => {
                    (false, Some(*length))
                }
                _ => return Err(unsupported("unsupported binary length")),
            };
            let name = if fixed { "BINARY" } else { "VARBINARY" };
            let length = match width {
                Some(n) => Precision::Specified(
                    u32::try_from(n).map_err(|_| unsupported("binary length exceeds its range"))?,
                ),
                None if fixed => Precision::Specified(1),
                None => Precision::Unspecified,
            };
            (
                TypeKind::Binary { fixed, length },
                width.map_or_else(|| name.into(), |n| format!("{name}({n})")),
            )
        }
        DataType::Time(width, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
        | DataType::Timestamp(width, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            let is_time = matches!(data_type, DataType::Time(..));
            let (name, maximum) = if is_time {
                ("TIME", 9)
            } else {
                ("TIMESTAMP", 6)
            };
            let precision = Precision::Specified(width.unwrap_or(maximum).min(maximum) as u32);
            let zone = TimeZone::None;
            let kind = if is_time {
                TypeKind::Time { precision, zone }
            } else {
                TypeKind::Timestamp { precision, zone }
            };
            (
                kind,
                width.map_or_else(|| name.into(), |n| format!("{name}({n})")),
            )
        }
        DataType::Varchar(size)
        | DataType::CharVarying(size)
        | DataType::CharacterVarying(size)
        | DataType::Char(size)
        | DataType::Character(size) => {
            use sqlparser_064::ast::CharacterLength;
            let fixed = matches!(data_type, DataType::Char(_) | DataType::Character(_));
            let name = if fixed { "CHAR" } else { "VARCHAR" };
            let (length, spelling) = match size {
                None => (
                    if fixed {
                        Precision::Specified(1)
                    } else {
                        Precision::Unspecified
                    },
                    name.to_string(),
                ),
                Some(CharacterLength::IntegerLength { length, unit: None })
                    if *length <= i32::MAX as u64 =>
                {
                    (
                        Precision::Specified(*length as u32),
                        format!("{name}({length})"),
                    )
                }
                _ => return Err(unsupported("unsupported character length")),
            };
            (
                TypeKind::Character {
                    fixed,
                    length,
                    metadata: CharacterMetadata::implicit(),
                },
                spelling,
            )
        }
        _ => match checked_type(data_type, nullable)?.kind {
            Kind::Boolean => (TypeKind::Boolean {}, "BOOLEAN".into()),
            Kind::Decimal { precision, scale } => (
                TypeKind::Decimal {
                    precision: precision.into(),
                    scale: scale.into(),
                    explicit_scale: true,
                },
                format!("DECIMAL({precision}, {scale})"),
            ),
            Kind::Integer { bits: 8 } => integer(IntegerWidth::I8, true, "TINYINT"),
            Kind::Integer { bits: 16 } => integer(IntegerWidth::I16, true, "SMALLINT"),
            Kind::Integer { bits: 32 } => integer(IntegerWidth::I32, true, "INTEGER"),
            Kind::Integer { bits: 64 } => integer(IntegerWidth::I64, true, "BIGINT"),
            Kind::Double => (
                TypeKind::Float {
                    flavor: FloatFlavor::Double,
                },
                "DOUBLE".into(),
            ),
            _ => {
                return Err(unsupported(
                    "table type outside the direct projection slice",
                ));
            }
        },
    };
    Ok(kind)
}
