//! Exhaustive type-shape policy for the pinned Calcite grammar.
//!
//! SqlTypeName1 has no width; SqlTypeName2 and CharacterTypeName allow one
//! integer precision; SqlTypeName3 allows precision and signed scale. A bare
//! name can also be a user-defined type, even when sqlparser gives it a builtin
//! variant. Accepting that spelling does not assign it builtin SQL semantics.

use sqlparser_064::ast::{
    ArrayElemTypeDef, BinaryLength, CharacterLength, DataType, ExactNumberInfo, MapBracketKind,
    StructBracketKind, TimezoneInfo,
};

fn precision(value: u64) -> bool {
    i32::try_from(value).is_ok()
}

fn numeric(info: &ExactNumberInfo) -> bool {
    match info {
        ExactNumberInfo::None => true,
        ExactNumberInfo::Precision(p) => precision(*p),
        // Calcite parses the unsigned magnitude before applying a minus sign.
        ExactNumberInfo::PrecisionAndScale(p, scale) => {
            precision(*p) && precision(scale.unsigned_abs())
        }
    }
}

pub(super) fn allows(data_type: &DataType) -> bool {
    use DataType::*;
    match data_type {
        Character(length) | Char(length) | CharacterVarying(length) | CharVarying(length)
        | Varchar(length) => match length {
            None => true,
            Some(CharacterLength::IntegerLength { length, unit }) => {
                unit.is_none() && precision(*length)
            }
            Some(CharacterLength::Max) => false,
        },
        Binary(length) => length.is_none_or(precision),
        Varbinary(length) => match length {
            None => true,
            Some(BinaryLength::IntegerLength { length }) => precision(*length),
            Some(BinaryLength::Max) => false,
        },
        Numeric(info) | Decimal(info) | Dec(info) => numeric(info),
        Float(info) | Double(info) | BigNumeric(info) | BigDecimal(info) => {
            matches!(info, ExactNumberInfo::None)
        }
        TinyInt(width) | SmallInt(width) | Int(width) | Integer(width) | BigInt(width)
        | TinyIntUnsigned(width) | SmallIntUnsigned(width) | IntUnsigned(width)
        | IntegerUnsigned(width) | BigIntUnsigned(width)
        // These bare spellings can be user-defined names; parameters cannot.
        | Int2(width) | Int4(width) | Int8(width) | MediumInt(width)
        | Clob(width) | Blob(width) | Bytes(width) | String(width) | Bit(width)
        | VarBit(width) | TimestampNtz(width) => width.is_none(),
        Nvarchar(length) => length.is_none(),
        Time(p, zone) | Timestamp(p, zone) => match zone {
            TimezoneInfo::None | TimezoneInfo::WithTimeZone | TimezoneInfo::WithLocalTimeZone | TimezoneInfo::WithoutTimeZone => {
                p.is_none_or(precision)
            }
            // TIMETZ/TIMESTAMPTZ are identifiers, not parameterized builtins.
            TimezoneInfo::Tz => p.is_none(),
        },
        Interval { fields, precision } => fields.is_some() && precision.is_none(),
        Array(element) => match element {
            ArrayElemTypeDef::Qualified(_, size) => size.is_none(),
            ArrayElemTypeDef::None | ArrayElemTypeDef::AngleBracket(_)
            | ArrayElemTypeDef::SquareBracket(_, _) | ArrayElemTypeDef::Parenthesis(_)
            | ArrayElemTypeDef::ParenthesisNotNull(_) => false,
        },
        Map(_, _, brackets) => match brackets {
            MapBracketKind::AngleBrackets => true,
            MapBracketKind::Parentheses | MapBracketKind::ParenthesesNotNull => false,
        },
        // ROW's adapter is the only source of Parentheses in this dialect.
        Struct(_, brackets) => match brackets {
            StructBracketKind::Parentheses => true,
            StructBracketKind::AngleBrackets => false,
        },
        // These wrappers are emitted by the ROW/function nullability adapters.
        // The upstream Nullable(...) production is disabled for our dialect.
        Nullable(_) => true,
        Custom(_, _) => true, // Identifier and parameter checks run separately.
        Table(_) | NamedTable { name: _, columns: _ }
        | CharacterLargeObject(_) | CharLargeObject(_)
        | DecimalUnsigned(_) | DecUnsigned(_) | FloatUnsigned(_)
        | Int2Unsigned(_) | Int4Unsigned(_) | Int8Unsigned(_) | MediumIntUnsigned(_)
        | SignedInteger | UnsignedInteger | RealUnsigned | DoubleUnsigned(_)
        | DoublePrecisionUnsigned | Zerofill(_) | Datetime(_) | Datetime64(_, _)
        | Collate(_, _) | FixedString(_) | BitVarying(_)
        | Tuple(_) | Nested(_) | Object(_) | Enum(_, _) | Set(_) | Union(_)
        | LowCardinality(_) | Unspecified | AnyType => false,
        Uuid | TinyBlob | MediumBlob | LongBlob | UTinyInt | USmallInt | Int16 | Int32
        | Int64 | Int128 | Int256 | HugeInt | UHugeInt | UInt8 | UInt16 | UInt32
        | UInt64 | UInt128 | UInt256 | UBigInt | Signed | Unsigned | Float4 | Float32
        | Float64 | Real | Float8 | DoublePrecision | Bool | Boolean | Date | Date32
        | JSON | JSONB | Regclass | Text | TinyText | MediumText | LongText | Bytea
        | Trigger | GeometricType(_) | TsVector | TsQuery => true,
    }
}

#[cfg(test)]
mod tests {
    use super::super::parse;

    #[test]
    fn unsupported_type_fields_are_rejected_in_columns_casts_and_nested_rows() {
        for ty in [
            "INT(10)",
            "INT2 UNSIGNED",
            "FLOAT(10,2)",
            "DOUBLE UNSIGNED",
            "VARCHAR(MAX)",
            "CHAR(10 OCTETS)",
            "NVARCHAR(10)",
            "CLOB(10)",
            "BYTES(10)",
            "ARRAY<INT>",
            "DATETIME",
            "SIGNED INTEGER",
            "CHAR LARGE OBJECT",
            "ENUM('a','b')",
            "OBJECT(x INT)",
            "BIGNUMERIC(10,2)",
            "FIXEDSTRING(10)",
            "VARCHAR(2147483648)",
            "DECIMAL(10,-2147483648)",
        ] {
            for sql in [
                format!("CREATE TABLE t(x {ty})"),
                format!("SELECT CAST(NULL AS {ty})"),
                format!("CREATE TABLE t(x ROW(y {ty}))"),
            ] {
                assert!(!parse(&sql).is_ok(), "{sql}");
            }
        }
        for ty in [
            "INT",
            "INT UNSIGNED",
            "TINYINT UNSIGNED",
            "INT2",
            "FLOAT",
            "DOUBLE",
            "VARCHAR(10)",
            "VARCHAR(2147483647)",
            "DECIMAL(10,-2)",
            "NVARCHAR",
            "CLOB",
            "BYTES",
            "INT ARRAY ARRAY",
            "MAP<INT,VARCHAR>",
            "ROW(y INT NULL)",
        ] {
            let sql = format!("CREATE TABLE t(x {ty})");
            assert!(parse(&sql).is_ok(), "{sql}");
        }
    }
}
