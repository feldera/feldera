//! Arrow fields carry the SQL facets used by P0's common inference rules.
use arrow_schema::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, plan_datafusion_err, plan_err};
use datafusion_expr::planner::TypePlanner;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_sql::sqlparser::ast::DataType as AstType;
use feldera_type_derivation_probe::{
    model::{DerivedType, Kind, Outcome, SqlType},
    rewritten, sampled_arity,
};
use std::{collections::HashMap, sync::Arc};

const SQL_KIND: &str = "feldera.probe.sql_kind";

fn arrow_type(kind: Kind) -> Result<DataType> {
    Ok(match kind {
        Kind::Boolean => DataType::Boolean,
        Kind::Integer { bits: 8 } => DataType::Int8,
        Kind::Integer { bits: 16 } => DataType::Int16,
        Kind::Integer { bits: 32 } => DataType::Int32,
        Kind::Integer { bits: 64 } => DataType::Int64,
        Kind::Real => DataType::Float32,
        Kind::Double => DataType::Float64,
        Kind::Decimal { precision, scale } => DataType::Decimal128(precision, scale as i8),
        Kind::Character { .. } => DataType::Utf8,
        Kind::Binary { .. } => DataType::Binary,
        Kind::Date => DataType::Date32,
        Kind::Timestamp { precision: 6 } => DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => return plan_err!("unsupported SQL type {kind:?}"),
    })
}

pub fn field(name: &str, ty: SqlType) -> Result<FieldRef> {
    let mut field = Field::new(name, arrow_type(ty.kind)?, ty.nullable);
    // Arrow has no fixed/variable SQL string width. The other supported
    // scalar kinds are recoverable from their physical type.
    if matches!(ty.kind, Kind::Character { .. } | Kind::Binary { .. }) {
        let kind = serde_json::to_string(&ty.kind).map_err(|e| plan_datafusion_err!("{e}"))?;
        field.set_metadata(HashMap::from([(SQL_KIND.into(), kind)]));
    }
    Ok(Arc::new(field))
}

pub fn sql_type(field: &Field) -> Result<SqlType> {
    let kind = if let Some(metadata) = field.metadata().get(SQL_KIND) {
        let kind = serde_json::from_str(metadata).map_err(|e| plan_datafusion_err!("{e}"))?;
        if &arrow_type(kind)? != field.data_type() {
            return plan_err!("SQL field metadata disagrees with Arrow type");
        }
        kind
    } else {
        match field.data_type() {
            DataType::Boolean => Kind::Boolean,
            DataType::Int8 => Kind::Integer { bits: 8 },
            DataType::Int16 => Kind::Integer { bits: 16 },
            DataType::Int32 => Kind::Integer { bits: 32 },
            DataType::Int64 => Kind::Integer { bits: 64 },
            DataType::Float32 => Kind::Real,
            DataType::Float64 => Kind::Double,
            DataType::Decimal128(precision, scale) if *scale >= 0 => Kind::Decimal {
                precision: *precision,
                scale: *scale as u8,
            },
            DataType::Date32 => Kind::Date,
            DataType::Timestamp(TimeUnit::Microsecond, None) => Kind::Timestamp { precision: 6 },
            _ => return plan_err!("Arrow field loses SQL type information: {field}"),
        }
    };
    let ty = SqlType {
        kind,
        nullable: field.is_nullable(),
    };
    if !ty.is_valid() {
        return plan_err!("unsupported SQL field {field}");
    }
    Ok(ty)
}

#[derive(Debug)]
pub struct Types;

impl TypePlanner for Types {
    fn plan_type_field(&self, ast_type: &AstType) -> Result<Option<FieldRef>> {
        ast_field("", ast_type, true).map(Some)
    }
}

pub fn ast_field(name: &str, ast_type: &AstType, nullable: bool) -> Result<FieldRef> {
    let spelling = ast_type.to_string().replace(", ", ",").to_ascii_uppercase();
    let spelling = if spelling == "INT" {
        "INTEGER"
    } else {
        &spelling
    };
    let ty = SqlType::parse(spelling, nullable).map_err(|error| plan_datafusion_err!("{error}"))?;
    field(name, ty)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SignatureFunction {
    name: String,
    operation: String,
    signature: Signature,
}

impl ScalarUDFImpl for SignatureFunction {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        plan_err!("{} needs SQL argument fields", self.name)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let types = args
            .arg_fields
            .iter()
            .map(|field| sql_type(field))
            .collect::<Result<Vec<_>>>()?;
        match rewritten::infer(&self.operation, &types) {
            Outcome::Bound {
                result: DerivedType::Scalar(result),
                ..
            } => field(&self.name, result),
            Outcome::Rejected { .. } => plan_err!("Feldera rejects {}({types:?})", self.name),
            _ => plan_err!("outside signature probe scope: {}({types:?})", self.name),
        }
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!("{} is an SQL planning signature", self.name)
    }
}

pub fn function(name: &str) -> Option<Arc<ScalarUDF>> {
    let operation = name.to_ascii_uppercase();
    let arity = sampled_arity(&operation)?;
    Some(Arc::new(ScalarUDF::new_from_impl(SignatureFunction {
        name: name.to_owned(),
        operation,
        signature: Signature::any(arity, Volatility::Immutable),
    })))
}
