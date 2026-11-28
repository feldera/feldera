use super::computed::Computed;
use crate::{
    closure::RowParameter,
    integer::SignedInteger,
    source_table::{Ids, RowLayout},
    Error,
};
use feldera_sql_rel_model::{
    catalog::CatalogSchema,
    expression_objects::{ObjectKind, ObjectPosition},
    expressions::ExprId,
    operators::OperatorObject,
    program::Program,
    types::TypeKind,
    TypeId,
};
use serde_json::Value;

pub(crate) enum Field {
    Column(usize),
    Integer(SignedInteger),
    NullableInteger(SignedInteger),
    Double(DoubleLiteral),
    Computed(Box<Computed>),
}

/// Rex digests for approximate literals include their SQL type inside calls.
#[derive(Clone, Copy)]
pub(crate) struct DoubleLiteral(Option<u64>);

impl DoubleLiteral {
    pub(super) fn resolve(program: &Program, id: ExprId) -> Result<Self, Error> {
        use feldera_sql_rel_model::{literals::LiteralValue, types::FloatFlavor};
        let expr = &program.expressions.expressions[id.0 as usize];
        let ty = &program.table.type_table.types[expr.ty.0 as usize];
        let ObjectKind::Literal { literal } = &expr.kind else {
            return Err(Error("expected a DOUBLE literal".into()));
        };
        if !matches!(
            ty.kind,
            TypeKind::Float {
                flavor: FloatFlavor::Double
            }
        ) {
            return Err(Error("expected a DOUBLE operand".into()));
        }
        match literal.value {
            LiteralValue::Float64 { bits } if !ty.nullable => Ok(Self(Some(bits))),
            LiteralValue::Null {} if ty.nullable => Ok(Self(None)),
            _ => Err(Error("unsupported DOUBLE literal type".into())),
        }
    }

    pub(super) fn is_null(self) -> bool {
        self.0.is_none()
    }

    fn spelling(self) -> (String, String) {
        self.0.map_or_else(
            || ("null".into(), "NULL".into()),
            |bits| crate::values::double_spelling(f64::from_bits(bits)),
        )
    }

    pub(super) fn digest(self) -> String {
        format!("{}:DOUBLE", self.spelling().0)
    }
    pub(super) fn sql(self) -> String {
        self.spelling().1
    }

    pub(super) fn lower(self, ids: &mut Ids) -> Value {
        let mut expression = ids.generated("DBSPDoubleLiteral");
        expression["type"] =
            ids.singleton(feldera_sql_ir::TypeKind::Double, "DOUBLE", self.is_null());
        if let Some(bits) = self.0 {
            expression["value"] = (bits as i64).into();
        }
        expression
    }
}

pub(super) fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub(super) fn column_name(schema: &CatalogSchema, index: usize) -> Result<&str, Error> {
    schema
        .columns
        .get(index)
        .map(|column| column.name.name.as_str())
        .ok_or_else(|| Error(format!("projection field {index} outside input schema")))
}

impl Field {
    pub(super) fn resolve(program: &Program, id: ExprId, output: TypeId) -> Result<Self, Error> {
        let expr = &program.expressions.expressions[id.0 as usize];
        if expr.ty != output {
            return Err(Error(
                "projection type differs from the output column".into(),
            ));
        }
        let ty = &program.table.type_table.types[output.0 as usize];
        match &expr.kind {
            ObjectKind::InputRef { object } => Ok(Self::Column(
                program.table.objects[object.0 as usize].field as usize,
            )),
            ObjectKind::Literal { .. }
                if matches!(
                    ty.kind,
                    TypeKind::Float {
                        flavor: feldera_sql_rel_model::types::FloatFlavor::Double
                    }
                ) =>
            {
                DoubleLiteral::resolve(program, id).map(Self::Double)
            }
            ObjectKind::Literal { .. } if !ty.nullable => {
                Ok(Self::Integer(SignedInteger::resolve(program, id)?))
            }
            ObjectKind::Call { .. } if matches!(expr.position, ObjectPosition::Present { .. }) => {
                Computed::resolve(program, id)
                    .map(Box::new)
                    .map(Self::Computed)
            }
            ObjectKind::Call {
                operator,
                arguments,
            } if ty.nullable && matches!(expr.position, ObjectPosition::Absent {}) => {
                let resolved = &program.expressions.operators[operator.0 as usize];
                let builtin = &program.expressions.operator_objects[resolved.object.0 as usize];
                let [input] = arguments.as_slice() else {
                    return Err(Error("unsupported projection call".into()));
                };
                if !matches!(builtin, OperatorObject::Builtin { registry, registry_version, key }
                    if registry == "calcite-s1-fixture" && registry_version == "1" && key == "SqlStdOperatorTable.CAST")
                {
                    return Err(Error("unsupported projection call".into()));
                }
                let literal = SignedInteger::resolve(program, *input)?;
                if matches!(ty.kind, TypeKind::Integer { signed: true, width } if width == literal.width)
                {
                    return Ok(Self::NullableInteger(literal));
                }
                Err(Error("unsupported projection cast".into()))
            }
            _ => Err(Error("unsupported projection expression".into())),
        }
    }

    pub(super) fn digest(&self) -> String {
        match self {
            Self::Column(i) => format!("${i}"),
            Self::Integer(n) => n.digest(),
            Self::Double(n) => n.digest(),
            Self::NullableInteger(n) => format!("CAST({}):{}", n.digest(), n.sql_type()),
            Self::Computed(call) => call.digest(),
        }
    }

    pub(super) fn sql(&self, schema: &CatalogSchema, alias: &str) -> Result<String, Error> {
        let value = match self {
            Self::Column(i) => {
                let name = column_name(schema, *i)?;
                if name == alias {
                    return Ok(quote(name));
                }
                quote(name)
            }
            Self::Integer(n) => n.value.to_string(),
            Self::Double(n) if n.is_null() => "CAST(NULL AS DOUBLE)".into(),
            Self::Double(n) => n.sql(),
            Self::NullableInteger(n) => format!("CAST({} AS {})", n.value, n.sql_type()),
            Self::Computed(call) => call.sql(schema)?,
        };
        if alias
            .get(..5)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("EXPR$"))
        {
            Ok(value)
        } else {
            Ok(format!("{value} AS {}", quote(alias)))
        }
    }

    pub(super) fn lower(
        &self,
        ids: &mut Ids,
        parameter: &mut RowParameter,
        ty: &TypeKind,
        output: &RowLayout<'_>,
        index: usize,
        position: &Value,
    ) -> Result<Value, Error> {
        match self {
            Self::Computed(call) => call.lower(ids, parameter),
            Self::Integer(n) => Ok(n.lower(ids)),
            Self::Double(n) => Ok(n.lower(ids)),
            Self::NullableInteger(n) => {
                let mut cast = ids.node("DBSPCastExpression", position);
                cast["source"] = n.lower(ids);
                cast["type"] = output.column_type(ids, index);
                cast["safe"] = "SqlUnsafe".into();
                Ok(cast)
            }
            Self::Column(i) => {
                let field = parameter.read(ids, *i)?;
                if matches!(
                    ty,
                    TypeKind::Character { .. }
                        | TypeKind::Binary { .. }
                        | TypeKind::Array { .. }
                        | TypeKind::Map { .. }
                        | TypeKind::Row { .. }
                        | TypeKind::Uuid {}
                        | TypeKind::Variant {}
                ) {
                    let mut clone = ids.generated("DBSPCloneExpression");
                    clone["type"] = field["type"].clone();
                    clone["expression"] = field;
                    Ok(clone)
                } else {
                    Ok(field)
                }
            }
        }
    }
}
