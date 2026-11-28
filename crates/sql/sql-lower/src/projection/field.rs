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
            ObjectKind::Literal { .. } if !ty.nullable => {
                Ok(Self::Integer(SignedInteger::resolve(program, id)?))
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
            Self::NullableInteger(n) => format!("CAST({}):{}", n.digest(), n.sql_type()),
        }
    }

    pub(super) fn sql(&self, schema: &CatalogSchema, alias: &str) -> String {
        let quote = |name: &str| format!("`{}`", name.replace('`', "``"));
        let value = match self {
            Self::Column(i) => {
                let name = &schema.columns[*i].name.name;
                if name == alias {
                    return quote(name);
                }
                quote(name)
            }
            Self::Integer(n) => n.value.to_string(),
            Self::NullableInteger(n) => format!("CAST({} AS {})", n.value, n.sql_type()),
        };
        format!("{value} AS {}", quote(alias))
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
            Self::Integer(n) => Ok(n.lower(ids)),
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
