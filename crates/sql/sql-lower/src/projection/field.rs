use super::{
    array::ArrayConstructor, computed::Computed, interval_count::cast, literal::ProjectedLiteral,
};
use crate::{
    closure::RowParameter,
    source_table::{Ids, RowLayout},
    view::reference,
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
    Computed(Box<Computed>),
    Array(Box<ArrayConstructor>),
    Literal(Box<ProjectedLiteral>),
    NullableLiteral(Box<ProjectedLiteral>),
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
        if let Some(literal) = ProjectedLiteral::wrapped(program, id)? {
            return Ok(Self::Literal(Box::new(literal)));
        }
        match &expr.kind {
            ObjectKind::InputRef { object } => Ok(Self::Column(
                program.table.objects[object.0 as usize].field as usize,
            )),
            ObjectKind::Literal { .. } => ProjectedLiteral::resolve(program, id)
                .map(Box::new)
                .map(Self::Literal),
            ObjectKind::Call { .. } if matches!(ty.kind, TypeKind::Array { .. }) => {
                ArrayConstructor::resolve(program, id)
                    .map(Box::new)
                    .map(Self::Array)
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
                let value = ProjectedLiteral::resolve(program, *input)?;
                if value.ty.kind == ty.kind
                    && !value.ty.nullable
                    && matches!(
                        ty.kind,
                        TypeKind::Character { .. }
                            | TypeKind::Binary { .. }
                            | TypeKind::Integer { signed: true, .. }
                    )
                {
                    return Ok(Self::NullableLiteral(Box::new(value)));
                }
                Err(Error("unsupported projection cast".into()))
            }
            _ => Err(Error("unsupported projection expression".into())),
        }
    }

    pub(super) fn digest(&self) -> String {
        match self {
            Self::Column(i) => format!("${i}"),
            Self::Computed(call) => call.digest(),
            Self::Array(array) => array.digest.clone(),
            Self::Literal(value) => value.digest.clone(),
            Self::NullableLiteral(value) => value.cast_digest(),
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
            Self::Computed(call) => call.sql(schema)?,
            Self::Array(array) => array.sql.clone(),
            Self::Literal(value) => value.sql(),
            Self::NullableLiteral(value) => value.cast_sql(),
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
            Self::Array(array) => array.lower(ids),
            Self::Literal(value) => value.lower(ids),
            Self::NullableLiteral(value) => {
                let literal = value.lower(ids)?;
                let target = output.column_type(ids, index);
                let expression = cast(ids, position, literal, target);
                if matches!(ty, TypeKind::Character { .. } | TypeKind::Binary { .. }) {
                    let mut clone = ids.node("DBSPCloneExpression", position);
                    clone["type"] = reference(&expression["type"])?;
                    clone["expression"] = expression;
                    Ok(clone)
                } else {
                    Ok(expression)
                }
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
