//! A projected ARRAY holds a fresh element type and converts its required
//! elements to that type when another element makes the array's elements nullable.

use super::interval_count::cast;
use crate::{
    source_table::{expression_range, ColumnLayout, Ids},
    values::{scalar::ScalarLiteral, type_name},
    view::reference,
    Error,
};
use feldera_sql_rel_model::{
    expression_objects::ObjectKind,
    expressions::ExprId,
    literals::LiteralValue,
    operators::OperatorObject,
    program::Program,
    types::{Cardinality, IntegerWidth, Precision, SqlType, TypeKind},
};
use serde_json::Value;

pub(crate) struct ArrayConstructor {
    layout: ColumnLayout,
    elements: Vec<(SqlType, ScalarLiteral)>,
    element_nullable: bool,
    position: Value,
    pub(super) digest: String,
    pub(super) sql: String,
}

impl ArrayConstructor {
    pub(super) fn resolve(program: &Program, id: ExprId) -> Result<Self, Error> {
        let expr = &program.expressions.expressions[id.0 as usize];
        let ObjectKind::Call {
            operator,
            arguments: args,
        } = &expr.kind
        else {
            return Err(Error("expected an ARRAY constructor".into()));
        };
        let operator = &program.expressions.operators[operator.0 as usize];
        if program.expressions.operator_objects[operator.object.0 as usize]
            != OperatorObject::array_constructor()
        {
            return Err(Error("unsupported array projection call".into()));
        }
        let result = expr.ty;
        let types = &program.table.type_table;
        let result_type = &types.types[result.0 as usize];
        let TypeKind::Array {
            element,
            cardinality: Cardinality::Unbounded,
        } = result_type.kind
        else {
            return Err(Error("ARRAY constructor requires an array result".into()));
        };
        if result_type.nullable {
            return Err(Error("ARRAY constructor requires a non-null result".into()));
        }
        let element_type = &types.types[element.0 as usize];
        let mut type_text = type_name::render(types, element)?;
        if matches!(element_type.kind, TypeKind::Character { .. }) {
            type_text.push_str(" CHARACTER SET \"UTF-8\"");
        }
        let mut elements = Vec::with_capacity(args.len());
        let mut digests = Vec::with_capacity(args.len());
        let mut texts = Vec::with_capacity(args.len());
        for id in args {
            let expr = &program.expressions.expressions[id.0 as usize];
            let ObjectKind::Literal { literal } = &expr.kind else {
                return Err(Error("expected a scalar literal ARRAY element".into()));
            };
            let ty = &types.types[expr.ty.0 as usize];
            if ty.kind != element_type.kind || (ty.nullable && !element_type.nullable) {
                return Err(Error(
                    "ARRAY element differs from the inferred common type".into(),
                ));
            }
            let value = ScalarLiteral::resolve(ty, &literal.value)?;
            let skip_type = !ty.nullable
                && match (&ty.kind, &literal.value) {
                    (
                        TypeKind::Boolean {}
                        | TypeKind::Date {}
                        | TypeKind::Integer {
                            width: IntegerWidth::I32,
                            signed: true,
                        }
                        | TypeKind::Time {
                            precision: Precision::Specified(0),
                            ..
                        }
                        | TypeKind::Timestamp {
                            precision: Precision::Specified(0),
                            ..
                        },
                        _,
                    ) => true,
                    (
                        TypeKind::Character {
                            fixed: true,
                            length: Precision::Specified(n),
                            ..
                        },
                        LiteralValue::Character { value },
                    ) => value.encode_utf16().count() == *n as usize,
                    _ => false,
                };
            digests.push(if skip_type {
                value.digest.clone()
            } else {
                format!("{}:{type_text}", value.digest)
            });
            texts.push(value.sql.clone());
            elements.push((ty.clone(), value));
        }
        let layout = types
            .column_type(result)
            .ok_or_else(|| Error("unsupported ARRAY type".into()))?;
        Ok(Self {
            layout: ColumnLayout::lower(&layout)?,
            elements,
            element_nullable: element_type.nullable,
            position: expression_range(&expr.position),
            digest: format!("ARRAY({})", digests.join(", ")),
            sql: format!("ARRAY[{}]", texts.join(", ")),
        })
    }

    pub(super) fn lower(&self, ids: &mut Ids) -> Result<Value, Error> {
        let ty = self.layout.node(ids);
        let element = reference(&ty["typeArgs"][0])?;
        let mut data = Vec::with_capacity(self.elements.len());
        for (ty, cell) in &self.elements {
            let mut scalar = ColumnLayout::scalar(ty)?.node(ids);
            let literal = cell.expression(ids, ty, &mut scalar, None)?;
            data.push(if self.element_nullable && !ty.nullable {
                cast(ids, &self.position, literal, element.clone())
            } else {
                literal
            });
        }
        let mut array = ids.node("DBSPArrayExpression", &self.position);
        array["type"] = ty;
        array["data"] = data.into();
        Ok(array)
    }
}

pub(super) enum ArrayLengthArgument {
    Constructor(Box<ArrayConstructor>),
    Null,
}

impl ArrayLengthArgument {
    pub(super) fn function(&self) -> &'static str {
        match self {
            Self::Null => "cardinalityVecN",
            Self::Constructor(_) => "cardinalityVec",
        }
    }

    pub(super) fn digest(&self) -> &str {
        match self {
            Self::Null => "null:NULL",
            Self::Constructor(array) => &array.digest,
        }
    }

    pub(super) fn sql(&self) -> &str {
        match self {
            Self::Null => "NULL",
            Self::Constructor(array) => &array.sql,
        }
    }

    pub(super) fn lower(&self, ids: &mut Ids) -> Result<Value, Error> {
        if let Self::Constructor(array) = self {
            return array.lower(ids);
        }
        // nullLiteralToNullArray supplies the runtime overload with a nullable NULL ARRAY.
        let element = ids.singleton(feldera_sql_ir::TypeKind::Null, "NULL", true);
        let mut ty = ids.generated("DBSPTypeArray");
        ty["typeArgs"] = vec![element].into();
        ty["code"] = "ARRAY".into();
        ty["name"] = "Array".into();
        ty["mayBeNull"] = true.into();
        let mut null = ids.generated("DBSPArrayExpression");
        null["type"] = ty;
        Ok(null)
    }
}
