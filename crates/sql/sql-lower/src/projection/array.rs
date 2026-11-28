//! A projected ARRAY holds a fresh element type and converts its required
//! elements to that type when another element makes the array's elements nullable.

use super::{interval_count::cast, literal::ProjectedLiteral};
use crate::{
    source_table::{expression_range, ColumnLayout, Ids},
    view::reference,
    Error,
};
use feldera_sql_rel_model::{
    expression_objects::ObjectKind,
    expressions::ExprId,
    operators::OperatorObject,
    program::Program,
    types::{Cardinality, TypeKind},
};
use serde_json::Value;

pub(crate) struct ArrayConstructor {
    layout: ColumnLayout,
    elements: Vec<ProjectedLiteral>,
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
        let mut elements = Vec::with_capacity(args.len());
        let mut digests = Vec::with_capacity(args.len());
        let mut texts = Vec::with_capacity(args.len());
        for id in args {
            let cell = ProjectedLiteral::resolve(program, *id)?;
            let ty = &cell.ty;
            if ty.kind != element_type.kind || (ty.nullable && !element_type.nullable) {
                return Err(Error(
                    "ARRAY element differs from the inferred common type".into(),
                ));
            }
            digests.push(cell.digest.clone());
            texts.push(cell.value.sql.clone());
            elements.push(cell);
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
        for cell in &self.elements {
            let literal = cell.lower(ids)?;
            data.push(if self.element_nullable && !cell.ty.nullable {
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
