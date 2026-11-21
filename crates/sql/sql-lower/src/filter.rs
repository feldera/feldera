//! Compile a bound, signed-integer comparison into a DBSP filter closure.

use super::{
    closure::RowParameter,
    source_table::{range, Ids, RowLayout},
    view::reference,
    Error,
};
use feldera_sql_rel_model::{
    catalog::CatalogSchema,
    expression_objects::{ObjectKind, ObjectPosition},
    expressions::ExprId,
    literals::LiteralValue,
    operators::OperatorObject,
    program::Program,
    relation_properties::Properties,
    types::{IntegerWidth, TypeKind},
};
use serde_json::{json, Value};

enum Operand {
    Column(usize),
    Integer(i32),
}

impl Operand {
    fn resolve(program: &Program, id: ExprId) -> Result<Self, Error> {
        let expr = &program.expressions.expressions[id.0 as usize];
        match &expr.kind {
            ObjectKind::InputRef { object } => Ok(Self::Column(
                program.table.objects[object.0 as usize].field as usize,
            )),
            ObjectKind::Literal { literal } => {
                let LiteralValue::Integer { value } = &literal.value else {
                    return Err(Error("expected an integer filter literal".into()));
                };
                if !matches!(
                    program.table.type_table.types[literal.ty.0 as usize].kind,
                    TypeKind::Integer {
                        width: IntegerWidth::I32,
                        signed: true
                    }
                ) {
                    return Err(Error("expected an INT filter literal".into()));
                }
                Ok(Self::Integer(
                    value
                        .parse()
                        .map_err(|_| Error("INT literal outside range".into()))?,
                ))
            }
            _ => Err(Error("unsupported filter operand".into())),
        }
    }

    fn render(&self, schema: &CatalogSchema) -> (String, String) {
        match self {
            Self::Column(i) => (
                format!("${i}"),
                format!("`{}`", schema.columns[*i].name.name.replace('`', "``")),
            ),
            Self::Integer(n) => (n.to_string(), n.to_string()),
        }
    }

    fn lower(&self, ids: &mut Ids, parameter: &mut RowParameter) -> Result<Value, Error> {
        match self {
            Self::Column(i) => parameter.read(ids, *i),
            Self::Integer(n) => {
                let mut ty = ids.generated("DBSPTypeInteger");
                ty["width"] = 32.into();
                ty["signed"] = true.into();
                ty["code"] = "INT32".into();
                ty["mayBeNull"] = false.into();
                let mut literal = ids.generated("DBSPI32Literal");
                literal["type"] = ty;
                literal["value"] = n.to_string().into();
                Ok(literal)
            }
        }
    }
}

pub(super) struct Predicate {
    left: Operand,
    right: Operand,
    symbol: &'static str,
    opcode: &'static str,
    nullable: bool,
    position: Value,
}

impl Predicate {
    pub(super) fn resolve(program: &Program, id: u32) -> Result<(u32, Option<Self>), Error> {
        let relation = &program.properties.relations[id as usize];
        if let Properties::Scan { .. } = relation.fields {
            return Ok((id, None));
        }
        let Properties::Filter { variables } = &relation.fields else {
            return Err(Error(
                "expected a scan below the filter or projection".into(),
            ));
        };
        let [input] = program.producers.relations[id as usize].inputs.as_slice() else {
            return Err(Error("filter requires one input".into()));
        };
        if !variables.is_empty()
            || !relation.metadata.traits.is_empty()
            || !relation.metadata.hints.is_empty()
        {
            return Err(Error("unsupported filter metadata".into()));
        }
        let [root] = super::projection::expressions(program, id)? else {
            return Err(Error("filter requires one predicate".into()));
        };
        let expr = &program.expressions.expressions[root.0 as usize];
        let ObjectKind::Call {
            operator,
            arguments,
        } = &expr.kind
        else {
            return Err(Error("expected a comparison predicate".into()));
        };
        let resolved = &program.expressions.operators[operator.0 as usize];
        let OperatorObject::Builtin {
            registry,
            registry_version,
            key,
        } = &program.expressions.operator_objects[resolved.object.0 as usize]
        else {
            return Err(Error("expected a built-in comparison".into()));
        };
        if registry != "calcite-s1-fixture" || registry_version != "1" {
            return Err(Error("unsupported comparison registry".into()));
        }
        let (symbol, opcode) = match key.as_str() {
            "SqlStdOperatorTable.EQUALS" => ("=", "EQ"),
            "SqlStdOperatorTable.NOT_EQUALS" => ("<>", "NEQ"),
            "SqlStdOperatorTable.LESS_THAN" => ("<", "LT"),
            "SqlStdOperatorTable.LESS_THAN_OR_EQUAL" => ("<=", "LTE"),
            "SqlStdOperatorTable.GREATER_THAN" => (">", "GT"),
            "SqlStdOperatorTable.GREATER_THAN_OR_EQUAL" => (">=", "GTE"),
            _ => return Err(Error("unsupported comparison operator".into())),
        };
        let [left, right] = arguments.as_slice() else {
            return Err(Error("comparison requires two operands".into()));
        };
        let type_of = |id: ExprId| {
            &program.table.type_table.types
                [program.expressions.expressions[id.0 as usize].ty.0 as usize]
        };
        if !matches!(type_of(*left).kind, TypeKind::Integer { signed: true, .. })
            || type_of(*left).kind != type_of(*right).kind
        {
            return Err(Error(
                "filter requires signed integers of equal width".into(),
            ));
        }
        let ObjectPosition::Present { position, .. } = &expr.position else {
            return Err(Error("missing comparison source position".into()));
        };
        Ok((
            *input,
            Some(Self {
                left: Operand::resolve(program, *left)?,
                right: Operand::resolve(program, *right)?,
                symbol,
                opcode,
                nullable: type_of(*root).nullable,
                position: range(
                    position.line,
                    position.column,
                    position.end_line,
                    position.end_column,
                ),
            }),
        ))
    }

    pub(super) fn relation(
        &self,
        input: &CatalogSchema,
        scan: &Value,
        id: usize,
        plan: u64,
    ) -> Value {
        let (left, left_sql) = self.left.render(input);
        let (right, right_sql) = self.right.render(input);
        let condition = format!("{}({left}, {right})", self.symbol);
        let child = scan["text"].as_str().expect("scan text");
        let sql = scan["sql"].as_str().expect("scan SQL");
        json!({"id":id,"relId":id,"planId":plan,"tableScan":false,
            "digest":format!("LogicalFilter.(input=LogicalTableScan#{},condition={condition})",scan["relId"]),
            "text":format!("LogicalFilter(condition=[{condition}])\n  {child}"),
            "sql":format!("{sql}\nWHERE {left_sql} {} {right_sql}",self.symbol),"statefulSqlName":null,"origin":scan["origin"]})
    }

    pub(super) fn closure(
        &self,
        ids: &mut Ids,
        layout: &RowLayout<'_>,
        ordinal: u64,
    ) -> Result<Value, Error> {
        let mut parameter = RowParameter::new(ids, layout, ordinal)?;
        let ty = ids.boolean(self.nullable);
        let mut body = ids.node("DBSPBinaryExpression", &self.position);
        body["left"] = self.left.lower(ids, &mut parameter)?;
        body["right"] = self.right.lower(ids, &mut parameter)?;
        body["opcode"] = self.opcode.into();
        let result = if self.nullable {
            body["type"] = ty;
            let mut boolean = ids.generated("DBSPTypeBool");
            boolean["code"] = "BOOL".into();
            boolean["mayBeNull"] = false.into();
            let mut wrap = ids.node("DBSPUnaryExpression", &self.position);
            wrap["source"] = body;
            wrap["type"] = reference(&boolean)?;
            wrap["opcode"] = "WRAP_BOOL".into();
            body = wrap;
            boolean
        } else {
            body["type"] = reference(&ty)?;
            ty
        };
        parameter.close(ids, result, body, &self.position)
    }
}
