//! Lower a bound table predicate into its relational provenance and DBSP closure.

mod boolean;

use super::{
    closure::RowParameter,
    integer::SignedInteger,
    source_table::{range, Ids, RowLayout},
    view::reference,
    Error,
};
use boolean::BooleanField;
use feldera_sql_rel_model::{
    catalog::CatalogSchema,
    expression_objects::{ObjectKind, ObjectPosition},
    expressions::ExprId,
    operators::OperatorObject,
    program::Program,
    relation_properties::Properties,
    types::{IntegerWidth, TypeKind},
};
use serde_json::{json, Value};

enum Operand {
    Column(usize),
    Integer(SignedInteger),
    CastColumn {
        column: usize,
        width: IntegerWidth,
        nullable: bool,
        position: Value,
    },
}

impl Operand {
    fn resolve(program: &Program, id: ExprId) -> Result<Self, Error> {
        let expr = &program.expressions.expressions[id.0 as usize];
        match &expr.kind {
            ObjectKind::InputRef { object } => Ok(Self::Column(
                program.table.objects[object.0 as usize].field as usize,
            )),
            ObjectKind::Literal { .. } => Ok(Self::Integer(SignedInteger::resolve(program, id)?)),
            ObjectKind::Call {
                operator,
                arguments,
            } => {
                let resolved = &program.expressions.operators[operator.0 as usize];
                let builtin = &program.expressions.operator_objects[resolved.object.0 as usize];
                let [argument] = arguments.as_slice() else {
                    return Err(Error("filter cast requires one column".into()));
                };
                if !matches!(builtin, OperatorObject::Builtin {registry, registry_version, key}
                    if registry == "calcite-s1-fixture" && registry_version == "1" && key == "SqlStdOperatorTable.CAST")
                {
                    return Err(Error("unsupported filter operand call".into()));
                }
                let input = &program.expressions.expressions[argument.0 as usize];
                let ObjectKind::InputRef { object } = input.kind else {
                    return Err(Error("expected an integer column in filter cast".into()));
                };
                let types = &program.table.type_table.types;
                let target = &types[expr.ty.0 as usize];
                let source = &types[input.ty.0 as usize];
                let TypeKind::Integer {
                    width,
                    signed: true,
                } = target.kind
                else {
                    return Err(Error("expected signed integer filter cast".into()));
                };
                if source.nullable != target.nullable
                    || !matches!(source.kind, TypeKind::Integer { width: from, signed: true } if from.bits() < width.bits())
                {
                    return Err(Error("expected a widening integer filter cast".into()));
                }
                let ObjectPosition::Present { position, .. } = &expr.position else {
                    return Err(Error("missing filter cast position".into()));
                };
                Ok(Self::CastColumn {
                    column: program.table.objects[object.0 as usize].field as usize,
                    width,
                    nullable: target.nullable,
                    position: range(
                        position.line,
                        position.column,
                        position.end_line,
                        position.end_column,
                    ),
                })
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
            Self::Integer(n) => (n.value.to_string(), n.value.to_string()),
            Self::CastColumn {
                column,
                width,
                nullable,
                ..
            } => {
                let (field, sql) = Self::Column(*column).render(schema);
                let name = width.signed_sql_name();
                let required = if *nullable { "" } else { " NOT NULL" };
                (
                    format!("CAST({field}):{name}{required}"),
                    format!("CAST({sql} AS {name})"),
                )
            }
        }
    }

    fn lower(&self, ids: &mut Ids, parameter: &mut RowParameter) -> Result<Value, Error> {
        match self {
            Self::Column(i) => parameter.read(ids, *i),
            Self::Integer(n) => Ok(n.lower(ids)),
            Self::CastColumn {
                column,
                width,
                nullable,
                position,
            } => {
                let mut ty = ids.node("DBSPTypeInteger", &range(0, 0, 0, 0));
                let bits = width.bits();
                ty["width"] = bits.into();
                ty["signed"] = true.into();
                ty["code"] = format!("INT{bits}").into();
                ty["mayBeNull"] = (*nullable).into();
                let mut cast = ids.node("DBSPCastExpression", position);
                cast["source"] = parameter.read(ids, *column)?;
                cast["type"] = ty;
                cast["safe"] = "SqlUnsafe".into();
                Ok(cast)
            }
        }
    }
}

pub(super) struct Predicate {
    condition: Condition,
    position: Value,
}

enum Condition {
    Field(BooleanField),
    Comparison {
        left: Operand,
        right: Operand,
        symbol: &'static str,
        opcode: &'static str,
        nullable: bool,
    },
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
        Ok((*input, Some(Self::from_expression(program, *root)?)))
    }

    fn from_expression(program: &Program, root: ExprId) -> Result<Self, Error> {
        let type_of = |id: ExprId| {
            &program.table.type_table.types
                [program.expressions.expressions[id.0 as usize].ty.0 as usize]
        };
        if !matches!(type_of(root).kind, TypeKind::Boolean {}) {
            return Err(Error("filter requires a BOOLEAN predicate".into()));
        }
        let expr = &program.expressions.expressions[root.0 as usize];
        let position = match &expr.position {
            ObjectPosition::Present { position, .. } => range(
                position.line,
                position.column,
                position.end_line,
                position.end_column,
            ),
            ObjectPosition::Absent {} => range(0, 0, 0, 0),
        };
        if matches!(expr.kind, ObjectKind::InputRef { .. }) {
            return Ok(Self {
                position,
                condition: Condition::Field(BooleanField::resolve(program, root, false)?),
            });
        }
        let ObjectKind::Call {
            operator,
            arguments,
        } = &expr.kind
        else {
            return Err(Error("expected a Boolean field or a predicate call".into()));
        };
        if matches!(expr.position, ObjectPosition::Absent {}) {
            return Err(Error("missing filter call source position".into()));
        }
        let resolved = &program.expressions.operators[operator.0 as usize];
        let OperatorObject::Builtin {
            registry,
            registry_version,
            key,
        } = &program.expressions.operator_objects[resolved.object.0 as usize]
        else {
            return Err(Error("expected a built-in filter predicate".into()));
        };
        if registry != "calcite-s1-fixture" || registry_version != "1" {
            return Err(Error("unsupported filter operator registry".into()));
        }
        if key == "SqlStdOperatorTable.IS_TRUE" {
            let [argument] = arguments.as_slice() else {
                return Err(Error("IS TRUE requires one operand".into()));
            };
            return Ok(Self {
                position,
                condition: Condition::Field(BooleanField::resolve(program, *argument, true)?),
            });
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
        if !matches!(type_of(*left).kind, TypeKind::Integer { signed: true, .. })
            || type_of(*left).kind != type_of(*right).kind
        {
            return Err(Error(
                "filter requires signed integers of equal width".into(),
            ));
        }
        Ok(Self {
            position,
            condition: Condition::Comparison {
                left: Operand::resolve(program, *left)?,
                right: Operand::resolve(program, *right)?,
                symbol,
                opcode,
                nullable: type_of(root).nullable,
            },
        })
    }

    pub(super) fn relation(
        &self,
        input: &CatalogSchema,
        scan: &Value,
        id: usize,
        plan: u64,
    ) -> Value {
        let (condition, predicate_sql) = match &self.condition {
            Condition::Comparison {
                left,
                right,
                symbol,
                ..
            } => {
                let (left, left_sql) = left.render(input);
                let (right, right_sql) = right.render(input);
                (
                    format!("{symbol}({left}, {right})"),
                    format!("{left_sql} {symbol} {right_sql}"),
                )
            }
            Condition::Field(field) => field.render(input),
        };
        let child = scan["text"].as_str().expect("scan text");
        let sql = scan["sql"].as_str().expect("scan SQL");
        json!({"id":id,"relId":id,"planId":plan,"tableScan":false,
            "digest":format!("LogicalFilter.(input=LogicalTableScan#{},condition={condition})",scan["relId"]),
            "text":format!("LogicalFilter(condition=[{condition}])\n  {child}"),
            "sql":format!("{sql}\nWHERE {predicate_sql}"),"statefulSqlName":null,"origin":scan["origin"]})
    }

    pub(super) fn closure(
        &self,
        ids: &mut Ids,
        layout: &RowLayout<'_>,
        ordinal: u64,
    ) -> Result<Value, Error> {
        let mut parameter = RowParameter::new(ids, layout, ordinal)?;
        let (result, body) = match &self.condition {
            Condition::Field(field) => field.lower(ids, &mut parameter, &self.position)?,
            Condition::Comparison {
                left,
                right,
                opcode,
                nullable,
                ..
            } => {
                let ty = ids.singleton(feldera_sql_ir::TypeKind::Bool, "BOOL", *nullable);
                let mut body = ids.node("DBSPBinaryExpression", &self.position);
                body["left"] = left.lower(ids, &mut parameter)?;
                body["right"] = right.lower(ids, &mut parameter)?;
                body["opcode"] = (*opcode).into();
                let result = if *nullable {
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
                (result, body)
            }
        };
        parameter.close(ids, result, body, &self.position)
    }
}
