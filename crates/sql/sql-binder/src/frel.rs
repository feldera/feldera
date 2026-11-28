//! Lower resolved scalar projections into the shared S1 expression arena.

use super::{BoundExpression, Column, ExpressionKind, Failure, unsupported};
use feldera_sql_rel_model::{
    TypeId, TypeTable,
    expressions::{
        Binding, BindingId, BindingKind, ExprId, ExprKind, Expression, ExpressionSchemaVersion,
        ExpressionTable, OperatorId, ResolvedOperator, Scope, ScopeId,
    },
    literals::{ExactDecimal, Literal, LiteralValue},
    operators::{ABS_KEY, OperatorObject, OperatorObjectId},
    source::{Identifier, Origin, Position, Source, SourceId},
    types::{
        Field, FloatFlavor, IntegerWidth, Precision, SqlType, StructKind, TimeZone, TypeKind,
        TypeSchemaVersion,
    },
};
use feldera_type_derivation_probe::model::{Kind, SqlType as BoundType};
use std::collections::HashMap;

const SOURCE: SourceId = SourceId(0);
const INPUT: BindingId = BindingId(0);
const SCOPE: ScopeId = ScopeId(0);
pub(super) const CALCITE: &str = "calcite-s1-fixture";
pub(super) const FELDERA: &str = "feldera-s1-builtins";

struct Emitter<'a> {
    sql: &'a str,
    output: ExpressionTable,
    builtins: HashMap<&'static str, OperatorObjectId>,
    types: Vec<(BoundType, TypeId)>,
}

impl<'a> Emitter<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            output: ExpressionTable {
                schema_version: ExpressionSchemaVersion::V4,
                type_table: TypeTable {
                    schema_version: TypeSchemaVersion::V0,
                    sources: vec![Source {
                        name: "(no input file)".into(),
                        text: sql.into(),
                    }],
                    types: Vec::new(),
                    roots: Vec::new(),
                },
                operator_objects: Vec::new(),
                operators: Vec::new(),
                bindings: Vec::new(),
                scopes: vec![Scope {
                    parent: None,
                    bindings: Vec::new(),
                }],
                expressions: Vec::new(),
                roots: Vec::new(),
            },
            builtins: HashMap::new(),
            types: Vec::new(),
        }
    }

    fn push_type(&mut self, ty: SqlType) -> TypeId {
        let id = TypeId(self.output.type_table.types.len() as u32);
        self.output.type_table.types.push(ty);
        id
    }

    fn set_input(&mut self, columns: &[Column]) -> Result<(), Failure> {
        let fields = columns
            .iter()
            .map(|column| {
                let name = if column.quoted {
                    column.name.clone()
                } else {
                    column.name.to_lowercase()
                };
                Ok(Field {
                    name: Identifier {
                        name,
                        // A resolved row stores the final field name verbatim.
                        quoted: true,
                        origin: Origin::Absent {},
                    },
                    ty: self.scalar_type(column.ty)?,
                })
            })
            .collect::<Result<Vec<_>, Failure>>()?;
        let row = self.push_type(SqlType {
            nullable: false,
            origin: Origin::Absent {},
            kind: TypeKind::Row {
                fields,
                struct_kind: StructKind::PeekFieldsNoExpand,
            },
        });
        self.output.bindings.push(Binding {
            kind: BindingKind::Input,
            ty: row,
        });
        self.output.scopes[0].bindings.push(INPUT);
        self.output.type_table.roots.push(row);
        Ok(())
    }

    fn scalar_type(&mut self, ty: BoundType) -> Result<TypeId, Failure> {
        if let Some((_, id)) = self.types.iter().find(|(known, _)| *known == ty) {
            return Ok(*id);
        }
        let kind = match ty.kind {
            Kind::Boolean => TypeKind::Boolean {},
            Kind::Integer { bits } => {
                let width = match bits {
                    8 => IntegerWidth::I8,
                    16 => IntegerWidth::I16,
                    32 => IntegerWidth::I32,
                    64 => IntegerWidth::I64,
                    _ => return Err(unsupported("unknown SQL integer width")),
                };
                TypeKind::Integer {
                    width,
                    signed: true,
                }
            }
            Kind::Real => TypeKind::Float {
                flavor: FloatFlavor::Real,
            },
            Kind::Double => TypeKind::Float {
                flavor: FloatFlavor::Double,
            },
            Kind::Decimal { precision, scale } => TypeKind::Decimal {
                precision: precision.into(),
                scale: scale.into(),
                explicit_scale: true,
            },
            Kind::Date => TypeKind::Date {},
            Kind::Timestamp { precision } => TypeKind::Timestamp {
                precision: Precision::Specified(precision.into()),
                zone: TimeZone::None,
            },
            _ => {
                return Err(unsupported(
                    "character and binary metadata are outside the FRel projection slice",
                ));
            }
        };
        let id = self.push_type(SqlType {
            nullable: ty.nullable,
            origin: Origin::Absent {},
            kind,
        });
        self.types.push((ty, id));
        Ok(id)
    }

    fn call(
        &mut self,
        registry: &str,
        key: &'static str,
        ty: TypeId,
        arguments: Vec<ExprId>,
    ) -> ExprKind {
        let object = *self.builtins.entry(key).or_insert_with(|| {
            let id = OperatorObjectId(self.output.operator_objects.len() as u32);
            self.output.operator_objects.push(OperatorObject::Builtin {
                registry: registry.into(),
                registry_version: "1".into(),
                key: key.into(),
            });
            id
        });
        let parameters = arguments
            .iter()
            .map(|arg| self.output.expressions[arg.0 as usize].ty)
            .collect();
        let operator = OperatorId(self.output.operators.len() as u32);
        self.output.operators.push(ResolvedOperator {
            object,
            parameters,
            result: ty,
        });
        ExprKind::Call {
            operator,
            arguments,
        }
    }

    fn expression(&mut self, bound: &BoundExpression, range: [u64; 4]) -> Result<ExprId, Failure> {
        let ty = self.scalar_type(bound.ty)?;
        // Input and literal locations stay in the bound syntax because the
        // corresponding Rex nodes cannot carry them.
        let origin = match bound.kind {
            ExpressionKind::Binary { .. } | ExpressionKind::Function { .. } => {
                source_range(self.sql, range)?
            }
            _ => Origin::Absent {},
        };
        let kind = match &bound.kind {
            ExpressionKind::Column { ordinal } => ExprKind::InputRef {
                binding: INPUT,
                field: *ordinal as u32,
            },
            ExpressionKind::TypedLiteral { literal, .. } => ExprKind::Literal {
                literal: Literal {
                    ty,
                    origin: origin.clone(),
                    value: literal_value(bound.ty, literal)?,
                },
            },
            ExpressionKind::FoldedLiteral { value } => ExprKind::Literal {
                literal: Literal {
                    ty,
                    origin: origin.clone(),
                    value: value.clone(),
                },
            },
            ExpressionKind::Binary { operator, operands } => {
                let checked = matches!(bound.ty.kind, Kind::Integer { .. });
                let key = match (*operator, checked) {
                    ("+", true) => "SqlStdOperatorTable.CHECKED_PLUS",
                    ("-", true) => "SqlStdOperatorTable.CHECKED_MINUS",
                    ("*", true) => "SqlStdOperatorTable.CHECKED_MULTIPLY",
                    ("/", true) => "SqlStdOperatorTable.CHECKED_DIVIDE",
                    ("+", false) => "SqlStdOperatorTable.PLUS",
                    ("-", false) => "SqlStdOperatorTable.MINUS",
                    ("*", false) => "SqlStdOperatorTable.MULTIPLY",
                    ("/", false) => "SqlStdOperatorTable.DIVIDE",
                    _ => {
                        return Err(unsupported(
                            "binary operator outside the FRel registry subset",
                        ));
                    }
                };
                let arguments = self.arguments(operands)?;
                self.call(CALCITE, key, ty, arguments)
            }
            ExpressionKind::Function { name, operands } => {
                let key = match name.as_str() {
                    "ABS" => ABS_KEY,
                    _ => return Err(unsupported("function outside the FRel registry subset")),
                };
                let arguments = self.arguments(operands)?;
                self.call(FELDERA, key, ty, arguments)
            }
            ExpressionKind::Coerce { inner } => {
                let argument = self.expression(inner, inner.span)?;
                self.call(CALCITE, "SqlStdOperatorTable.CAST", ty, vec![argument])
            }
        };
        let id = ExprId(self.output.expressions.len() as u32);
        self.output.expressions.push(Expression {
            ty,
            scope: SCOPE,
            origin,
            kind,
        });
        Ok(id)
    }

    fn arguments(&mut self, operands: &[BoundExpression]) -> Result<Vec<ExprId>, Failure> {
        operands
            .iter()
            .map(|operand| self.expression(operand, operand.span))
            .collect()
    }
}

pub(super) fn emit(
    sql: &str,
    expression: &BoundExpression,
    range: [u64; 4],
    columns: Option<&[Column]>,
) -> Result<ExpressionTable, Failure> {
    let mut emitter = Emitter::new(sql);
    if let Some(columns) = columns {
        emitter.set_input(columns)?;
    }
    let root = emitter.expression(expression, range)?;
    let result = emitter.output.expressions[root.0 as usize].ty;
    emitter.output.roots.push(root);
    emitter.output.type_table.roots.push(result);
    emitter
        .output
        .validate(Default::default(), Default::default(), Default::default())
        .map_err(|error| unsupported(format!("invalid FRel expression: {error}")))?;
    Ok(emitter.output)
}

fn source_range(
    sql: &str,
    [line, column, end_line, end_column]: [u64; 4],
) -> Result<Origin, Failure> {
    if line == 0 && end_line == 0 {
        return Ok(Origin::Absent {});
    }
    let convert = |line: u64, column: u64, start: bool| {
        let line = u32::try_from(line).ok()?;
        let chars = usize::try_from(column.checked_sub(1)?).ok()?;
        let text = sql.split('\n').nth(line.checked_sub(1)? as usize)?;
        let prefix: usize = text.chars().take(chars).map(char::len_utf16).sum();
        if text.chars().count() < chars {
            return None;
        }
        Some(Position {
            line,
            column: u32::try_from(prefix).ok()? + u32::from(start),
        })
    };
    let start =
        convert(line, column, true).ok_or_else(|| unsupported("invalid FRel source start"))?;
    let end = convert(end_line, end_column, false)
        .ok_or_else(|| unsupported("invalid FRel source end"))?;
    Ok(Origin::Source {
        source: SOURCE,
        start,
        end,
    })
}

fn literal_value(ty: BoundType, raw: &str) -> Result<LiteralValue, Failure> {
    if raw.eq_ignore_ascii_case("NULL") {
        return Ok(LiteralValue::Null {});
    }
    let invalid =
        || unsupported("literal cannot be represented exactly in the FRel projection slice");
    let value = match ty.kind {
        Kind::Boolean => LiteralValue::Boolean {
            value: raw.to_ascii_lowercase().parse().map_err(|_| invalid())?,
        },
        Kind::Integer { bits } => {
            let value: i64 = raw.parse().map_err(|_| invalid())?;
            let fits = match bits {
                8 => i8::try_from(value).is_ok(),
                16 => i16::try_from(value).is_ok(),
                32 => i32::try_from(value).is_ok(),
                64 => true,
                _ => false,
            };
            if !fits {
                return Err(invalid());
            }
            LiteralValue::Integer {
                value: value.to_string(),
            }
        }
        Kind::Decimal { scale, .. } => {
            let (integer, fraction) = raw.split_once('.').unwrap_or((raw, ""));
            let (negative, integer) = integer
                .strip_prefix('-')
                .map_or((false, integer), |part| (true, part));
            let scale = usize::from(scale);
            if (integer.is_empty() && fraction.is_empty())
                || fraction.len() > scale
                || !integer
                    .bytes()
                    .chain(fraction.bytes())
                    .all(|byte| byte.is_ascii_digit())
            {
                return Err(invalid());
            }
            let coefficient = format!("{integer}{fraction}{}", "0".repeat(scale - fraction.len()));
            let digits = coefficient.trim_start_matches('0');
            let coefficient = if digits.is_empty() {
                "0".into()
            } else if negative {
                format!("-{digits}")
            } else {
                digits.into()
            };
            LiteralValue::Decimal {
                value: ExactDecimal {
                    coefficient,
                    scale: scale as i32,
                },
            }
        }
        Kind::Real => {
            let value: f32 = raw.parse().map_err(|_| invalid())?;
            if !value.is_finite() {
                return Err(invalid());
            }
            LiteralValue::Float32 {
                bits: value.to_bits(),
            }
        }
        Kind::Double => {
            let value: f64 = raw.parse().map_err(|_| invalid())?;
            if !value.is_finite() {
                return Err(invalid());
            }
            LiteralValue::Float64 {
                bits: value.to_bits(),
            }
        }
        _ => return Err(invalid()),
    };
    Ok(value)
}
