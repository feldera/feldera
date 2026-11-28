//! Lower CAST(datetime subtraction AS integer), preserving the interval unit
//! and each implicit conversion in Java's circuit and plan.

use crate::{
    source_table::{range, Ids},
    values::temporal::{TemporalKind, TemporalLiteral},
    view::reference,
    Error,
};
use feldera_sql_rel_model::{
    expression_objects::{ObjectKind, ObjectPosition},
    expressions::ExprId,
    operators::OperatorObject,
    program::Program,
    relation_metadata::Coordinates,
    types::{IntegerWidth, IntervalEnd, IntervalUnit, Precision, TimeZone, TypeKind},
};
use serde_json::Value;

fn operands<'a>(
    program: &'a Program,
    id: ExprId,
    builtin: &str,
) -> Result<(&'a [ExprId], Value), Error> {
    let expr = &program.expressions.expressions[id.0 as usize];
    let ObjectKind::Call {
        operator,
        arguments,
    } = &expr.kind
    else {
        return Err(Error(format!("expected a {builtin} call")));
    };
    let resolved = &program.expressions.operators[operator.0 as usize];
    if !matches!(&program.expressions.operator_objects[resolved.object.0 as usize],
        OperatorObject::Builtin { registry, registry_version, key } if registry == "calcite-s1-fixture"
            && registry_version == "1" && key == builtin)
    {
        return Err(Error(format!("expected a {builtin} call")));
    }
    let ObjectPosition::Present { position, .. } = &expr.position else {
        return Err(Error(format!("missing position for {builtin}")));
    };
    Ok((arguments, location(position)))
}

fn location(position: &Coordinates) -> Value {
    range(
        position.line,
        position.column,
        position.end_line,
        position.end_column,
    )
}

pub(super) fn cast(ids: &mut Ids, position: &Value, source: Value, ty: Value) -> Value {
    let mut expr = ids.node("DBSPCastExpression", position);
    expr["source"] = source;
    expr["type"] = ty;
    expr["safe"] = "SqlUnsafe".into();
    expr
}

struct Datetime {
    literal: TemporalLiteral,
    timestamp_cast: Option<(u32, Value)>,
}

impl Datetime {
    fn resolve(program: &Program, id: ExprId) -> Result<Self, Error> {
        let expr = &program.expressions.expressions[id.0 as usize];
        let ty = &program.table.type_table.types[expr.ty.0 as usize];
        let (literal, timestamp_cast) = if matches!(expr.kind, ObjectKind::Call { .. }) {
            let TypeKind::Timestamp {
                precision: Precision::Specified(p),
                zone: TimeZone::None,
            } = ty.kind
            else {
                return Err(Error("expected a cast to a local timestamp".into()));
            };
            let (args, pos) = operands(program, id, "SqlStdOperatorTable.CAST")?;
            let &[source] = args else {
                return Err(Error("timestamp cast requires one operand".into()));
            };
            (
                &program.expressions.expressions[source.0 as usize],
                Some((p, pos)),
            )
        } else {
            (expr, None)
        };
        let ObjectKind::Literal { literal: value } = &literal.kind else {
            return Err(Error(
                "expected a datetime literal in interval subtraction".into(),
            ));
        };
        let source = &program.table.type_table.types[literal.ty.0 as usize];
        if ty.nullable || source.nullable {
            return Err(Error(
                "nullable datetime subtraction is outside the supported projections".into(),
            ));
        }
        let literal = TemporalLiteral::resolve(&source.kind, &value.value)?;
        if timestamp_cast.is_some() && literal.kind != TemporalKind::Date {
            return Err(Error(
                "expected DATE as the source of a datetime cast".into(),
            ));
        }
        Ok(Self {
            literal,
            timestamp_cast,
        })
    }

    fn kind(&self) -> TemporalKind {
        self.timestamp_cast
            .as_ref()
            .map_or(self.literal.kind, |(p, _)| TemporalKind::Timestamp(*p))
    }

    fn digest(&self) -> String {
        let value = self.literal.typed_digest();
        match self.timestamp_cast {
            Some((p, _)) => format!("CAST({value}):TIMESTAMP({p}) NOT NULL"),
            None => value,
        }
    }

    fn sql(&self) -> String {
        match self.timestamp_cast {
            Some((p, _)) => format!("CAST({} AS TIMESTAMP({p}))", self.literal.sql),
            None => self.literal.sql.clone(),
        }
    }

    fn lower(&self, ids: &mut Ids) -> Value {
        let mut expr = self.literal.expression(ids);
        expr["type"] = self.literal.kind.lower_type(ids);
        if let Some((_, pos)) = &self.timestamp_cast {
            let ty = self.kind().lower_type(ids);
            cast(ids, pos, expr, ty)
        } else {
            expr
        }
    }
}

pub(super) struct IntervalCount {
    left: Datetime,
    right: Datetime,
    unit: IntervalUnit,
    width: IntegerWidth,
    minus_position: Value,
    cast_position: Value,
}

impl IntervalCount {
    pub(super) fn resolve(program: &Program, id: ExprId) -> Result<Self, Error> {
        let (args, cast_position) = operands(program, id, "SqlStdOperatorTable.CAST")?;
        let &[minus] = args else {
            return Err(Error("interval cast requires one operand".into()));
        };
        let (args, minus_position) = operands(program, minus, "SqlStdOperatorTable.MINUS_DATE")?;
        let &[left, right] = args else {
            return Err(Error("datetime subtraction requires two operands".into()));
        };
        let ty = |id: ExprId| {
            let expr = &program.expressions.expressions[id.0 as usize];
            &program.table.type_table.types[expr.ty.0 as usize]
        };
        let TypeKind::Integer {
            width,
            signed: true,
        } = ty(id).kind
        else {
            return Err(Error("expected a signed integer count".into()));
        };
        let TypeKind::Interval {
            start: unit,
            end: IntervalEnd::Single,
            leading_precision: Precision::Unspecified,
            fractional_precision: Precision::Unspecified,
        } = ty(minus).kind
        else {
            return Err(Error("expected a single-unit datetime difference".into()));
        };
        if ty(id).nullable
            || ty(minus).nullable
            || matches!(unit, IntervalUnit::Quarter | IntervalUnit::Week)
        {
            return Err(Error("unsupported interval count type".into()));
        }
        let left = Datetime::resolve(program, left)?;
        let right = Datetime::resolve(program, right)?;
        if left.kind().sql_name() != right.kind().sql_name()
            && !matches!(
                (left.kind(), right.kind()),
                (TemporalKind::Timestamp(_), _) | (_, TemporalKind::Timestamp(_))
            )
        {
            return Err(Error(
                "expected matching datetime types or a common timestamp".into(),
            ));
        }
        Ok(Self {
            left,
            right,
            unit,
            width,
            minus_position,
            cast_position,
        })
    }

    pub(super) fn digest(&self) -> String {
        format!(
            "CAST(-({}, {})):{} NOT NULL",
            self.left.digest(),
            self.right.digest(),
            self.width.signed_sql_name()
        )
    }

    pub(super) fn sql(&self) -> String {
        format!(
            "CAST(({} - {}) AS {})",
            self.left.sql(),
            self.right.sql(),
            self.width.signed_sql_name()
        )
    }

    pub(super) fn lower(&self, ids: &mut Ids, result_type: Value) -> Result<Value, Error> {
        let mut left = self.left.lower(ids);
        let mut right = self.right.lower(ids);
        if self.left.kind().sql_name() != self.right.kind().sql_name() {
            // Both operands share the timestamp type. Its first occurrence in
            // the checkpoint must carry the definition for the Java reader.
            if matches!(self.left.kind(), TemporalKind::Timestamp(_)) {
                right = cast(ids, &self.minus_position, right, reference(&left["type"])?);
            } else {
                let ty_ref = reference(&right["type"])?;
                let ty = std::mem::replace(&mut right["type"], ty_ref);
                left = cast(ids, &self.minus_position, left, ty);
            }
        }
        use IntervalUnit::*;
        let (class, code, units_field, units) = match self.unit {
            Year => (
                "DBSPTypeLongInterval",
                "INTERVAL_LONG",
                "longUnits",
                "YEARS",
            ),
            Month => (
                "DBSPTypeLongInterval",
                "INTERVAL_LONG",
                "longUnits",
                "MONTHS",
            ),
            unit => {
                let name = match unit {
                    Day => "DAYS",
                    Hour => "HOURS",
                    Minute => "MINUTES",
                    Second => "SECONDS",
                    _ => return Err(Error("unsupported day-time interval unit".into())),
                };
                (
                    "DBSPTypeShortInterval",
                    "INTERVAL_SHORT",
                    "shortUnits",
                    name,
                )
            }
        };
        let mut ty = ids.node(class, &range(0, 0, 0, 0));
        ty["units"] = units.into();
        ty["code"] = code.into();
        ty["mayBeNull"] = false.into();
        let mut difference = ids.node("DBSPTimeAddSub", &self.minus_position);
        difference["left"] = left;
        difference["right"] = right;
        difference["type"] = ty;
        difference["opcode"] = "SUB".into();
        difference[units_field] = units.into();
        Ok(cast(ids, &self.cast_position, difference, result_type))
    }
}
