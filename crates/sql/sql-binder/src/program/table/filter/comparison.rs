//! Signed integer comparisons retain validated SQL spelling even when a cast folds.
use super::{Predicate, ProjectInput, ProjectValue, TableOperand, boolean};
use crate::{
    BinaryOperator, CastKind, Expr, Failure, Spanned,
    frel::CALCITE,
    program::{
        graph::insert_type, literal::Cell, source::DeclarationTokens, table::null_cast_type,
    },
    unsupported,
};
use feldera_sql_rel_model::{
    TypeTable,
    expression_objects::ObjectPosition,
    literals::LiteralValue,
    relation_metadata::Coordinates,
    source::Origin,
    types::{IntegerWidth, SqlType, TypeKind},
};
use sqlparser_064::ast::UnaryOperator;

struct Operand {
    value: TableOperand,
    width: IntegerWidth,
    nullable: bool,
    pretty: String,
    diagnostic: String,
}

fn signed_width(kind: &TypeKind) -> Result<IntegerWidth, Failure> {
    match kind {
        TypeKind::Integer {
            width,
            signed: true,
        } => Ok(*width),
        _ => Err(unsupported(
            "table comparison requires signed integer operands",
        )),
    }
}

fn integer_type(
    types: &mut TypeTable,
    width: IntegerWidth,
    nullable: bool,
) -> feldera_sql_rel_model::TypeId {
    insert_type(
        types,
        SqlType {
            origin: Origin::Absent {},
            kind: TypeKind::Integer {
                width,
                signed: true,
            },
            nullable,
        },
    )
}

impl Operand {
    fn column(
        types: &TypeTable,
        (column, pretty, diagnostic): (ProjectInput, String, String),
    ) -> Result<Self, Failure> {
        let ty = &types.types[column.ty.0 as usize];
        Ok(Self {
            value: TableOperand::Column(column),
            width: signed_width(&ty.kind)?,
            nullable: ty.nullable,
            pretty,
            diagnostic,
        })
    }

    fn literal(types: &mut TypeTable, expr: &Expr) -> Result<Self, Failure> {
        if let Expr::Nested(inner) = expr {
            return Self::literal(types, inner);
        }
        let (number, width, pretty, diagnostic) = match expr {
            Expr::Cast {
                kind: kind @ (CastKind::Cast | CastKind::DoubleColon),
                expr,
                data_type,
                format: None,
            } => {
                let cell = Cell::parse(expr)?;
                let number: i64 = match &cell.value {
                    LiteralValue::Integer { value } => value.parse().expect("checked i64 literal"),
                    LiteralValue::Character { value } => value
                        .trim_matches(' ')
                        .parse()
                        .map_err(|_| unsupported("comparison cast requires runtime evaluation"))?,
                    _ => {
                        return Err(unsupported(
                            "expected integer or character comparison literal",
                        ));
                    }
                };
                let (target, spelling) = null_cast_type(data_type)?;
                let width = signed_width(&target)?;
                if !width.contains_signed(number) {
                    return Err(unsupported("comparison cast requires runtime evaluation"));
                }
                let render = |quoted| {
                    let value = cell.text.render(quoted);
                    let name = spelling.spelling(quoted);
                    match kind {
                        CastKind::Cast => format!("CAST({value} AS {name})"),
                        _ => format!("({value} :: {name})"),
                    }
                };
                (number, width, render(true), render(false))
            }
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } if matches!(
                **expr,
                Expr::Cast {
                    kind: CastKind::DoubleColon,
                    ..
                }
            ) =>
            {
                let mut inner = Self::literal(types, expr)?;
                let TableOperand::Integer { ref mut value, .. } = inner.value else {
                    unreachable!()
                };
                *value = value
                    .checked_neg()
                    .filter(|n| inner.width.contains_signed(*n))
                    .ok_or_else(|| unsupported("integer negation outside its type"))?;
                inner.pretty = format!("- {}", inner.pretty);
                inner.diagnostic = format!("(- {})", inner.diagnostic);
                return Ok(inner);
            }
            _ => {
                let cell = Cell::parse(expr)?;
                let LiteralValue::Integer { value } = cell.value else {
                    return Err(unsupported("expected signed integer comparison literal"));
                };
                (
                    value.parse().expect("checked i64 literal"),
                    signed_width(&cell.kind)?,
                    value.clone(),
                    value,
                )
            }
        };
        Ok(Self {
            value: TableOperand::Integer {
                ty: integer_type(types, width, false),
                value: number,
            },
            pretty,
            diagnostic,
            width,
            nullable: false,
        })
    }

    fn widen(&mut self, types: &mut TypeTable, width: IntegerWidth, position: Coordinates) {
        if self.width == width {
            return;
        }
        let name = width.signed_sql_name();
        self.pretty = format!("CAST({} AS {name})", self.pretty);
        self.diagnostic = format!("CAST({} AS {name})", self.diagnostic);
        let ty = integer_type(types, width, self.nullable);
        match &mut self.value {
            TableOperand::Integer { ty: literal_ty, .. } => *literal_ty = ty,
            column => {
                self.value = TableOperand::Computed(Box::new(ProjectValue::Call {
                    registry: CALCITE,
                    key: "SqlStdOperatorTable.CAST",
                    arguments: vec![column.clone()],
                    ty,
                    position: ObjectPosition::Present {
                        position,
                        internal: None,
                    },
                }));
            }
        }
        self.width = width;
    }
}

pub(super) fn bind(
    types: &mut TypeTable,
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    column: &impl Fn(&Expr) -> Result<(ProjectInput, String, String), Failure>,
    tokens: &DeclarationTokens<'_>,
) -> Result<Predicate, Failure> {
    let (key, token) = match op {
        BinaryOperator::Eq => ("SqlStdOperatorTable.EQUALS", "="),
        BinaryOperator::NotEq => ("SqlStdOperatorTable.NOT_EQUALS", "<>"),
        BinaryOperator::Lt => ("SqlStdOperatorTable.LESS_THAN", "<"),
        BinaryOperator::LtEq => ("SqlStdOperatorTable.LESS_THAN_OR_EQUAL", "<="),
        BinaryOperator::Gt => ("SqlStdOperatorTable.GREATER_THAN", ">"),
        BinaryOperator::GtEq => ("SqlStdOperatorTable.GREATER_THAN_OR_EQUAL", ">="),
        _ => return Err(unsupported("predicate outside the table filter slice")),
    };
    let mut lhs = Operand::column(types, column(left)?)?;
    let mut rhs = match right {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            Operand::column(types, column(right)?)?
        }
        _ => Operand::literal(types, right)?,
    };
    if matches!((&lhs.value, &rhs.value), (TableOperand::Column(a), TableOperand::Column(b)) if a.field == b.field)
    {
        return Err(unsupported(
            "self-comparison requires separate simplification",
        ));
    }
    if lhs.width.bits() < rhs.width.bits() {
        lhs.widen(types, rhs.width, tokens.coordinates(left.span())?);
    } else if rhs.width.bits() < lhs.width.bits() {
        rhs.widen(types, lhs.width, tokens.coordinates(right.span())?);
    }
    let position = ObjectPosition::user(tokens.comparison_position(left, right)?);
    Ok(Predicate::Expression {
        value: ProjectValue::Call {
            registry: CALCITE,
            key,
            arguments: vec![lhs.value, rhs.value],
            ty: insert_type(types, boolean(lhs.nullable || rhs.nullable)),
            position,
        },
        pretty: format!("{} {token} {}", lhs.pretty, rhs.pretty),
        diagnostic: format!("({} {token} {})", lhs.diagnostic, rhs.diagnostic),
    })
}
