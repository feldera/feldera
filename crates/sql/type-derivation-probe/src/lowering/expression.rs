//! Expression nodes and the compact Java S2 text format.
use super::literals::Literal;
use crate::model::{Kind, SqlType};

#[derive(Clone)]
pub(super) struct Expression {
    pub(super) ty: SqlType,
    pub(super) kind: ExpressionKind,
}

#[derive(Clone)]
pub(super) enum ExpressionKind {
    Column {
        variable: usize,
        ordinal: usize,
    },
    Zero,
    TrimSpace,
    Literal(Literal),
    Null,
    Cast(Box<Expression>),
    Clone(Box<Expression>),
    Unwrap(Box<Expression>),
    If {
        condition: Box<Expression>,
        positive: Box<Expression>,
        negative: Box<Expression>,
    },
    IsNull(Box<Expression>),
    Not(Box<Expression>),
    Call {
        function: String,
        arguments: Vec<Expression>,
    },
}

impl Expression {
    pub(super) fn cast(self, ty: SqlType) -> Self {
        if self.ty == ty {
            return self;
        }
        Self {
            ty,
            kind: ExpressionKind::Cast(Box::new(self)),
        }
    }

    pub(super) fn convert_to(self, kind: Kind) -> Self {
        let target = self.ty.with_kind(kind);
        self.cast(target)
    }

    pub(super) fn is_null_literal(&self) -> bool {
        matches!(self.kind, ExpressionKind::Null)
    }

    pub(super) fn null(ty: SqlType) -> Self {
        Self {
            ty: SqlType {
                nullable: true,
                ..ty
            },
            kind: ExpressionKind::Null,
        }
    }

    pub(super) fn validator_cast(mut self, target: SqlType) -> Self {
        if self.ty == target {
            return self;
        }
        if self.is_null_literal() {
            return Self::null(target);
        }
        if target.character()
            && let ExpressionKind::Literal(Literal::Timestamp { iso, .. }) = &self.kind
        {
            return Self {
                ty: target,
                kind: ExpressionKind::Literal(Literal::String(format!("{iso}.000000"))),
            };
        }
        if self.ty.character() && matches!(self.kind, ExpressionKind::Cast(_)) {
            self = Self {
                ty: self.ty,
                kind: ExpressionKind::Clone(Box::new(self)),
            };
        }
        self.cast(target)
    }

    pub(super) fn print(&self) -> String {
        match &self.kind {
            ExpressionKind::Column { variable, ordinal } => {
                let suffix = if self.ty.nullable {
                    ".as_ref().cloned()"
                } else if self.ty.character() || matches!(self.ty.kind, Kind::Binary { .. }) {
                    ".clone()"
                } else {
                    ""
                };
                format!("(*t_{variable}).{ordinal}{suffix}")
            }
            ExpressionKind::Zero => "0i32".into(),
            ExpressionKind::TrimSpace => r#"SqlString::from(arcstr::literal!(" "))"#.into(),
            ExpressionKind::Null => format!(
                "None::<{}>",
                print_type(SqlType {
                    nullable: false,
                    ..self.ty
                })
            ),
            ExpressionKind::Literal(literal) => literal.print(self.ty),
            ExpressionKind::Cast(source) => {
                let target = cast_type(self.ty);
                format!("(({target}){})", source.print())
            }
            ExpressionKind::Clone(source) => format!("{}.clone()", source.print()),
            ExpressionKind::Unwrap(source) => format!("{}.unwrap()", source.print()),
            ExpressionKind::IsNull(source) => {
                if source.is_null_literal() {
                    "true".into()
                } else if let ExpressionKind::Column { variable, ordinal } = source.kind
                    && source.ty.character()
                {
                    format!("(*t_{variable}).{ordinal}.as_ref().is_none()")
                } else {
                    format!("{}.is_none()", source.print())
                }
            }
            ExpressionKind::Not(source) => format!("(!{})", source.print()),
            ExpressionKind::If {
                condition,
                positive,
                negative,
            } => format!(
                "(if {} {{\n{}\n}} else {{\n{}\n}})",
                condition.print(),
                indent(&positive.print()),
                indent(&negative.print())
            ),
            ExpressionKind::Call {
                function,
                arguments,
            } => {
                let arguments: Vec<_> = arguments.iter().map(Self::print).collect();
                match arguments.as_slice() {
                    [argument] => format!("{function}({argument})"),
                    _ => format!(
                        "{function}(\n{})",
                        arguments
                            .iter()
                            .map(|arg| indent(arg))
                            .collect::<Vec<_>>()
                            .join(",\n")
                    ),
                }
            }
        }
    }
}

pub(super) fn indent(text: &str) -> String {
    format!("    {}", text.replace('\n', "\n    "))
}

fn nullability(ty: SqlType) -> &'static str {
    if ty.nullable { "?" } else { "" }
}

pub(super) fn scalar_name(kind: Kind) -> String {
    match kind {
        Kind::Boolean => "b".into(),
        Kind::Integer { bits } => format!("i{bits}"),
        Kind::Real => "f".into(),
        Kind::Double => "d".into(),
        Kind::Decimal { .. } => "SqlDecimal".into(),
        Kind::Character { .. } => "s".into(),
        Kind::Binary { .. } => "bytes".into(),
        Kind::Date => "Date".into(),
        Kind::Timestamp { .. } => "Timestamp".into(),
    }
}

pub(super) fn cast_type(ty: SqlType) -> String {
    let base = match ty.kind {
        Kind::Decimal { precision, scale } => format!("DECIMAL({precision}, {scale})"),
        Kind::Character {
            fixed,
            length: Some(length),
        } => format!("s({length},{fixed})"),
        _ => scalar_name(ty.kind),
    };
    format!("{base}{}", nullability(ty))
}

pub(super) fn print_type(ty: SqlType) -> String {
    let base = match ty.kind {
        Kind::Real => "F32".into(),
        Kind::Double => "F64".into(),
        Kind::Decimal { precision, scale } => format!("SqlDecimal<{precision}, {scale}>"),
        Kind::Character { .. } => "SqlString".into(),
        Kind::Boolean => "bool".into(),
        Kind::Binary { .. } => "ByteArray".into(),
        _ => scalar_name(ty.kind),
    };
    format!("{base}{}", nullability(ty))
}
