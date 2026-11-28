//! Type and render scalar literal ARRAY constructors before folding their consumers.

use super::{
    ConstantOperand, graph,
    literal::Cell,
    literal_type::widen,
    source::{DeclarationTokens, SqlDisplay},
};
use crate::{Expr, Failure, unsupported};
use feldera_sql_rel_model::{
    TypeId,
    expression_objects::ObjectPosition,
    expressions::ExprId,
    literals::{DecimalRounding, LiteralValue},
    operators::OperatorObject,
    program::Program,
    relation_metadata::Coordinates,
    source::Origin,
    types::{Cardinality, IntegerWidth, Precision, SqlType, TypeKind},
};
use sqlparser_064::{
    ast::{Array, Spanned},
    tokenizer::Span,
};

pub(super) struct LiteralArray {
    cells: Vec<Cell>,
    element: SqlType,
}

impl LiteralArray {
    pub(super) fn parse(array: &Array) -> Result<Self, Failure> {
        if array.elem.is_empty() {
            return Err(unsupported("empty ARRAY requires an inferred element type"));
        }
        let cells = array
            .elem
            .iter()
            .map(Cell::parse)
            .collect::<Result<Vec<_>, _>>()?;
        let mut element = SqlType {
            kind: TypeKind::Null {},
            nullable: false,
            origin: Origin::Absent {},
        };
        for cell in &cells {
            if cell.needs_non_bmp_wrapper() {
                return Err(unsupported(
                    "ARRAY element requires a runtime character conversion",
                ));
            }
            widen(&mut element.kind, &cell.kind)?;
            element.nullable |= matches!(cell.value, LiteralValue::Null {});
        }
        Ok(Self { cells, element })
    }

    // An INSERT prints the unvalidated VALUES text. Its assignment path currently
    // supports constructors whose scalar elements already have a common type.
    pub(super) fn for_insert(array: &Array) -> Result<Self, Failure> {
        let array = Self::parse(array)?;
        if array.element.nullable || array.cells.iter().any(|c| c.kind != array.element.kind) {
            return Err(unsupported("INSERT ARRAY elements require coercion"));
        }
        Ok(array)
    }

    fn render(&self, quoted: bool, validated: bool) -> Result<String, Failure> {
        let mut fields = Vec::with_capacity(self.cells.len());
        for cell in &self.cells {
            let text = if validated && let LiteralValue::Uuid { bytes } = &cell.value {
                format!(
                    "UUID '{}'",
                    feldera_sql_rel_model::literals::uuid_to_text(*bytes)
                )
            } else {
                cell.text.render(quoted).into_owned()
            };
            // Calcite infers an untyped NULL operand from the first operand
            // before converting the array to its least restrictive element type.
            let source = if matches!(cell.kind, TypeKind::Null {}) {
                &self.cells[0].kind
            } else {
                &cell.kind
            };
            let cast = validated
                && (source != &self.element.kind
                    || self.element.nullable != matches!(cell.value, LiteralValue::Null {}));
            fields.push(if cast {
                format!("CAST({text} AS {})", cast_type(&self.element.kind, quoted)?)
            } else {
                text
            });
        }
        let sql = format!("ARRAY[{}]", fields.join(", "));
        Ok(if quoted { sql } else { format!("({sql})") })
    }

    pub(super) fn insert_sql(&self, quoted: bool) -> String {
        self.render(quoted, false)
            .expect("unvalidated ARRAY text uses only the source literals")
    }

    pub(super) fn emit(
        &self,
        program: &mut Program,
        position: ObjectPosition,
    ) -> Result<(ExprId, TypeId), Failure> {
        let element = graph::insert_type(&mut program.table.type_table, self.element.clone());
        let ty = graph::insert_type(
            &mut program.table.type_table,
            SqlType {
                nullable: false,
                origin: Origin::Absent {},
                kind: TypeKind::Array {
                    element,
                    cardinality: Cardinality::Unbounded,
                },
            },
        );
        let mut arguments = Vec::with_capacity(self.cells.len());
        for cell in &self.cells {
            let nullable = matches!(cell.value, LiteralValue::Null {});
            let literal_type = graph::insert_type(
                &mut program.table.type_table,
                SqlType {
                    nullable,
                    ..self.element.clone()
                },
            );
            let value = if !nullable && let TypeKind::Decimal { scale, .. } = self.element.kind {
                let value = cell
                    .value
                    .as_exact_decimal()
                    .and_then(|n| n.at_sql_scale(scale, DecimalRounding::HalfUp))
                    .ok_or_else(|| unsupported("ARRAY element exceeds the common decimal type"))?;
                LiteralValue::Decimal { value }
            } else {
                cell.value.clone()
            };
            arguments.push(graph::append_literal(program, literal_type, value));
        }
        let root = graph::append_constant_call(
            program,
            OperatorObject::array_constructor(),
            arguments,
            ty,
            position,
        );
        Ok((root, ty))
    }

    pub(super) fn projection(
        &self,
        program: &mut Program,
        tokens: &DeclarationTokens<'_>,
        span: Span,
        enclosing: usize,
        fold: bool,
    ) -> Result<ConstantOperand, Failure> {
        let mut display = SqlDisplay::atom(self.render(true, true)?);
        display.diagnostic = self.render(false, true)?;
        let position = ObjectPosition::user(tokens.scalar_array_position(span, enclosing)?);
        let (root, ty) = self.emit(program, position)?;
        Ok(ConstantOperand {
            root,
            ty,
            folded: false,
            // The validator casts non-null UUIDs to the nullable element type.
            // Reducing this cast changes the ARRAY Rex digest.
            reduction_changed: fold
                && self.element.nullable
                && matches!(self.element.kind, TypeKind::Uuid {})
                && self
                    .cells
                    .iter()
                    .any(|c| matches!(c.value, LiteralValue::Uuid { .. })),
            display,
        })
    }
}

fn cast_type(kind: &TypeKind, quoted: bool) -> Result<String, Failure> {
    Ok(match kind {
        TypeKind::Integer {
            width,
            signed: true,
        } => width.signed_sql_name().into(),
        TypeKind::Boolean {} => "BOOLEAN".into(),
        TypeKind::Date {} => "DATE".into(),
        TypeKind::Uuid {} => "UUID".into(),
        TypeKind::Decimal {
            precision, scale, ..
        } => format!("DECIMAL({precision}, {scale})"),
        TypeKind::Time {
            precision: Precision::Specified(p),
            ..
        } => format!("TIME({p})"),
        TypeKind::Timestamp {
            precision: Precision::Specified(p),
            ..
        } => format!("TIMESTAMP({p})"),
        TypeKind::Character {
            fixed,
            length: Precision::Specified(n),
            ..
        } => {
            let name = if *fixed { "CHAR" } else { "VARCHAR" };
            let charset = if quoted { " CHARACTER SET `UTF-8`" } else { "" };
            format!("{name}({n}){charset}")
        }
        TypeKind::Binary {
            fixed,
            length: Precision::Specified(n),
        } => {
            let name = if *fixed { "BINARY" } else { "VARBINARY" };
            format!("{name}({n})")
        }
        _ => return Err(unsupported("unsupported ARRAY element coercion")),
    })
}

// Calls with these operand types are excluded by ReduceExpressionsRule.
fn can_fold_elements(kind: &TypeKind) -> bool {
    !matches!(
        kind,
        TypeKind::Time { .. }
            | TypeKind::Timestamp { .. }
            | TypeKind::Variant {}
            | TypeKind::Interval { .. }
            | TypeKind::Float { .. }
    )
}

/// The rule compares the whole batch of Rex digests. When any value changes,
/// all reducible ARRAY roots receive the executor's synthesized source position.
pub(super) fn replace_folded_constructors(program: &mut Program, roots: &[ExprId]) {
    let types = &program.table.type_table.types;
    for root in roots {
        let expr = &mut program.expressions.expressions[root.0 as usize];
        if let TypeKind::Array { element, .. } = types[expr.ty.0 as usize].kind
            && can_fold_elements(&types[element.0 as usize].kind)
            && let feldera_sql_rel_model::expression_objects::ObjectKind::Call { operator, .. } =
                expr.kind
            && program.expressions.operator_objects
                [program.expressions.operators[operator.0 as usize].object.0 as usize]
                == OperatorObject::array_constructor()
        {
            expr.position = ObjectPosition::Absent {};
        }
    }
}

pub(super) fn length(
    program: &mut Program,
    tokens: &DeclarationTokens<'_>,
    name: &str,
    args: &[&Expr],
    position: Coordinates,
    fold: bool,
) -> Result<ConstantOperand, Failure> {
    let &[arg] = args else {
        return Err(unsupported("array length requires one argument"));
    };
    let (arg, enclosing) = super::without_parentheses(arg);
    let array = match arg {
        Expr::Array(array) => Some(LiteralArray::parse(array)?),
        Expr::Value(v) if v.value == crate::Value::Null => None,
        _ => return Err(unsupported("expected a literal ARRAY or NULL")),
    };
    let ty = graph::insert_type(
        &mut program.table.type_table,
        SqlType {
            kind: TypeKind::Integer {
                width: IntegerWidth::I32,
                signed: true,
            },
            nullable: array.is_none(),
            origin: Origin::Absent {},
        },
    );
    let mut display = if let Some(array) = &array {
        let mut display = SqlDisplay::atom(array.render(true, true)?);
        display.diagnostic = array.render(false, true)?;
        display
    } else {
        SqlDisplay::atom("NULL".into())
    };
    display.pretty = format!("{name}({})", display.pretty);
    let call = format!("{name}({})", display.diagnostic);
    display.diagnostic = if name == "CARDINALITY" {
        call
    } else {
        format!("({call})")
    };
    let folded = fold
        && array
            .as_ref()
            .is_none_or(|a| can_fold_elements(&a.element.kind));
    let root = if folded {
        let value = array.map_or(LiteralValue::Null {}, |a| LiteralValue::Integer {
            value: a.cells.len().to_string(),
        });
        graph::append_literal(program, ty, value)
    } else {
        let input = match array {
            Some(array) => {
                array
                    .emit(
                        program,
                        ObjectPosition::user(tokens.scalar_array_position(arg.span(), enclosing)?),
                    )?
                    .0
            }
            None => Cell::parse(arg)?.emit(program, tokens, arg)?.0,
        };
        let table = if name == "CARDINALITY" {
            "SqlStdOperatorTable"
        } else {
            "SqlLibraryOperators"
        };
        graph::append_constant_call(
            program,
            OperatorObject::Builtin {
                registry: crate::frel::CALCITE.into(),
                registry_version: "1".into(),
                key: format!("{table}.{name}"),
            },
            vec![input],
            ty,
            ObjectPosition::user(position),
        )
    };
    Ok(ConstantOperand {
        root,
        ty,
        folded,
        reduction_changed: folded,
        display,
    })
}
