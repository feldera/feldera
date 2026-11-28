//! Check literal INSERT assignments supported by the default pipeline compiler.
//! Both freshly bound SQL and detached S1 must resolve the destination in the
//! catalog at the statement, before lowering can issue the pipeline warning.

use crate::{
    TypeId,
    catalog::Name,
    expression_objects::ObjectKind,
    expressions::ExprId,
    literals::{DecimalRounding, LiteralValue, date_from_text},
    operators::OperatorObject,
    program::Program,
    relation_properties::Properties,
    statements::{Statement, TargetColumn},
    typed_literals::{check_cardinality, decimal_fits, integer_fits},
    types::{SqlType, TimeZone, TypeKind},
};
use std::collections::HashSet;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AssignmentWarning {
    pub kind: &'static str,
    pub message: String,
}

/// Check a complete positional INSERT or an explicit permutation. Assignment
/// casts outside this subset require the general DML translator. The caller
/// supplies a validated S1 graph or the binder's own arena references.
pub fn validate(
    program: &Program,
    preceding: &[Statement],
    target: &Name,
    columns: Option<&[TargetColumn]>,
    relation: u32,
) -> Result<Vec<AssignmentWarning>, String> {
    let schema = preceding
        .iter()
        .find_map(|statement| match statement {
            Statement::CreateTable {
                source,
                name,
                schema,
                ..
            } if source.visible && name == target => program.catalog.schemas.get(*schema as usize),
            _ => None,
        })
        .ok_or_else(|| format!("INSERT target {} is not a declared table", target.name))?;
    let destinations: Vec<_> = match columns {
        None => schema.columns.iter().collect(),
        Some(columns) => {
            let mut seen = HashSet::new();
            if columns.len() != schema.columns.len() {
                return Err("INSERT requires all destination columns".into());
            }
            columns
                .iter()
                .map(|target| {
                    let column = schema
                        .columns
                        .iter()
                        .find(|column| column.name == target.name)
                        .ok_or_else(|| format!("unknown INSERT column {}", target.display_name))?;
                    if !seen.insert(column.index) {
                        return Err(format!("duplicate INSERT column {}", target.display_name));
                    }
                    Ok(column)
                })
                .collect::<Result<_, String>>()?
        }
    };
    let types = &program.table.type_table.types;
    let props = &program.properties.relations[relation as usize];
    let producer = &program.producers.relations[relation as usize];
    let (tuples, projected) = match &props.fields {
        Properties::Values { tuples } => (*tuples, false),
        Properties::Project { variables, .. } if variables.is_empty() => {
            let [input] = producer.inputs.as_slice() else {
                return Err("constant INSERT projection requires one input".into());
            };
            if !matches!(
                program.properties.relations[*input as usize].fields,
                Properties::Values { tuples: 1 }
            ) {
                return Err("constant INSERT projection requires a single VALUES row".into());
            }
            (1, true)
        }
        _ => return Err("INSERT source requires literal VALUES or a constant projection".into()),
    };
    let TypeKind::Row { fields, .. } = &types[producer.row.0 as usize].kind else {
        return Err("INSERT source requires a row type".into());
    };
    if destinations.is_empty() || fields.len() != destinations.len() || tuples == 0 {
        return Err("INSERT column count differs from VALUES".into());
    }
    let mut sites = program
        .expressions
        .relation_uses
        .iter()
        .filter(|site| site.relation == relation);
    let site = sites.next().ok_or("missing INSERT source values")?;
    if sites.next().is_some() || Some(site.roots.len()) != fields.len().checked_mul(tuples as usize)
    {
        return Err("unexpected INSERT source shape".into());
    }
    // The compiler evaluates casts after permuting each row into table order.
    let mut order: Vec<_> = destinations.iter().enumerate().collect();
    order.sort_unstable_by_key(|(_, dest)| dest.index);
    let mut warnings = Vec::new();
    let mut reported = HashSet::new();
    for row in site.roots.chunks_exact(fields.len()) {
        for &(i, dest) in &order {
            let id = row[i];
            if projected && matches!(types[fields[i].ty.0 as usize].kind, TypeKind::Array { .. }) {
                validate_array_assignment(program, id, fields[i].ty, dest.ty)?;
                continue;
            }
            let value = source_value(program, id, fields[i].ty, projected)?;
            if !fits_source(value, &types[fields[i].ty.0 as usize]) {
                return Err("literal does not fit the INSERT source row".into());
            }
            // VALUES casts to the destination while compiling its cells. A
            // project keeps its own types until TableContents adds the row,
            // after the simplifier has already emitted its cast warnings.
            if let Some(warning) = assignment(value, &types[dest.ty.0 as usize], &dest.name.name)?
                && !projected
                && reported.insert(warning.clone())
            {
                warnings.push(warning);
            }
        }
    }
    Ok(warnings)
}

/// An ARRAY constructor in a constant projection has a typed child for
/// each element. Check both the constructor and the destination array.
fn validate_array_assignment(
    program: &Program,
    id: ExprId,
    source: TypeId,
    target: TypeId,
) -> Result<(), String> {
    let types = &program.table.type_table.types;
    let graph = &program.expressions;
    let expr = &graph.expressions[id.0 as usize];
    let (
        TypeKind::Array {
            element: source_element,
            cardinality: source_bound,
        },
        TypeKind::Array {
            element: target_element,
            cardinality: target_bound,
        },
    ) = (
        &types[source.0 as usize].kind,
        &types[target.0 as usize].kind,
    )
    else {
        return Err("expected an array destination for ARRAY in INSERT".into());
    };
    let ObjectKind::Call {
        operator,
        arguments,
    } = &expr.kind
    else {
        return Err("expected an ARRAY constructor in INSERT projection".into());
    };
    let signature = &graph.operators[operator.0 as usize];
    for (bound, path) in [
        (source_bound, "INSERT source"),
        (target_bound, "INSERT destination"),
    ] {
        check_cardinality(arguments.len(), *bound, path).map_err(|e| e.to_string())?;
    }
    if arguments.is_empty()
        && types[source_element.0 as usize].kind != types[target_element.0 as usize].kind
    {
        return Err("empty ARRAY requires an element type conversion".into());
    }
    if expr.ty != source
        || graph.operator_objects[signature.object.0 as usize]
            != OperatorObject::array_constructor()
    {
        return Err("unexpected constructor for INSERT array".into());
    }
    for &argument in arguments {
        let child = &graph.expressions[argument.0 as usize];
        let ObjectKind::Literal { literal } = &child.kind else {
            return Err("expected literal elements in INSERT array".into());
        };
        if child.ty != *source_element
            || !fits_source(&literal.value, &types[source_element.0 as usize])
            || !fits_source(&literal.value, &types[target_element.0 as usize])
        {
            return Err("array element requires an unsupported INSERT cast".into());
        }
    }
    Ok(())
}

/// A projected INSERT can contain Feldera's identity call on a supplementary
/// character literal. Other calls require full expression evaluation.
fn source_value(
    program: &Program,
    id: ExprId,
    field: TypeId,
    projected: bool,
) -> Result<&LiteralValue, String> {
    let graph = &program.expressions;
    let expr = &graph.expressions[id.0 as usize];
    if projected && expr.ty != field {
        return Err("projected INSERT expression differs from its column type".into());
    }
    match &expr.kind {
        ObjectKind::Literal { literal } => Ok(&literal.value),
        ObjectKind::Call { .. } if projected => program
            .non_bmp_literal_input(id)
            .map(|(_, literal)| &literal.value)
            .ok_or_else(|| "unsupported call in constant INSERT projection".into()),
        _ => Err("expected a literal in INSERT VALUES".into()),
    }
}

fn fits_source(value: &LiteralValue, ty: &SqlType) -> bool {
    match (value, &ty.kind) {
        (LiteralValue::Null {}, _) => ty.nullable,
        (LiteralValue::Boolean { .. }, TypeKind::Boolean {}) => true,
        (LiteralValue::Uuid { .. }, TypeKind::Uuid {}) => true,
        (LiteralValue::Date { .. }, TypeKind::Date {}) => true,
        (
            LiteralValue::Time {
                nanoseconds_since_midnight,
            },
            TypeKind::Time {
                zone: TimeZone::None,
                precision,
            },
        )
        | (
            LiteralValue::Timestamp {
                nanoseconds_since_midnight,
                ..
            },
            TypeKind::Timestamp {
                zone: TimeZone::None,
                precision,
            },
        ) => precision.contains_fraction(*nanoseconds_since_midnight),
        (
            LiteralValue::Decimal { value },
            TypeKind::Decimal {
                precision, scale, ..
            },
        ) => decimal_fits(value, *precision, *scale),
        (LiteralValue::Integer { value }, TypeKind::Integer { width, signed }) => {
            integer_fits(value, *width, *signed)
        }
        (LiteralValue::Character { value }, TypeKind::Character { length, .. }) => {
            length.contains_length(value.encode_utf16().count())
        }
        (LiteralValue::Binary { value }, TypeKind::Binary { length, .. }) => {
            length.contains_length(value.len())
        }
        _ => false,
    }
}

/// INSERT casts can truncate a string or leave a failed conversion for the
/// runtime. Pipeline definitions retain the conversion warnings.
fn assignment(
    value: &LiteralValue,
    target: &SqlType,
    column: &str,
) -> Result<Option<AssignmentWarning>, String> {
    let result = match (value, &target.kind) {
        (
            LiteralValue::Time { .. },
            TypeKind::Time {
                zone: TimeZone::None,
                ..
            },
        )
        | (
            LiteralValue::Timestamp { .. },
            TypeKind::Timestamp {
                zone: TimeZone::None,
                ..
            },
        )
        | (
            LiteralValue::Boolean { .. }
            | LiteralValue::Integer { .. }
            | LiteralValue::Decimal { .. }
            | LiteralValue::Character { .. },
            TypeKind::Variant {},
        )
        // A pipeline discards the INSERT's rows. Conversions left as runtime
        // casts by Simplify still accept the assignment and produce no warning.
        | (
            LiteralValue::Character { .. }
            | LiteralValue::Integer { .. }
            | LiteralValue::Decimal { .. }
            | LiteralValue::Boolean { .. }
            | LiteralValue::Date { .. }
            | LiteralValue::Time { .. }
            | LiteralValue::Timestamp { .. }
            | LiteralValue::Binary { .. },
            TypeKind::Character { .. },
        )
        | (
            LiteralValue::Character { .. }
            | LiteralValue::Integer { .. }
            | LiteralValue::Decimal { .. }
            | LiteralValue::Boolean { .. },
            TypeKind::Float { .. },
        )
        | (
            LiteralValue::Boolean { .. } | LiteralValue::Integer { .. } | LiteralValue::Decimal { .. },
            TypeKind::Integer { .. } | TypeKind::Boolean {},
        ) => None,
        (LiteralValue::Character { value }, TypeKind::Boolean {}) => {
            let text = value.trim_matches(|c: char| c <= ' ');
            (!text.eq_ignore_ascii_case("true") && !text.eq_ignore_ascii_case("false")).then(|| AssignmentWarning {
                kind: "Suspicious argument",
                message: format!("String '{value}' cannot be interpreted as a BOOLEAN"),
            })
        }
        (LiteralValue::Character { value }, TypeKind::Uuid {}) => {
            crate::literals::uuid_from_text(value)
                .is_none()
                .then(|| AssignmentWarning {
                    kind: "Not a UUID",
                    message: format!(" String '{value}' cannot be interpreted as a UUID"),
                })
        }
        (LiteralValue::Character { value }, TypeKind::Date {}) => {
            let text = value.trim_matches(|c: char| c <= ' ');
            date_from_text(text).is_none().then(|| AssignmentWarning {
                kind: "Suspicious argument",
                message: format!("String '{value}' cannot be interpreted as a DATE"),
            })
        }
        (LiteralValue::Character { value }, TypeKind::Integer { width, signed }) => {
            let text = value.trim_matches(|c: char| c <= ' ');
            let fits = text.parse::<i64>().is_ok()
                && (*signed || !text.starts_with('-'))
                && integer_fits(text, *width, *signed);
            (!fits).then(|| AssignmentWarning {
                kind: "Not a number",
                message: format!(" String '{value}' cannot be interpreted as a number"),
            })
        }
        (
            _,
            TypeKind::Decimal {
                precision, scale, ..
            },
        ) if let Some(decimal) = value.as_exact_decimal() => {
            let fits = decimal
                .at_sql_scale(*scale, DecimalRounding::Truncate)
                .is_some_and(|v| decimal_fits(&v, *precision, *scale));
            if fits {
                None
            } else {
                let (_, spelling) = decimal
                    .sql_and_digest()
                    .ok_or("decimal assignment exceeds SQL limits")?;
                Some(AssignmentWarning {
                    kind: "Invalid DECIMAL",
                    message: format!(
                        "cannot represent {spelling} as DECIMAL({precision}, {scale}): precision of DECIMAL type too small to represent value"
                    ),
                })
            }
        }
        _ if fits_source(value, target) => None,
        _ => return Err(format!("unsupported INSERT assignment to {column}")),
    };
    Ok(result)
}
