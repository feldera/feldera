//! Wire decoding and validation for the disposable analysis slice.

pub(crate) mod balanced_joins;
pub(crate) mod canonical_form;
pub(crate) mod chain_collapse;
pub(crate) mod chains;
pub(crate) mod clone_fanout;
mod connector_metadata;
mod cse;
pub(crate) mod delta_expansion;
mod determinism;
pub(crate) mod expand_aggregate_zero;
pub(crate) mod expand_casts;
mod expression_guards;
pub(crate) mod gen2_clones;
mod generated_closure;
mod identity;
pub(crate) mod implement_joins;
pub(crate) mod implement_now;
pub(crate) mod implement_statics;
pub(crate) mod inactive_passes;
pub(crate) mod incrementalize;
pub(crate) mod indexed_inputs;
pub(crate) mod inner_cse;
mod join_hints;
pub(crate) mod limiters;
pub(crate) mod linear_retention;
pub(crate) mod optimize_distinct;
pub(crate) mod optimize_incremental;
pub(crate) mod optimize_projections;
pub(crate) mod order_error_consumers;
pub(crate) mod pull_filter;
pub(crate) mod reachability;
mod regions;
pub(crate) mod runtime_error_wrappers;
pub(crate) mod separate_integrators;
mod sharing_guards;
pub(crate) mod simplify;
pub(crate) mod sql_index;
pub(crate) mod star_joins;
pub(crate) mod stream_kinds;
pub(crate) mod trace_retention;
pub(crate) mod unbounded_state;
pub(crate) mod unused_fields;
mod wire_edits;
pub(crate) use connector_metadata::connector_metadata_violation;
pub(crate) use determinism::nondeterministic_initializers;
pub(crate) use generated_closure::{advance_name, GENERATED_NAME};
pub(crate) use identity::identity_maps;
pub(crate) use join_hints::conflicting_join_hints;
pub(crate) use regions::conflicting_regions;

use crate::model::*;
use crate::provenance::Provenance;
use crate::{Error, Result};
use serde_json::{json, Value};
use std::{
    cell::Cell,
    collections::{HashMap, HashSet},
};

fn field<'a>(node: &'a Value, name: &str) -> Result<&'a Value> {
    node.get(name)
        .ok_or_else(|| Error(format!("missing field {name}")))
}

fn text<'a>(node: &'a Value, name: &str) -> Result<&'a str> {
    field(node, name)?
        .as_str()
        .ok_or_else(|| Error(format!("{name} must be a string")))
}

fn array<'a>(node: &'a Value, name: &str) -> Result<&'a [Value]> {
    field(node, name)?
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| Error(format!("{name} must be an array")))
}

fn uint(node: &Value, name: &str) -> Result<u64> {
    field(node, name)?
        .as_u64()
        .ok_or_else(|| Error(format!("{name} must be an unsigned integer")))
}

fn boolean(node: &Value, name: &str) -> Result<bool> {
    field(node, name)?
        .as_bool()
        .ok_or_else(|| Error(format!("{name} must be a boolean")))
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct WirePort {
    operator: u64,
    output: u64,
}

fn wire_port(value: &Value) -> Result<WirePort> {
    Ok(WirePort {
        operator: uint(field(value, "operator")?, "node")?,
        output: uint(value, "outputNumber")?,
    })
}

fn view(outer: &Value, definitions: &Definitions<'_>) -> Result<OperatorKind> {
    let metadata = field(outer, "metadata")?;
    let name = text(field(outer, "viewName")?, "name")?;
    let recursive = boolean(metadata, "recursive")?;
    let mut has_lateness = false;
    for column in array(metadata, "columns")? {
        if !column.is_object() {
            return Err(Error("view column metadata must be an object".into()));
        }
        if let Some(lateness) = column.get("lateness") {
            definitions.resolve(lateness)?;
            has_lateness = true;
        }
    }
    // The probe uses the default unquoted identifier casing.
    Ok(OperatorKind::View {
        preserve: recursive || has_lateness || name == "error_view",
    })
}

fn nesting(depth: usize) -> Result<()> {
    if depth > 128 {
        return Err(Error(
            "analysis nesting exceeds 128 (possibly a cyclic inner graph)".into(),
        ));
    }
    Ok(())
}

struct Definitions<'a> {
    inner: HashMap<u64, &'a Value>,
    type_nodes: Cell<usize>,
}

impl<'a> Definitions<'a> {
    fn read(circuit: &'a Value) -> Result<Self> {
        feldera_sql_difftest::compare_circuits(circuit, circuit)
            .map_err(|e| Error(e.to_string()))?;
        let mut pending = Vec::new();
        // Only inner fields enter this table. Outer ids have a separate namespace.
        let mut outer: Vec<_> = array(circuit, "allOperators")?
            .iter()
            .chain(array(circuit, "declarations")?)
            .collect();
        while let Some(node) = outer.pop() {
            if node["class"] == "DBSPNestedOperator" {
                outer.extend(array(node, "allOperators")?);
            }
            for (name, value) in node
                .as_object()
                .ok_or_else(|| Error("outer node must be an object".into()))?
            {
                if !matches!(
                    name.as_str(),
                    "inputs" | "outputs" | "allOperators" | "annotations" | "provenance"
                ) {
                    pending.push(value);
                }
            }
        }
        let mut inner = HashMap::new();
        while let Some(node) = pending.pop() {
            match node {
                Value::Object(object) => {
                    if object.contains_key("class") && object.contains_key("id") {
                        inner.insert(uint(node, "id")?, node);
                    }
                    pending.extend(object.iter().filter_map(|(name, value)| {
                        (!matches!(name.as_str(), "annotations" | "provenance")).then_some(value)
                    }));
                }
                Value::Array(values) => pending.extend(values),
                _ => {}
            }
        }
        Ok(Self {
            inner,
            type_nodes: Cell::new(0),
        })
    }

    fn resolve(&self, node: &'a Value) -> Result<&'a Value> {
        if node.get("class").is_some() {
            return Ok(node);
        }
        let id = uint(node, "node")?;
        self.inner
            .get(&id)
            .copied()
            .ok_or_else(|| Error("undefined inner node".into()))
    }

    fn call_name(&self, expression: &'a Value) -> Result<Option<&'a str>> {
        if expression["class"] != "DBSPApplyExpression" {
            return Ok(None);
        }
        let function = self.resolve(field(expression, "function")?)?;
        if function["class"] != "DBSPPathExpression" {
            return Ok(None);
        }
        let path = self.resolve(field(function, "path")?)?;
        let [segment] = array(path, "components")? else {
            return Ok(None);
        };
        Ok(Some(text(self.resolve(segment)?, "identifier")?))
    }

    fn ty(&self, node: &'a Value, depth: usize) -> Result<Type> {
        nesting(depth)?;
        let count = self.type_nodes.get();
        if count >= 100_000 {
            return Err(Error("type expansion budget exceeded".into()));
        }
        self.type_nodes.set(count + 1);
        let node = self.resolve(node)?;
        let nullable = boolean(node, "mayBeNull")?;
        let kind = match text(node, "class")? {
            "DBSPTypeNull" => TypeKind::Null,
            "DBSPTypeRef" => TypeKind::Ref(Box::new(self.ty(field(node, "type")?, depth + 1)?)),
            "DBSPTypeTuple" | "DBSPTypeRawTuple" => {
                let fields = array(node, "tupFields")?
                    .iter()
                    .map(|v| self.ty(v, depth + 1))
                    .collect::<Result<_>>()?;
                if text(node, "class")? == "DBSPTypeTuple" {
                    TypeKind::Tuple(fields)
                } else {
                    TypeKind::RawTuple(fields)
                }
            }
            "DBSPTypeInteger" => TypeKind::Integer {
                width: uint(node, "width")?,
                signed: boolean(node, "signed")?,
            },
            "DBSPTypeString" => TypeKind::String {
                precision: field(node, "precision")?
                    .as_i64()
                    .ok_or_else(|| Error("invalid string precision".into()))?,
                fixed: boolean(node, "fixed")?,
            },
            "DBSPTypeDecimal" => TypeKind::Decimal {
                precision: uint(node, "precision")?,
                scale: uint(node, "scale")?,
            },
            "DBSPTypeSqlResult" => {
                let arguments = array(node, "typeArgs")?;
                if arguments.len() != 1 {
                    return Err(Error("SQL result needs one type argument".into()));
                }
                TypeKind::Result(Box::new(self.ty(&arguments[0], depth + 1)?))
            }
            kind => TypeKind::Scalar(match kind {
                "DBSPTypeBool" => Scalar::Bool,
                "DBSPTypeReal" => Scalar::Real,
                "DBSPTypeDouble" => Scalar::Double,
                "DBSPTypeDate" => Scalar::Date,
                "DBSPTypeTime" => Scalar::Time,
                "DBSPTypeTimestamp" => Scalar::Timestamp,
                "DBSPTypeTimestampTz" => Scalar::TimestampTz,
                "DBSPTypeVariant" => Scalar::Variant,
                "DBSPTypeUuid" => Scalar::Uuid,
                "DBSPTypeUSize" => Scalar::USize,
                "DBSPTypeVoid" => Scalar::Void,
                "DBSPTypeAny" => Scalar::Any,
                "DBSPTypeStr" => Scalar::Str,
                _ => return Err(Error(format!("unsupported analysis type {kind}"))),
            }),
        };
        Ok(Type { nullable, kind })
    }
}

#[derive(Clone, Copy)]
enum Binding {
    Parameter(usize),
    Local(ExprId),
}

struct Expressions<'a, 'b> {
    definitions: &'b Definitions<'a>,
    expressions: Vec<Expr>,
    bindings: HashMap<String, Binding>,
    // A shared node may occur in different lexical environments.
    cache: HashMap<(u64, u64), ExprId>,
    scope: u64,
    next_scope: u64,
    unhandled: bool,
}

impl<'a> Expressions<'a, '_> {
    fn new_scope(&mut self) {
        self.next_scope += 1;
        self.scope = self.next_scope;
    }

    fn expression(&mut self, wire: &'a Value, depth: usize) -> Result<ExprId> {
        nesting(depth)?;
        let node = self.definitions.resolve(wire)?;
        let key = (uint(node, "id")?, self.scope);
        if let Some(id) = self.cache.get(&key) {
            return Ok(*id);
        }
        if self.expressions.len() >= 100_000 {
            return Err(Error("expression budget exceeded".into()));
        }
        let kind = match text(node, "class")? {
            "DBSPVariablePath" => match self.bindings.get(text(node, "variable")?) {
                Some(Binding::Parameter(index)) => ExprKind::Parameter(*index),
                Some(Binding::Local(id)) => ExprKind::Copy(*id),
                None => ExprKind::Unknown, // Static values have no parameter lineage.
            },
            "DBSPTupleExpression" | "DBSPRawTupleExpression" => {
                if node.get("fields").is_none_or(Value::is_null) {
                    ExprKind::Unknown
                } else {
                    ExprKind::Tuple(
                        array(node, "fields")?
                            .iter()
                            .map(|v| self.expression(v, depth + 1))
                            .collect::<Result<_>>()?,
                    )
                }
            }
            "DBSPFieldExpression" | "DBSPCustomOrdField" => ExprKind::Field(
                self.expression(field(node, "expression")?, depth + 1)?,
                usize::try_from(uint(node, "fieldNo")?)
                    .map_err(|_| Error("field index too large".into()))?,
            ),
            "DBSPBorrowExpression" => {
                ExprKind::Borrow(self.expression(field(node, "expression")?, depth + 1)?)
            }
            "DBSPDerefExpression" => {
                ExprKind::Deref(self.expression(field(node, "expression")?, depth + 1)?)
            }
            "DBSPCloneExpression"
            | "DBSPUnwrapExpression"
            | "DBSPSomeExpression"
            | "DBSPUnwrapCustomOrdExpression" => {
                ExprKind::Copy(self.expression(field(node, "expression")?, depth + 1)?)
            }
            "DBSPHandleErrorExpression" => {
                ExprKind::Copy(self.expression(field(node, "source")?, depth + 1)?)
            }
            "DBSPCastExpression" => {
                let source = self.definitions.resolve(field(node, "source")?)?;
                let safe = match text(node, "safe")? {
                    "SqlSafe" | "UnwrapResultSafe" => true,
                    "SqlUnsafe" | "UnwrapResultUnsafe" | "RustCast" | "Saturating" => false,
                    _ => return Err(Error("unsupported cast safety kind".into())),
                };
                ExprKind::Cast {
                    source: self.expression(source, depth + 1)?,
                    from: self.definitions.ty(field(source, "type")?, 0)?,
                    to: self.definitions.ty(field(node, "type")?, 0)?,
                    safe,
                }
            }
            "DBSPIfExpression" => {
                self.expression(field(node, "condition")?, depth + 1)?;
                let positive = self.expression(field(node, "positive")?, depth + 1)?;
                if let Some(negative) = node.get("negative").filter(|v| !v.is_null()) {
                    ExprKind::Intersection(positive, self.expression(negative, depth + 1)?)
                } else {
                    ExprKind::Unknown
                }
            }
            "DBSPBinaryExpression" => {
                let left = self.expression(field(node, "left")?, depth + 1)?;
                let right = self.expression(field(node, "right")?, depth + 1)?;
                if text(node, "opcode")? == "COALESCE" {
                    ExprKind::Intersection(left, right)
                } else {
                    ExprKind::Unknown
                }
            }
            "DBSPBlockExpression" => self.block(node, depth + 1)?,
            "DBSPReturnExpression" | "DBSPAssignmentExpression" => {
                self.unhandled = true;
                ExprKind::Unknown
            }
            "DBSPUnaryExpression" => {
                self.expression(field(node, "source")?, depth + 1)?;
                ExprKind::Unknown
            }
            "DBSPApplyExpression" => {
                self.expression(field(node, "function")?, depth + 1)?;
                for argument in array(node, "arguments")? {
                    self.expression(argument, depth + 1)?;
                }
                ExprKind::Unknown
            }
            "DBSPPathExpression" => ExprKind::Unknown,
            "DBSPBoolLiteral"
            | "DBSPI8Literal"
            | "DBSPI16Literal"
            | "DBSPI32Literal"
            | "DBSPI64Literal"
            | "DBSPI128Literal"
            | "DBSPUSizeLiteral"
            | "DBSPDecimalLiteral"
            | "DBSPStringLiteral"
            | "DBSPRealLiteral"
            | "DBSPDoubleLiteral"
            | "DBSPDateLiteral"
            | "DBSPTimeLiteral"
            | "DBSPTimestampLiteral"
            | "DBSPTimestampTzLiteral"
            | "DBSPNullLiteral"
            | "DBSPVariantNullLiteral" => ExprKind::Unknown,
            kind => return Err(Error(format!("unsupported lineage expression {kind}"))),
        };
        let id = ExprId(self.expressions.len());
        self.expressions.push(Expr { kind });
        self.cache.insert(key, id);
        Ok(id)
    }

    fn block(&mut self, node: &'a Value, depth: usize) -> Result<ExprKind> {
        nesting(depth)?;
        let outer = self.bindings.clone();
        let scope = self.scope;
        self.new_scope();
        for statement in array(node, "contents")? {
            let statement = self.definitions.resolve(statement)?;
            match text(statement, "class")? {
                "DBSPLetStatement" => {
                    let initializer = field(statement, "initializer")?;
                    let value = self.expression(initializer, depth + 1)?;
                    self.bindings
                        .insert(text(statement, "variable")?.into(), Binding::Local(value));
                    self.new_scope();
                }
                "DBSPExpressionStatement" => {
                    self.expression(field(statement, "expression")?, depth + 1)?;
                }
                kind => return Err(Error(format!("unsupported lineage statement {kind}"))),
            }
        }
        let result = if let Some(last) = node.get("lastExpression").filter(|v| !v.is_null()) {
            ExprKind::Copy(self.expression(last, depth + 1)?)
        } else {
            ExprKind::Unknown
        };
        self.bindings = outer;
        self.scope = scope;
        Ok(result)
    }
}

fn closure(definitions: &Definitions<'_>, wire: &Value) -> Result<Closure> {
    let node = definitions.resolve(wire)?;
    if text(node, "class")? != "DBSPClosureExpression" {
        return Err(Error(
            "map function must be a closure in this subset".into(),
        ));
    }
    let mut decoder = Expressions {
        definitions,
        expressions: Vec::new(),
        bindings: HashMap::new(),
        cache: HashMap::new(),
        scope: 0,
        next_scope: 0,
        unhandled: false,
    };
    let mut parameters = Vec::new();
    for (index, parameter) in array(node, "parameters")?.iter().enumerate() {
        let parameter = definitions.resolve(parameter)?;
        if decoder
            .bindings
            .insert(text(parameter, "name")?.into(), Binding::Parameter(index))
            .is_some()
        {
            return Err(Error("duplicate closure parameter name".into()));
        }
        parameters.push(definitions.ty(field(parameter, "type")?, 0)?);
    }
    let body = decoder.expression(field(node, "body")?, 0)?;
    Ok(Closure {
        parameters,
        expressions: decoder.expressions,
        body,
        has_unhandled_control_flow: decoder.unhandled,
    })
}

fn constant(
    definitions: &Definitions<'_>,
    wire: &Value,
    stream: CollectionKind,
) -> Result<OperatorKind> {
    let value = definitions.resolve(wire)?;
    if text(value, "class")? == "DBSPIndexedZSetExpression" {
        // Java represents only empty indexed constants, with no row/weight arrays.
        if value.get("data").is_some() || value.get("weights").is_some() {
            return Err(Error(
                "indexed constants must use the empty encoding".into(),
            ));
        }
        return Ok(OperatorKind::Constant {
            weights: Vec::new(),
            indexed: true,
            stream,
        });
    }
    if text(value, "class")? != "DBSPZSetExpression" {
        return Err(Error("unsupported constant expression".into()));
    }
    let weights = array(value, "weights")?
        .iter()
        .map(|v| {
            v.as_i64()
                .ok_or_else(|| Error("constant weight must be an i64".into()))
        })
        .collect::<Result<Vec<_>>>()?;
    if weights.len() != array(value, "data")?.len() {
        return Err(Error("constant rows/weights length mismatch".into()));
    }
    Ok(OperatorKind::Constant {
        weights,
        indexed: false,
        stream,
    })
}

fn filter_predicate(definitions: &Definitions<'_>, function: &Value) -> Result<FilterPredicate> {
    let closure = definitions.resolve(function)?;
    if text(closure, "class")? != "DBSPClosureExpression" {
        return Err(Error("expected filter closure".into()));
    }
    let body = definitions.resolve(field(closure, "body")?)?;
    predicate(definitions, closure, body, 0, &mut 4096)
}

fn predicate(
    definitions: &Definitions<'_>,
    closure: &Value,
    body: &Value,
    depth: usize,
    budget: &mut usize,
) -> Result<FilterPredicate> {
    nesting(depth)?;
    *budget = budget
        .checked_sub(1)
        .ok_or_else(|| Error("constant operand expansion budget exceeded".into()))?;
    if let ConstantMapBody::Field(column) = row_access(definitions, closure, body)? {
        return Ok(FilterPredicate::RowField(column));
    }
    if body["class"] == "DBSPIsNullExpression" {
        let source = definitions.resolve(field(body, "expression")?)?;
        if let Some(operand) = scalar_operand(definitions, closure, source, depth + 1, budget)? {
            let nullable = definitions.ty(field(source, "type")?, 0)?.nullable;
            return Ok(FilterPredicate::IsNull { operand, nullable });
        }
        return Ok(FilterPredicate::Dynamic);
    }
    if body["class"] == "DBSPUnaryExpression" {
        let wrap: fn(Box<FilterPredicate>) -> FilterPredicate = match body["opcode"].as_str() {
            Some("NOT") => FilterPredicate::Not,
            Some("WRAP_BOOL") => FilterPredicate::WrapBool,
            _ => return Ok(FilterPredicate::Dynamic),
        };
        let source = definitions.resolve(field(body, "source")?)?;
        let inner = predicate(definitions, closure, source, depth + 1, budget)?;
        return Ok(if matches!(inner, FilterPredicate::Dynamic) {
            FilterPredicate::Dynamic
        } else {
            wrap(Box::new(inner))
        });
    }
    if body["class"] == "DBSPBinaryExpression" {
        let kind = match body["opcode"].as_str() {
            Some("EQ") => Comparison::Equal,
            Some("NEQ") => Comparison::NotEqual,
            Some("LT") => Comparison::Less,
            Some("LTE") => Comparison::LessOrEqual,
            Some("GT") => Comparison::Greater,
            Some("GTE") => Comparison::GreaterOrEqual,
            _ => return Ok(FilterPredicate::Dynamic),
        };
        let left = definitions.resolve(field(body, "left")?)?;
        let right = definitions.resolve(field(body, "right")?)?;
        if let (Some(left_operand), Some(right_operand)) = (
            scalar_operand(definitions, closure, left, depth + 1, budget)?,
            scalar_operand(definitions, closure, right, depth + 1, budget)?,
        ) {
            if definitions.ty(field(left, "type")?, 0)?
                == definitions.ty(field(right, "type")?, 0)?
            {
                return Ok(FilterPredicate::Comparison {
                    kind,
                    left: left_operand,
                    right: right_operand,
                });
            }
        }
    }
    if text(body, "class")? != "DBSPBoolLiteral" {
        return Ok(FilterPredicate::Dynamic);
    }
    match body.get("value") {
        Some(Value::Bool(value)) => Ok(FilterPredicate::Literal(*value)),
        None | Some(Value::Null) => Ok(FilterPredicate::Null),
        _ => Err(Error("invalid boolean filter literal".into())),
    }
}

fn scalar_operand(
    definitions: &Definitions<'_>,
    closure: &Value,
    expression: &Value,
    depth: usize,
    budget: &mut usize,
) -> Result<Option<ScalarOperand>> {
    nesting(depth)?;
    *budget = budget
        .checked_sub(1)
        .ok_or_else(|| Error("constant operand expansion budget exceeded".into()))?;
    if matches!(
        expression["class"].as_str(),
        Some("DBSPI8Literal" | "DBSPI32Literal" | "DBSPI64Literal" | "DBSPBoolLiteral")
    ) {
        return Ok(Some(ScalarOperand::Literal(inner_identity(expression)?)));
    }
    if !matches!(
        expression["class"].as_str(),
        Some("DBSPBinaryExpression" | "DBSPFieldExpression" | "DBSPDerefExpression")
    ) {
        return Ok(None);
    }
    let ty = definitions.ty(field(expression, "type")?, 0)?;
    let foldable_integer = matches!(
        ty.kind,
        TypeKind::Integer {
            width: 8 | 32,
            signed: true
        }
    );
    if foldable_integer && expression["class"] == "DBSPBinaryExpression" {
        let kind = match expression["opcode"].as_str() {
            Some("ADD") => ArithmeticKind::Add,
            Some("SUB") => ArithmeticKind::Subtract,
            Some("MUL") => ArithmeticKind::Multiply,
            _ => return Ok(None),
        };
        let left = definitions.resolve(field(expression, "left")?)?;
        let right = definitions.resolve(field(expression, "right")?)?;
        let left_nullable = definitions.ty(field(left, "type")?, 0)?.nullable;
        let right_nullable = definitions.ty(field(right, "type")?, 0)?.nullable;
        if let (Some(left), Some(right)) = (
            scalar_operand(definitions, closure, left, depth + 1, budget)?,
            scalar_operand(definitions, closure, right, depth + 1, budget)?,
        ) {
            return Ok(Some(ScalarOperand::Arithmetic(Box::new(Arithmetic {
                kind,
                left,
                right,
                left_nullable,
                right_nullable,
            }))));
        }
    }
    if matches!(
        ty.kind,
        TypeKind::Scalar(Scalar::Bool)
            | TypeKind::Integer {
                width: 8 | 32 | 64,
                signed: true
            }
    ) {
        match row_access(definitions, closure, expression)? {
            ConstantMapBody::Field(column) => return Ok(Some(ScalarOperand::Field(column))),
            ConstantMapBody::Identity => return Ok(Some(ScalarOperand::Row)),
            _ => {}
        }
    }
    Ok(None)
}

pub(crate) fn constant_inputs(
    wire: &Value,
    circuit: &Circuit,
) -> Result<(Vec<ConstantInput>, ConstantLiterals)> {
    let definitions = Definitions::read(wire)?;
    let mut literal_budget = 100_000usize;
    let inputs = array(wire, "allOperators")?
        .iter()
        .zip(&circuit.operators)
        .enumerate()
        .map(|(index, (node, operator))| {
            let value = if let OperatorKind::Constant {
                weights,
                indexed,
                stream: CollectionKind::Snapshot,
            } = &operator.kind
            {
                let function = definitions.resolve(field(node, "function")?)?;
                let value = if *indexed {
                    ConstantValue::Indexed {
                        output_type: inner_identity(field(node, "outputType")?)?,
                        function: inner_identity(function)?,
                    }
                } else {
                    let rows = array(function, "data")?
                        .iter()
                        .zip(weights)
                        .map(|(row, weight)| Ok((inner_identity(row)?, *weight)))
                        .collect::<Result<Vec<_>>>()?;
                    ConstantValue::Rows {
                        element_type: inner_identity(field(function, "elementType")?)?,
                        rows,
                    }
                };
                Some(Constant {
                    origin: OperatorId(index),
                    value,
                })
            } else {
                None
            };
            let map = if node["class"] == "DBSPMapOperator" {
                Some(constant_map(
                    &definitions,
                    field(node, "function")?,
                    &mut literal_budget,
                )?)
            } else {
                None
            };
            Ok(ConstantInput { value, map })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((inputs, constant_literals(&definitions)?))
}

fn constant_literals(definitions: &Definitions<'_>) -> Result<ConstantLiterals> {
    let mut literals = ConstantLiterals {
        last_id: definitions.inner.keys().copied().max().unwrap_or_default(),
        ..ConstantLiterals::default()
    };
    for (&id, node) in &definitions.inner {
        let scalar = match text(node, "class")? {
            "DBSPTupleExpression" => {
                let fields = array(node, "fields")?
                    .iter()
                    .map(inner_identity)
                    .collect::<Result<Vec<_>>>()?;
                literals.tuples.insert(id, fields);
                continue;
            }
            "DBSPBoolLiteral" => ScalarLiteral::Bool(node["value"].as_bool()),
            "DBSPI8Literal" => ScalarLiteral::Int8(integer_literal(node)?),
            "DBSPI32Literal" => ScalarLiteral::Int32(integer_literal(node)?),
            "DBSPI64Literal" => ScalarLiteral::Int64(integer_literal(node)?),
            _ => continue,
        };
        literals.scalars.insert(id, scalar);
    }
    Ok(literals)
}

fn integer_literal<T: std::str::FromStr>(node: &Value) -> Result<Option<T>> {
    match node.get("value") {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .and_then(|text| text.parse().ok())
            .map(Some)
            .ok_or_else(|| Error("invalid integer constant literal".into())),
    }
}

fn constant_map(
    definitions: &Definitions<'_>,
    function: &Value,
    literal_budget: &mut usize,
) -> Result<ConstantMap> {
    let closure = definitions.resolve(function)?;
    let body = definitions.resolve(field(closure, "body")?)?;
    let function_type = definitions.resolve(field(closure, "type")?)?;
    let element_type = inner_identity(field(function_type, "resultType")?)?;
    let access = row_access(definitions, closure, body)?;
    let body = match access {
        ConstantMapBody::Other if literal_row(definitions, body, 0, literal_budget)? => {
            ConstantMapBody::Literal(inner_identity(body)?)
        }
        ConstantMapBody::Other if body["class"] == "DBSPBinaryExpression" => {
            match scalar_operand(definitions, closure, body, 0, &mut 4096)? {
                Some(ScalarOperand::Arithmetic(expression)) => {
                    ConstantMapBody::Arithmetic(*expression)
                }
                _ => ConstantMapBody::Other,
            }
        }
        _ => access,
    };
    Ok(ConstantMap { element_type, body })
}

fn row_access(
    definitions: &Definitions<'_>,
    closure: &Value,
    body: &Value,
) -> Result<ConstantMapBody> {
    let [parameter] = array(closure, "parameters")? else {
        return Ok(ConstantMapBody::Other);
    };
    let (target, access) = if body["class"] == "DBSPFieldExpression" {
        let target = definitions.resolve(field(body, "expression")?)?;
        let column = usize::try_from(uint(body, "fieldNo")?)
            .map_err(|_| Error("field index too large".into()))?;
        (target, ConstantMapBody::Field(column))
    } else {
        (body, ConstantMapBody::Identity)
    };
    if target["class"] != "DBSPDerefExpression" {
        return Ok(ConstantMapBody::Other);
    }
    let var = definitions.resolve(field(target, "expression")?)?;
    let parameter = definitions.resolve(parameter)?;
    if var["class"] == "DBSPVariablePath" && var["variable"] == parameter["name"] {
        Ok(access)
    } else {
        Ok(ConstantMapBody::Other)
    }
}

fn literal_row(
    definitions: &Definitions<'_>,
    wire: &Value,
    depth: usize,
    budget: &mut usize,
) -> Result<bool> {
    nesting(depth)?;
    *budget = budget
        .checked_sub(1)
        .ok_or_else(|| Error("constant literal expansion budget exceeded".into()))?;
    let value = definitions.resolve(wire)?;
    match text(value, "class")? {
        "DBSPI8Literal" | "DBSPI32Literal" | "DBSPI64Literal" | "DBSPBoolLiteral" => Ok(true),
        "DBSPTupleExpression" => {
            for child in array(value, "fields")? {
                if !literal_row(definitions, child, depth + 1, budget)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

pub(crate) fn circuit(wire: &Value) -> Result<Circuit> {
    decode_circuit(wire, None)
}

pub(crate) fn stream_circuit(wire: &Value) -> Result<Circuit> {
    let kinds = stream_kinds::infer(wire)?;
    if kinds.len() != array(wire, "allOperators")?.len() {
        return Err(Error("stream rewrites require a flat circuit".into()));
    }
    decode_circuit(wire, Some(&kinds))
}

fn decode_circuit(
    wire: &Value,
    stream_kinds: Option<&[Vec<Option<stream_kinds::Kind>>]>,
) -> Result<Circuit> {
    let definitions = Definitions::read(wire)?;
    let mut operators: Vec<Operator> = Vec::new();
    let mut ids = HashMap::new();
    for node in array(wire, "allOperators")? {
        let name = text(node, "class")?;
        let mut inputs = Vec::new();
        for input in array(node, "inputs")? {
            let source = uint(field(input, "operator")?, "node")?;
            let operator = *ids
                .get(&source)
                .ok_or_else(|| Error("input must refer to an earlier operator".into()))?;
            let output = usize::try_from(uint(input, "outputNumber")?)
                .map_err(|_| Error("port number too large".into()))?;
            let id: OperatorId = operator;
            if output >= operators[id.0].kind.outputs() {
                return Err(Error("invalid output port number".into()));
            }
            inputs.push(Port { operator, output });
        }
        let (kind, arity) = match name {
            "DBSPSourceMultisetOperator" => (OperatorKind::Source { primary_key: false }, Some(0)),
            "DBSPSourceMapOperator" => (OperatorKind::Source { primary_key: true }, Some(0)),
            "DBSPSourceLazyMapOperator" if stream_kinds.is_some() => {
                (OperatorKind::Source { primary_key: true }, Some(0))
            }
            "DBSPNowOperator" => (OperatorKind::Now, Some(0)),
            "DBSPConstantOperator" | "DBSPIncrementalConstantOperator" => {
                let stream = if name == "DBSPConstantOperator" {
                    CollectionKind::Snapshot
                } else {
                    CollectionKind::Delta
                };
                (
                    constant(&definitions, field(node, "function")?, stream)?,
                    Some(0),
                )
            }
            "DBSPDistinctOperator" => (OperatorKind::Distinct(CollectionKind::Delta), Some(1)),
            "DBSPStreamDistinctOperator" => {
                (OperatorKind::Distinct(CollectionKind::Snapshot), Some(1))
            }
            "DBSPSumOperator" => (OperatorKind::Sum, None),
            "DBSPNegateOperator" => (OperatorKind::Negate, Some(1)),
            "DBSPPositiveOperator" => (OperatorKind::Positive, Some(1)),
            "DBSPNoopOperator" => (OperatorKind::Noop, Some(1)),
            "DBSPWeightValidatorOperator" => {
                text(node, "message")?;
                (OperatorKind::WeightValidator, Some(1))
            }
            "DBSPFilterOperator" => (
                OperatorKind::Filter(filter_predicate(&definitions, field(node, "function")?)?),
                Some(1),
            ),
            "DBSPViewOperator" => (view(node, &definitions)?, Some(1)),
            "DBSPSinkOperator" => (OperatorKind::Sink, Some(1)),
            "DBSPDelayOperator" => (OperatorKind::Delay, Some(1)),
            "DBSPDifferentiateOperator" => (OperatorKind::Differentiate, Some(1)),
            "DBSPIntegrateOperator" => (OperatorKind::Integrate, Some(1)),
            "DBSPIntegrateTraceRetainKeysOperator"
            | "DBSPIntegrateTraceRetainValuesOperator"
            | "DBSPIntegrateTraceRetainNValuesOperator" => (OperatorKind::RetainTrace, Some(2)),
            "DBSPMapOperator" | "DBSPMapIndexOperator" => (
                OperatorKind::Map(closure(&definitions, field(node, "function")?)?),
                Some(1),
            ),
            "DBSPDeindexOperator" => (
                OperatorKind::Deindex(closure(&definitions, field(node, "function")?)?),
                Some(1),
            ),
            "DBSPFlatMapOperator" => (OperatorKind::FlatMap, Some(1)),
            "DBSPFlatMapIndexOperator" => (OperatorKind::FlatMapIndex, Some(1)),
            "DBSPWindowOperator" => (OperatorKind::Window, Some(2)),
            "DBSPChainAggregateOperator" => (OperatorKind::ChainAggregate, Some(1)),
            "DBSPStreamJoinOperator"
            | "DBSPStreamJoinIndexOperator"
            | "DBSPJoinOperator"
            | "DBSPJoinIndexOperator" => {
                let stream = match name {
                    "DBSPStreamJoinOperator" | "DBSPStreamJoinIndexOperator" => {
                        CollectionKind::Snapshot
                    }
                    _ => CollectionKind::Delta,
                };
                (
                    OperatorKind::InnerJoin(
                        closure(&definitions, field(node, "function")?)?,
                        stream,
                    ),
                    Some(2),
                )
            }
            "DBSPWaterlineOperator" | "DBSPApplyOperator" => (OperatorKind::SingleValue, Some(1)),
            "DBSPApply2Operator" => (OperatorKind::SingleValue, Some(2)),
            "DBSPApplyNOperator" => (OperatorKind::SingleValue, None),
            "DBSPControlledKeyFilterOperator" => (OperatorKind::ControlledFilter, Some(2)),
            _ => match stream_kinds {
                Some(kinds) => (stream_operator(name, &kinds[operators.len()]), None),
                None => return Err(Error(format!("unsupported multiset operator {name}"))),
            },
        };
        if arity.is_some_and(|arity| inputs.len() != arity)
            || matches!(kind, OperatorKind::Sum) && inputs.is_empty()
        {
            return Err(Error(format!("invalid input count for {name}")));
        }
        ids.insert(uint(node, "id")?, OperatorId(operators.len()));
        operators.push(Operator { inputs, kind });
    }
    Ok(Circuit { operators })
}

fn stream_operator(name: &str, kinds: &[Option<stream_kinds::Kind>]) -> OperatorKind {
    use EmptyInputRule::*;
    let empty = match name {
        "DBSPAggregateOperator"
        | "DBSPAggregateLinearPostprocessOperator"
        | "DBSPAsofJoinOperator"
        | "DBSPHopOperator"
        | "DBSPIndexedTopKOperator"
        | "DBSPLagOperator"
        | "DBSPLeftJoinFilterMapOperator"
        | "DBSPLeftJoinIndexOperator"
        | "DBSPPartitionedRollingAggregateOperator"
        | "DBSPRankOperator"
        | "DBSPRowNumberOperator"
        | "DBSPStreamAggregateOperator" => First,
        "DBSPStarJoinIndexOperator" | "DBSPStarJoinOperator" => Any,
        "DBSPAtomicSumOperator" => AtomicSum,
        "DBSPSubtractOperator" => Subtract,
        "DBSPStreamAntiJoinOperator" => StreamAntiJoin,
        "DBSPLeftJoinOperator" => LeftJoin,
        _ => Keep,
    };
    let collection = match kinds.first() {
        Some(Some(stream_kinds::Kind::Collection)) => Some(CollectionKind::Snapshot),
        Some(Some(stream_kinds::Kind::Delta)) => Some(CollectionKind::Delta),
        _ => None,
    };
    OperatorKind::StreamOperator {
        outputs: kinds.len(),
        empty,
        collection,
    }
}

/// Re-emit the retained graph in Java's field traversal order, so the Java
/// decoder encounters each inner definition before its references. Serializing
/// a filtered JSON array alone can drop definitions owned by removed operators.
pub(crate) fn retain_operators(wire: &Value, retained: &[bool]) -> Result<Value> {
    let mut filtered = wire.clone();
    let operators = filtered["allOperators"]
        .as_array_mut()
        .expect("validated operator array");
    let mut index = 0;
    operators.retain(|_| {
        let keep = retained[index];
        index += 1;
        keep
    });
    encode_rewrite(wire, &filtered)
}

pub(crate) fn remove_system_table(wire: &Value, table: &str) -> Result<Value> {
    let operators = array(wire, "allOperators")?;
    let mut matching = operators.iter().enumerate().filter(|(_, node)| {
        node["class"] == "DBSPSourceMultisetOperator" && node["tableName"]["name"] == table
    });
    let Some((index, target)) = matching.next() else {
        Definitions::read(wire)?;
        return Ok(wire.clone());
    };
    if matching.next().is_some() {
        return Err(Error(format!("duplicate system {table} table source")));
    }
    let id = uint(target, "id")?;
    // The table is safe to remove only when no operator reads it. This
    // structural check does not need multiset rules for unrelated operators.
    for node in operators {
        for input in array(node, "inputs")? {
            if wire_port(input)?.operator == id {
                return Err(Error(format!("system {table} table still has consumers")));
            }
        }
    }
    let mut retained = vec![true; operators.len()];
    retained[index] = false;
    let mut rewritten = retain_operators(wire, &retained)?;
    let inputs = rewritten["metadata"]["inputs"]
        .as_array_mut()
        .ok_or_else(|| Error("missing input table schema".into()))?;
    inputs.retain(|entry| entry["name"] != table);
    Ok(rewritten)
}

pub(crate) fn rewrite_operators(
    wire: &Value,
    circuit: &Circuit,
    edits: Vec<Edit>,
) -> Result<Value> {
    rewrite_with_generated_literals(wire, circuit, edits, &HashMap::new())
}

pub(crate) fn rewrite_with_generated_literals(
    wire: &Value,
    circuit: &Circuit,
    edits: Vec<Edit>,
    generated: &HashMap<u64, GeneratedInteger>,
) -> Result<Value> {
    let definitions = Definitions::read(wire)?;
    let original = array(wire, "allOperators")?;
    let mut next_inner_id = definitions
        .inner
        .keys()
        .chain(generated.keys())
        .copied()
        .max()
        .unwrap_or_default();
    let mut fresh = || {
        next_inner_id = next_inner_id
            .checked_add(1)
            .ok_or_else(|| Error("no fresh inner identity for empty stream".into()))?;
        Ok(next_inner_id)
    };
    let mut next_outer_id = max_outer_id(wire)?;
    let mut retained = Vec::with_capacity(original.len());
    let mut next_name = wire
        .get(GENERATED_NAME)
        .map(|_| uint(wire, GENERATED_NAME))
        .transpose()?;
    let mut replaced = Vec::with_capacity(original.len());
    let mut output_types = Vec::with_capacity(original.len());
    let mut original_types = Vec::with_capacity(original.len());

    for ((original_node, operator), edit) in original.iter().zip(&circuit.operators).zip(edits) {
        let original_type = original_node.get("outputType").or_else(|| {
            if matches!(
                operator.kind,
                OperatorKind::View { .. } | OperatorKind::Sink
            ) {
                let port = operator.inputs[0];
                if port.output == 0 {
                    original_types[port.operator.0]
                } else {
                    original[port.operator.0].get("errorType")
                }
            } else {
                None
            }
        });
        original_types.push(original_type);
        let mut node = original_node.clone();
        let mut input_output_type = None;
        let was_replaced = match edit {
            Edit::Bypass => {
                replaced.push(false);
                output_types.push(None);
                continue;
            }
            Edit::MoveDifferentiate { map, input } => {
                next_outer_id = next_outer_id.checked_add(1).ok_or_else(|| {
                    Error("no fresh operator identity for differentiation".into())
                })?;
                // Keep the original map until the surrounding Java repeat
                // reaches DeadCode. The new map becomes the differential's output.
                node["id"] = json!(next_outer_id);
                node["annotations"] = json!([]);
                node["outputType"] = json!({"node": input_type(input, original, &output_types)?});
                node["inputs"] = json!([{
                    "outputNumber": input.output,
                    "operator": {"node": original[input.operator.0]["id"]},
                }]);
                retained.push(node);
                let template = &original[map.0];
                node = template.clone();
                node["id"] = original_node["id"].clone();
                node["inputs"] = json!([{"outputNumber": 0, "operator": {"node": next_outer_id}}]);
                for key in ["function", "outputType"] {
                    node[key] = inner_reference(field(template, key)?)?;
                }
                true
            }
            Edit::MapUnmatchedLeft(left) => {
                let name = next_name.as_mut().ok_or_else(|| {
                    Error("left join rewrite needs the generated-name counter".into())
                })?;
                let collection = input_type(left, original, &output_types)?;
                let collection = definitions
                    .inner
                    .get(&collection)
                    .ok_or_else(|| Error("missing left join input type".into()))?;
                let function = generated_closure::unmatched_left(
                    &definitions,
                    collection,
                    field(original_node, "function")?,
                    *name,
                    &mut fresh,
                )?;
                *name = advance_name(*name, 1)?;
                let mut output = definitions
                    .resolve(
                        original_type
                            .ok_or_else(|| Error("missing left join output type".into()))?,
                    )?
                    .clone();
                output["id"] = fresh()?.into();
                node["class"] = "DBSPMapOperator".into();
                node["annotations"] = json!([]);
                node["inputs"] = json!([{
                    "outputNumber": left.output,
                    "operator": {"node": original[left.operator.0]["id"]},
                }]);
                node["outputType"] = output;
                node["function"] = function;
                node.as_object_mut()
                    .expect("operator object")
                    .remove("balanced");
                true
            }
            Edit::Empty(stream) => {
                let ty = definitions.resolve(
                    original_type.ok_or_else(|| Error("missing type for empty stream".into()))?,
                )?;
                let (output_type, mut function) = match text(ty, "class")? {
                    "DBSPTypeZSet" => {
                        // A fresh unindexed constant owns a new Z-set type.
                        // Its element type is shared with the original output.
                        let mut output_type = ty.clone();
                        output_type["id"] = fresh()?.into();
                        let function = json!({
                            "class": "DBSPZSetExpression", "id": fresh()?,
                            "elementType": inner_reference(field(ty, "elementType")?)?,
                            "data": [], "type": inner_reference(&output_type)?, "weights": [],
                        });
                        (output_type, function)
                    }
                    "DBSPTypeIndexedZSet" => {
                        let output_type = inner_reference(ty)?;
                        let function = json!({
                            "class": "DBSPIndexedZSetExpression", "id": fresh()?,
                            "type": output_type,
                        });
                        (output_type, function)
                    }
                    _ => return Err(Error("empty stream requires a Z-set output type".into())),
                };
                if ty.get("sourceEmpty").is_some() {
                    // Both empty literal constructors use CalciteObject.EMPTY.
                    function["sourceEmpty"] = true.into();
                }
                let class = match stream {
                    CollectionKind::Snapshot => "DBSPConstantOperator",
                    CollectionKind::Delta => "DBSPIncrementalConstantOperator",
                };
                node = constant_operator(original_node, class, output_type, function, json!([]));
                true
            }
            Edit::SnapshotConstant(source) => {
                let delta = &original[source.operator.0];
                node = constant_operator(
                    original_node,
                    "DBSPConstantOperator",
                    inner_reference(field(delta, "outputType")?)?,
                    inner_reference(field(delta, "function")?)?,
                    delta
                        .get("annotations")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                );
                true
            }
            Edit::FoldedConstant(constant) => {
                let (output_type, function) = match constant.value {
                    ConstantValue::Indexed {
                        output_type,
                        function,
                    } => (json!({"node": output_type}), json!({"node": function})),
                    ConstantValue::Rows { element_type, rows } => {
                        let element = definitions
                            .inner
                            .get(&element_type)
                            .ok_or_else(|| Error("missing constant row type".into()))?;
                        let (data, weights): (Vec<_>, Vec<_>) = rows
                            .into_iter()
                            .map(|(row, weight)| {
                                Ok((emit_constant_row(&definitions, generated, row)?, weight))
                            })
                            .collect::<Result<Vec<_>>>()?
                            .into_iter()
                            .unzip();
                        let mut output_type = json!({"class": "DBSPTypeZSet", "id": fresh()?});
                        for key in ["sourceEmpty", "position"] {
                            if let Some(value) = element.get(key) {
                                output_type[key] = value.clone();
                            }
                        }
                        output_type["elementType"] = json!({"node": element_type});
                        output_type["name"] = "WSet".into();
                        output_type["code"] = "ZSET".into();
                        output_type["mayBeNull"] = false.into();
                        let mut function = json!({
                            "class": "DBSPZSetExpression", "id": fresh()?,
                            "elementType": {"node": element_type}, "data": data,
                            "type": inner_reference(&output_type)?, "weights": weights,
                        });
                        if element.get("sourceEmpty").is_some() {
                            function["sourceEmpty"] = true.into();
                        }
                        (output_type, function)
                    }
                };
                node = constant_operator(
                    &original[constant.origin.0],
                    "DBSPConstantOperator",
                    output_type,
                    function,
                    json!([]),
                );
                node["id"] = original_node["id"].clone();
                true
            }
            edit @ (Edit::Retain(_) | Edit::ReplaceDeindex(_) | Edit::ReplaceIdentity(_)) => {
                let replacement = match &edit {
                    Edit::ReplaceDeindex(_) => Some("DBSPMapOperator"),
                    Edit::ReplaceIdentity(_) => Some("DBSPNoopOperator"),
                    _ => None,
                };
                let ports = match &edit {
                    Edit::Retain(ports) | Edit::ReplaceDeindex(ports) => ports.as_slice(),
                    Edit::ReplaceIdentity(port) => std::slice::from_ref(port),
                    _ => unreachable!("matched retaining or replacing an operator"),
                };
                if let Some(class) = replacement {
                    node["class"] = class.into();
                    node["annotations"] = json!([]);
                }
                let class = text(&node, "class")?;
                let is_noop = class == "DBSPNoopOperator";
                if matches!(class, "DBSPViewOperator" | "DBSPSinkOperator")
                    && node.get("outputType").is_none()
                {
                    // The wire omits types inferred from view inputs.
                    input_output_type = Some(input_type(ports[0], original, &output_types)?);
                }
                let inputs: Vec<_> = ports
                    .iter()
                    .map(|port| {
                        json!({
                            "outputNumber": port.output,
                            "operator": {"node": original[port.operator.0]["id"]},
                        })
                    })
                    .collect();
                // Retain wire IDs for replacement nodes, but account for the
                // new object identity when a downstream constructor runs.
                let changed = replacement.is_some()
                    || array(&node, "inputs")? != inputs.as_slice()
                    || ports.iter().any(|port| replaced[port.operator.0]);
                if changed {
                    if class == "DBSPControlledKeyFilterOperator" {
                        return Err(Error(
                            "rewriting a controlled filter requires regenerating its error type"
                                .into(),
                        ));
                    }
                    if node.get("outputType").is_some() && wire_edits::output_type_from_input(class)
                    {
                        // Those constructors take their type object from the
                        // first rewritten input when rebuilding the operator.
                        node["outputType"] =
                            json!({"node": input_type(ports[0], original, &output_types)?});
                    }
                    node["inputs"] = inputs.into();
                    if is_noop {
                        if let Some(name) = next_name.as_mut() {
                            let source = input_type(ports[0], original, &output_types)?;
                            let collection = definitions
                                .inner
                                .get(&source)
                                .ok_or_else(|| Error("missing collection type for noop".into()))?;
                            node["function"] = generated_closure::function(
                                &definitions,
                                collection,
                                *name,
                                &mut fresh,
                            )?;
                            *name = advance_name(*name, 1)?;
                        } else if matches!(edit, Edit::ReplaceIdentity(_)) {
                            return Err(Error(
                                "identity replacement needs the generated-name counter".into(),
                            ));
                        }
                    }
                }
                changed
            }
        };
        output_types.push(
            node.get("outputType")
                .map(inner_identity)
                .transpose()?
                .or(input_output_type),
        );
        retained.push(node);
        replaced.push(was_replaced);
    }
    let mut rewritten = wire.clone();
    rewritten["allOperators"] = retained.into();
    if let Some(name) = next_name {
        rewritten[GENERATED_NAME] = name.into();
    }
    encode_rewrite(wire, &rewritten)
}

fn emit_constant_row(
    definitions: &Definitions<'_>,
    generated: &HashMap<u64, GeneratedInteger>,
    row: u64,
) -> Result<Value> {
    let Some(literal) = generated.get(&row) else {
        return Ok(json!({"node": row}));
    };
    let mut definition = definitions
        .inner
        .get(&literal.template)
        .copied()
        .ok_or_else(|| Error("missing generated integer literal template".into()))?
        .clone();
    definition["id"] = row.into();
    definition["value"] = literal.value.to_string().into();
    Ok(definition)
}

fn constant_operator(
    origin: &Value,
    class: &str,
    output_type: Value,
    function: Value,
    annotations: Value,
) -> Value {
    let mut node = json!({"class": class, "id": origin["id"]});
    for key in [
        "provenance",
        "sourceEmpty",
        "sourcePosition",
        "sourcePositions",
    ] {
        if let Some(value) = origin.get(key) {
            node[key] = value.clone();
        }
    }
    node["annotations"] = annotations;
    node["inputs"] = json!([]);
    node["outputType"] = output_type;
    node["function"] = function;
    node
}

fn inner_reference(node: &Value) -> Result<Value> {
    Ok(json!({"node": inner_identity(node)?}))
}

fn inner_identity(node: &Value) -> Result<u64> {
    let key = if node.get("class").is_some() {
        "id"
    } else {
        "node"
    };
    uint(node, key)
}

fn input_type(port: Port, original: &[Value], output_types: &[Option<u64>]) -> Result<u64> {
    if port.output == 0 {
        output_types[port.operator.0]
            .ok_or_else(|| Error("missing effective type of rewritten input port".into()))
    } else {
        inner_identity(field(&original[port.operator.0], "errorType")?)
    }
}

fn max_outer_id(wire: &Value) -> Result<u64> {
    let mut id = uint(wire, "id")?;
    for node in array(wire, "allOperators")?
        .iter()
        .chain(array(wire, "declarations")?)
    {
        id = id.max(uint(node, "id")?);
    }
    Ok(id)
}

pub(crate) fn insert_weight_validation(wire: &Value, circuit: &Circuit) -> Result<Value> {
    let original = array(wire, "allOperators")?;
    let mut next_id = max_outer_id(wire)?;
    let mut validators = vec![None; original.len()];
    let mut operators = Vec::with_capacity(original.len());
    for (index, (node, operator)) in original.iter().zip(&circuit.operators).enumerate() {
        let mut consumer = node.clone();
        let inputs = consumer["inputs"]
            .as_array_mut()
            .expect("validated input list");
        for (input, port) in inputs.iter_mut().zip(&operator.inputs) {
            if let Some(validator) = validators[port.operator.0] {
                input["operator"]["node"] = json!(validator);
            }
        }
        operators.push(consumer);
        if matches!(operator.kind, OperatorKind::Source { primary_key: false }) {
            next_id = next_id
                .checked_add(1)
                .ok_or_else(|| Error("no fresh operator identity for weight validation".into()))?;
            let table = text(field(node, "tableName")?, "name")?;
            let mut validator = json!({"class": "DBSPWeightValidatorOperator", "id": next_id});
            for field in ["provenance", "sourcePosition"] {
                if let Some(value) = node.get(field) {
                    validator[field] = value.clone();
                }
            }
            validator["annotations"] = json!([]);
            validator["inputs"] =
                json!([{"outputNumber": 0, "operator": {"node": uint(node, "id")?}}]);
            validator["outputType"] = inner_reference(field(node, "outputType")?)?;
            validator["message"] = json!(format!("Table {table}"));
            operators.push(validator);
            validators[index] = Some(next_id);
        }
    }
    let mut rewritten = wire.clone();
    rewritten["allOperators"] = operators.into();
    encode_rewrite(wire, &rewritten)
}

pub(crate) fn insert_output_distinct(
    wire: &Value,
    distinct: impl Iterator<Item = bool>,
) -> Result<Value> {
    let original = array(wire, "allOperators")?;
    let mut provenance = Provenance::read(original)?;
    let ids = original
        .iter()
        .map(|node| Ok((uint(node, "id")?, node)))
        .collect::<Result<HashMap<_, _>>>()?;
    let mut next_id = max_outer_id(wire)?;

    let mut operators = Vec::with_capacity(original.len());
    for (node, insert) in original.iter().zip(distinct) {
        let mut node = node.clone();
        if insert {
            if node.get("sourcePosition").is_some() {
                return Err(Error(
                    "cannot derive intermediate provenance from position-only observation fields"
                        .into(),
                ));
            }
            next_id = next_id
                .checked_add(1)
                .ok_or_else(|| Error("no fresh operator identity for output distinct".into()))?;
            let input = &array(&node, "inputs")?[0];
            let source_id = uint(field(input, "operator")?, "node")?;
            let source = ids
                .get(&source_id)
                .copied()
                .ok_or_else(|| Error("undefined sink input".into()))?;
            let name = if uint(input, "outputNumber")? == 0 {
                "outputType"
            } else {
                "errorType"
            };
            let output_type = inner_reference(field(source, name)?)?;
            let mut new_operator = json!({
                "class": "DBSPStreamDistinctOperator", "id": next_id,
            });
            if let Some(origin) = node.get("provenance") {
                new_operator["provenance"] = provenance.intermediate(origin)?;
            }
            new_operator["annotations"] = json!([]);
            new_operator["inputs"] = json!([input]);
            new_operator["outputType"] = output_type;
            operators.push(new_operator);
            node["inputs"] = json!([{"outputNumber": 0, "operator": {"node": next_id}}]);
        }
        operators.push(node);
    }
    let mut rewritten = wire.clone();
    rewritten["allOperators"] = operators.into();
    encode_rewrite(wire, &rewritten)
}

pub(crate) fn encode_rewrite(original: &Value, rewritten: &Value) -> Result<Value> {
    let definitions = Definitions::read(original)?;
    encode_with_inner(original, rewritten, &definitions, &HashMap::new())
}

fn encode_with_inner(
    original: &Value,
    rewritten: &Value,
    definitions: &Definitions<'_>,
    replacements: &HashMap<u64, Value>,
) -> Result<Value> {
    let mut encoder = Encoder {
        definitions,
        replacements,
        emitted: HashSet::new(),
    };
    let mut result = encoder.value(rewritten, false, true, 0)?;
    Provenance::read(array(original, "allOperators")?)?.relocate(
        result["allOperators"]
            .as_array_mut()
            .expect("validated operator array"),
    )?;
    feldera_sql_difftest::compare_circuits(&result, &result).map_err(|e| Error(e.to_string()))?;
    Ok(result)
}

struct Encoder<'a, 'b> {
    definitions: &'b Definitions<'a>,
    replacements: &'b HashMap<u64, Value>,
    emitted: HashSet<u64>,
}

impl Encoder<'_, '_> {
    fn value(&mut self, value: &Value, inner: bool, root: bool, depth: usize) -> Result<Value> {
        nesting(depth)?;
        match value {
            Value::Object(object) => {
                if inner
                    && ((object.contains_key("class") && object.contains_key("id"))
                        || (object.len() == 1 && object.contains_key("node")))
                {
                    let id = if object.contains_key("class") {
                        uint(value, "id")?
                    } else {
                        uint(value, "node")?
                    };
                    if !self.emitted.insert(id) {
                        return Ok(serde_json::json!({"node": id}));
                    }
                    let definition = self
                        .replacements
                        .get(&id)
                        .or_else(|| self.definitions.inner.get(&id).copied())
                        .or_else(|| object.contains_key("class").then_some(value))
                        .ok_or_else(|| {
                            Error(format!("missing inner definition {id} during encoding"))
                        })?;
                    return self.object(definition, false, depth + 1);
                }
                self.object(value, root, depth + 1)
            }
            Value::Array(values) => values
                .iter()
                .map(|v| self.value(v, inner, false, depth + 1))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array),
            _ => Ok(value.clone()),
        }
    }

    fn object(&mut self, value: &Value, root: bool, depth: usize) -> Result<Value> {
        let object = value.as_object().expect("validated object");
        let mut encoded = serde_json::Map::new();
        for (name, child) in object {
            let plain = matches!(name.as_str(), "annotations" | "provenance")
                || (root && name == "metadata");
            let inner = !matches!(name.as_str(), "allOperators" | "declarations")
                && !(name == "operator" && object.contains_key("outputNumber"));
            let child = if plain {
                child.clone()
            } else {
                self.value(child, inner, false, depth + 1)?
            };
            encoded.insert(name.clone(), child);
        }
        Ok(Value::Object(encoded))
    }
}
