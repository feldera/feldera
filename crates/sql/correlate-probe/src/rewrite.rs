/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Feldera adaptation: bounded FRel port of RelDecorrelator's value generator,
// Project, Filter, SetOp, Correlate and field-access frame rules at Calcite
// 4bf922bbbc1cbd570a2f201339a46a2d8f7a763b. Scalar aggregates are in aggregate.rs;
// grouped aggregates, window, sort and nested right correlations are refused.

mod aggregate;
mod sort;

use crate::{
    Error, Fragment,
    recipe::{Column, Expression, Node, PlanId, Recipe},
};
use feldera_sql_rel_model::{
    TypeId,
    expression_objects::ObjectKind,
    expressions::ExprId,
    operators::OperatorObject,
    relation_properties::{JoinKind, Properties},
    types::TypeKind,
};
use std::collections::{BTreeMap, BTreeSet};

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
struct Frame {
    node: PlanId,
    columns: Vec<Column>,
    payload: Vec<usize>,
    // One correlation identity is permitted per dependent subtree. The identity
    // is checked before any field is entered in this map.
    correlations: BTreeMap<u32, usize>,
}

struct Domain {
    identity: u32,
    node: PlanId,
    fields: Vec<u32>,
    columns: Vec<Column>,
}

struct Rewriter<'a> {
    fragment: &'a Fragment,
    expressions: Vec<Vec<ExprId>>,
    dependencies: Vec<Option<BTreeSet<u32>>>,
    rewritten: Vec<Option<Frame>>,
    nodes: Vec<Node>,
    remaining: usize,
    expansion: ExpansionBudget,
    removed: usize,
}

pub(crate) fn rewrite(fragment: &Fragment) -> Result<Recipe> {
    let count = fragment.producers.relations.len();
    let mut expressions = vec![None; count];
    for usage in &fragment.expressions.relation_uses {
        let slot = &mut expressions[usage.relation as usize];
        if slot.is_some() {
            return Err(Error::Unsupported(
                "a relation has multiple evaluation environments",
            ));
        }
        *slot = Some(usage.roots.clone());
    }
    for relation in &fragment.properties.relations {
        // SQL-to-rel attaches alias labels to FROM relations. Original/Rebuild
        // and Set preserve their metadata; Project/Filter recipes explicitly
        // identify their originals for alias preservation by the S1 adapter.
        // No optimizer hint or hint on a removed Correlate is supported.
        let preserves_alias = matches!(
            relation.fields,
            Properties::Scan { .. }
                | Properties::Values { .. }
                | Properties::Project { .. }
                | Properties::Filter { .. }
                | Properties::Set { .. }
                | Properties::Aggregate { .. }
                | Properties::Sort { .. }
        );
        if relation
            .metadata
            .hints
            .iter()
            .any(|hint| !preserves_alias || hint.name != "alias")
        {
            return Err(Error::Unsupported("relational hints need a rewrite policy"));
        }
    }
    // Factoring a value generator changes evaluation frequency. Only the pinned
    // deterministic standard operators used by this experiment are eligible.
    for operator in &fragment.expressions.operator_objects {
        match operator {
            OperatorObject::SumZero {} => (),
            OperatorObject::Builtin {
                registry,
                registry_version,
                key,
            } if registry_version == "1"
                && ((registry == "calcite-s1-fixture" && deterministic(key))
                    || (registry == "feldera-s1-internals" && key == "array-aggregate")) => {}
            _ => {
                return Err(Error::Unsupported(
                    "operator determinism is not established",
                ));
            }
        }
    }
    let mut rewriter = Rewriter {
        fragment,
        expressions: expressions
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect(),
        dependencies: vec![None; count],
        rewritten: vec![None; count],
        nodes: Vec::new(),
        remaining: 100_000,
        expansion: ExpansionBudget::new(),
        removed: 0,
    };
    let mut roots = Vec::new();
    for root in &fragment.producers.roots {
        roots.push(rewriter.root(*root, 0)?.node);
    }
    Ok(Recipe {
        schema_version: "correlation-recipe-v1",
        id: fragment.id.clone(),
        nodes: rewriter.nodes,
        roots,
        removed_correlates: rewriter.removed,
    })
}

fn deterministic(key: &str) -> bool {
    matches!(
        key.strip_prefix("SqlStdOperatorTable."),
        Some(
            "EQUALS"
                | "NOT_EQUALS"
                | "LESS_THAN"
                | "LESS_THAN_OR_EQUAL"
                | "GREATER_THAN"
                | "GREATER_THAN_OR_EQUAL"
                | "IS_NOT_DISTINCT_FROM"
                | "AND"
                | "OR"
                | "NOT"
                | "IS_NULL"
                | "IS_NOT_NULL"
                | "IS_TRUE"
                | "IS_NOT_TRUE"
                | "IS_FALSE"
                | "IS_NOT_FALSE"
                | "CASE"
                | "COALESCE"
                | "CAST"
                | "COUNT"
                | "SUM"
                | "SUM0"
                | "MIN"
                | "MAX"
                | "AVG"
                | "SINGLE_VALUE"
                | "PLUS"
                | "MINUS"
                | "MULTIPLY"
                | "DIVIDE"
                | "CHECKED_PLUS"
                | "CHECKED_MINUS"
                | "CHECKED_MULTIPLY"
                | "SEARCH"
                | "YEAR"
                | "CHAR_LENGTH"
                | "SUBSTRING"
                | "LIKE"
        )
    )
}

impl Rewriter<'_> {
    fn tick(&mut self, depth: usize) -> Result<()> {
        if depth > 128 || self.remaining == 0 {
            return Err(Error::Limit);
        }
        self.remaining -= 1;
        Ok(())
    }

    fn push(&mut self, node: Node) -> PlanId {
        let id = PlanId(self.nodes.len() as u32);
        self.nodes.push(node);
        id
    }

    fn row_width(&self, ty: TypeId) -> usize {
        let TypeKind::Row { fields, .. } =
            &self.fragment.table.type_table.types[ty.0 as usize].kind
        else {
            unreachable!("validated relation row")
        };
        fields.len()
    }

    fn columns(&mut self, ty: TypeId) -> Result<Vec<Column>> {
        let TypeKind::Row { fields, .. } =
            &self.fragment.table.type_table.types[ty.0 as usize].kind
        else {
            unreachable!("validated relation row")
        };
        self.expansion.fields(fields.len())?;
        for field in fields {
            self.expansion.name(field.name.name.len())?;
        }
        Ok(fields
            .iter()
            .map(|field| Column {
                name: field.name.name.clone(),
                ty: field.ty,
            })
            .collect())
    }

    fn original(&mut self, relation: u32) -> Result<Frame> {
        let columns = self.columns(self.fragment.producers.relations[relation as usize].row)?;
        self.expansion.fields(columns.len())?;
        Ok(Frame {
            node: self.push(Node::Original { relation }),
            payload: (0..columns.len()).collect(),
            columns,
            correlations: BTreeMap::new(),
        })
    }

    fn expression_dependencies(
        &mut self,
        id: ExprId,
        depth: usize,
        out: &mut BTreeSet<u32>,
    ) -> Result<()> {
        self.tick(depth)?;
        match self.fragment.expressions.expressions[id.0 as usize]
            .kind
            .clone()
        {
            ObjectKind::Correlation { identity } => {
                if !out.contains(&identity) {
                    self.expansion.fields(1)?;
                    out.insert(identity);
                }
            }
            ObjectKind::FieldAccess { input, .. } => {
                self.expression_dependencies(input, depth + 1, out)?
            }
            ObjectKind::Call { arguments, .. } | ObjectKind::WindowAggregate { arguments, .. } => {
                for child in arguments {
                    self.expression_dependencies(child, depth + 1, out)?;
                }
            }
            ObjectKind::Lambda { body, .. } => {
                self.expression_dependencies(body, depth + 1, out)?
            }
            _ => (),
        }
        Ok(())
    }

    fn dependencies(&mut self, id: u32, depth: usize) -> Result<BTreeSet<u32>> {
        self.tick(depth)?;
        if let Some(value) = &self.dependencies[id as usize] {
            self.expansion.fields(value.len())?;
            return Ok(value.clone());
        }
        let mut result = BTreeSet::new();
        for expression in self.expressions[id as usize].clone() {
            self.expression_dependencies(expression, depth + 1, &mut result)?;
        }
        for input in self.fragment.producers.relations[id as usize]
            .inputs
            .clone()
        {
            let child = self.dependencies(input, depth + 1)?;
            self.expansion.fields(child.len())?;
            result.extend(child);
        }
        self.expansion.fields(result.len())?;
        self.dependencies[id as usize] = Some(result.clone());
        Ok(result)
    }

    fn root(&mut self, id: u32, depth: usize) -> Result<Frame> {
        self.tick(depth)?;
        if let Some(frame) = &self.rewritten[id as usize] {
            self.expansion.frame(frame)?;
            return Ok(frame.clone());
        }
        let relation = self.fragment.producers.relations[id as usize].clone();
        let frame = match self.fragment.properties.relations[id as usize]
            .fields
            .clone()
        {
            Properties::Correlate {
                correlation,
                required,
                join_kind,
            } => {
                if !matches!(join_kind, JoinKind::Inner | JoinKind::Left) {
                    return Err(Error::Unsupported("correlate join kind"));
                }
                if required.is_empty() {
                    return Err(Error::Unsupported("empty correlation domain"));
                }
                let left = self.root(relation.inputs[0], depth + 1)?;
                let right_id = relation.inputs[1];
                if self
                    .dependencies(right_id, depth + 1)?
                    .iter()
                    .any(|id| *id != correlation)
                {
                    return Err(Error::Unsupported("nested or foreign correlation"));
                }
                self.expansion.fields(required.len())?;
                for field in &required {
                    self.expansion
                        .name(left.columns[*field as usize].name.len())?;
                }
                let mut columns: Vec<_> = required
                    .iter()
                    .map(|field| left.columns[*field as usize].clone())
                    .collect();
                self.name_internal_columns(&mut columns, 0..0)?;
                self.expansion.fields(required.len())?;
                let expressions = required
                    .iter()
                    .map(|field| Expression::Input {
                        field: *field as usize,
                        ty: left.columns[*field as usize].ty,
                    })
                    .collect();
                self.expansion.columns(&columns)?;
                let projection = self.push(Node::Project {
                    relation: None,
                    input: left.node,
                    expressions,
                    columns: columns.clone(),
                });
                let domain = Domain {
                    identity: correlation,
                    fields: required,
                    node: self.push(Node::Distinct { input: projection }),
                    columns,
                };
                let mut right = self.dependent(right_id, &domain, depth + 1)?;
                // Every required field participates in the final null-safe join,
                // including fields used by only one branch of a set operation.
                right = self.supply(right, &domain)?;
                self.expansion.fields(domain.fields.len())?;
                let keys = domain
                    .fields
                    .iter()
                    .map(|field| (*field as usize, right.correlations[field]))
                    .collect();
                let join = self.push(Node::Join {
                    left: left.node,
                    right: right.node,
                    join_kind,
                    keys,
                });
                let columns = self.columns(relation.row)?;
                payload_width(
                    &right,
                    self.row_width(self.fragment.producers.relations[right_id as usize].row),
                    "correlate right payload",
                )?;
                if columns.len() != left.columns.len() + right.payload.len() {
                    return Err(Error::Invalid(
                        "correlate output width differs from its inputs".into(),
                    ));
                }
                let indices = (0..left.columns.len())
                    .chain(right.payload.iter().map(|field| left.columns.len() + field));
                self.expansion.fields(columns.len())?;
                let expressions = indices
                    .zip(&columns)
                    .map(|(field, column)| Expression::Input {
                        field,
                        ty: column.ty,
                    })
                    .collect();
                self.removed += 1;
                self.expansion.fields(columns.len())?;
                self.expansion.columns(&columns)?;
                Frame {
                    node: self.push(Node::Project {
                        relation: None,
                        input: join,
                        expressions,
                        columns: columns.clone(),
                    }),
                    payload: (0..columns.len()).collect(),
                    columns,
                    correlations: BTreeMap::new(),
                }
            }
            _ => {
                // A root traversal has no enclosing correlation binding. Only
                // dependent() may translate a correlated expression. Validation
                // of an explicit FRel environment alone does not establish that
                // a binding is supplied by this root's relational ancestors.
                let mut free = BTreeSet::new();
                for expression in self.expressions[id as usize].clone() {
                    self.expression_dependencies(expression, depth + 1, &mut free)?;
                }
                if !free.is_empty() {
                    return Err(Error::Unsupported(
                        "free correlation outside a dependent subtree",
                    ));
                }
                let inputs = relation
                    .inputs
                    .iter()
                    .map(|input| self.root(*input, depth + 1).map(|frame| frame.node))
                    .collect::<Result<Vec<_>>>()?;
                if inputs.is_empty() {
                    self.original(id)?
                } else {
                    let columns = self.columns(relation.row)?;
                    self.expansion.fields(columns.len())?;
                    Frame {
                        node: self.push(Node::Rebuild {
                            relation: id,
                            inputs,
                        }),
                        payload: (0..columns.len()).collect(),
                        columns,
                        correlations: BTreeMap::new(),
                    }
                }
            }
        };
        self.expansion.frame(&frame)?;
        self.rewritten[id as usize] = Some(frame.clone());
        Ok(frame)
    }

    fn supply(&mut self, mut frame: Frame, domain: &Domain) -> Result<Frame> {
        if domain
            .fields
            .iter()
            .all(|field| frame.correlations.contains_key(field))
        {
            return Ok(frame);
        }
        self.expansion.fields(domain.fields.len())?;
        let keys = domain
            .fields
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                frame
                    .correlations
                    .get(field)
                    .map(|position| (*position, index))
            })
            .collect();
        frame.node = self.push(Node::Join {
            left: frame.node,
            right: domain.node,
            join_kind: JoinKind::Inner,
            keys,
        });
        let offset = frame.columns.len();
        self.expansion.columns(&domain.columns)?;
        frame.columns.extend(domain.columns.iter().cloned());
        self.expansion.fields(domain.fields.len())?;
        frame.correlations = domain
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| (*field, offset + index))
            .collect();
        Ok(frame)
    }

    fn dependent(&mut self, id: u32, domain: &Domain, depth: usize) -> Result<Frame> {
        self.tick(depth)?;
        if self.dependencies(id, depth + 1)?.is_empty() {
            return self.root(id, depth + 1);
        }
        let relation = self.fragment.producers.relations[id as usize].clone();
        let local = self.expressions[id as usize].clone();
        match self.fragment.properties.relations[id as usize]
            .fields
            .clone()
        {
            Properties::Project { variables, .. } => {
                if !variables.is_empty() {
                    return Err(Error::Unsupported("project correlation declarations"));
                }
                let child = self.dependent(relation.inputs[0], domain, depth + 1)?;
                let frame = self.supply(child, domain)?;
                let mut expressions = local
                    .iter()
                    .map(|id| self.expression(*id, &frame, domain, depth + 1))
                    .collect::<Result<Vec<_>>>()?;
                let mut columns = self.columns(relation.row)?;
                self.expansion.fields(columns.len())?;
                let payload = (0..columns.len()).collect();
                let mut correlations = BTreeMap::new();
                for (field, position) in &frame.correlations {
                    // One hidden column, expression and correlation-map entry.
                    self.expansion.fields(3)?;
                    self.expansion.name(frame.columns[*position].name.len())?;
                    correlations.insert(*field, columns.len());
                    let column = frame.columns[*position].clone();
                    expressions.push(Expression::Input {
                        field: *position,
                        ty: column.ty,
                    });
                    columns.push(column);
                }
                self.name_internal_columns(&mut columns, 0..self.row_width(relation.row))?;
                self.expansion.columns(&columns)?;
                Ok(Frame {
                    node: self.push(Node::Project {
                        relation: Some(id),
                        input: frame.node,
                        expressions,
                        columns: columns.clone(),
                    }),
                    columns,
                    payload,
                    correlations,
                })
            }
            Properties::Filter { variables } => {
                if !variables.is_empty() {
                    return Err(Error::Unsupported("filter correlation declarations"));
                }
                let child = self.dependent(relation.inputs[0], domain, depth + 1)?;
                let mut frame = self.supply(child, domain)?;
                payload_width(&frame, self.row_width(relation.row), "filter output")?;
                let condition = self.expression(local[0], &frame, domain, depth + 1)?;
                frame.node = self.push(Node::Filter {
                    relation: id,
                    input: frame.node,
                    condition,
                });
                Ok(frame)
            }
            Properties::Set { .. } => {
                let mut inputs = Vec::new();
                self.expansion.columns(&domain.columns)?;
                let mut columns = domain.columns.clone();
                columns.extend(self.columns(relation.row)?);
                let payload = domain.fields.len()..columns.len();
                self.name_internal_columns(&mut columns, payload)?;
                for child in relation.inputs {
                    let child_frame = self.dependent(child, domain, depth + 1)?;
                    let frame = self.supply(child_frame, domain)?;
                    payload_width(
                        &frame,
                        columns.len() - domain.fields.len(),
                        "set operation branch",
                    )?;
                    let positions = domain
                        .fields
                        .iter()
                        .map(|field| frame.correlations[field])
                        .chain(frame.payload.iter().copied());
                    self.expansion.fields(columns.len())?;
                    let expressions = positions
                        .zip(&columns)
                        .map(|(field, column)| self.input_expression(&frame, field, column.ty))
                        .collect::<Result<Vec<_>>>()?;
                    self.expansion.columns(&columns)?;
                    inputs.push(self.push(Node::Project {
                        relation: None,
                        input: frame.node,
                        expressions,
                        columns: columns.clone(),
                    }));
                }
                self.expansion.fields(columns.len() - domain.fields.len())?;
                self.expansion.fields(domain.fields.len())?;
                Ok(Frame {
                    node: self.push(Node::Set {
                        relation: id,
                        inputs,
                    }),
                    payload: (domain.fields.len()..columns.len()).collect(),
                    columns,
                    correlations: domain
                        .fields
                        .iter()
                        .enumerate()
                        .map(|(index, field)| (*field, index))
                        .collect(),
                })
            }
            Properties::Aggregate {
                group,
                groups,
                calls,
            } => {
                if groups.as_slice() != [group.as_slice()] {
                    return Err(Error::Unsupported("aggregate grouping sets"));
                }
                if group.is_empty() {
                    self.scalar_aggregate(id, &calls, domain, depth + 1)
                } else {
                    self.grouped_aggregate(id, &group, &calls, domain, depth + 1)
                }
            }
            Properties::Sort {
                collation,
                offset: false,
                fetch: true,
            } => self.partitioned_fetch(id, &collation, domain, depth + 1),
            _ => Err(Error::Unsupported(
                "correlated join, window, sort, unnest or nested correlate",
            )),
        }
    }

    /// Reserve visible payload names, then name hidden columns deterministically.
    /// Set callers run this once on the common schema, before visiting branches.
    /// Final Correlate projections keep the original row names unchanged.
    fn name_internal_columns(
        &mut self,
        columns: &mut [Column],
        payload: std::ops::Range<usize>,
    ) -> Result<()> {
        let mut names = BTreeSet::new();
        self.expansion.fields(payload.len())?;
        for index in payload.clone() {
            self.expansion.name(columns[index].name.len())?;
            if !names.insert(columns[index].name.clone()) {
                return Err(Error::Invalid(
                    "original projection payload names are not distinct".into(),
                ));
            }
        }
        for (index, column) in columns.iter_mut().enumerate() {
            if payload.contains(&index) {
                continue;
            }
            let mut suffix = 0_usize;
            loop {
                // Both usize decimal strings plus the prefix fit within 64
                // bytes. Charge before creating each candidate and its map copy.
                self.expansion.fields(1)?;
                self.expansion.name(64)?;
                let candidate = if suffix == 0 {
                    format!("__correlation_{index}")
                } else {
                    format!("__correlation_{index}_{suffix}")
                };
                if !names.contains(&candidate) {
                    self.expansion.name(candidate.len())?;
                    names.insert(candidate.clone());
                    column.name = candidate;
                    break;
                }
                suffix += 1;
            }
        }
        Ok(())
    }

    /// Input references always describe the actual child's field. Nullable
    /// widening is a separate, explicit recipe node; no other cast is inferred.
    fn input_expression(
        &mut self,
        frame: &Frame,
        field: usize,
        expected: TypeId,
    ) -> Result<Expression> {
        let actual = frame
            .columns
            .get(field)
            .ok_or_else(|| Error::Invalid("projection field outside rewritten row".into()))?
            .ty;
        let input = Expression::Input { field, ty: actual };
        let types = &self.fragment.table.type_table.types;
        let source = &types[actual.0 as usize];
        let target = &types[expected.0 as usize];
        if actual == expected || (source.kind == target.kind && source.nullable == target.nullable)
        {
            return Ok(input);
        }
        if source.kind == target.kind && !source.nullable && target.nullable {
            self.expansion.fields(1)?;
            return Ok(Expression::WidenNullability {
                input: Box::new(input),
                ty: expected,
            });
        }
        Err(Error::Unsupported(
            "projection coercion beyond top-level nullable widening",
        ))
    }

    fn expression(
        &mut self,
        id: ExprId,
        frame: &Frame,
        domain: &Domain,
        depth: usize,
    ) -> Result<Expression> {
        self.tick(depth)?;
        let expression = self.fragment.expressions.expressions[id.0 as usize].clone();
        Ok(match expression.kind {
            ObjectKind::InputRef { object } => {
                let field = *frame
                    .payload
                    .get(self.fragment.table.objects[object.0 as usize].field as usize)
                    .ok_or_else(|| {
                        Error::Invalid("input reference outside rewritten payload".into())
                    })?;
                self.input_expression(frame, field, expression.ty)?
            }
            ObjectKind::FieldAccess { input, field } => {
                if let ObjectKind::Correlation { identity } =
                    self.fragment.expressions.expressions[input.0 as usize].kind
                {
                    if identity != domain.identity {
                        return Err(Error::Unsupported("foreign correlated field"));
                    }
                    let position = *frame.correlations.get(&field).ok_or(Error::Unsupported(
                        "correlated field absent from required columns",
                    ))?;
                    self.input_expression(frame, position, expression.ty)?
                } else {
                    Expression::Field {
                        expression: id,
                        input: Box::new(self.expression(input, frame, domain, depth + 1)?),
                    }
                }
            }
            ObjectKind::Call { arguments, .. } => Expression::Call {
                expression: id,
                arguments: arguments
                    .iter()
                    .map(|arg| self.expression(*arg, frame, domain, depth + 1))
                    .collect::<Result<_>>()?,
            },
            ObjectKind::Literal { .. } | ObjectKind::SearchArgument { .. } => {
                Expression::Existing { expression: id }
            }
            _ => {
                return Err(Error::Unsupported(
                    "whole-row correlation, lambda or window expression",
                ));
            }
        })
    }
}

fn payload_width(frame: &Frame, expected: usize, context: &str) -> Result<()> {
    if frame.payload.len() != expected
        || frame
            .payload
            .iter()
            .any(|field| *field >= frame.columns.len())
    {
        return Err(Error::Invalid(format!(
            "{context} width differs from rewritten payload"
        )));
    }
    Ok(())
}

/// Bound cumulative generated/copied schema slots and owned name bytes, not
/// allocator-specific RSS. Vector capacity and BTreeMap overhead are bounded by
/// these logical entries. Charge before allocating, including memoized returns:
/// a small shared type or DAG can otherwise cause large repeated frame copies.
struct ExpansionBudget {
    fields_remaining: usize,
    name_bytes_remaining: usize,
}

impl ExpansionBudget {
    fn new() -> Self {
        Self {
            fields_remaining: 100_000,
            name_bytes_remaining: 8 * 1024 * 1024,
        }
    }

    fn fields(&mut self, count: usize) -> Result<()> {
        self.fields_remaining = self
            .fields_remaining
            .checked_sub(count)
            .ok_or(Error::Limit)?;
        Ok(())
    }

    fn name(&mut self, bytes: usize) -> Result<()> {
        self.name_bytes_remaining = self
            .name_bytes_remaining
            .checked_sub(bytes)
            .ok_or(Error::Limit)?;
        Ok(())
    }

    fn columns(&mut self, columns: &[Column]) -> Result<()> {
        self.fields(columns.len())?;
        for column in columns {
            self.name(column.name.len())?;
        }
        Ok(())
    }

    fn frame(&mut self, frame: &Frame) -> Result<()> {
        self.columns(&frame.columns)?;
        self.fields(frame.payload.len())?;
        self.fields(frame.correlations.len())
    }
}
