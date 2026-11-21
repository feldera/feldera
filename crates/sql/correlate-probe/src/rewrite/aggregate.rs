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
// Feldera adaptation of RelDecorrelator.decorrelateRel(Aggregate) and
// rewriteScalarAggregate at Calcite 4bf922bbbc1cbd570a2f201339a46a2d8f7a763b.
// Supports one GROUP BY set and ordinary unary aggregates.

use super::{Domain, Frame, Result, Rewriter};
use crate::{
    Error,
    recipe::{Expression, Node},
};
use feldera_sql_rel_model::{
    operators::OperatorObject,
    relation_properties::{Aggregate, JoinKind, OptionalField, OptionalFields, Properties},
    types::{IntegerWidth, TypeKind},
};
use std::collections::BTreeMap;

impl Rewriter<'_> {
    pub(super) fn scalar_aggregate(
        &mut self,
        id: u32,
        calls: &[Aggregate],
        domain: &Domain,
        depth: usize,
    ) -> Result<Frame> {
        self.tick(depth)?;
        let counts = self.check_aggregate_calls(id, calls, true)?;

        let relation = &self.fragment.producers.relations[id as usize];
        let row = relation.row;
        let child = self.dependent(relation.inputs[0], domain, depth + 1)?;
        let child = self.supply(child, domain)?;
        let width = domain.fields.len();

        // Final payload names remain original. Hidden group names avoid them;
        // the input projection then reserves those group names and names its
        // remaining fields independently. Every mapping below is ordinal.
        let mut columns = self.columns(row)?;
        self.expansion.columns(&domain.columns)?;
        columns.extend(domain.columns.iter().cloned());
        self.name_internal_columns(&mut columns, 0..calls.len())?;
        self.expansion.columns(&columns[calls.len()..])?;
        let mut input_columns = columns[calls.len()..].to_vec();
        self.expansion.fields(width + child.payload.len())?;
        let mut input_expressions = Vec::with_capacity(width + child.payload.len());
        for field in &domain.fields {
            let position = child.correlations[field];
            input_expressions.push(Expression::Input {
                field: position,
                ty: child.columns[position].ty,
            });
        }
        for position in &child.payload {
            let column = &child.columns[*position];
            self.expansion.columns(std::slice::from_ref(column))?;
            input_columns.push(column.clone());
            input_expressions.push(Expression::Input {
                field: *position,
                ty: column.ty,
            });
        }
        self.name_internal_columns(&mut input_columns, 0..width)?;
        let input = self.push(Node::Project {
            relation: None,
            input: child.node,
            expressions: input_expressions,
            columns: input_columns,
        });
        self.expansion.fields(width + calls.len())?;
        let arguments = self.aggregate_arguments(calls, |ordinal| {
            (ordinal < child.payload.len()).then_some(width + ordinal)
        })?;
        let grouped = self.push(Node::Aggregate {
            relation: id,
            input,
            group: (0..width).collect(),
            arguments,
        });
        self.expansion.fields(width)?;
        let joined = self.push(Node::Join {
            left: domain.node,
            right: grouped,
            join_kind: JoinKind::Left,
            keys: (0..width).map(|index| (index, index)).collect(),
        });

        // An empty right input removes a grouped row. Reattach the complete
        // domain before any parent HAVING or Project sees the scalar result.
        // Hidden keys come from the left domain even on unmatched rows.
        self.expansion.fields(columns.len() + calls.len() + width)?;
        let mut expressions = Vec::with_capacity(columns.len());
        for (index, call) in calls.iter().enumerate() {
            let input = Expression::OuterInput {
                field: 2 * width + index,
                ty: call.ty,
            };
            expressions.push(if counts[index] {
                Expression::ZeroIfNull {
                    input: Box::new(input),
                    ty: call.ty,
                }
            } else {
                input
            });
        }
        for (index, column) in domain.columns.iter().enumerate() {
            expressions.push(Expression::Input {
                field: index,
                ty: column.ty,
            });
        }
        self.expansion.columns(&columns)?;
        let node = self.push(Node::Project {
            relation: None,
            input: joined,
            expressions,
            columns: columns.clone(),
        });
        Ok(Frame {
            node,
            columns,
            payload: (0..calls.len()).collect(),
            correlations: domain
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| (*field, calls.len() + index))
                .collect::<BTreeMap<_, _>>(),
        })
    }

    pub(super) fn grouped_aggregate(
        &mut self,
        id: u32,
        group: &[u32],
        calls: &[Aggregate],
        domain: &Domain,
        depth: usize,
    ) -> Result<Frame> {
        self.tick(depth)?;
        self.check_aggregate_calls(id, calls, false)?;
        let relation = &self.fragment.producers.relations[id as usize];
        let row = relation.row;
        let child = self.dependent(relation.inputs[0], domain, depth + 1)?;
        let child = self.supply(child, domain)?;
        let group_payload =
            group
                .iter()
                .map(|field| {
                    child.payload.get(*field as usize).copied().ok_or_else(|| {
                        Error::Invalid("aggregate group outside child payload".into())
                    })
                })
                .collect::<Result<Vec<_>>>()?;
        let domain_positions = domain.fields.iter().map(|field| child.correlations[field]);
        let mut keys: Vec<_> = group_payload
            .iter()
            .copied()
            .chain(domain_positions)
            .collect();
        self.expansion.fields(keys.len() + calls.len())?;
        keys.sort_unstable();
        if keys
            .windows(2)
            .any(|positions| positions[0] == positions[1])
        {
            return Err(Error::Unsupported("shared aggregate and domain positions"));
        }
        let arguments =
            self.aggregate_arguments(calls, |index| child.payload.get(index).copied())?;
        let mut grouped_columns = Vec::with_capacity(keys.len() + calls.len());
        for key in &keys {
            let column = &child.columns[*key];
            self.expansion.columns(std::slice::from_ref(column))?;
            grouped_columns.push(column.clone());
        }
        let mut columns = self.columns(row)?;
        let payload_len = columns.len();
        if payload_len != group.len() + calls.len() {
            return Err(Error::Invalid("aggregate payload width differs".into()));
        }
        self.expansion.columns(&columns[group.len()..])?;
        grouped_columns.extend(columns[group.len()..].iter().cloned());
        self.expansion.columns(&domain.columns)?;
        columns.extend(domain.columns.iter().cloned());
        self.name_internal_columns(&mut columns, 0..payload_len)?;
        let group_positions = group_payload
            .into_iter()
            .chain(domain.fields.iter().map(|field| child.correlations[field]))
            .map(|field| {
                keys.binary_search(&field)
                    .map_err(|_| Error::Invalid("missing rewritten group field".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        let positions = group_positions[..group.len()]
            .iter()
            .copied()
            .chain(keys.len()..keys.len() + calls.len())
            .chain(group_positions[group.len()..].iter().copied());
        let input = Frame {
            node: self.push(Node::Aggregate {
                relation: id,
                input: child.node,
                group: keys,
                arguments,
            }),
            columns: grouped_columns,
            payload: Vec::new(),
            correlations: BTreeMap::new(),
        };
        self.expansion.fields(columns.len())?;
        let expressions = positions
            .zip(&columns)
            .map(|(field, column)| self.input_expression(&input, field, column.ty))
            .collect::<Result<Vec<_>>>()?;
        // Grouped aggregates emit no row for an empty group. The final
        // correlate handles LEFT padding, without the scalar COUNT repair.
        self.expansion.columns(&columns)?;
        let node = self.push(Node::Project {
            relation: None,
            input: input.node,
            expressions,
            columns: columns.clone(),
        });
        Ok(Frame {
            node,
            columns,
            payload: (0..payload_len).collect(),
            correlations: domain
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| (*field, payload_len + index))
                .collect(),
        })
    }

    fn check_aggregate_calls(
        &mut self,
        id: u32,
        calls: &[Aggregate],
        scalar: bool,
    ) -> Result<Vec<bool>> {
        if (scalar && calls.is_empty()) || !self.expressions[id as usize].is_empty() {
            return Err(Error::Unsupported("aggregate without ordinary calls"));
        }
        self.expansion.fields(calls.len())?;
        let mut counts = Vec::with_capacity(calls.len());
        for call in calls {
            let OperatorObject::Builtin {
                registry,
                registry_version,
                key,
            } = &self.fragment.expressions.operator_objects[call.operator.0 as usize]
            else {
                return Err(Error::Unsupported("aggregate operator"));
            };
            let count = key == "SqlStdOperatorTable.COUNT";
            let ordinary_builtin = registry == "calcite-s1-fixture"
                && matches!(
                    key.as_str(),
                    "SqlStdOperatorTable.COUNT"
                        | "SqlStdOperatorTable.MIN"
                        | "SqlStdOperatorTable.MAX"
                        | "SqlStdOperatorTable.SUM"
                        | "SqlStdOperatorTable.AVG"
                        | "SqlStdOperatorTable.SINGLE_VALUE"
                );
            let array = registry == "feldera-s1-internals" && key == "array-aggregate";
            if array {
                let child = self.fragment.producers.relations[id as usize].inputs[0] as usize;
                if !matches!(
                    &self.fragment.properties.relations[child].fields,
                    Properties::Sort { collation, offset: false, fetch: true } if !collation.is_empty()
                ) {
                    return Err(Error::Unsupported(
                        "ARRAY_AGG requires an ordered FETCH input",
                    ));
                }
            }
            if (!ordinary_builtin && !array)
                || registry_version != "1"
                || call.distinct
                || call.approximate
                || call.ignore_nulls
                || call.direct_arguments != 0
                || !matches!(call.filter, OptionalField::Absent {})
                || !matches!(call.distinct_keys, OptionalFields::Absent {})
                || !call.collation.is_empty()
                || (count && call.arguments.len() > 1)
                || (!count && call.arguments.len() != 1)
            {
                return Err(Error::Unsupported("aggregate call metadata or arguments"));
            }
            let ty = &self.fragment.table.type_table.types[call.ty.0 as usize];
            if (count && ty.nullable) || (scalar && !count && !ty.nullable) {
                return Err(Error::Unsupported("aggregate result nullability"));
            }
            if count
                && !matches!(
                    ty.kind,
                    TypeKind::Integer {
                        signed: true,
                        width: IntegerWidth::I64
                    }
                )
            {
                return Err(Error::Unsupported("COUNT result must be signed BIGINT"));
            }
            counts.push(count);
        }

        Ok(counts)
    }

    fn aggregate_arguments(
        &mut self,
        calls: &[Aggregate],
        position: impl Fn(usize) -> Option<usize>,
    ) -> Result<Vec<Vec<usize>>> {
        calls
            .iter()
            .map(|call| {
                self.expansion.fields(call.arguments.len())?;
                call.arguments
                    .iter()
                    .map(|argument| {
                        position(*argument as usize).ok_or_else(|| {
                            Error::Invalid("aggregate argument outside child payload".into())
                        })
                    })
                    .collect()
            })
            .collect()
    }
}
