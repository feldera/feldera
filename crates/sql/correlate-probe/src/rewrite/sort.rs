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
// Feldera adaptation of RelDecorrelator.decorrelateSortWithRowNumber
// at Calcite 4bf922bbbc1cbd570a2f201339a46a2d8f7a763b.

use super::{Domain, Frame, Result, Rewriter};
use crate::{Error, recipe::Node};
use feldera_sql_rel_model::{
    expression_objects::ObjectKind, literals::LiteralValue, relation_properties::SortField,
};

impl Rewriter<'_> {
    pub(super) fn partitioned_fetch(
        &mut self,
        id: u32,
        collation: &[SortField],
        domain: &Domain,
        depth: usize,
    ) -> Result<Frame> {
        self.tick(depth)?;
        let [fetch] = self.expressions[id as usize].as_slice() else {
            return Err(Error::Unsupported(
                "correlated FETCH requires one integer limit",
            ));
        };
        let ObjectKind::Literal { literal } =
            &self.fragment.expressions.expressions[fetch.0 as usize].kind
        else {
            return Err(Error::Unsupported(
                "correlated FETCH requires an integer literal",
            ));
        };
        let LiteralValue::Integer { value } = &literal.value else {
            return Err(Error::Unsupported(
                "correlated FETCH requires an integer literal",
            ));
        };
        let limit = value
            .parse::<u32>()
            .ok()
            .filter(|limit| (1..=i32::MAX as u32).contains(limit))
            .ok_or(Error::Unsupported(
                "correlated FETCH limit is outside the supported range",
            ))?;
        if domain.fields.is_empty() {
            return Err(Error::Unsupported("correlated FETCH requires a domain"));
        }
        if collation.is_empty() && limit != 1 {
            return Err(Error::Unsupported(
                "unordered correlated FETCH requires LIMIT 1",
            ));
        }
        let relation = &self.fragment.producers.relations[id as usize];
        let child = self.dependent(relation.inputs[0], domain, depth + 1)?;
        let mut frame = self.supply(child, domain)?;
        self.expansion
            .fields(collation.len() + domain.fields.len())?;
        let order = collation
            .iter()
            .map(|field| {
                frame
                    .payload
                    .get(field.field as usize)
                    .copied()
                    .ok_or_else(|| Error::Invalid("sort field outside rewritten payload".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        let partition = domain
            .fields
            .iter()
            .map(|field| frame.correlations[field])
            .collect();
        frame.node = self.push(Node::PartitionedFetch {
            relation: id,
            input: frame.node,
            partition,
            order,
            limit,
        });
        Ok(frame)
    }
}
