/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Feldera adaptation: bounded typed metadata validation; trait-alternative
// ordering follows Calcite RelCollationImpl and RelDistributions at
// 4bf922bbbc1cbd570a2f201339a46a2d8f7a763b.

//! Common relational traits and hints. Fixed tagged variants prevent a decoder
//! from loading classes or silently discarding unfamiliar planner metadata.
use crate::ValidationError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationMetadata {
    pub traits: Vec<Trait>,
    pub hints: Vec<Hint>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Trait {
    ConventionNone {},
    Collation { values: Vec<Vec<SortField>> },
    Distribution { values: Vec<Distribution> },
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Distribution {
    pub distribution: DistributionKind,
    pub keys: Vec<u32>,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DistributionKind {
    Singleton,
    HashDistributed,
    RangeDistributed,
    RandomDistributed,
    RoundRobinDistributed,
    BroadcastDistributed,
    Any,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Hint {
    pub name: String,
    pub position: Coordinates,
    pub inherit_path: Vec<u32>,
    pub list: Vec<String>,
    /// A map has no semantic order. The wire sorts keys and rejects duplicates.
    pub options: Vec<HintOption>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HintOption {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Direction {
    Ascending,
    StrictlyAscending,
    Descending,
    StrictlyDescending,
    Clustered,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NullOrder {
    First,
    Last,
    Unspecified,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SortField {
    pub field: u32,
    pub direction: Direction,
    pub null_direction: NullOrder,
}

/// Parser coordinates are intrinsic; a statement/function use supplies source.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Coordinates {
    #[schemars(range(max = 2147483647))]
    pub line: u32,
    #[schemars(range(max = 2147483647))]
    pub column: u32,
    #[schemars(range(max = 2147483647))]
    pub end_line: u32,
    #[schemars(range(max = 2147483647))]
    pub end_column: u32,
    pub quoted: bool,
}

type Result<T> = std::result::Result<T, ValidationError>;
pub(crate) fn require(value: bool, message: &'static str) -> Result<()> {
    if value {
        Ok(())
    } else {
        Err(ValidationError {
            path: "relation_properties".into(),
            message,
        })
    }
}

pub(crate) struct Budget {
    remaining: usize,
    strings: usize,
}
impl Budget {
    pub(crate) fn new(references: usize) -> Self {
        Self {
            remaining: references,
            strings: 1024 * 1024,
        }
    }

    pub(crate) fn count(&mut self, count: usize) -> Result<()> {
        self.remaining = self.remaining.checked_sub(count).ok_or(ValidationError {
            path: "relation_properties".into(),
            message: "property references exceed limit",
        })?;
        Ok(())
    }
    pub(crate) fn name(&mut self, name: &str) -> Result<()> {
        self.strings = self
            .strings
            .checked_sub(name.len())
            .ok_or(ValidationError {
                path: "relation_properties".into(),
                message: "property strings exceed limit",
            })?;
        Ok(())
    }
    pub(crate) fn fields(&mut self, fields: &[u32], width: usize, set: bool) -> Result<()> {
        self.count(fields.len())?;
        require(
            fields.iter().all(|field| (*field as usize) < width),
            "property field outside row",
        )?;
        require(
            !set || fields.windows(2).all(|pair| pair[0] < pair[1]),
            "bit set is not strictly ordered",
        )
    }
    pub(crate) fn collation(&mut self, fields: &[SortField], width: usize) -> Result<()> {
        self.count(fields.len())?;
        let mut seen = std::collections::HashSet::new();
        require(
            fields
                .iter()
                .all(|field| (field.field as usize) < width && seen.insert(field.field)),
            "invalid collation field",
        )
    }
}

impl RelationMetadata {
    pub(crate) fn validate(&self, width: usize, budget: &mut Budget) -> Result<()> {
        budget.count(self.traits.len() + self.hints.len())?;
        let mut definitions = std::collections::HashSet::new();
        for trait_value in &self.traits {
            let definition = match trait_value {
                Trait::ConventionNone {} => 0,
                Trait::Collation { values } => {
                    budget.count(values.len())?;
                    require(!values.is_empty(), "empty collation trait alternatives")?;
                    // Calcite orders alternatives by field ordinals alone.
                    require(
                        values.windows(2).all(|pair| {
                            pair[0]
                                .iter()
                                .map(|field| field.field)
                                .cmp(pair[1].iter().map(|field| field.field))
                                .is_lt()
                        }),
                        "collation alternatives are not strictly ordered",
                    )?;
                    for collation in values {
                        budget.collation(collation, width)?;
                    }
                    1
                }
                Trait::Distribution { values } => {
                    budget.count(values.len())?;
                    require(!values.is_empty(), "empty distribution trait alternatives")?;
                    require(
                        values.windows(2).all(|pair| {
                            (pair[0].distribution, &pair[0].keys)
                                < (pair[1].distribution, &pair[1].keys)
                        }),
                        "distribution alternatives are not strictly ordered",
                    )?;
                    for distribution in values {
                        budget.fields(&distribution.keys, width, false)?;
                        require(
                            distribution.distribution != DistributionKind::HashDistributed
                                || distribution.keys.windows(2).all(|pair| pair[0] <= pair[1]),
                            "hash distribution keys are not ordered",
                        )?;
                        require(
                            matches!(
                                distribution.distribution,
                                DistributionKind::HashDistributed
                                    | DistributionKind::RangeDistributed
                            ) || distribution.keys.is_empty(),
                            "distribution does not accept keys",
                        )?;
                    }
                    2
                }
            };
            require(definitions.insert(definition), "duplicate trait definition")?;
        }
        for hint in &self.hints {
            hint.validate(budget)?;
        }
        Ok(())
    }
}

impl Hint {
    pub(crate) fn validate(&self, budget: &mut Budget) -> Result<()> {
        let hint = self;
        // Hints can also be checked without a statement source environment.
        require(
            [
                hint.position.line,
                hint.position.column,
                hint.position.end_line,
                hint.position.end_column,
            ]
            .into_iter()
            .chain(hint.inherit_path.iter().copied())
            .all(|value| value <= i32::MAX as u32),
            "hint position or path exceeds the adapter's signed-integer range",
        )?;
        budget.name(&hint.name)?;
        require(!hint.name.is_empty(), "empty hint name")?;
        budget.count(hint.inherit_path.len() + hint.list.len() + hint.options.len())?;
        require(
            hint.list.is_empty() || hint.options.is_empty(),
            "hint mixes list and map options",
        )?;
        for value in &hint.list {
            budget.name(value)?;
        }
        require(
            hint.options
                .windows(2)
                .all(|pair| pair[0].key < pair[1].key),
            "hint map keys are not strictly ordered",
        )?;
        for option in &hint.options {
            budget.name(&option.key)?;
            budget.name(&option.value)?;
        }
        Ok(())
    }
}
