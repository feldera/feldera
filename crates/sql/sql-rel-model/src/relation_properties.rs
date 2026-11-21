//! Operator-specific relational properties. Expression slots follow the explicit
//! local-field walk; they are not global expression object identities.
//!
//! These descriptors complement producer topology. Catalog objects, traits,
//! hints, and contextual expression uses still belong to the full S1 adapter.

use crate::relation_metadata::{Budget, require};
pub use crate::relation_metadata::{Coordinates, Direction, NullOrder, SortField};
use crate::{
    TypeId, ValidationError, ValidationLimits,
    input_producers::{InputProducers, RelationForm},
    input_uses::InputUses,
    types::TypeKind,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum PropertiesVersion {
    #[serde(rename = "relation-properties-v1")]
    V1,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationProperties {
    pub schema_version: PropertiesVersion,
    pub relations: Vec<RelationProperty>,
    pub operators: OperatorArena,
}

/// Fragments own their aggregate operators; whole programs share the expression
/// arena so declarations and every call retain the same operator identity.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorArena {
    Owned {
        objects: Vec<crate::operators::OperatorObject>,
    },
    Shared {},
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationProperty {
    pub metadata: crate::relation_metadata::RelationMetadata,
    pub fields: Properties,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Asof,
    LeftAsof,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Aggregate {
    pub operator: crate::operators::OperatorObjectId,
    pub ty: TypeId,
    // A tagged value requires the wire to distinguish missing from absent.
    pub name: OptionalName,
    pub distinct: bool,
    pub approximate: bool,
    pub ignore_nulls: bool,
    pub arguments: Vec<u32>,
    pub direct_arguments: u32,
    pub filter: OptionalField,
    pub distinct_keys: OptionalFields,
    pub collation: Vec<SortField>,
    pub position: Coordinates,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OptionalName {
    Absent {},
    Present { name: String },
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OptionalField {
    Absent {},
    Present { field: u32 },
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OptionalFields {
    Absent {},
    Present { fields: Vec<u32> },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowBound {
    UnboundedPreceding,
    Preceding,
    CurrentRow,
    Following,
    UnboundedFollowing,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WindowExclusion {
    ExcludeNoOther,
    ExcludeCurrentRow,
    ExcludeTies,
    ExcludeGroup,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WindowGroup {
    pub keys: Vec<u32>,
    pub rows: bool,
    pub lower: WindowBound,
    pub upper: WindowBound,
    pub exclusion: WindowExclusion,
    pub collation: Vec<SortField>,
    pub calls: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Properties {
    Scan {
        name: Vec<String>,
    },
    Project {
        expressions: u32,
        variables: Vec<u32>,
    },
    Filter {
        variables: Vec<u32>,
    },
    Values {
        tuples: u32,
    },
    Set {
        all: bool,
    },
    Join {
        join_kind: JoinKind,
        variables: Vec<u32>,
        semi_join_done: bool,
        system_fields: Vec<TypeId>,
    },
    Aggregate {
        group: Vec<u32>,
        groups: Vec<Vec<u32>>,
        calls: Vec<Aggregate>,
    },
    Window {
        groups: Vec<WindowGroup>,
    },
    Sort {
        collation: Vec<SortField>,
        offset: bool,
        fetch: bool,
    },
    Uncollect {
        ordinality: bool,
        outer: bool,
        expand_struct_fields: bool,
        aliases: Vec<String>,
        passthrough: Vec<u32>,
        collections: Vec<u32>,
    },
    Collect {
        field: String,
        collection: CollectionKind,
    },
    Correlate {
        correlation: u32,
        required: Vec<u32>,
        join_kind: JoinKind,
    },
    TableFunction {
        element: ElementType,
        mappings: ColumnMappings,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CollectionKind {
    Array,
    Multiset,
    Map,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ElementType {
    Absent,
    ObjectArray,
    Object,
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ColumnMappings {
    Absent {},
    Present { mappings: Vec<ColumnMapping> },
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnMapping {
    pub output: u32,
    pub input: u32,
    pub column: u32,
    pub derived: bool,
}

type Result<T> = std::result::Result<T, ValidationError>;
impl RelationProperties {
    pub fn validate(
        &self,
        producers: &InputProducers,
        uses: &InputUses,
        limits: ValidationLimits,
        states: usize,
    ) -> Result<()> {
        self.validate_with_operators(producers, uses, limits, states, None)
    }

    pub(crate) fn validate_with_operators(
        &self,
        producers: &InputProducers,
        uses: &InputUses,
        limits: ValidationLimits,
        states: usize,
        shared: Option<&[crate::operators::OperatorObject]>,
    ) -> Result<()> {
        let operators = match &self.operators {
            OperatorArena::Owned { objects } => objects.as_slice(),
            OperatorArena::Shared {} => shared.ok_or(ValidationError {
                path: "relation_properties".into(),
                message: "shared operator arena is missing",
            })?,
        };
        producers.validate(uses, limits, states)?;
        require(
            self.relations.len() == producers.relations.len(),
            "property arena size differs",
        )?;
        require(
            operators.len() <= limits.types,
            "aggregate operator arena exceeds limit",
        )?;
        let row = |ty: TypeId| match &uses.type_table.types[ty.0 as usize].kind {
            TypeKind::Row { fields, .. } => fields.as_slice(),
            _ => unreachable!("producer validator checks row types"),
        };
        let width = |ty: TypeId| row(ty).len();
        let mut budget = Budget::new(limits.references);
        budget.count(operators.len())?;
        let mut builtins = std::collections::HashSet::new();
        // Shared declarations are validated and budgeted by ExpressionObjects.
        let owned = match &self.operators {
            OperatorArena::Owned { objects } => objects.as_slice(),
            OperatorArena::Shared {} => &[],
        };
        for object in owned {
            object.validate(&uses.type_table, "aggregate operator")?;
            match object {
                crate::operators::OperatorObject::SumZero {} => {}
                crate::operators::OperatorObject::Builtin {
                    registry,
                    registry_version,
                    key,
                } => {
                    for name in [registry, registry_version, key] {
                        budget.name(name)?;
                    }
                    require(
                        builtins.insert((registry, registry_version, key)),
                        "duplicate aggregate builtin identity",
                    )?;
                }
                _ => return require(false, "unsupported aggregate operator object"),
            }
        }
        for (record, producer) in self.relations.iter().zip(&producers.relations) {
            budget.count(1)?;
            let output = width(producer.row);
            let input = producer
                .inputs
                .first()
                .map_or(0, |id| width(producers.relations[*id as usize].row));
            use Properties as P;
            use RelationForm as F;
            record.metadata.validate(output, &mut budget)?;
            match (&record.fields, producer.form) {
                (P::Scan { name }, F::LogicalTableScan | F::JdbcTableScan) => {
                    require(!name.is_empty(), "empty table name")?;
                    budget.count(name.len())?;
                    for part in name {
                        budget.name(part)?;
                    }
                }
                (
                    P::Project {
                        expressions,
                        variables,
                    },
                    F::LogicalProject,
                ) => {
                    require(*expressions as usize == output, "project width differs")?;
                    budget.fields(variables, u32::MAX as usize, true)?;
                }
                (P::Filter { variables }, F::LogicalFilter) => {
                    budget.fields(variables, u32::MAX as usize, true)?
                }
                (P::Values { tuples }, F::LogicalValues) => {
                    budget.count((*tuples as usize).checked_mul(output.max(1)).ok_or(
                        ValidationError {
                            path: "relation_properties".into(),
                            message: "tuple count overflow",
                        },
                    )?)?
                }
                (P::Set { .. }, F::LogicalUnion | F::LogicalIntersect | F::LogicalMinus) => {}
                (
                    P::Join {
                        join_kind,
                        variables,
                        system_fields,
                        ..
                    },
                    F::LogicalJoin | F::LogicalAsofJoin,
                ) => {
                    require(
                        matches!(join_kind, JoinKind::Asof | JoinKind::LeftAsof)
                            == (producer.form == F::LogicalAsofJoin),
                        "join kind differs from relation form",
                    )?;
                    budget.fields(variables, u32::MAX as usize, true)?;
                    budget.count(system_fields.len())?;
                    for ty in system_fields {
                        uses.type_table.reference(*ty, "join system field")?;
                    }
                }
                (
                    P::Aggregate {
                        group,
                        groups,
                        calls,
                    },
                    F::LogicalAggregate,
                ) => {
                    budget.fields(group, input, true)?;
                    budget.count(groups.len() + calls.len())?;
                    for grouping in groups {
                        budget.fields(grouping, input, true)?;
                        require(
                            grouping
                                .iter()
                                .all(|field| group.binary_search(field).is_ok()),
                            "grouping set is not a subset",
                        )?;
                    }
                    require(
                        output == group.len() + calls.len(),
                        "aggregate output width differs",
                    )?;
                    for (index, call) in calls.iter().enumerate() {
                        uses.type_table.reference(call.ty, "aggregate result")?;
                        require(
                            row(producer.row)[group.len() + index].ty == call.ty,
                            "aggregate result differs from output field",
                        )?;
                        budget.count(1)?;
                        require(
                            (call.operator.0 as usize) < operators.len(),
                            "aggregate operator outside arena",
                        )?;
                        require(
                            matches!(
                                operators[call.operator.0 as usize],
                                crate::operators::OperatorObject::Builtin { .. }
                                    | crate::operators::OperatorObject::SumZero {}
                                    | crate::operators::OperatorObject::AggregateFunction { .. }
                            ),
                            "aggregate call requires an aggregate operator",
                        )?;
                        if let OptionalName::Present { name } = &call.name {
                            budget.name(name)?;
                        }
                        budget.fields(&call.arguments, input, false)?;
                        budget.count(call.direct_arguments as usize)?;
                        if let OptionalField::Present { field } = &call.filter {
                            budget.fields(&[*field], input, false)?;
                            let field_type =
                                row(producers.relations[producer.inputs[0] as usize].row)
                                    [*field as usize]
                                    .ty;
                            require(
                                matches!(
                                    uses.type_table.types[field_type.0 as usize].kind,
                                    TypeKind::Boolean {}
                                ),
                                "aggregate filter requires boolean field",
                            )?;
                        }
                        if let OptionalFields::Present { fields } = &call.distinct_keys {
                            budget.fields(fields, input, true)?;
                        }
                        budget.collation(&call.collation, input)?;
                    }
                }
                (P::Window { groups }, F::LogicalWindow) => {
                    budget.count(groups.len())?;
                    let mut calls = 0usize;
                    for group in groups {
                        budget.fields(&group.keys, input + producer.constants.len(), true)?;
                        budget.collation(&group.collation, input + producer.constants.len())?;
                        budget.count(group.calls as usize)?;
                        calls += group.calls as usize;
                    }
                    require(output == input + calls, "window output width differs")?;
                }
                (P::Sort { collation, .. }, F::LogicalSort) => {
                    budget.collation(collation, input)?
                }
                (
                    P::Uncollect {
                        aliases,
                        passthrough,
                        collections,
                        ..
                    },
                    F::Uncollect,
                ) => {
                    budget.count(aliases.len())?;
                    for name in aliases {
                        budget.name(name)?;
                    }
                    budget.fields(passthrough, input, true)?;
                    budget.fields(collections, input, true)?;
                    // A field may both pass through as a collection and be
                    // unnested. These are two independent output mappings.
                    require(
                        !collections.is_empty(),
                        "uncollect requires a collection field",
                    )?;
                    require(
                        aliases.is_empty() || aliases.len() == input,
                        "uncollect alias count differs",
                    )?;
                }
                (P::Collect { field, .. }, F::Collect) => budget.name(field)?,
                (P::Correlate { required, .. }, F::LogicalCorrelate) => {
                    budget.fields(required, input, true)?
                }
                (P::TableFunction { mappings, .. }, F::LogicalTableFunctionScan) => {
                    if let ColumnMappings::Present { mappings } = mappings {
                        budget.count(mappings.len())?;
                        for mapping in mappings {
                            require(
                                (mapping.output as usize) < output
                                    && (mapping.input as usize) < producer.inputs.len(),
                                "table function mapping outside row or inputs",
                            )?;
                            let source = &producers.relations
                                [producer.inputs[mapping.input as usize] as usize];
                            require(
                                (mapping.column as usize) < width(source.row),
                                "table function source column outside row",
                            )?;
                        }
                    }
                }
                _ => return require(false, "property variant differs from relation form"),
            }
        }
        Ok(())
    }
}
