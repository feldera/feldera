//! Typed, ordered inputs to circuit lowering. Statement text is diagnostic data;
//! importing these records does not parse SQL or run name/type inference.
use crate::{
    TypeId,
    catalog::{Name, nullable_schema},
    operators::{DiagnosticSource, OperatorObjectId, QualifiedName},
    relation_metadata::{Coordinates, Hint, SortField},
    source::{Origin, SourceId},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatementSource {
    pub source: SourceId,
    pub kind: String,
    pub visible: bool,
    pub statement_text: String,
    pub diagnostic_text: String,
    pub position: Coordinates,
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<bool>")]
    pub position_internal: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Statement {
    CreateTable {
        source: StatementSource,
        schema: u32,
        name: Name,
        name_diagnostic: DiagnosticSource,
        foreign_keys: Vec<ForeignKey>,
    },
    CreateView {
        source: StatementSource,
        schema: u32,
        name: Name,
        view_kind: ViewKind,
        emit_final: i32,
        root: RelRoot,
    },
    DeclareView {
        source: StatementSource,
        schema: u32,
        name: Name,
    },
    DropTable {
        source: StatementSource,
        name: Name,
        if_exists: bool,
    },
    CreateAggregate {
        source: StatementSource,
        function: OperatorObjectId,
    },
    CreateFunction {
        source: StatementSource,
        function: OperatorObjectId,
    },
    CreateType {
        source: StatementSource,
        name: Name,
        name_diagnostic: DiagnosticSource,
        ty: TypeId,
        #[serde(deserialize_with = "Option::deserialize")]
        #[schemars(required, schema_with = "nullable_schema::<Vec<Attribute>>")]
        attributes: Option<Vec<Attribute>>,
    },
    CreateIndex {
        source: StatementSource,
        name: Name,
        target: Name,
        name_position: Coordinates,
        target_position: Coordinates,
        columns: Vec<IndexColumn>,
    },
    SetOption {
        source: StatementSource,
        name: Name,
        values: u32,
    },
    ModifyTable {
        source: StatementSource,
        name: Name,
        insert: bool,
        #[serde(deserialize_with = "Option::deserialize")]
        #[schemars(required, schema_with = "nullable_schema::<u32>")]
        relation: Option<u32>,
        #[serde(deserialize_with = "Option::deserialize")]
        #[schemars(required, schema_with = "nullable_schema::<Vec<TargetColumn>>")]
        columns: Option<Vec<TargetColumn>>,
    },
}

impl Statement {
    pub fn source(&self) -> &StatementSource {
        match self {
            Self::CreateTable { source, .. }
            | Self::CreateView { source, .. }
            | Self::DeclareView { source, .. }
            | Self::DropTable { source, .. }
            | Self::CreateFunction { source, .. }
            | Self::CreateAggregate { source, .. }
            | Self::CreateType { source, .. }
            | Self::CreateIndex { source, .. }
            | Self::SetOption { source, .. }
            | Self::ModifyTable { source, .. } => source,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ViewKind {
    Materialized,
    Local,
    Standard,
}

impl ViewKind {
    /// SQL introducer used when rendering a bound view declaration.
    pub fn create_sql(self) -> &'static str {
        match self {
            Self::Standard => "CREATE VIEW",
            Self::Materialized => "CREATE MATERIALIZED VIEW",
            Self::Local => "CREATE LOCAL VIEW",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelRoot {
    pub relation: u32,
    pub validated_type: TypeId,
    pub kind: String,
    pub fields: Vec<ResultField>,
    pub collation: Vec<SortField>,
    pub hints: Vec<Hint>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResultField {
    pub index: u32,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Attribute {
    pub name: Name,
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<Name>")]
    pub named_type: Option<Name>,
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<bool>")]
    pub nullable: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexColumn {
    pub name: Name,
    pub position: Coordinates,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetColumn {
    pub name: Name,
    pub position: Coordinates,
    pub display_name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKey {
    pub source: TableColumns,
    pub target: TableColumns,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TableColumns {
    pub position: Origin,
    pub table: QualifiedName,
    pub columns: Vec<QualifiedName>,
}
