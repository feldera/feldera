//! Typed catalog metadata. Expressions live in the shared object graph; catalog
//! columns claim ordered slots from a constant-value use, preserving sharing.
use crate::{
    TypeId, TypeTable, ValidationError, ValidationLimits,
    expression_objects::{ExpressionObjects, SourceContext},
    input_producers::{InputProducers, RelationForm},
    operators::DiagnosticSource,
    relation_properties::{Properties, RelationProperties},
    source::{Origin, SourceId},
    types::TypeKind,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Name {
    pub name: String,
    pub quoted: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSchema {
    pub source: SourceId,
    pub name: Name,
    pub diagnostic: DiagnosticSource,
    pub columns: Vec<Column>,
    /// Preserve the distinction between absent and present-but-empty properties.
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<Vec<Property>>")]
    pub properties: Option<Vec<Property>>,
    pub values: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: Name,
    /// Zero-based position in the schema, within Calcite's signed-int domain.
    #[schemars(range(max = 2147483647))]
    pub index: u32,
    pub ty: TypeId,
    pub diagnostic: DiagnosticSource,
    pub primary_key: bool,
    pub unused: bool,
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<u32>")]
    pub lateness: Option<u32>,
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<u32>")]
    pub default_value: Option<u32>,
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<Origin>")]
    pub default_position: Option<Origin>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Property {
    pub name: String,
    pub value: String,
    pub key_position: Origin,
    pub value_position: Origin,
}

impl CatalogSchema {
    /// Check this schema's metadata and ownership of value slots. The enclosing
    /// program must also validate the shared type and expression graphs.
    pub fn validate(
        &self,
        types: &TypeTable,
        expressions: &ExpressionObjects,
        limits: ValidationLimits,
    ) -> Result<(), ValidationError> {
        let mut budget = MetadataBudget {
            remaining: limits.source_bytes,
        };
        self.validate_metadata(types, expressions, limits, &mut budget)
    }

    fn validate_metadata(
        &self,
        types: &TypeTable,
        expressions: &ExpressionObjects,
        limits: ValidationLimits,
        budget: &mut MetadataBudget,
    ) -> Result<(), ValidationError> {
        let error = |message| ValidationError {
            path: "catalog".into(),
            message,
        };
        if self.source.0 as usize >= types.sources.len() {
            return Err(error("catalog source outside arena"));
        }
        let properties = self.properties.as_deref().unwrap_or_default();
        if self.columns.len().saturating_add(properties.len()) > limits.types {
            return Err(error("catalog entries exceed limit"));
        }
        let values = expressions
            .value_uses
            .get(self.values as usize)
            .ok_or_else(|| error("catalog value use outside arena"))?;
        if expressions
            .environments
            .get(values.environment.0 as usize)
            .map(|environment| &environment.source)
            != Some(&SourceContext::Present {
                source: self.source,
            })
        {
            return Err(error("catalog value source differs from declaration"));
        }
        budget.text(&self.name.name)?;
        budget.diagnostic(&self.diagnostic, types)?;
        let position = |origin: &Origin| {
            if origin.source().is_some_and(|source| source != self.source) {
                Err(error("catalog position source differs from declaration"))
            } else {
                Ok(())
            }
        };
        position(&self.diagnostic.origin)?;
        let mut slot = 0;
        for (index, column) in self.columns.iter().enumerate() {
            if column.index as usize != index || column.index > i32::MAX as u32 {
                return Err(error("catalog column index differs from position"));
            }
            budget.text(&column.name.name)?;
            budget.diagnostic(&column.diagnostic, types)?;
            position(&column.diagnostic.origin)?;
            types.reference(column.ty, "catalog column type")?;
            for value in [column.lateness, column.default_value]
                .into_iter()
                .flatten()
            {
                if value as usize != slot {
                    return Err(error("catalog value slots are out of order"));
                }
                let root = values
                    .roots
                    .get(slot)
                    .ok_or_else(|| error("catalog value slot outside use"))?;
                if root.0 as usize >= expressions.expressions.len() {
                    return Err(error("catalog value outside expression arena"));
                }
                slot += 1;
            }
            if let Some(position) = &column.default_position {
                types.origin(position, "catalog default position")?;
                if position
                    .source()
                    .is_some_and(|source| source != self.source)
                {
                    return Err(error("catalog default source differs from declaration"));
                }
            }
        }
        if slot != values.roots.len() {
            return Err(error("catalog has unclaimed value slots"));
        }
        let mut names = HashSet::new();
        for property in properties {
            budget.text(&property.name)?;
            budget.text(&property.value)?;
            if !names.insert(&property.name) {
                return Err(error("duplicate catalog property"));
            }
            position(&property.key_position)?;
            position(&property.value_position)?;
            types.origin(&property.key_position, "catalog property key")?;
            types.origin(&property.value_position, "catalog property value")?;
        }
        Ok(())
    }
}

struct MetadataBudget {
    remaining: usize,
}
impl MetadataBudget {
    fn text(&mut self, text: &str) -> Result<(), ValidationError> {
        self.remaining = self
            .remaining
            .checked_sub(text.len())
            .ok_or(ValidationError {
                path: "catalog".into(),
                message: "catalog text exceeds limit",
            })?;
        Ok(())
    }
    fn diagnostic(
        &mut self,
        diagnostic: &DiagnosticSource,
        types: &TypeTable,
    ) -> Result<(), ValidationError> {
        self.text(&diagnostic.text)?;
        self.text(&diagnostic.internal_text)?;
        types.origin(&diagnostic.origin, "catalog diagnostic")
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum CatalogVersion {
    #[serde(rename = "catalog-v1")]
    V1,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub schema_version: CatalogVersion,
    pub schemas: Vec<CatalogSchema>,
    pub tables: Vec<Table>,
    pub scans: Vec<Scan>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub schema: u32,
    pub row: TypeId,
    pub name: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scan {
    pub relation: u32,
    pub table: u32,
}

impl Catalog {
    /// Validate catalog edges against already validated expression and relation
    /// graphs. This adapter supports logical scans; JDBC needs a separate adapter.
    pub fn validate(
        &self,
        types: &TypeTable,
        expressions: &ExpressionObjects,
        producers: &InputProducers,
        properties: &RelationProperties,
        limits: ValidationLimits,
    ) -> Result<(), ValidationError> {
        let error = |message| ValidationError {
            path: "catalog".into(),
            message,
        };
        let mut entries = self
            .schemas
            .len()
            .saturating_add(self.tables.len())
            .saturating_add(self.scans.len());
        let mut budget = MetadataBudget {
            remaining: limits.source_bytes,
        };
        let mut values = HashSet::new();
        for schema in &self.schemas {
            entries = entries
                .saturating_add(schema.columns.len())
                .saturating_add(schema.properties.as_ref().map_or(0, Vec::len));
            if entries > limits.types {
                return Err(error("catalog entries exceed limit"));
            }
            if !values.insert(schema.values) {
                return Err(error("catalog schemas share a value use"));
            }
            schema.validate_metadata(types, expressions, limits, &mut budget)?;
        }
        if entries > limits.types {
            return Err(error("catalog entries exceed limit"));
        }
        let mut references = self.scans.len().saturating_mul(2);
        for table in &self.tables {
            let schema = self
                .schemas
                .get(table.schema as usize)
                .ok_or_else(|| error("catalog table schema outside arena"))?;
            types.reference(table.row, "catalog table row")?;
            let TypeKind::Row { fields, .. } = &types.types[table.row.0 as usize].kind else {
                return Err(error("catalog table requires a row type"));
            };
            references = references
                .saturating_add(fields.len())
                .saturating_add(table.name.len())
                .saturating_add(2);
            if references > limits.references {
                return Err(error("catalog references exceed limit"));
            }
            if fields.len() != schema.columns.len()
                || fields.iter().zip(&schema.columns).any(|(field, column)| {
                    field.ty != column.ty || field.name.name != column.name.name
                })
            {
                return Err(error("catalog row differs from schema fields"));
            }
            if table.name.is_empty() {
                return Err(error("catalog table requires a qualified name"));
            }
            for name in &table.name {
                budget.text(name)?;
            }
        }
        let mut scans = HashSet::new();
        for scan in &self.scans {
            let producer = producers
                .relations
                .get(scan.relation as usize)
                .ok_or_else(|| error("catalog scan outside relation arena"))?;
            let table = self
                .tables
                .get(scan.table as usize)
                .ok_or_else(|| error("catalog scan table outside arena"))?;
            if !scans.insert(scan.relation) {
                return Err(error("duplicate catalog scan edge"));
            }
            if producer.form != RelationForm::LogicalTableScan || producer.row != table.row {
                return Err(error("catalog scan form or row differs"));
            }
            let Some(property) = properties.relations.get(scan.relation as usize) else {
                return Err(error("catalog scan has no relation properties"));
            };
            if !matches!(&property.fields, Properties::Scan { name } if name == &table.name) {
                return Err(error("catalog scan qualified name differs"));
            }
        }
        for (id, relation) in producers.relations.iter().enumerate() {
            match relation.form {
                RelationForm::LogicalTableScan if !scans.contains(&(id as u32)) => {
                    return Err(error("table scan has no catalog edge"));
                }
                RelationForm::JdbcTableScan => {
                    return Err(error("JDBC catalog adapter is not implemented"));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

// These fields must be present, but JSON null is a meaningful value. Schemars'
// `required` alone removes null from Option<T>'s schema, so retain it explicitly.
pub(crate) fn nullable_schema<T: JsonSchema>(
    generator: &mut schemars::r#gen::SchemaGenerator,
) -> schemars::schema::Schema {
    generator.subschema_for::<Option<T>>()
}
