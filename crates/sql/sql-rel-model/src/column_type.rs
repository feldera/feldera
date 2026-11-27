//! SQL column types with collection references resolved for schema and circuit emission.

use crate::{
    TypeId, TypeTable, ValidationLimits,
    types::{Cardinality, SqlType, TypeKind},
};
use serde_json::{Value, json};

pub enum ColumnType<'a> {
    Scalar(&'a SqlType),
    Row {
        nullable: bool,
        fields: Vec<(&'a str, Self)>,
    },
    Array {
        nullable: bool,
        element: Box<Self>,
    },
    Map {
        nullable: bool,
        key: Box<Self>,
        value: Box<Self>,
    },
}

impl TypeTable {
    /// Limit the expanded tree as a shared type can occur in both MAP arguments.
    pub fn column_type(&self, id: TypeId) -> Option<ColumnType<'_>> {
        let limits = ValidationLimits::default();
        let mut remaining = limits.references;
        self.expand_column(id, limits.depth, &mut remaining)
    }

    fn expand_column(
        &self,
        id: TypeId,
        depth: usize,
        remaining: &mut usize,
    ) -> Option<ColumnType<'_>> {
        let depth = depth.checked_sub(1)?;
        *remaining = remaining.checked_sub(1)?;
        let ty = self.types.get(id.0 as usize)?;
        let nullable = ty.nullable;
        Some(match ty.kind {
            TypeKind::Array {
                element,
                cardinality: Cardinality::Unbounded,
            } => ColumnType::Array {
                nullable,
                element: Box::new(self.expand_column(element, depth, remaining)?),
            },
            TypeKind::Map { key, value } => {
                if !self.types.get(key.0 as usize)?.kind.can_be_map_key() {
                    return None;
                }
                ColumnType::Map {
                    nullable,
                    key: Box::new(self.expand_column(key, depth, remaining)?),
                    value: Box::new(self.expand_column(value, depth, remaining)?),
                }
            }
            TypeKind::Row { ref fields, .. } => ColumnType::Row {
                nullable,
                fields: fields
                    .iter()
                    .map(|field| {
                        Some((
                            field.name.name.as_str(),
                            self.expand_column(field.ty, depth, remaining)?,
                        ))
                    })
                    .collect::<Option<_>>()?,
            },
            TypeKind::Array { .. }
            | TypeKind::Multiset { .. }
            | TypeKind::Named { .. }
            | TypeKind::Function { .. } => return None,
            _ => ColumnType::Scalar(ty),
        })
    }
}

impl TypeKind {
    pub fn can_be_map_key(&self) -> bool {
        !matches!(self, Self::Map { .. } | Self::Null {})
    }
}

impl ColumnType<'_> {
    /// Public columntype descriptor, in the Java schema writer's key order.
    pub fn descriptor(&self) -> Option<Value> {
        match self {
            Self::Scalar(ty) => ty.scalar_column_type(),
            Self::Row { nullable, fields } => {
                let fields = fields
                    .iter()
                    .map(|(name, field)| {
                        let mut column = field.descriptor()?;
                        column["name"] = (*name).into();
                        Some(column)
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(json!({"fields":fields, "nullable":nullable}))
            }
            Self::Array { nullable, element } => Some(json!({
                "component": element.descriptor()?, "nullable": nullable, "type": "ARRAY"
            })),
            Self::Map {
                nullable,
                key,
                value,
            } => Some(json!({
                "key": key.descriptor()?, "nullable": nullable, "type": "MAP", "value": value.descriptor()?
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{source::Origin, types::TypeSchemaVersion};

    #[test]
    fn expansion_checks_references_depth_and_shared_children() {
        let column = |kind| SqlType {
            kind,
            nullable: false,
            origin: Origin::Absent {},
        };
        let mut table = TypeTable {
            schema_version: TypeSchemaVersion::V0,
            sources: vec![],
            roots: vec![],
            types: vec![
                column(TypeKind::Boolean {}),
                column(TypeKind::Map {
                    key: TypeId(0),
                    value: TypeId(0),
                }),
            ],
        };
        assert!(table.expand_column(TypeId(1), 2, &mut 3).is_some());
        assert!(table.expand_column(TypeId(1), 1, &mut 3).is_none());
        assert!(table.expand_column(TypeId(1), 2, &mut 2).is_none());
        assert!(table.column_type(TypeId(2)).is_none());
        table.types.push(column(TypeKind::Map {
            key: TypeId(1),
            value: TypeId(0),
        }));
        assert!(table.column_type(TypeId(2)).is_none());
    }
}
