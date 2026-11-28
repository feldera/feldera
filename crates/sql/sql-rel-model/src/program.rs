//! Composition and ownership checks for typed lowering inputs.
use crate::{
    LiteralLimits, ObjectLimits, TypeId, ValidationError, ValidationLimits,
    catalog::Catalog,
    expression_objects::{ExpressionObjects, ObjectKind, SourceContext},
    expressions::ExprId,
    input_producers::InputProducers,
    input_uses::InputUses,
    operators::{DiagnosticSource, OperatorObject, QualifiedName},
    relation_metadata::{Budget as RelationBudget, Coordinates},
    relation_properties::RelationProperties,
    source::{Origin, Position, SourceId},
    statements::{RelRoot, Statement},
    types::TypeKind,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

type Result<T> = std::result::Result<T, ValidationError>;
fn require(condition: bool, message: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(ValidationError {
            path: "program".into(),
            message,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ProgramVersion {
    #[serde(rename = "statement-program-v5")]
    V5,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Program {
    pub schema_version: ProgramVersion,
    pub id: String,
    pub table: InputUses,
    pub expressions: ExpressionObjects,
    pub producers: InputProducers,
    pub properties: RelationProperties,
    pub catalog: Catalog,
    pub statements: Vec<Statement>,
    pub diagnostics: crate::diagnostics::FrontendDiagnostics,
    pub context: crate::compilation_context::CompilationContext,
    /// Relative table-scan creation order controls combined provenance.
    /// Entries are all scan relation IDs, not global allocation IDs.
    pub table_scan_order: Vec<u32>,
}

impl Program {
    /// Unwrap a supplementary character literal when both the registered
    /// operator and its result type match the literal conversion.
    pub fn non_bmp_literal_input(&self, id: ExprId) -> Option<(ExprId, &crate::literals::Literal)> {
        let graph = &self.expressions;
        let expr = graph.expressions.get(id.0 as usize)?;
        let ObjectKind::Call {
            operator,
            arguments,
        } = &expr.kind
        else {
            return None;
        };
        let &[child] = arguments.as_slice() else {
            return None;
        };
        let signature = graph.operators.get(operator.0 as usize)?;
        if graph.operator_objects.get(signature.object.0 as usize)?
            != &OperatorObject::non_bmp_literal()
        {
            return None;
        }
        let input = graph.expressions.get(child.0 as usize)?;
        let ObjectKind::Literal { literal } = &input.kind else {
            return None;
        };
        let types = &self.table.type_table.types;
        let result = literal
            .value
            .non_bmp_result_type(types.get(input.ty.0 as usize)?)?;
        (&result == types.get(expr.ty.0 as usize)?).then_some((child, literal))
    }

    pub fn validate(
        &self,
        types: ValidationLimits,
        literals: LiteralLimits,
        objects: ObjectLimits,
    ) -> Result<()> {
        require(
            !self.id.is_empty() && self.id.len() <= 4096,
            "invalid program identity",
        )?;
        self.expressions.validate(
            &self.table,
            &self.producers,
            &self.properties,
            types,
            literals,
            objects,
        )?;
        self.catalog.validate(
            &self.table.type_table,
            &self.expressions,
            &self.producers,
            &self.properties,
            types,
        )?;
        require(
            self.statements.len() <= types.types,
            "statement count exceeds limit",
        )?;
        self.diagnostics
            .validate(self.statements.len(), &self.table.type_table, types)?;
        self.context.validate(&self.table.type_table, types)?;
        use crate::input_producers::RelationForm;
        let is_scan = |form| {
            matches!(
                form,
                RelationForm::LogicalTableScan | RelationForm::JdbcTableScan
            )
        };
        let scan_count = self
            .producers
            .relations
            .iter()
            .filter(|relation| is_scan(relation.form))
            .count();
        require(
            self.table_scan_order.len() == scan_count,
            "table-scan order count differs",
        )?;
        let mut seen = vec![false; self.producers.relations.len()];
        for &id in &self.table_scan_order {
            let id = id as usize;
            require(
                id < seen.len() && !seen[id],
                "invalid or repeated table-scan identity",
            )?;
            require(
                is_scan(self.producers.relations[id].form),
                "scan order references a non-scan relation",
            )?;
            seen[id] = true;
        }
        let mut validator = Validator {
            program: self,
            bytes: types.source_bytes,
            references: types.references,
            metadata: RelationBudget::new(types.references),
            source: SourceId(0),
        };
        let mut roots = Vec::new();
        let mut values: HashSet<u32> = self
            .catalog
            .schemas
            .iter()
            .map(|schema| schema.values)
            .collect();
        let mut schemas = HashSet::new();
        for statement in &self.statements {
            let source = statement.source();
            require(
                (source.source.0 as usize) < self.table.type_table.sources.len(),
                "statement source outside arena",
            )?;
            require(
                source.visible == (source.source == SourceId(0)),
                "statement visibility differs from source context",
            )?;
            validator.source = source.source;
            for text in [
                &source.kind,
                &source.statement_text,
                &source.diagnostic_text,
            ] {
                validator.text(text)?;
            }
            require(
                matches!(
                    source.kind.as_str(),
                    "CREATE_TABLE"
                        | "CREATE_VIEW"
                        | "CREATE_TYPE"
                        | "CREATE_FUNCTION"
                        | "CREATE_INDEX"
                        | "DROP_TABLE"
                        | "SET_OPTION"
                        | "INSERT"
                        | "DELETE"
                        | "OTHER"
                ),
                "unknown statement source kind",
            )?;
            require(
                source.position_internal.is_none() || !source.position.quoted,
                "extended source position cannot be quoted",
            )?;
            validator.position(&source.position)?;
            match statement {
                Statement::CreateTable {
                    schema,
                    name,
                    name_diagnostic,
                    foreign_keys,
                    ..
                } => {
                    validator.schema(*schema, name, false)?;
                    require(
                        schemas.insert(*schema),
                        "catalog schema has multiple declarations",
                    )?;
                    validator.diagnostic(name_diagnostic)?;
                    validator.count(foreign_keys.len())?;
                    for key in foreign_keys {
                        require(
                            key.source.columns.len() == key.target.columns.len(),
                            "foreign key column counts differ",
                        )?;
                        for side in [&key.source, &key.target] {
                            self.table
                                .type_table
                                .origin(&side.position, "foreign key position")?;
                            validator.qualified(&side.table)?;
                            validator.count(side.columns.len())?;
                            for column in &side.columns {
                                validator.qualified(column)?;
                            }
                        }
                    }
                }
                Statement::CreateView {
                    schema,
                    name,
                    root,
                    emit_final,
                    ..
                } => {
                    validator.schema(*schema, name, false)?;
                    require(
                        schemas.insert(*schema),
                        "catalog schema has multiple declarations",
                    )?;
                    require(
                        *emit_final >= -1
                            && (*emit_final == -1
                                || (*emit_final as usize)
                                    < self.catalog.schemas[*schema as usize].columns.len()),
                        "emit_final column outside schema",
                    )?;
                    validator.root(root)?;
                    roots.push(root.relation);
                }
                Statement::DeclareView { schema, name, .. } => {
                    validator.schema(*schema, name, true)?;
                    require(
                        schemas.insert(*schema),
                        "catalog schema has multiple declarations",
                    )?;
                    require(
                        self.catalog.schemas[*schema as usize].properties.is_none(),
                        "declared view cannot have properties",
                    )?;
                }
                Statement::DropTable { name, .. } => validator.text(&name.name)?,
                Statement::CreateAggregate { function, .. } => {
                    validator.count(1)?;
                    require(
                        matches!(
                            self.expressions.operator_objects.get(function.0 as usize),
                            Some(OperatorObject::AggregateFunction { .. })
                        ),
                        "aggregate declaration requires an aggregate function",
                    )?;
                }
                Statement::CreateFunction { function, .. } => {
                    validator.count(1)?;
                    require(
                        matches!(
                            self.expressions.operator_objects.get(function.0 as usize),
                            Some(OperatorObject::Function { .. })
                        ),
                        "function statement requires a function object",
                    )?;
                }
                Statement::CreateType {
                    name,
                    name_diagnostic,
                    ty,
                    attributes,
                    ..
                } => {
                    validator.text(&name.name)?;
                    validator.diagnostic(name_diagnostic)?;
                    self.table
                        .type_table
                        .reference(*ty, "statement declared type")?;
                    if let Some(attributes) = attributes {
                        require(
                            validator.width(*ty)? == attributes.len(),
                            "type attribute count differs",
                        )?;
                        validator.count(attributes.len())?;
                        for attribute in attributes {
                            validator.text(&attribute.name.name)?;
                            if let Some(name) = &attribute.named_type {
                                validator.text(&name.name)?;
                            }
                        }
                    }
                }
                Statement::CreateIndex {
                    name,
                    target,
                    name_position,
                    target_position,
                    columns,
                    ..
                } => {
                    validator.text(&name.name)?;
                    validator.text(&target.name)?;
                    validator.position(name_position)?;
                    validator.position(target_position)?;
                    validator.count(columns.len())?;
                    for column in columns {
                        validator.text(&column.name.name)?;
                        validator.position(&column.position)?;
                    }
                }
                Statement::SetOption {
                    name,
                    values: usage,
                    ..
                } => {
                    validator.text(&name.name)?;
                    validator.count(1)?;
                    require(
                        self.expressions
                            .value_uses
                            .get(*usage as usize)
                            .is_some_and(|usage| usage.roots.len() == 1),
                        "SET requires exactly one value",
                    )?;
                    let environment = self.expressions.value_uses[*usage as usize].environment;
                    require(
                        self.expressions.environments[environment.0 as usize].source
                            == SourceContext::Present {
                                source: source.source,
                            },
                        "SET value source differs from declaration",
                    )?;
                    require(
                        values.insert(*usage),
                        "statement value use has multiple owners",
                    )?;
                }
                Statement::ModifyTable {
                    name,
                    relation,
                    columns,
                    ..
                } => {
                    validator.text(&name.name)?;
                    if let Some(relation) = relation {
                        validator.count(1)?;
                        require(
                            (*relation as usize) < self.producers.relations.len(),
                            "modification relation outside arena",
                        )?;
                        roots.push(*relation);
                    }
                    if let Some(columns) = columns {
                        validator.count(columns.len())?;
                        for column in columns {
                            validator.text(&column.name.name)?;
                            validator.text(&column.display_name)?;
                            validator.position(&column.position)?;
                        }
                    }
                }
            }
        }
        require(
            values.len() == self.expressions.value_uses.len(),
            "value use has no statement or catalog owner",
        )?;
        require(
            roots == self.producers.roots,
            "statement relation roots differ from producer roots",
        )
    }
}

struct Validator<'a> {
    program: &'a Program,
    bytes: usize,
    references: usize,
    metadata: RelationBudget,
    source: SourceId,
}
impl Validator<'_> {
    fn count(&mut self, count: usize) -> Result<()> {
        self.references = self.references.checked_sub(count).ok_or(ValidationError {
            path: "program".into(),
            message: "statement references exceed limit",
        })?;
        Ok(())
    }
    fn text(&mut self, text: &str) -> Result<()> {
        self.bytes = self.bytes.checked_sub(text.len()).ok_or(ValidationError {
            path: "program".into(),
            message: "statement text exceeds limit",
        })?;
        Ok(())
    }
    fn position(&self, position: &Coordinates) -> Result<()> {
        let start = Position {
            line: position.line,
            column: position.column,
        };
        let end = Position {
            line: position.end_line,
            column: position.end_column,
        };
        let coordinates = [start.line, start.column, end.line, end.column];
        let origin = if coordinates.iter().all(|x| *x == 0) {
            Origin::Absent {}
        } else if coordinates.contains(&0) {
            Origin::Generated {
                source: self.source,
                start,
                end,
            }
        } else {
            Origin::Source {
                source: self.source,
                start,
                end,
            }
        };
        self.program
            .table
            .type_table
            .origin(&origin, "statement position")
    }
    fn diagnostic(&mut self, diagnostic: &DiagnosticSource) -> Result<()> {
        require(
            diagnostic
                .origin
                .source()
                .is_none_or(|source| source == self.source),
            "statement diagnostic source differs from declaration",
        )?;
        self.text(&diagnostic.text)?;
        self.text(&diagnostic.internal_text)?;
        self.program
            .table
            .type_table
            .origin(&diagnostic.origin, "statement diagnostic")
    }
    fn qualified(&mut self, name: &QualifiedName) -> Result<()> {
        self.count(name.names.len() + 1)?;
        for part in &name.names {
            self.text(part)?;
        }
        if let crate::operators::ComponentPositions::Present { positions } = &name.components {
            self.count(positions.len())?;
        }
        name.validate(&self.program.table.type_table, "statement identifier")
    }
    fn schema(&mut self, id: u32, name: &crate::catalog::Name, declared: bool) -> Result<()> {
        self.count(1)?;
        self.text(&name.name)?;
        let schema = self
            .program
            .catalog
            .schemas
            .get(id as usize)
            .ok_or(ValidationError {
                path: "program".into(),
                message: "statement schema outside catalog",
            })?;
        require(
            schema.source == self.source,
            "statement and catalog sources differ",
        )?;
        require(
            schema.name.quoted == name.quoted
                && if declared {
                    schema.name.name.strip_suffix("-decl") == Some(name.name.as_str())
                } else {
                    schema.name.name == name.name
                },
            "statement and catalog names differ",
        )
    }
    fn width(&self, ty: TypeId) -> Result<usize> {
        self.program
            .table
            .type_table
            .reference(ty, "statement row type")?;
        match &self.program.table.type_table.types[ty.0 as usize].kind {
            TypeKind::Row { fields, .. } => Ok(fields.len()),
            _ => Err(ValidationError {
                path: "program".into(),
                message: "statement requires a row type",
            }),
        }
    }
    fn root(&mut self, root: &RelRoot) -> Result<()> {
        self.count(root.fields.len() + 2)?;
        self.text(&root.kind)?;
        require(
            matches!(
                root.kind.as_str(),
                "SELECT" | "UNION" | "INTERSECT" | "EXCEPT" | "VALUES" | "ORDER_BY" | "WITH"
            ),
            "unknown statement query kind",
        )?;
        let relation = self
            .program
            .producers
            .relations
            .get(root.relation as usize)
            .ok_or(ValidationError {
                path: "program".into(),
                message: "statement root outside relation arena",
            })?;
        let width = self.width(relation.row)?;
        require(
            self.width(root.validated_type)? == root.fields.len(),
            "statement result projection width differs",
        )?;
        for field in &root.fields {
            require(
                (field.index as usize) < width,
                "statement projection outside row",
            )?;
            self.text(&field.name)?;
        }
        self.metadata.collation(&root.collation, width)?;
        self.metadata.count(root.hints.len())?;
        for hint in &root.hints {
            hint.validate(&mut self.metadata)?;
            self.position(&hint.position)?;
        }
        Ok(())
    }
}
