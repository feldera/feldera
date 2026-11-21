use crate::graph::check_graph;
use crate::{
    LiteralLimits, TypeId, ValidatedTypes, ValidationError, ValidationLimits,
    expressions::{BindingId, BindingKind, ExprKind, Expression, ExpressionTable, ScopeId},
    operators::{ComponentPositions, FunctionBody, OperatorObject, RowFieldNames},
    types::TypeKind,
};

/// Bounds for this draft verifier, independent of production resource budgets.
#[derive(Clone, Copy, Debug)]
pub struct ExpressionLimits {
    pub entries: usize,
    pub references: usize,
    pub depth: usize,
    pub identifier_bytes: usize,
}

impl Default for ExpressionLimits {
    fn default() -> Self {
        Self {
            entries: 100_000,
            references: 1_000_000,
            depth: 256,
            identifier_bytes: 1024 * 1024,
        }
    }
}

type Result<T> = std::result::Result<T, ValidationError>;

fn require(condition: bool, path: &str, message: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(ValidationError {
            path: path.into(),
            message,
        })
    }
}

impl ExpressionTable {
    /// Check every entry, including orphans. This establishes structural and
    /// lexical invariants, not operator registry membership, SQL type inference,
    /// or Java import compatibility. Bound input bytes before deserialization.
    pub fn validate(
        &self,
        type_limits: ValidationLimits,
        literal_limits: LiteralLimits,
        limits: ExpressionLimits,
    ) -> Result<()> {
        let types = self.type_table.validated(type_limits)?;
        self.check_limits(limits)?;
        self.check_operators()?;
        let owners = self.binding_owners()?;
        let scope_edges: Vec<Vec<usize>> = self
            .scopes
            .iter()
            .map(|scope| scope.parent.iter().map(|p| p.0 as usize).collect())
            .collect();
        check_graph(&scope_edges, limits.depth, "scopes")?;
        for (i, binding) in self.bindings.iter().enumerate() {
            let path = format!("bindings[{i}]");
            self.type_table.reference(binding.ty, &path)?;
            if matches!(binding.kind, BindingKind::Input | BindingKind::Correlation) {
                require(
                    matches!(
                        self.type_table.types[binding.ty.0 as usize].kind,
                        TypeKind::Row { .. }
                    ),
                    &path,
                    "input and correlation bindings require row types",
                )?;
            }
        }
        self.check_headers()?;
        let edges: Vec<Vec<usize>> = self
            .expressions
            .iter()
            .map(|expr| expr.kind.children().map(|id| id.0 as usize).collect())
            .collect();
        let postorder = check_graph(&edges, limits.depth, "expressions")?;
        // Sharing a binding-free subgraph across scopes does not establish purity
        // or authorize common-subexpression elimination, caching, or hoisting.
        let mut scope_independent = vec![false; self.expressions.len()];
        for id in postorder {
            scope_independent[id] = match &self.expressions[id].kind {
                ExprKind::Literal { .. } => true,
                ExprKind::Call { .. } | ExprKind::FieldAccess { .. } => self.expressions[id]
                    .kind
                    .children()
                    .all(|child| scope_independent[child.0 as usize]),
                ExprKind::InputRef { .. }
                | ExprKind::Correlation { .. }
                | ExprKind::LambdaRef { .. }
                | ExprKind::Lambda { .. }
                | ExprKind::WindowAggregate { .. } => false,
            };
        }
        check_graph(
            &self.construction_edges(&edges)?,
            limits.depth,
            "construction",
        )?;
        self.check_function_roots(&scope_independent)?;
        self.check_expressions(&types, literal_limits, &owners, &scope_independent)
    }

    fn check_operators(&self) -> Result<()> {
        let mut builtins = std::collections::HashSet::new();
        for (index, object) in self.operator_objects.iter().enumerate() {
            require(
                !matches!(
                    object,
                    OperatorObject::Function {
                        body: FunctionBody::ObjectSql { .. },
                        ..
                    }
                ),
                "operator_objects",
                "whole-plan function environment in expression fragment",
            )?;
            object.validate(&self.type_table, &format!("operator_objects[{index}]"))?;
            if let OperatorObject::Builtin {
                registry,
                registry_version,
                key,
            } = object
            {
                require(
                    builtins.insert((registry, registry_version, key)),
                    &format!("operator_objects[{index}]"),
                    "duplicate builtin object identity",
                )?;
            }
        }
        for (i, operator) in self.operators.iter().enumerate() {
            let path = format!("operators[{i}]");
            require(
                (operator.object.0 as usize) < self.operator_objects.len(),
                &path,
                "operator object reference is outside its arena",
            )?;
            for ty in operator
                .parameters
                .iter()
                .chain(std::iter::once(&operator.result))
            {
                self.type_table.reference(*ty, &path)?;
            }
        }
        Ok(())
    }

    fn check_headers(&self) -> Result<()> {
        for (i, root) in self.roots.iter().enumerate() {
            require(
                (root.0 as usize) < self.expressions.len(),
                &format!("roots[{i}]"),
                "expression reference is outside its arena",
            )?;
        }
        // Check all headers before following any edge or reading a child's type.
        for (i, expr) in self.expressions.iter().enumerate() {
            let path = format!("expressions[{i}]");
            self.type_table.reference(expr.ty, &path)?;
            self.type_table
                .origin(&expr.origin, &format!("{path}.origin"))?;
            require(
                (expr.scope.0 as usize) < self.scopes.len(),
                &path,
                "scope reference is outside its arena",
            )?;
            if let Some((operator, _)) = expr.kind.call() {
                require(
                    (operator.0 as usize) < self.operators.len(),
                    &path,
                    "operator reference is outside its arena",
                )?;
            }
            for child in expr.kind.children() {
                require(
                    (child.0 as usize) < self.expressions.len(),
                    &path,
                    "expression reference is outside its arena",
                )?;
            }
        }
        Ok(())
    }
    fn check_expressions(
        &self,
        types: &ValidatedTypes<'_>,
        literal_limits: LiteralLimits,
        owners: &[ScopeId],
        scope_independent: &[bool],
    ) -> Result<()> {
        let mut lambda_scopes = vec![false; self.scopes.len()];
        for (i, expr) in self.expressions.iter().enumerate() {
            let path = format!("expressions[{i}]");
            match &expr.kind {
                ExprKind::Literal { literal } => {
                    require(
                        literal.ty == expr.ty && literal.origin == expr.origin,
                        &path,
                        "literal header differs from its expression",
                    )?;
                    types
                        .validate_literal(literal, literal_limits)
                        .map_err(|mut error| {
                            error.path = format!("{path}.kind.{}", error.path);
                            error
                        })?;
                }
                ExprKind::InputRef { binding, field } => {
                    let ty =
                        self.binding(*binding, BindingKind::Input, expr.scope, owners, &path)?;
                    self.field_type(ty, *field, expr.ty, &path)?;
                }
                ExprKind::Correlation { binding } => {
                    let ty = self.binding(
                        *binding,
                        BindingKind::Correlation,
                        expr.scope,
                        owners,
                        &path,
                    )?;
                    require(
                        ty == expr.ty,
                        &path,
                        "binding type differs from expression type",
                    )?;
                }
                ExprKind::LambdaRef { binding } => {
                    let ty = self.binding(
                        *binding,
                        BindingKind::LambdaParameter,
                        expr.scope,
                        owners,
                        &path,
                    )?;
                    require(
                        ty == expr.ty,
                        &path,
                        "binding type differs from expression type",
                    )?;
                }
                ExprKind::FieldAccess { input, field } => self.field_type(
                    self.expressions[input.0 as usize].ty,
                    *field,
                    expr.ty,
                    &path,
                )?,
                ExprKind::Call { .. } | ExprKind::WindowAggregate { .. } => {
                    self.check_call(expr, &path)?
                }
                ExprKind::Lambda { .. } => {
                    self.check_lambda(expr, owners, &mut lambda_scopes, &path)?
                }
            }
            let child_scope = match &expr.kind {
                ExprKind::Lambda { body_scope, .. } => *body_scope,
                _ => expr.scope,
            };
            for child in expr.kind.children() {
                require(
                    self.expressions[child.0 as usize].scope == child_scope
                        || scope_independent[child.0 as usize],
                    &path,
                    "expression edge crosses an evaluation scope",
                )?;
            }
        }
        for (i, binding) in self.bindings.iter().enumerate() {
            require(
                binding.kind != BindingKind::LambdaParameter || lambda_scopes[owners[i].0 as usize],
                &format!("bindings[{i}]"),
                "lambda parameter has no owning lambda",
            )?;
        }
        Ok(())
    }
    fn check_call(&self, expr: &Expression, path: &str) -> Result<()> {
        let (operator, arguments) = expr.kind.call().expect("call checker receives a call");
        let operator = self
            .operators
            .get(operator.0 as usize)
            .ok_or_else(|| ValidationError {
                path: path.into(),
                message: "operator reference is outside its arena",
            })?;
        require(
            operator.result == expr.ty && operator.parameters.len() == arguments.len(),
            path,
            "call signature differs from its resolved operator",
        )?;
        for (argument, ty) in arguments.iter().zip(&operator.parameters) {
            require(
                self.expressions[argument.0 as usize].ty == *ty,
                path,
                "argument type differs from resolved operator parameter",
            )?;
        }
        Ok(())
    }
    fn check_lambda(
        &self,
        expr: &Expression,
        owners: &[ScopeId],
        lambda_scopes: &mut [bool],
        path: &str,
    ) -> Result<()> {
        let ExprKind::Lambda {
            parameters,
            body_scope,
            body,
        } = &expr.kind
        else {
            unreachable!("lambda checker receives a lambda")
        };
        require(
            (body_scope.0 as usize) < self.scopes.len(),
            path,
            "lambda scope is outside its arena",
        )?;
        let scope = &self.scopes[body_scope.0 as usize];
        require(
            scope.parent == Some(expr.scope)
                && scope
                    .bindings
                    .iter()
                    .copied()
                    .eq(parameters.iter().map(|p| p.binding)),
            path,
            "lambda scope must declare exactly its ordered parameters",
        )?;
        require(
            !lambda_scopes[body_scope.0 as usize],
            path,
            "lambda scope has multiple owners",
        )?;
        lambda_scopes[body_scope.0 as usize] = true;
        // Calcite RexLambda.getType() is the body result type. Its ordered
        // parameter bindings carry the signature independently.
        require(
            self.expressions[body.0 as usize].ty == expr.ty,
            path,
            "lambda result differs from its body type",
        )?;
        for parameter in parameters {
            let declaration = &self.expressions[parameter.declaration.0 as usize];
            require(
                matches!(declaration.kind, ExprKind::LambdaRef { binding } if binding == parameter.binding),
                path,
                "lambda declaration must reference its parameter binding",
            )?;
            let parameter_type = self.binding(
                parameter.binding,
                BindingKind::LambdaParameter,
                *body_scope,
                owners,
                path,
            )?;
            require(
                declaration.ty == parameter_type,
                path,
                "lambda declaration type differs from its binding",
            )?;
        }

        Ok(())
    }

    fn check_limits(&self, limits: ExpressionLimits) -> Result<()> {
        let counts = [
            self.expressions.len(),
            self.bindings.len(),
            self.scopes.len(),
            self.operators.len(),
            self.operator_objects.len(),
        ];
        let entries = counts.into_iter().try_fold(0usize, usize::checked_add);
        require(
            entries.is_some_and(|n| n <= limits.entries && n <= u32::MAX as usize),
            "expressions",
            "arena entries exceed the validation limit",
        )?;
        let mut references = self.roots.len();
        let mut add = |count| -> Result<()> {
            references = references
                .checked_add(count)
                .ok_or_else(|| ValidationError {
                    path: "expressions".into(),
                    message: "reference count overflow",
                })?;
            require(
                references <= limits.references,
                "expressions",
                "references exceed the validation limit",
            )
        };
        add(self.bindings.len())?;
        for scope in &self.scopes {
            add(scope.bindings.len())?;
            add(usize::from(scope.parent.is_some()))?;
        }
        let mut bytes = 0usize;
        let mut add_text = |text: &str| -> Result<()> {
            bytes = bytes
                .checked_add(text.len())
                .ok_or_else(|| ValidationError {
                    path: "operator_objects".into(),
                    message: "operator byte count overflow",
                })?;
            Ok(())
        };
        for operator in &self.operators {
            add(operator.parameters.len())?;
            add(2)?; // Signature result and operator object.
        }
        for object in &self.operator_objects {
            match object {
                OperatorObject::SumZero {} => {}
                OperatorObject::Builtin {
                    registry,
                    registry_version,
                    key,
                } => {
                    for text in [registry, registry_version, key] {
                        add_text(text)?;
                    }
                }
                OperatorObject::TypeConstructor { name, .. } => {
                    add(2)?; // Declared type and overall name position.
                    add(name.names.len())?;
                    for text in &name.names {
                        add_text(text)?;
                    }
                    if let ComponentPositions::Present { positions } = &name.components {
                        add(positions.len())?;
                    }
                }
                OperatorObject::RowConstructor { name, fields } => {
                    add_text(name)?;
                    if let RowFieldNames::Present { names } = fields {
                        add(names.len())?;
                        for field in names.iter().flatten() {
                            add_text(field)?;
                        }
                    }
                }
                OperatorObject::AggregateFunction {
                    name, parameters, ..
                } => {
                    add(name.names.len() + parameters.len() + 2)?;
                    for text in &name.names {
                        add_text(text)?;
                    }
                    for parameter in parameters {
                        add_text(&parameter.name)?;
                    }
                    if let ComponentPositions::Present { positions } = &name.components {
                        add(positions.len())?;
                    }
                }
                OperatorObject::Function {
                    name,
                    parameters,
                    diagnostic,
                    body,
                    ..
                } => {
                    add_text(name)?;
                    add_text(&diagnostic.text)?;
                    add_text(&diagnostic.internal_text)?;
                    add(3)?; // Return type and both source origins.
                    add(parameters.len())?;
                    for parameter in parameters {
                        add_text(&parameter.name)?;
                    }
                    if matches!(body, FunctionBody::Sql { .. }) {
                        add(3)?;
                        if matches!(
                            body,
                            FunctionBody::Sql {
                                source: crate::operators::FunctionSource::Captured { .. },
                                ..
                            }
                        ) {
                            add(1)?;
                        }
                    }
                }
            }
        }
        require(
            bytes <= limits.identifier_bytes,
            "operators",
            "operator identity bytes exceed the validation limit",
        )?;
        for expr in &self.expressions {
            add(2)?; // Header type and scope.
            add(expr.kind.children().count())?;
            match &expr.kind {
                ExprKind::InputRef { .. }
                | ExprKind::Correlation { .. }
                | ExprKind::LambdaRef { .. }
                | ExprKind::Call { .. }
                | ExprKind::WindowAggregate { .. } => add(1)?,
                ExprKind::Lambda { .. } => add(1)?, // Body scope; parameters below.
                ExprKind::Literal { .. } | ExprKind::FieldAccess { .. } => {}
            }
            if let ExprKind::Lambda { parameters, .. } = &expr.kind {
                add(parameters.len())?;
                for parameter in parameters {
                    bytes =
                        bytes
                            .checked_add(parameter.name.len())
                            .ok_or_else(|| ValidationError {
                                path: "expressions".into(),
                                message: "identifier byte count overflow",
                            })?;
                }
            }
        }
        require(
            bytes <= limits.identifier_bytes,
            "expressions",
            "operator and parameter name bytes exceed the validation limit",
        )?;
        Ok(())
    }

    fn binding_owners(&self) -> Result<Vec<ScopeId>> {
        let mut owners = vec![None; self.bindings.len()];
        for (i, scope) in self.scopes.iter().enumerate() {
            let path = format!("scopes[{i}]");
            if let Some(parent) = scope.parent {
                require(
                    (parent.0 as usize) < self.scopes.len(),
                    &path,
                    "parent scope is outside its arena",
                )?;
            }
            for binding in &scope.bindings {
                let owner = owners
                    .get_mut(binding.0 as usize)
                    .ok_or_else(|| ValidationError {
                        path: path.clone(),
                        message: "binding reference is outside its arena",
                    })?;
                require(owner.is_none(), &path, "binding has multiple declarations")?;
                *owner = Some(ScopeId(i as u32));
            }
        }
        owners
            .into_iter()
            .enumerate()
            .map(|(i, owner)| {
                owner.ok_or_else(|| ValidationError {
                    path: format!("bindings[{i}]"),
                    message: "binding has no declaring scope",
                })
            })
            .collect()
    }

    fn binding(
        &self,
        id: BindingId,
        kind: BindingKind,
        scope: ScopeId,
        owners: &[ScopeId],
        path: &str,
    ) -> Result<TypeId> {
        let binding = self
            .bindings
            .get(id.0 as usize)
            .ok_or_else(|| ValidationError {
                path: path.into(),
                message: "binding reference is outside its arena",
            })?;
        require(
            binding.kind == kind,
            path,
            "binding kind differs from reference kind",
        )?;
        let owner = owners[id.0 as usize];
        if kind == BindingKind::Input {
            require(
                owner == scope,
                path,
                "input binding belongs to another evaluation scope",
            )?;
        } else {
            let mut current = Some(scope);
            while let Some(scope) = current {
                if scope == owner {
                    return Ok(binding.ty);
                }
                current = self.scopes[scope.0 as usize].parent;
            }
            return Err(ValidationError {
                path: path.into(),
                message: "binding is outside the lexical scope",
            });
        }
        Ok(binding.ty)
    }

    fn field_type(&self, row: TypeId, field: u32, expected: TypeId, path: &str) -> Result<()> {
        let TypeKind::Row { fields, .. } = &self.type_table.types[row.0 as usize].kind else {
            return Err(ValidationError {
                path: path.into(),
                message: "field access requires a row type",
            });
        };
        require(
            fields
                .get(field as usize)
                .is_some_and(|f| self.type_table.reference_type_matches(f.ty, expected)),
            path,
            "field index, declared shape or nullability differs from its row",
        )
    }
}
