//! Function definitions share the expression/operator graph, but not the caller's
//! lexical input scope. Validate these edges before constructing the combined DAG.
use crate::{
    ValidationError,
    expressions::{BindingKind, ExpressionTable},
    operators::{FunctionBody, OperatorObject},
    types::TypeKind,
};

impl ExpressionTable {
    pub(crate) fn function_edges(&self) -> Result<Vec<Vec<usize>>, ValidationError> {
        let mut edges = vec![Vec::new(); self.operator_objects.len()];
        let mut owners = vec![false; self.scopes.len()];
        let mut public_scopes = vec![false; self.scopes.len()];
        for entry in &self.roots {
            public_scopes[self.expressions[entry.0 as usize].scope.0 as usize] = true;
        }
        for (index, object) in self.operator_objects.iter().enumerate() {
            let OperatorObject::Function {
                parameters,
                body:
                    FunctionBody::Sql {
                        scope, input, root, ..
                    },
                ..
            } = object
            else {
                continue;
            };
            let invalid = |message| ValidationError {
                path: format!("operator_objects[{index}].body"),
                message,
            };
            let scope_id = scope.0 as usize;
            let Some(declaration) = self.scopes.get(scope_id) else {
                return Err(invalid("function scope is outside its arena"));
            };
            let Some(binding) = self.bindings.get(input.0 as usize) else {
                return Err(invalid("function input is outside its arena"));
            };
            if owners[scope_id]
                || declaration.parent.is_some()
                || declaration.bindings.as_slice() != [*input]
            {
                return Err(invalid("function requires its own isolated input scope"));
            }
            owners[scope_id] = true;
            if public_scopes[scope_id] {
                return Err(invalid(
                    "function parameter scope cannot own a public entry root",
                ));
            }
            if binding.kind != BindingKind::Input {
                return Err(invalid("function parameters require an input binding"));
            }
            let TypeKind::Row { fields, .. } = &self.type_table.types[binding.ty.0 as usize].kind
            else {
                return Err(invalid("function parameter input requires a row type"));
            };
            if fields.len() != parameters.len()
                || !fields
                    .iter()
                    .zip(parameters)
                    .all(|(field, param)| field.name.name == param.name && field.ty == param.ty)
            {
                return Err(invalid(
                    "function input differs from its ordered parameters",
                ));
            }
            if root.0 as usize >= self.expressions.len() {
                return Err(invalid("function body expression is outside its arena"));
            }
            edges[index].push(root.0 as usize);
        }
        Ok(edges)
    }

    pub(crate) fn check_function_roots(&self, independent: &[bool]) -> Result<(), ValidationError> {
        for (index, object) in self.operator_objects.iter().enumerate() {
            if let OperatorObject::Function {
                body: FunctionBody::Sql { scope, root, .. },
                ..
            } = object
                && self.expressions[root.0 as usize].scope != *scope
                && !independent[root.0 as usize]
            {
                return Err(ValidationError {
                    path: format!("operator_objects[{index}].body"),
                    message: "function body crosses a parameter scope",
                });
            }
        }
        Ok(())
    }

    pub(crate) fn construction_edges(
        &self,
        expression_edges: &[Vec<usize>],
    ) -> Result<Vec<Vec<usize>>, ValidationError> {
        let mut edges = expression_edges.to_vec();
        let offset = self.expressions.len();
        for (index, expression) in self.expressions.iter().enumerate() {
            if let Some((operator, _)) = expression.kind.call() {
                edges[index].push(offset + self.operators[operator.0 as usize].object.0 as usize);
            }
        }
        edges.extend(self.function_edges()?);
        Ok(edges)
    }
}
