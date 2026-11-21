//! Construct immutable bindings with the source and type of their initializer.
use super::{CircuitEditor, Error, Expr, ExpressionId, Field, InnerKind, NodeId, Ty, TypeId};

impl CircuitEditor {
    pub fn let_expression(
        &mut self,
        variable: ExpressionId,
        initializer: ExpressionId,
        consumer: ExpressionId,
    ) -> Result<ExpressionId, Error> {
        self.make_expr(
            initializer.into(),
            Expr::Let,
            self.expression_type(consumer)?,
            [
                ("variable", Field::Reference(variable.into())),
                ("initializer", Field::Reference(initializer.into())),
                ("consumer", Field::Reference(consumer.into())),
            ],
        )
    }

    pub fn let_statement(
        &mut self,
        name: &str,
        initializer: ExpressionId,
    ) -> Result<NodeId, Error> {
        self.inner_at(
            initializer.into(),
            InnerKind::Other("DBSPLetStatement".into()),
            [
                (
                    "type",
                    Field::Reference(self.expression_type(initializer)?.into()),
                ),
                ("initializer", Field::Reference(initializer.into())),
                ("mutable", Field::Data(false.into())),
                ("variable", Field::Data(name.into())),
            ],
        )
    }

    /// LazyCell carries its result type and the inferred type of its initialization closure.
    pub fn lazy_type(&mut self, result: TypeId, closure: TypeId) -> Result<TypeId, Error> {
        let id = self.inner_at(
            result.into(),
            InnerKind::Type(Ty::Lazy),
            [
                (
                    "typeArgs",
                    Field::Array(vec![
                        Field::Reference(result.into()),
                        Field::Reference(closure.into()),
                    ]),
                ),
                ("name", Field::Data("LazyCell".into())),
                ("code", Field::Data("USER".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )?;
        self.type_id(id)
    }
}
