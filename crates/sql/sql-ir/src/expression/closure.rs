//! Construct tuples and closures over borrowed circuit values.

use super::{CircuitEditor, Error, Expr, ExpressionId, Field, InnerKind, NodeId, Ty, TypeId};
pub(super) fn refs(ids: impl IntoIterator<Item = NodeId>) -> Field {
    Field::Array(ids.into_iter().map(Field::Reference).collect())
}

pub struct RefParameter {
    declaration: NodeId,
    pub path: ExpressionId,
    pub argument: TypeId,
    reference: TypeId,
}

impl CircuitEditor {
    /// Construct a call before beta reduction, retaining the closure's result type.
    pub fn call_closure(
        &mut self,
        function: ExpressionId,
        arguments: &[ExpressionId],
    ) -> Result<ExpressionId, Error> {
        let ty = self.expression_type(function)?;
        let result = self.node(ty.into())?.edge("resultType")?;
        let call = self.empty_origin_node(
            function.into(),
            InnerKind::Expression(Expr::Apply),
            [
                ("type", Field::Reference(result)),
                ("function", Field::Reference(function.into())),
                (
                    "arguments",
                    refs(arguments.iter().copied().map(NodeId::from)),
                ),
            ],
        )?;
        self.expression(call)
    }

    /// Call an iterator method whose result type is inferred by Rust.
    pub fn iterator_method(
        &mut self,
        site: NodeId,
        name: &str,
        receiver: ExpressionId,
        args: &[ExpressionId],
    ) -> Result<ExpressionId, Error> {
        let path = self.function_path(site, name)?;
        let any = self.standard_type(site, Ty::Any)?;
        self.make_expr(
            site,
            Expr::ApplyMethod,
            any,
            [
                ("self", Field::Reference(receiver.into())),
                ("function", Field::Reference(path.into())),
                ("arguments", refs(args.iter().copied().map(NodeId::from))),
            ],
        )
    }

    pub fn ref_parameter(
        &mut self,
        argument: TypeId,
        next: &mut u64,
    ) -> Result<RefParameter, Error> {
        let reference = self.reference_type(argument)?;
        let path = self.variable(reference, next)?;
        let name = self
            .node(path.0)?
            .data("variable")
            .expect("generated variable")
            .clone();
        let declaration = self.empty_origin_node(
            argument.0,
            InnerKind::Parameter,
            [
                ("type", Field::Reference(reference.0)),
                ("name", Field::Data(name)),
            ],
        )?;
        Ok(RefParameter {
            declaration,
            path,
            argument,
            reference,
        })
    }

    pub fn tuple_type(
        &mut self,
        sample: NodeId,
        raw: bool,
        fields: impl IntoIterator<Item = NodeId>,
    ) -> Result<TypeId, Error> {
        let (kind, code) = if raw {
            (Ty::RawTuple, "RAW_TUPLE")
        } else {
            (Ty::Tuple, "TUPLE")
        };
        self.empty_origin_node(
            sample,
            InnerKind::Type(kind),
            [
                ("tupFields", refs(fields)),
                ("code", Field::Data(code.into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )
        .map(TypeId)
    }

    pub fn tuple_expr(
        &mut self,
        sample: NodeId,
        raw: bool,
        fields: &[ExpressionId],
    ) -> Result<ExpressionId, Error> {
        let types = fields
            .iter()
            .map(|&f| Ok(self.expression_type(f)?.0))
            .collect::<Result<Vec<_>, Error>>()?;
        let ty = self.tuple_type(sample, raw, types)?;
        let kind = if raw { Expr::RawTuple } else { Expr::Tuple };
        self.empty_origin_node(
            sample,
            InnerKind::Expression(kind),
            [
                ("type", Field::Reference(ty.0)),
                ("fields", refs(fields.iter().map(|f| f.0))),
            ],
        )
        .map(ExpressionId)
    }

    /// Construct a tuple with an existing type, including its declared struct
    /// identity and nullability. The expression constructor checks every field.
    pub fn tuple_with_type(
        &mut self,
        ty: TypeId,
        fields: &[ExpressionId],
    ) -> Result<ExpressionId, Error> {
        self.tuple_with_type_at(ty.0, ty, fields)
    }

    /// Attribute a tuple constructor to the expression that produced its fields.
    pub fn tuple_with_type_at(
        &mut self,
        site: NodeId,
        ty: TypeId,
        fields: &[ExpressionId],
    ) -> Result<ExpressionId, Error> {
        let raw = match self.type_kind(ty)? {
            Ty::RawTuple => true,
            Ty::Tuple => false,
            _ => return Err(Error::new("expected tuple type")),
        };
        let members = self.node(ty.0)?.edges("tupFields")?;
        if members.len() != fields.len() {
            return Err(Error::new("tuple arity differs from its type"));
        }
        for (member, &field) in members.into_iter().zip(fields) {
            if !raw && !self.same_type(self.type_id(member)?, self.expression_type(field)?)? {
                return Err(Error::new("tuple expression field type mismatch"));
            }
        }
        self.make_expr(
            site,
            if raw { Expr::RawTuple } else { Expr::Tuple },
            ty,
            [("fields", refs(fields.iter().map(|f| f.0)))],
        )
    }

    pub fn standard_type(&mut self, sample: NodeId, kind: Ty) -> Result<TypeId, Error> {
        let code = match kind {
            Ty::Any => "ANY",
            Ty::String => "STRING",
            Ty::Str => "STR",
            Ty::Weight => "USER",
            Ty::RawTuple => "RAW_TUPLE",
            Ty::Bool => "BOOL",
            Ty::USize => "USIZE",
            Ty::Void => "VOID",
            _ => return Err(Error::new("expected a canonical closure type")),
        };
        self.primitive_singleton(sample, kind, code, false)
    }

    fn next_variable_name(next: &mut u64) -> Result<String, Error> {
        let name = format!("t_{next}");
        *next = next
            .checked_add(1)
            .ok_or_else(|| Error::new("generated names exhausted"))?;
        Ok(name)
    }

    pub fn variable(&mut self, ty: TypeId, next: &mut u64) -> Result<ExpressionId, Error> {
        self.named_variable(ty, &Self::next_variable_name(next)?)
    }

    pub fn variable_at(
        &mut self,
        site: NodeId,
        ty: TypeId,
        next: &mut u64,
    ) -> Result<ExpressionId, Error> {
        let name = Self::next_variable_name(next)?;
        self.make_expr(
            site,
            Expr::VariablePath,
            ty,
            [("variable", Field::Data(name.into()))],
        )
    }

    pub fn named_variable(&mut self, ty: TypeId, name: &str) -> Result<ExpressionId, Error> {
        self.empty_origin_node(
            ty.0,
            InnerKind::Expression(Expr::VariablePath),
            [
                ("type", Field::Reference(ty.0)),
                ("variable", Field::Data(name.into())),
            ],
        )
        .map(ExpressionId)
    }

    pub fn reference_type(&mut self, ty: TypeId) -> Result<TypeId, Error> {
        self.reference_type_with_mutability(ty, false)
    }

    pub fn reference_type_with_mutability(
        &mut self,
        ty: TypeId,
        mutable: bool,
    ) -> Result<TypeId, Error> {
        self.inner_at(
            ty.0,
            InnerKind::Type(Ty::Ref),
            [
                ("type", Field::Reference(ty.0)),
                ("mutable", Field::Data(mutable.into())),
                ("code", Field::Data("REF".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )
        .map(TypeId)
    }

    pub fn borrow_value(&mut self, value: ExpressionId) -> Result<ExpressionId, Error> {
        let ty = self.reference_type(self.expression_type(value)?)?;
        self.make_expr(
            value.into(),
            Expr::Borrow,
            ty,
            [
                ("expression", Field::Reference(value.into())),
                ("mut", Field::Data(false.into())),
            ],
        )
    }

    fn dereference_type(&mut self, source: TypeId) -> Result<TypeId, Error> {
        let kind = self.type_kind(source)?;
        if kind == Ty::Ref {
            return self.type_id(self.node(source.0)?.edge("type")?);
        }
        match kind {
            Ty::TypedBox | Ty::Lazy => {
                let args = self.node(source.0)?.edges("typeArgs")?;
                self.type_id(
                    *args
                        .first()
                        .ok_or_else(|| Error::new("missing pointee type"))?,
                )
            }
            Ty::Array | Ty::Map => self.collection_storage_type(source, self.nullable(source)?),
            _ => Err(Error::new("expected a dereferenceable type")),
        }
    }

    /// The owned Rust representation of a SQL array or map.
    pub fn collection_storage_type(
        &mut self,
        source: TypeId,
        nullable: bool,
    ) -> Result<TypeId, Error> {
        let args = self.node(source.0)?.edges("typeArgs")?;
        let (target, name) = match (self.type_kind(source)?, args.as_slice()) {
            (Ty::Array, [_]) => (Ty::Vec, "Vec"),
            (Ty::Map, [_, _]) => (Ty::BTreeMap, "BTreeMap"),
            _ => return Err(Error::new("expected an array or map")),
        };
        self.inner_at(
            args[0],
            InnerKind::Type(target),
            [
                ("typeArgs", refs(args)),
                ("name", Field::Data(name.into())),
                ("code", Field::Data("USER".into())),
                ("mayBeNull", Field::Data(nullable.into())),
            ],
        )
        .map(TypeId)
    }

    pub fn dereference(&mut self, path: ExpressionId) -> Result<ExpressionId, Error> {
        let ty = self.dereference_type(self.expression_type(path)?)?;
        self.make_expr(
            path.0,
            Expr::Deref,
            ty,
            [("expression", Field::Reference(path.0))],
        )
    }

    /// Declare a closure argument at the source location chosen by its constructor.
    pub fn parameter_at(&mut self, origin: NodeId, path: ExpressionId) -> Result<NodeId, Error> {
        let name = self
            .node(path.0)?
            .data("variable")
            .cloned()
            .ok_or_else(|| Error::new("closure variable needs a name"))?;
        let ty = self.expression_type(path)?;
        self.inner_at(
            origin,
            InnerKind::Parameter,
            [
                ("name", Field::Data(name)),
                ("type", Field::Reference(ty.0)),
            ],
        )
    }

    pub fn closure_from_variables(
        &mut self,
        body: ExpressionId,
        paths: &[ExpressionId],
    ) -> Result<ExpressionId, Error> {
        let parameters = paths
            .iter()
            .map(|&path| self.parameter_at(path.0, path))
            .collect::<Result<Vec<_>, _>>()?;
        self.closure_with_parameters(body, &parameters)
    }

    /// Read a borrowed argument at a new expression occurrence.
    pub fn deref_parameter(&mut self, p: &RefParameter) -> Result<ExpressionId, Error> {
        self.make_expr(
            p.path.0,
            Expr::Deref,
            p.argument,
            [("expression", Field::Reference(p.path.0))],
        )
    }

    pub fn closure(
        &mut self,
        body: ExpressionId,
        params: &[RefParameter],
    ) -> Result<ExpressionId, Error> {
        let declarations = params.iter().map(|p| p.declaration).collect::<Vec<_>>();
        let types = params.iter().map(|p| p.reference.0).collect::<Vec<_>>();
        self.closure_parts(body, &declarations, &types)
    }

    pub fn closure_with_parameters(
        &mut self,
        body: ExpressionId,
        declarations: &[NodeId],
    ) -> Result<ExpressionId, Error> {
        let types = declarations
            .iter()
            .map(|&id| {
                let p = self.node(id)?;
                if !matches!(p.inner_kind(), Some(InnerKind::Parameter)) {
                    return Err(Error::new("expected a closure parameter"));
                }
                p.edge("type")
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.closure_parts(body, declarations, &types)
    }

    pub fn closure_type(&mut self, body: ExpressionId, types: &[NodeId]) -> Result<NodeId, Error> {
        let result = self.expression_type(body)?;
        self.empty_origin_node(
            body.0,
            InnerKind::Type(Ty::Function),
            [
                ("resultType", Field::Reference(result.0)),
                ("parameterTypes", refs(types.iter().copied())),
                ("code", Field::Data("FUNCTION".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )
    }

    fn closure_parts(
        &mut self,
        body: ExpressionId,
        declarations: &[NodeId],
        types: &[NodeId],
    ) -> Result<ExpressionId, Error> {
        let ty = self.closure_type(body, types)?;
        self.empty_origin_node(
            body.0,
            InnerKind::Expression(Expr::Closure),
            [
                ("type", Field::Reference(ty)),
                ("parameters", refs(declarations.iter().copied())),
                ("body", Field::Reference(body.0)),
            ],
        )
        .map(ExpressionId)
    }
}

impl CircuitEditor {
    /// Build the indexed stream type returned by a key/value projection.
    pub fn indexed_zset_from_pair(&mut self, row: TypeId) -> Result<TypeId, Error> {
        let [key, value] = self
            .node(row.0)?
            .edges("tupFields")?
            .try_into()
            .map_err(|_| Error::new("indexed map requires a key and value pair"))?;
        self.indexed_zset_type(row.into(), self.type_id(key)?, self.type_id(value)?)
    }

    pub fn custom_order_type(
        &mut self,
        site: NodeId,
        row: TypeId,
        comparator: TypeId,
    ) -> Result<TypeId, Error> {
        self.inner_at(
            site,
            InnerKind::Type(Ty::WithCustomOrd),
            [
                ("typeArgs", refs([row.into(), comparator.into()])),
                ("name", Field::Data("WithCustomOrd".into())),
                ("code", Field::Data("USER".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )
        .map(TypeId)
    }

    pub fn indexed_zset_type(
        &mut self,
        site: NodeId,
        key: TypeId,
        value: TypeId,
    ) -> Result<TypeId, Error> {
        let ty = self.inner_at(
            site,
            InnerKind::Type(Ty::IndexedZSet),
            [
                ("keyType", Field::Reference(key.into())),
                ("elementType", Field::Reference(value.into())),
                ("name", Field::Data("IndexedWSet".into())),
                ("code", Field::Data("INDEXED_ZSET".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )?;
        self.type_id(ty)
    }

    pub fn zset_type(&mut self, element: TypeId) -> Result<TypeId, Error> {
        self.inner_at(
            element.0,
            InnerKind::Type(Ty::ZSet),
            [
                ("elementType", Field::Reference(element.0)),
                ("name", Field::Data("WSet".into())),
                ("code", Field::Data("ZSET".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )
        .map(TypeId)
    }

    /// Empty multiset with the same element identity as its consuming view.
    pub fn empty_zset(&mut self, element: TypeId) -> Result<ExpressionId, Error> {
        let ty = self.zset_type(element)?.0;
        self.empty_origin_node(
            element.0,
            InnerKind::Expression(Expr::ZSet),
            [
                ("elementType", Field::Reference(element.0)),
                ("type", Field::Reference(ty)),
                ("data", Field::Array(vec![])),
                ("weights", Field::Array(vec![])),
            ],
        )
        .map(ExpressionId)
    }

    /// A multiset containing one copy of an existing expression.
    pub fn singleton_zset(&mut self, value: ExpressionId) -> Result<ExpressionId, Error> {
        let element = self.expression_type(value)?;
        let ty = self.zset_type(element)?;
        self.empty_origin_node(
            value.0,
            InnerKind::Expression(Expr::ZSet),
            [
                ("elementType", Field::Reference(element.0)),
                ("type", Field::Reference(ty.0)),
                ("data", refs([value.0])),
                ("weights", Field::Array(vec![Field::Data(1.into())])),
            ],
        )
        .map(ExpressionId)
    }
}
