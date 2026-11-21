use super::{handle_result, sql_result, visit_expression, ExpandCasts};
use crate::{Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
    TypeKind as Ty,
};

fn reference(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

fn field_name(editor: &mut CircuitEditor, site: NodeId, name: &str) -> Result<ExpressionId, Error> {
    let string = editor.standard_type(site, Ty::String)?;
    editor.make_expr(
        string.into(),
        Expr::StringLiteral,
        string,
        [
            ("value", Field::Data(name.into())),
            ("charset", Field::Data("UTF-8".into())),
        ],
    )
}

pub(super) fn array_type(
    editor: &mut CircuitEditor,
    element: TypeId,
    nullable: bool,
) -> Result<TypeId, Error> {
    let id = editor.inner_at(
        element.into(),
        InnerKind::Type(Ty::Array),
        [
            ("name", Field::Data("Array".into())),
            ("code", Field::Data("ARRAY".into())),
            ("mayBeNull", Field::Data(nullable.into())),
            ("typeArgs", Field::Array(vec![reference(element)])),
        ],
    )?;
    editor.type_id(id)
}

fn call(
    editor: &mut CircuitEditor,
    origin: NodeId,
    name: &str,
    result: TypeId,
    args: &[ExpressionId],
) -> Result<ExpressionId, Error> {
    let function = editor.function_path(origin, name)?;
    editor.make_expr(
        origin,
        Expr::Apply,
        result,
        [
            ("function", reference(function)),
            (
                "arguments",
                Field::Array(args.iter().copied().map(reference).collect()),
            ),
        ],
    )
}

impl ExpandCasts {
    pub(super) fn record_to_variant(
        editor: &mut CircuitEditor,
        origin: NodeId,
        source: ExpressionId,
        target: TypeId,
    ) -> Result<NodeId, Error> {
        let ty = editor.expression_type(source)?;
        let names = editor
            .original_field_names(ty)?
            .ok_or_else(|| Error::new("casting a ROW to VARIANT requires named fields"))?;
        if names.len() != editor.node(ty.into())?.edges("tupFields")?.len() {
            return Err(Error::new("record metadata does not match ROW arity"));
        }
        let string = editor.standard_type(origin, Ty::String)?;
        let variant = editor.variant_type(origin, false)?;
        let map = editor.inner_at(
            string.into(),
            InnerKind::Type(Ty::Map),
            [
                (
                    "typeArgs",
                    Field::Array(vec![reference(string), reference(variant)]),
                ),
                ("name", Field::Data("Map".into())),
                ("code", Field::Data("MAP".into())),
                ("mayBeNull", Field::Data(editor.nullable(ty)?.into())),
            ],
        )?;
        let mut keys = Vec::with_capacity(names.len());
        let mut values = Vec::with_capacity(names.len());
        for (i, name) in names.iter().enumerate() {
            keys.push(reference(field_name(editor, origin, name)?));
            let field = editor.tuple_field(source, i)?;
            let owned = editor.clone_if_needed(field)?;
            values.push(reference(editor.cast(origin, owned, variant)?));
        }
        let data = editor.empty_origin_node(
            origin,
            InnerKind::Expression(Expr::Map),
            [
                ("type", reference(map)),
                ("keys", Field::Array(keys)),
                ("values", Field::Array(values)),
            ],
        )?;
        let target = editor.variant_type(origin, editor.nullable(target)?)?;
        editor
            .cast(origin, editor.expression(data)?, target)
            .map(Into::into)
    }

    pub(super) fn array_to_variant(
        &mut self,
        editor: &mut CircuitEditor,
        origin: NodeId,
        source: ExpressionId,
        variant: TypeId,
        target: TypeId,
    ) -> Result<NodeId, Error> {
        let from = editor.expression_type(source)?;
        let member = editor.type_id(editor.node(from.into())?.edges("typeArgs")?[0])?;
        let parameter = editor.ref_parameter(member, &mut self.next_name)?;
        let value = editor.deref_parameter(&parameter)?;
        let owned = editor.clone_if_needed(value)?;
        let body = editor.cast(origin, owned, variant)?;
        let converter = editor.closure(body, &[parameter])?;
        let array = array_type(editor, variant, editor.nullable(from)?)?;
        let converted = editor.make_expr(
            origin,
            Expr::Binary,
            array,
            [
                ("left", reference(source)),
                ("right", reference(converter)),
                ("opcode", Field::Data("ARRAY_CONVERT".into())),
            ],
        )?;
        let target = editor.variant_type(origin, editor.nullable(target)?)?;
        editor.cast(origin, converted, target).map(Into::into)
    }

    // A safe element conversion propagates a failed result to the enclosing collection.
    // Only the outermost SAFE_CAST turns that failure into SQL NULL.
    fn element(
        &mut self,
        editor: &mut CircuitEditor,
        origin: NodeId,
        source: ExpressionId,
        target: TypeId,
        safe: bool,
        depth: usize,
    ) -> Result<ExpressionId, Error> {
        if !safe {
            let value = self.expand(editor, origin, source, target, false, depth)?;
            return editor.expression(value);
        }
        let result = sql_result(editor, target)?;
        let nullable = editor.with_nullability(target, true)?;
        let expanded = self.expand(editor, origin, source, nullable, true, depth)?;
        let node = editor.node(expanded)?;
        let value = if matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::Cast)))
            && node.data("safe").and_then(|v| v.as_str()) == Some("UnwrapResultSafe")
        {
            editor.expression(node.edge("source")?)?
        } else {
            let value = editor.expression(expanded)?;
            call(editor, origin, "wrap_sql_result", result, &[value])?
        };
        if editor.nullable(target)? {
            Ok(value)
        } else {
            call(editor, origin, "unwrap_sql_result", result, &[value])
        }
    }

    fn converter(
        &mut self,
        editor: &mut CircuitEditor,
        origin: NodeId,
        from: TypeId,
        to: TypeId,
        safe: bool,
        depth: usize,
    ) -> Result<ExpressionId, Error> {
        let parameter = editor.ref_parameter(from, &mut self.next_name)?;
        let input = editor.deref_parameter(&parameter)?;
        let input = match editor.type_kind(from)? {
            Ty::RawTuple | Ty::Tuple | Ty::Struct => input,
            Ty::Array | Ty::Map if safe => input,
            _ => editor.clone_if_needed(input)?,
        };
        let body = self.element(editor, origin, input, to, safe, depth)?;
        editor.closure(body, &[parameter])
    }

    pub(super) fn collection(
        &mut self,
        editor: &mut CircuitEditor,
        origin: NodeId,
        source: ExpressionId,
        target: TypeId,
        safe: bool,
        depth: usize,
    ) -> Result<NodeId, Error> {
        if editor.type_kind(target)? == Ty::Array {
            let from = editor.expression_type(source)?;
            let args = editor.node(from.into())?.edges("typeArgs")?;
            let [member] = args.as_slice() else {
                return Err(Error::new("ARRAY requires one element type"));
            };
            if editor.type_kind(editor.type_id(*member)?)? == Ty::Any
                && editor.expr_kind(source)? == Expr::Array
            {
                let kind = InnerKind::Expression(Expr::Array);
                return match editor.node(source.into())?.field("data") {
                    None if editor.nullable(target)? => {
                        editor.empty_origin_node(origin, kind, [("type", reference(target))])
                    }
                    Some(Field::Array(items)) if items.is_empty() => editor.empty_origin_node(
                        origin,
                        kind,
                        [
                            ("type", reference(target)),
                            ("data", Field::Array(Vec::new())),
                        ],
                    ),
                    _ => Err(Error::new("could not infer a type for array elements")),
                };
            }
            let input = if safe {
                editor.clone_value(source)?
            } else {
                source
            };
            return self.convert_array(editor, origin, input, target, safe, depth);
        }
        let from = editor.expression_type(source)?;
        let src = editor.node(from.into())?.edges("typeArgs")?;
        let dst = editor.node(target.into())?.edges("typeArgs")?;
        let ([src_key, src_value], [dst_key, dst_value]) = (src.as_slice(), dst.as_slice()) else {
            return Err(Error::new("MAP requires two type arguments"));
        };
        let key = self.converter(
            editor,
            origin,
            editor.type_id(*src_key)?,
            editor.type_id(*dst_key)?,
            safe,
            depth,
        )?;
        let value = self.converter(
            editor,
            origin,
            editor.type_id(*src_value)?,
            editor.type_id(*dst_value)?,
            safe,
            depth,
        )?;
        let functions = editor.tuple_expr(origin, true, &[key, value])?;
        let ty = editor.inner_at(
            *dst_key,
            InnerKind::Type(Ty::Map),
            [
                ("name", Field::Data("Map".into())),
                ("code", Field::Data("MAP".into())),
                (
                    "typeArgs",
                    Field::Array(dst.into_iter().map(reference).collect()),
                ),
                ("mayBeNull", Field::Data(editor.nullable(from)?.into())),
            ],
        )?;
        let ty = editor.type_id(ty)?;
        let result = if safe { sql_result(editor, ty)? } else { ty };
        let left = if safe {
            source
        } else {
            editor.clone_value(source)?
        };
        let converted = editor.make_expr(
            origin,
            Expr::Binary,
            result,
            [
                ("left", reference(left)),
                ("right", reference(functions)),
                (
                    "opcode",
                    Field::Data(
                        if safe {
                            "MAP_CONVERT_SAFE"
                        } else {
                            "MAP_CONVERT"
                        }
                        .into(),
                    ),
                ),
            ],
        )?;
        if safe {
            handle_result(editor, origin, converted, target, true)
        } else {
            editor.cast(origin, converted, target).map(Into::into)
        }
    }

    pub(super) fn convert_array(
        &mut self,
        editor: &mut CircuitEditor,
        origin: NodeId,
        input: ExpressionId,
        target: TypeId,
        safe: bool,
        depth: usize,
    ) -> Result<NodeId, Error> {
        let from = editor.expression_type(input)?;
        let src = editor.node(from.into())?.edges("typeArgs")?;
        let dst = editor.node(target.into())?.edges("typeArgs")?;
        let ([src], [dst]) = (src.as_slice(), dst.as_slice()) else {
            return Err(Error::new("ARRAY requires one element type"));
        };
        let member = editor.type_id(*dst)?;
        let function =
            self.converter(editor, origin, editor.type_id(*src)?, member, safe, depth)?;
        let array = array_type(editor, member, safe || editor.nullable(from)?)?;
        let result = if safe {
            sql_result(editor, array)?
        } else {
            array
        };
        let converted = editor.make_expr(
            origin,
            Expr::Binary,
            result,
            [
                ("left", reference(input)),
                ("right", reference(function)),
                (
                    "opcode",
                    Field::Data(
                        if safe {
                            "ARRAY_CONVERT_SAFE"
                        } else {
                            "ARRAY_CONVERT"
                        }
                        .into(),
                    ),
                ),
            ],
        )?;
        if safe {
            handle_result(editor, origin, converted, target, true)
        } else {
            editor.cast(origin, converted, target).map(Into::into)
        }
    }

    pub(super) fn row(
        &mut self,
        editor: &mut CircuitEditor,
        origin: NodeId,
        source: ExpressionId,
        target: TypeId,
        safe: bool,
        depth: usize,
    ) -> Result<NodeId, Error> {
        let from = editor.expression_type(source)?;
        let types = editor.node(target.into())?.edges("tupFields")?;
        let variant = editor.type_kind(from)? == Ty::Variant;
        let names = editor.original_field_names(target)?;
        if names
            .as_ref()
            .is_some_and(|names| names.len() != types.len())
        {
            return Err(Error::new("record metadata does not match ROW arity"));
        }
        if !variant && editor.node(from.into())?.edges("tupFields")?.len() != types.len() {
            return Err(Error::new("ROW cast arity mismatch"));
        }
        let mut fields = Vec::with_capacity(types.len());
        for (index, to) in types.into_iter().enumerate() {
            let field = if variant {
                let (key, opcode) = if let Some(names) = &names {
                    (field_name(editor, origin, &names[index])?, "VARIANT_INDEX")
                } else {
                    let key_type = editor.standard_type(origin, Ty::USize)?;
                    (
                        editor.make_expr(
                            key_type.into(),
                            Expr::USizeLiteral,
                            key_type,
                            [("value", Field::Data(index.to_string().into()))],
                        )?,
                        "RUST_INDEX",
                    )
                };
                let value = editor.clone_if_needed(source)?;
                let variant = editor.variant_type(origin, true)?;
                editor.make_expr(
                    origin,
                    Expr::Binary,
                    variant,
                    [
                        ("left", reference(value)),
                        ("right", reference(key)),
                        ("opcode", Field::Data(opcode.into())),
                    ],
                )?
            } else {
                editor.tuple_field(source, index)?
            };
            let to = editor.type_id(to)?;
            let value = if safe {
                // Inline the one-argument field converter. Substitution also rebuilds
                // calls that refer to the formal parameter with their generated origin.
                let ty = editor.expression_type(field)?;
                let parameter = editor.ref_parameter(ty, &mut self.next_name)?;
                let input = editor.deref_parameter(&parameter)?;
                let argument = match editor.type_kind(ty)? {
                    Ty::Array | Ty::Map | Ty::RawTuple | Ty::Tuple | Ty::Struct => input,
                    _ => editor.clone_if_needed(input)?,
                };
                let body = self.element(editor, origin, argument, to, true, depth)?;
                let inlined = Rewrite::translating_expressions(InlineField {
                    input: input.into(),
                    field: field.into(),
                })
                .apply(editor, body.into())?;
                let reduced =
                    Rewrite::translating_expressions(ReduceField).apply(editor, inlined)?;
                editor.expression(reduced)?
            } else {
                let field = if editor.type_kind(target)? == Ty::RawTuple {
                    field
                } else {
                    editor.clone_if_needed(field)?
                };
                self.element(editor, origin, field, to, false, depth)?
            };
            fields.push(value);
        }
        let converted = if safe {
            let result = sql_result(editor, target)?;
            let name = format!("Tup{}::from_results", fields.len());
            let tuple = call(editor, origin, &name, result, &fields)?;
            // The from_results call is generated by the unlocated function constructor.
            let tuple = editor.derive_unlocated_inner(tuple.into(), [])?;
            let tuple = editor.expression(tuple)?;
            handle_result(editor, origin, tuple, target, true)?
        } else {
            if editor.type_kind(target)? == Ty::RawTuple {
                editor.tuple_expr(origin, true, &fields)?.into()
            } else {
                editor
                    .tuple_with_type_at(source.into(), target, &fields)?
                    .into()
            }
        };
        if !editor.nullable(from)? {
            return Ok(converted);
        }
        let boolean = editor.empty_origin_node(
            origin,
            InnerKind::Type(Ty::Bool),
            [
                ("code", Field::Data("BOOL".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )?;
        // is_null peels cloning because it only inspects the option tag.
        let mut tested = source;
        while editor.expr_kind(tested)? == Expr::Clone {
            tested = editor.expression(editor.node(tested.into())?.edge("expression")?)?;
        }
        let condition = editor.make_expr(
            tested.into(),
            Expr::IsNull,
            editor.type_id(boolean)?,
            [("expression", reference(tested))],
        )?;
        let absent = if editor.nullable(target)? {
            editor.typed_null(target)?
        } else {
            let message = if safe {
                "Cast to non-nullable ROW applied to NULL"
            } else {
                "Cast to non-nullable value applied to NULL"
            };
            editor.make_expr(
                source.into(),
                Expr::Fail,
                target,
                [("message", Field::Data(message.into()))],
            )?
        };
        editor
            .make_expr(
                origin,
                Expr::If,
                editor.expression_type(absent)?,
                [
                    ("condition", reference(condition)),
                    ("positive", reference(absent)),
                    ("negative", reference(converted)),
                ],
            )
            .map(Into::into)
    }
}

/// The converter reads a reference parameter exactly through this dereference.
struct InlineField {
    input: NodeId,
    field: NodeId,
}

impl Transformation for InlineField {
    fn preorder(&mut self, editor: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        if id == self.input {
            Ok(Visit::Replace(self.field))
        } else {
            visit_expression(editor, id)
        }
    }
}

struct ReduceField;

impl Transformation for ReduceField {
    fn preorder(&mut self, editor: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        visit_expression(editor, id)
    }

    fn postorder(&mut self, editor: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        let node = editor.node(id)?;
        match node.inner_kind() {
            Some(InnerKind::Expression(Expr::Clone)) => {
                let source = editor.expression(node.edge("expression")?)?;
                if matches!(
                    editor.expr_kind(source)?,
                    Expr::Clone | Expr::ApplyMethod | Expr::Apply | Expr::If | Expr::Cast
                ) {
                    return Ok(source.into());
                }
            }
            Some(InnerKind::Expression(Expr::Binary))
                if matches!(
                    node.data("opcode").and_then(|op| op.as_str()),
                    Some("MAP_INDEX" | "SQL_INDEX" | "RUST_INDEX")
                ) =>
            {
                let left = editor.node(node.edge("left")?)?;
                if matches!(left.inner_kind(), Some(InnerKind::Expression(Expr::Clone))) {
                    return editor
                        .derive_inner(id, [("left", reference(left.edge("expression")?))]);
                }
            }
            _ => {}
        }
        Ok(id)
    }
}
