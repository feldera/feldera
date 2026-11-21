//! Build a projection of a borrowed stream element and rebind its consumers.
use crate::row_fields::RowFields;
use feldera_sql_expr::{Rewrite, Transformation, Visit, parameter_paths};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
    TypeKind as Ty,
};
use std::collections::{BTreeSet, HashSet};

fn link(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

/// One borrowed row, key or value. Raw tuples keep all
/// positions and represent a discarded scalar with the empty SQL tuple.
struct Component {
    ty: TypeId,
    used: Vec<bool>,
    tuple: bool,
    raw: bool,
}

impl Component {
    fn new(
        edit: &CircuitEditor,
        reference: NodeId,
        prefix: &[usize],
        paths: &BTreeSet<Vec<usize>>,
    ) -> Result<Self, Error> {
        let ty = edit.type_id(edit.node(reference)?.edge("type")?)?;
        let kind = edit.type_kind(ty)?;
        let tuple = matches!(kind, Ty::RawTuple | Ty::Tuple);
        let whole = paths.contains(&Vec::new()) || paths.contains(prefix);
        let used = if tuple {
            let mut shape = RowFields::new(edit.circuit());
            let mut used = Vec::new();
            for (i, field) in edit
                .node(ty.into())?
                .edges("tupFields")?
                .into_iter()
                .enumerate()
            {
                let mut path = prefix.to_vec();
                path.push(i);
                used.push(whole || !shape.unused(field, &mut path, paths, 0, 0)?);
            }
            used
        } else {
            vec![whole]
        };
        Ok(Self {
            ty,
            used,
            tuple,
            raw: kind == Ty::RawTuple,
        })
    }

    fn compressed_type(&self, edit: &mut CircuitEditor) -> Result<TypeId, Error> {
        if !self.tuple {
            return if self.used[0] {
                Ok(self.ty)
            } else {
                edit.tuple_type(self.ty.into(), false, [])
            };
        }
        let mut kept = Vec::new();
        for (ty, &used) in edit
            .node(self.ty.into())?
            .edges("tupFields")?
            .into_iter()
            .zip(&self.used)
        {
            if used {
                kept.push(ty);
            } else if self.raw {
                if matches!(edit.type_kind(edit.type_id(ty)?)?, Ty::Tuple | Ty::RawTuple) {
                    kept.push(ty);
                } else {
                    kept.push(edit.tuple_type(ty, false, [])?.into());
                }
            }
        }
        let ty = edit.tuple_type(self.ty.into(), self.raw, kept)?;
        edit.with_nullability(ty, edit.nullable(self.ty)?)
    }

    fn project(
        &self,
        edit: &mut CircuitEditor,
        value: ExpressionId,
    ) -> Result<ExpressionId, Error> {
        if !self.tuple {
            return if self.used[0] {
                edit.clone_if_needed(value)
            } else {
                edit.tuple_expr(value.into(), false, &[])
            };
        }
        let nullable = edit.nullable(self.ty)?;
        let mut kept = Vec::new();
        for (i, &used) in self.used.iter().enumerate() {
            if !used && !self.raw {
                continue;
            }
            let source = if nullable {
                let ty = edit.with_nullability(self.ty, false)?;
                edit.make_expr(
                    value.into(),
                    Expr::Unwrap,
                    ty,
                    [
                        ("expression", link(value)),
                        ("message", Field::Data("".into())),
                    ],
                )?
            } else {
                value
            };
            let field = edit.tuple_field(source, i)?;
            let nested_tuple = matches!(
                edit.type_kind(edit.expression_type(field)?)?,
                Ty::Tuple | Ty::RawTuple
            );
            kept.push(if used || nested_tuple {
                edit.clone_if_needed(field)?
            } else {
                edit.tuple_expr(value.into(), false, &[])?
            });
        }
        let tuple = edit.tuple_expr(value.into(), self.raw, &kept)?;
        if !nullable || self.raw {
            return Ok(tuple);
        }
        let tuple = edit.some_at(value.into(), tuple)?;
        let ty = edit.expression_type(tuple)?;
        let condition = edit.null_test(value)?;
        let null = edit.typed_null(ty)?;
        edit.make_expr(
            tuple.into(),
            Expr::If,
            ty,
            [
                ("condition", link(condition)),
                ("positive", link(null)),
                ("negative", link(tuple)),
            ],
        )
    }
}

enum Element {
    Row(Component),
    Indexed([Component; 2]),
}

pub(super) struct RowProjection {
    parameter_type: TypeId,
    element: Element,
}

fn parameter(edit: &CircuitEditor, function: NodeId) -> Result<NodeId, Error> {
    let [parameter] = edit
        .node(function)?
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("expected a unary projection closure"))?;
    Ok(parameter)
}

impl RowProjection {
    pub(super) fn new(
        edit: &CircuitEditor,
        function: NodeId,
        paths: &BTreeSet<Vec<usize>>,
    ) -> Result<Self, Error> {
        let param = parameter(edit, function)?;
        let ty = edit.type_id(edit.node(param)?.edge("type")?)?;
        let element = match edit.type_kind(ty)? {
            Ty::Ref => Element::Row(Component::new(edit, ty.into(), &[], paths)?),
            Ty::RawTuple => {
                let [key, value] = edit
                    .node(ty.into())?
                    .edges("tupFields")?
                    .try_into()
                    .map_err(|_| Error::new("expected a borrowed key and value"))?;
                Element::Indexed([
                    Component::new(edit, key, &[0], paths)?,
                    Component::new(edit, value, &[1], paths)?,
                ])
            }
            _ => return Err(Error::new("expected a borrowed row or indexed pair")),
        };
        Ok(Self {
            parameter_type: ty,
            element,
        })
    }

    pub(super) fn has_unused_fields(&self) -> bool {
        let components = match &self.element {
            Element::Row(row) => std::slice::from_ref(row),
            Element::Indexed(pair) => pair.as_slice(),
        };
        components.iter().any(|part| part.used.contains(&false))
    }

    pub(super) fn project(
        &self,
        edit: &mut CircuitEditor,
        next: &mut u64,
        reader_uses_input: bool,
    ) -> Result<ExpressionId, Error> {
        // A recorded field use borrows the original row again. A constant
        // reader keeps its declared reference when the filter supplies fields.
        let parameter_type = match &self.element {
            Element::Row(row) if reader_uses_input => edit.reference_type(row.ty)?,
            _ => self.parameter_type,
        };
        let parameter = edit.variable(parameter_type, next)?;
        let value = match &self.element {
            Element::Row(row) => {
                let source = edit.dereference(parameter)?;
                row.project(edit, source)?
            }
            Element::Indexed(pair) => {
                let mut values = Vec::with_capacity(2);
                for (i, component) in pair.iter().enumerate() {
                    let field = edit.tuple_field(parameter, i)?;
                    let source = edit.dereference(field)?;
                    values.push(component.project(edit, source)?);
                }
                edit.tuple_expr(self.parameter_type.into(), true, &values)?
            }
        };
        edit.closure_from_variables(value, &[parameter])
    }

    pub(super) fn rebind(
        &self,
        edit: &mut CircuitEditor,
        function: NodeId,
    ) -> Result<ExpressionId, Error> {
        let param = parameter(edit, function)?;
        let body = edit.node(function)?.edge("body")?;
        let name = edit
            .node(param)?
            .data("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::new("missing projection parameter name"))?
            .to_owned();
        let ty = match &self.element {
            Element::Row(row) => {
                let compressed = row.compressed_type(edit)?;
                edit.reference_type(compressed)?
            }
            Element::Indexed(pair) => {
                let mut references = Vec::with_capacity(2);
                for part in pair {
                    let compressed = part.compressed_type(edit)?;
                    references.push(edit.reference_type(compressed)?.into());
                }
                edit.tuple_type(self.parameter_type.into(), true, references)?
            }
        };
        let narrowed = edit.derive_inner(param, [("type", link(ty))])?;
        let bindings = parameter_paths(edit, body, param)?;
        let body = Rewrite::new(Rebind {
            bindings,
            ty,
            name,
            element: &self.element,
        })
        .apply(edit, body)?;
        let signature = edit.closure_type(edit.expression(body)?, &[ty.into()])?;
        let result = edit.derive_inner(
            function,
            [
                ("parameters", Field::Array(vec![link(narrowed)])),
                ("type", link(signature)),
                ("body", link(body)),
            ],
        )?;
        edit.expression(result)
    }
}

struct Rebind<'a> {
    bindings: HashSet<NodeId>,
    name: String,
    ty: TypeId,
    element: &'a Element,
}

fn field_number(edit: &CircuitEditor, id: NodeId) -> Result<usize, Error> {
    edit.node(id)?
        .data("fieldNo")
        .and_then(|v| v.as_u64())
        .and_then(|i| usize::try_from(i).ok())
        .ok_or_else(|| Error::new("expected a tuple field number"))
}

impl Rebind<'_> {
    /// Rebuild a direct column access at its new offset. Nested fields below
    /// this column retain their offsets when the expression visitor visits them.
    fn column(
        &self,
        edit: &mut CircuitEditor,
        field: NodeId,
    ) -> Result<Option<ExpressionId>, Error> {
        let deref = edit.node(edit.node(field)?.edge("expression")?)?;
        if !matches!(deref.inner_kind(), Some(InnerKind::Expression(Expr::Deref))) {
            return Ok(None);
        }
        let source = deref.edge("expression")?;
        let (part, side) = match self.element {
            Element::Row(row) if self.bindings.contains(&source) => (row, None),
            Element::Indexed(pair) => {
                let node = edit.node(source)?;
                if !matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::Field)))
                    || !self.bindings.contains(&node.edge("expression")?)
                {
                    return Ok(None);
                }
                let index = field_number(edit, source)?;
                (
                    pair.get(index)
                        .ok_or_else(|| Error::new("invalid key/value index"))?,
                    Some(index),
                )
            }
            _ => return Ok(None),
        };
        let old = field_number(edit, field)?;
        if part.used.get(old) != Some(&true) {
            return Err(Error::new("reader references a discarded column"));
        }
        let column = if part.raw {
            old
        } else {
            part.used[..old].iter().filter(|&&used| used).count()
        };
        let row = edit.named_variable(self.ty, &self.name)?;
        let source = match side {
            Some(i) => edit.tuple_field(row, i)?,
            None => row,
        };
        let source = edit.dereference(source)?;
        Ok(Some(edit.tuple_field(source, column)?))
    }
}

impl Transformation for Rebind<'_> {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        if self.bindings.contains(&id) {
            return Ok(Visit::Replace(
                edit.named_variable(self.ty, &self.name)?.into(),
            ));
        }
        match edit.node(id)?.inner_kind() {
            Some(InnerKind::Type(_) | InnerKind::Expression(Expr::Closure)) => Ok(Visit::Keep),
            Some(InnerKind::Expression(Expr::Field)) => Ok(match self.column(edit, id)? {
                Some(value) => Visit::Replace(value.into()),
                None => Visit::Children,
            }),
            _ => Ok(Visit::Children),
        }
    }
}
