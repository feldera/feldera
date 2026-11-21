//! Plan the balanced binary joins and final projection for one term of a star join.
use feldera_sql_expr::{flatten_tuple_fields, reduce_closure_body_with_warnings, SimplifyWarning};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, NodeId, TypeId, TypeKind as Ty};

/// Input indexes refer to the original inputs followed by earlier binary joins.
pub struct BinaryJoin {
    pub inputs: [usize; 2],
    pub output_type: TypeId,
    pub function: ExpressionId,
}

pub struct StarJoinExpansion {
    pub joins: Vec<BinaryJoin>,
    pub projection: ExpressionId,
}

struct Builder<'a> {
    edit: &'a mut CircuitEditor,
    next: &'a mut u64,
}
impl Builder<'_> {
    fn by_ref(&mut self, ty: TypeId) -> Result<ExpressionId, Error> {
        let ty = self.edit.reference_type(ty)?;
        self.edit.variable(ty, self.next)
    }

    /// Each field of a flattened tuple owns a copy of the expression reading it.
    fn flatten(&mut self, variable: ExpressionId) -> Result<Vec<ExpressionId>, Error> {
        let value = self.edit.dereference(variable)?;
        flatten_tuple_fields(self.edit, value)
    }

    fn combine(
        &mut self,
        key: TypeId,
        left_type: TypeId,
        right_type: TypeId,
    ) -> Result<(TypeId, TypeId, ExpressionId), Error> {
        let left_row = self.by_ref(left_type)?;
        let right_row = self.by_ref(right_type)?;
        let group = self.by_ref(key)?;
        let mut types = self.edit.node(left_type.into())?.edges("tupFields")?;
        types.extend(self.edit.node(right_type.into())?.edges("tupFields")?);
        let raw = self.edit.type_kind(left_type)? == Ty::RawTuple;
        let value_type = self.edit.tuple_type(left_type.into(), raw, types)?;
        let value_type = self
            .edit
            .with_nullability(value_type, self.edit.nullable(left_type)?)?;
        let result = self
            .edit
            .indexed_zset_type(value_type.into(), key, value_type)?;
        let keys = self.flatten(group)?;
        let keys = self.edit.tuple_expr(group.into(), false, &keys)?;
        let mut values = self.flatten(left_row)?;
        values.extend(self.flatten(right_row)?);
        let values = self.edit.tuple_expr(left_row.into(), false, &values)?;
        let pair = self.edit.tuple_expr(group.into(), true, &[keys, values])?;
        let function = self
            .edit
            .closure_from_variables(pair, &[group, left_row, right_row])?;
        Ok((value_type, result, function))
    }

    fn project(
        &mut self,
        key: TypeId,
        value: TypeId,
        function: NodeId,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<ExpressionId, Error> {
        let k = self.edit.reference_type(key)?;
        let v = self.edit.reference_type(value)?;
        let pair = self
            .edit
            .tuple_type(key.into(), true, [k.into(), v.into()])?;
        let var = self.edit.variable(pair, self.next)?;
        let key = self.edit.tuple_field(var, 0)?;
        let mut arguments = vec![self.edit.clone_if_needed(key)?.into()];
        let closure = self.edit.node(function)?;
        let parameters = closure.edges("parameters")?;
        let body = closure.edge("body")?;
        let mut column = 0;
        for param in parameters.iter().skip(1) {
            let reference = self.edit.node(*param)?.edge("type")?;
            let tuple = self.edit.node(reference)?.edge("type")?;
            let width = self.edit.node(tuple)?.edges("tupFields")?.len();
            let mut fields = Vec::with_capacity(width);
            for index in column..column + width {
                let value = self.edit.tuple_field(var, 1)?;
                let row = self.edit.dereference(value)?;
                let field = self.edit.tuple_field(row, index)?;
                fields.push(self.edit.clone_if_needed(field)?);
            }
            column += width;
            let tuple = self.edit.tuple_expr(var.into(), false, &fields)?;
            arguments.push(self.edit.borrow_value(tuple)?.into());
        }
        let reduced = reduce_closure_body_with_warnings(
            self.edit,
            body,
            &parameters,
            &arguments,
            Some(warnings),
        )?;
        let result = self.edit.expression(reduced)?;
        self.edit.closure_from_variables(result, &[var])
    }
}

/// Concatenate adjacent value rows at each level, carrying the odd row to the next
/// level. Both the executable expansion and the delta analysis use this pairing.
pub fn expand_star_term(
    edit: &mut CircuitEditor,
    function: NodeId,
    key: TypeId,
    rows: &[TypeId],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<StarJoinExpansion, Error> {
    if rows.is_empty() {
        return Err(Error::new("star join requires inputs"));
    }
    let mut builder = Builder { edit, next };
    let mut level: Vec<_> = rows.iter().copied().enumerate().collect();
    let mut joins = Vec::with_capacity(rows.len() - 1);
    while level.len() > 1 {
        let mut parents = Vec::with_capacity(level.len().div_ceil(2));
        let (pairs, unpaired) = level.as_chunks::<2>();
        for &[(left, left_type), (right, right_type)] in pairs {
            let (value, output_type, function) = builder.combine(key, left_type, right_type)?;
            parents.push((rows.len() + joins.len(), value));
            joins.push(BinaryJoin {
                inputs: [left, right],
                output_type,
                function,
            });
        }
        parents.extend_from_slice(unpaired);
        level = parents;
    }
    let projection = builder.project(key, level[0].1, function, warnings)?;
    Ok(StarJoinExpansion { joins, projection })
}
