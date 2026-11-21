//! Append-only aggregates accumulate the first row with an initializer and
//! every subsequent row with a step closure over the previous accumulator.
use super::*;
use feldera_sql_ir::ExpressionKind as Expr;

impl Expansion<'_> {
    fn arithmetic(
        &mut self,
        op: &str,
        left: ExpressionId,
        right: ExpressionId,
    ) -> Result<ExpressionId, Error> {
        let ty = self.edit.expression_type(left)?;
        self.edit.make_expr(
            left.into(),
            Expr::Binary,
            ty,
            [
                ("opcode", Field::Data(op.into())),
                ("left", link(left)),
                ("right", link(right)),
            ],
        )
    }

    pub(super) fn chain(
        &mut self,
        parts: &[NodeId],
        weight: ExpressionId,
    ) -> Result<NodeId, Error> {
        let mut types = Vec::with_capacity(parts.len());
        for &part in parts {
            let p = self.edit.node(part)?;
            let ty = if p.class() == "LinearAggregate" {
                self.edit
                    .node(self.edit.node(p.edge("map")?)?.edge("body")?)?
                    .edge("type")?
            } else {
                let step = self.edit.node(p.edge("increment")?)?;
                let params = step.edges("parameters")?;
                let first = params
                    .first()
                    .ok_or_else(|| Error::new("missing MIN/MAX accumulator"))?;
                self.edit.node(*first)?.edge("type")?
            };
            types.push(ty);
        }
        let ty = self.edit.tuple_type(self.site, false, types)?;
        let acc = self.edit.variable(ty, self.next)?;
        let input_ty = self.edit.reference_type(self.value)?;
        let row = self.edit.variable(input_ty, self.next)?;
        let post_var = self.pair_variable(ty)?;
        let mut init = Vec::new();
        let mut steps = Vec::new();
        let mut posts = Vec::new();
        let mut needs_post = false;
        for (i, &part) in parts.iter().enumerate() {
            let post_row = self.half(post_var, 1)?;
            let field = self.edit.tuple_field(post_row, i)?;
            let field = self.edit.clone_if_needed(field)?;
            let p = self.edit.node(part)?;
            if p.class() == "MinMaxAggregate" {
                let inc = p.edge("increment")?;
                let zero = self.edit.expression(p.edge("zero")?)?;
                let post = p.field("postProcess").and_then(Field::node_id);
                init.push(self.call(inc, &[zero, row, weight])?);
                let acc_field = self.edit.tuple_field(acc, i)?;
                steps.push(self.call(inc, &[acc_field, row, weight])?);
                if let Some(post) = post {
                    needs_post = true;
                    posts.push(self.call(post, &[field])?);
                } else {
                    posts.push(field);
                }
            } else {
                let map = p.edge("map")?;
                let post = p.edge("postProcess")?;
                let mapped = self.call(map, &[row])?;
                init.push(self.arithmetic("MUL_WEIGHT", mapped, weight)?);
                let mapped = self.call(map, &[row])?;
                let weighted = self.arithmetic("MUL_WEIGHT", mapped, weight)?;
                let previous = self.edit.tuple_field(acc, i)?;
                steps.push(self.arithmetic("ADD", weighted, previous)?);
                posts.push(self.call(post, &[field])?);
                needs_post = true;
            }
        }
        let init = self.edit.tuple_expr(self.site, false, &init)?;
        let init = self.edit.closure_from_variables(init, &[row, weight])?;
        let step = self.edit.tuple_expr(self.site, false, &steps)?;
        let step = self
            .edit
            .closure_from_variables(step, &[acc, row, weight])?;
        let output = self.edit.indexed_zset_type(ty.into(), self.key, ty)?;
        let chain = self.operator(
            Op::ChainAggregate,
            self.input,
            [
                ("outputType", link(output)),
                ("function", link(step)),
                ("init", link(init)),
            ],
        )?;
        if !needs_post {
            return Ok(chain);
        }
        let key = self.half(post_var, 0)?;
        let key = self.edit.clone_if_needed(key)?;
        let post = self.edit.tuple_expr(self.site, false, &posts)?;
        let pair = self.edit.tuple_expr(self.site, true, &[key, post])?;
        self.map_index(port(chain), pair, post_var)
    }
}
