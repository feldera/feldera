use super::*;
use feldera_sql_ir::{ExpressionKind as Expr, InnerKind};

impl Expansion<'_> {
    /// A retractable linear group is a differentiator, a delta aggregate, and
    /// an integral. An empty grouping still needs to count rows.
    pub(super) fn linear(
        &mut self,
        list: NodeId,
        parts: &[NodeId],
        output: TypeId,
    ) -> Result<NodeId, Error> {
        let input_ty = input_type(self.edit, self.input)?;
        let diff = self.operator(
            Op::Differentiate,
            self.input,
            [("outputType", link(input_ty))],
        )?;
        let row = self
            .edit
            .expression(self.edit.node(list)?.edge("rowVar")?)?;
        let (map, post) = if parts.is_empty() {
            let ty = self.edit.empty_origin_node(
                list,
                InnerKind::Type(Ty::Integer),
                [
                    ("width", Field::Data(64.into())),
                    ("signed", Field::Data(true.into())),
                    ("code", Field::Data("INT64".into())),
                    ("mayBeNull", Field::Data(false.into())),
                ],
            )?;
            let ty = self.edit.type_id(ty)?;
            let one = self.edit.make_expr(
                ty.into(),
                Expr::I64Literal,
                ty,
                [("value", Field::Data("1".into()))],
            )?;
            let var = self.edit.variable(ty, self.next)?;
            let tuple = self.edit.tuple_expr(list, false, &[])?;
            (
                self.edit.closure_from_variables(one, &[row])?,
                self.edit.closure_from_variables(tuple, &[var])?,
            )
        } else {
            let maps = parts
                .iter()
                .map(|&p| {
                    self.edit.expression(
                        self.edit
                            .node(self.edit.node(p)?.edge("map")?)?
                            .edge("body")?,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let map = if let [only] = maps.as_slice() {
                *only
            } else {
                self.edit.tuple_expr(list, false, &maps)?
            };
            let map = self.edit.closure_from_variables(map, &[row])?;
            let posts = parts
                .iter()
                .map(|&p| self.edit.node(p)?.edge("postProcess"))
                .collect::<Result<Vec<_>, _>>()?;
            let mut types = Vec::with_capacity(posts.len());
            for &post in &posts {
                let [param] = self
                    .edit
                    .node(post)?
                    .edges("parameters")?
                    .try_into()
                    .map_err(|_| Error::new("linear postprocessing needs one argument"))?;
                types.push(self.edit.node(param)?.edge("type")?);
            }
            let ty = if let [only] = types.as_slice() {
                self.edit.type_id(*only)?
            } else {
                let ty = self.edit.inner_at(
                    list,
                    InnerKind::Type(Ty::Tuple),
                    [
                        (
                            "tupFields",
                            Field::Array(types.into_iter().map(link).collect()),
                        ),
                        ("code", Field::Data("TUPLE".into())),
                        ("mayBeNull", Field::Data(false.into())),
                    ],
                )?;
                self.edit.type_id(ty)?
            };
            let var = self.edit.variable(ty, self.next)?;
            let mut values = Vec::with_capacity(posts.len());
            for (i, &post) in posts.iter().enumerate() {
                let arg = if posts.len() == 1 {
                    var
                } else {
                    self.edit.tuple_field(var, i)?
                };
                values.push(self.call(post, &[arg])?);
            }
            let result = self.edit.tuple_expr(list, false, &values)?;
            (map, self.edit.closure_from_variables(result, &[var])?)
        };
        let agg = self.operator(
            Op::AggregateLinearPostprocess,
            port(diff),
            [
                ("outputType", link(output)),
                ("function", link(map)),
                ("postProcess", link(post)),
            ],
        )?;
        self.operator(Op::Integrate, port(agg), [("outputType", link(output))])
    }
}
