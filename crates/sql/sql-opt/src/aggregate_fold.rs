//! Lower a classic aggregate list to the DBSP fold over its tuple of accumulators.
use feldera_sql_expr::{SimplifyWarning, reduce_closure_body_with_warnings};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
    TypeKind as Ty,
};

fn link(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

struct Component {
    accumulator: TypeId,
    weight: TypeId,
    zero: ExpressionId,
    increment: NodeId,
    post: NodeId,
    semigroup: NodeId,
}

impl Component {
    fn read(edit: &mut CircuitEditor, id: NodeId, next: &mut u64) -> Result<Self, Error> {
        let agg = edit.node(id)?;
        let zero = edit.expression(agg.edge("zero")?)?;
        let increment = agg.edge("increment")?;
        let post = agg.field("postProcess").and_then(Field::node_id);
        let semigroup = agg.edge("semigroup")?;
        let [acc, _, weight] = edit
            .node(increment)?
            .edges("parameters")?
            .try_into()
            .map_err(|_| Error::new("aggregate increment requires three parameters"))?;
        let accumulator = edit.type_id(edit.node(acc)?.edge("type")?)?;
        let weight = edit.type_id(edit.node(weight)?.edge("type")?)?;
        // Allocate default identity closures before allocating the combined parameters.
        // The generated names are observable in subsequent passes.
        let post = match post {
            Some(post) => post,
            None => {
                let arg = edit.variable(accumulator, next)?;
                edit.closure_from_variables(arg, &[arg])?.into()
            }
        };
        Ok(Self {
            accumulator,
            weight,
            zero,
            increment,
            post,
            semigroup,
        })
    }
    fn update(
        &self,
        edit: &mut CircuitEditor,
        field: ExpressionId,
        row: ExpressionId,
        weight: ExpressionId,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<NodeId, Error> {
        let step = call(edit, self.increment, &[field, row, weight], warnings)?;
        let body = edit.expression(edit.node(self.increment)?.edge("body")?)?;
        let update = if edit.type_kind(edit.expression_type(body)?)? == Ty::Void {
            step
        } else {
            let void = edit.standard_type(field.into(), Ty::Void)?;
            edit.make_expr(
                field.into(),
                Expr::Assignment,
                void,
                [("left", link(field)), ("right", link(step))],
            )?
        };
        edit.inner_at(
            update.into(),
            InnerKind::Other("DBSPExpressionStatement".into()),
            [("expression", link(update))],
        )
    }
}

fn call(
    edit: &mut CircuitEditor,
    closure: NodeId,
    args: &[ExpressionId],
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let node = edit.node(closure)?;
    let body = node.edge("body")?;
    let params = node.edges("parameters")?;
    let args = args.iter().copied().map(NodeId::from).collect::<Vec<_>>();
    let value = reduce_closure_body_with_warnings(edit, body, &params, &args, Some(warnings))?;
    edit.expression(value)
}

fn semigroup(
    edit: &mut CircuitEditor,
    parts: &[Component],
    list: NodeId,
    zero: ExpressionId,
) -> Result<NodeId, Error> {
    let (site, kind, code, name) = if parts.is_empty() {
        (list, Ty::User, "USER", "EmptySemigroup".to_owned())
    } else {
        (
            zero.into(),
            Ty::Semigroup,
            "SEMIGROUP",
            format!("Semigroup{}", parts.len()),
        )
    };
    let arguments = parts
        .iter()
        .map(|p| link(p.accumulator))
        .chain(parts.iter().map(|p| link(p.semigroup)))
        .collect();
    edit.inner_at(
        site,
        InnerKind::Type(kind),
        [
            ("typeArgs", Field::Array(arguments)),
            ("name", Field::Data(name.into())),
            ("code", Field::Data(code.into())),
            ("mayBeNull", Field::Data(false.into())),
        ],
    )
}

/// The classic backend expects a zero, an in-place update, and a final projection.
/// An empty group shares the compiler's empty tuple type between both closures.
pub fn lower_aggregate_fold(
    edit: &mut CircuitEditor,
    list: NodeId,
    empty_tuple: Option<TypeId>,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let aggregate = edit.node(list)?;
    let row = edit.expression(aggregate.edge("rowVar")?)?;
    let parts = aggregate
        .edges("aggregates")?
        .into_iter()
        .map(|part| Component::read(edit, part, next))
        .collect::<Result<Vec<_>, _>>()?;
    let accumulator_type = if parts.is_empty() {
        empty_tuple.ok_or_else(|| Error::new("empty fold needs the shared empty tuple type"))?
    } else {
        edit.tuple_type(list, false, parts.iter().map(|p| p.accumulator.into()))?
    };
    let weight_type = if let Some(first) = parts.first() {
        for part in &parts[1..] {
            if !edit.same_type(first.weight, part.weight)? {
                return Err(Error::new("aggregate increment weight types differ"));
            }
        }
        first.weight
    } else {
        edit.standard_type(list, Ty::Weight)?
    };
    let reference = edit.reference_type_with_mutability(accumulator_type, true)?;
    let accumulator = edit.variable(reference, next)?;
    let post_arg = edit.variable(accumulator_type, next)?;
    let weight = edit.variable(weight_type, next)?;
    let void = edit.standard_type(list, Ty::Void)?;
    let mut statements = Vec::with_capacity(parts.len());
    let mut posts = Vec::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        let target = edit.dereference(accumulator)?;
        let field = edit.tuple_field(target, i)?;
        statements.push(link(part.update(edit, field, row, weight, warnings)?));
        let field = edit.tuple_field(post_arg, i)?;
        let value = edit.clone_if_needed(field)?;
        posts.push(call(edit, part.post, &[value], warnings)?);
    }
    let step = if parts.is_empty() {
        edit.make_expr(void.into(), Expr::VoidLiteral, void, [])?
    } else {
        let block = edit.empty_origin_node(
            list,
            InnerKind::Expression(Expr::Block),
            [("contents", Field::Array(statements)), ("type", link(void))],
        )?;
        edit.expression(block)?
    };
    let increment = edit.closure_from_variables(step, &[accumulator, row, weight])?;
    let projected = edit.tuple_expr(list, false, &posts)?;
    let post = edit.closure_from_variables(projected, &[post_arg])?;
    let zeros = parts.iter().map(|p| p.zero).collect::<Vec<_>>();
    let zero = edit.tuple_expr(list, false, &zeros)?;
    let semigroup = semigroup(edit, &parts, list, zero)?;
    let ty = edit.closure_type(projected, &[edit.expression_type(row)?.into()])?;
    let ty = edit.type_id(ty)?;
    let site = if parts.is_empty() { zero.into() } else { list };
    edit.make_expr(
        site,
        Expr::Fold,
        ty,
        [
            ("semigroup", link(semigroup)),
            ("zero", link(zero)),
            ("increment", link(increment)),
            ("postProcess", link(post)),
        ],
    )
}
