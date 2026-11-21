//! Retractable extrema use an ordered value. ARG_MIN and ARG_MAX extract
//! the result from the pair of comparison and return values.
use super::*;
use crate::identity::identity_closure;
use feldera_sql_ir::{ExpressionKind as Expr, InnerKind};

impl Expansion<'_> {
    fn post_pair(
        &mut self,
        part: NodeId,
        value: TypeId,
        expected: TypeId,
    ) -> Result<ExpressionId, Error> {
        let post = self.edit.node(part)?.edge("postProcess")?;
        let [arg] = self
            .edit
            .node(post)?
            .edges("parameters")?
            .try_into()
            .map_err(|_| Error::new("invalid ARG_MIN/MAX postprocessor"))?;
        let ty = self.edit.type_id(self.edit.node(arg)?.edge("type")?)?;
        let first_type = self.edit.tuple_field_type(ty, 0)?;
        let result_type = self.edit.tuple_field_type(expected, 0)?;
        let reference = self.edit.reference_type(value)?;
        let var = self.edit.variable(reference, self.next)?;
        let row = self.edit.dereference(var)?;
        let compared = self.edit.tuple_field(row, 0)?;
        let first = self.edit.tuple_field(compared, 0)?;
        let first = self.edit.cast(first.into(), first, first_type)?;
        let second = self.edit.tuple_field(compared, 1)?;
        let second = self.edit.cast(part, second, result_type)?;
        let pair = self.edit.tuple_expr(part, true, &[first, second])?;
        let result = self.call(post, &[pair])?;
        let tuple = self.edit.tuple_expr(part, false, &[result])?;
        self.edit.closure_from_variables(tuple, &[var])
    }

    // DBSPTypeTuple.caster calls each column's own caster. Its nested bindings
    // consume generated names even after beta reduction eliminates those calls.
    fn caster(&mut self, from: TypeId, to: TypeId, depth: usize) -> Result<ExpressionId, Error> {
        if depth > 128 {
            return Err(Error::new("aggregate cast exceeds the type nesting limit"));
        }
        let reference = self.edit.reference_type(from)?;
        let var = self.edit.variable(reference, self.next)?;
        let result =
            if self.edit.type_kind(from)? == Ty::Tuple && self.edit.type_kind(to)? == Ty::Tuple {
                let src = self.edit.node(from.into())?.edges("tupFields")?;
                let dst = self.edit.node(to.into())?.edges("tupFields")?;
                if src.len() != dst.len() {
                    return Err(Error::new("aggregate cast changes row width"));
                }
                let mut fields = Vec::with_capacity(src.len());
                for (i, (&a, b)) in src.iter().zip(dst).enumerate() {
                    let closure =
                        self.caster(self.edit.type_id(a)?, self.edit.type_id(b)?, depth + 1)?;
                    let path = copy_expression(self.edit, var.into())?;
                    let row = self.edit.dereference(self.edit.expression(path)?)?;
                    let field = self.edit.tuple_field(row, i)?;
                    let arg = self.edit.borrow_value(field)?;
                    fields.push(self.call(closure.into(), &[arg])?);
                }
                let tuple = self.edit.tuple_expr(var.into(), false, &fields)?;
                let mut value = if self.edit.nullable(to)? {
                    self.edit.some_at(var.into(), tuple)?
                } else {
                    tuple
                };
                if self.edit.nullable(from)? {
                    let row = self.edit.dereference(var)?;
                    let condition = self.edit.null_test(row)?;
                    let empty = if self.edit.nullable(to)? {
                        self.edit.typed_null(to)?
                    } else {
                        self.edit.make_expr(
                            var.into(),
                            Expr::Fail,
                            to,
                            [(
                                "message",
                                Field::Data("Cast to non-nullable value applied to NULL".into()),
                            )],
                        )?
                    };
                    value = self.edit.make_expr(
                        var.into(),
                        Expr::If,
                        self.edit.expression_type(value)?,
                        [
                            ("condition", link(condition)),
                            ("positive", link(empty)),
                            ("negative", link(value)),
                        ],
                    )?;
                }
                value
            } else {
                let row = self.edit.dereference(var)?;
                let owned = self.edit.clone_if_needed(row)?;
                self.edit.cast(from.into(), owned, to)?
            };
        self.edit.closure_from_variables(result, &[var])
    }

    pub(super) fn min_max(&mut self, part: NodeId, expected: TypeId) -> Result<NodeId, Error> {
        let compared = self.edit.node(part)?.edge("comparedValue")?;
        let mut input = self.input;
        if !identity_closure(self.edit, compared, self.next)? {
            let var = self.pair_variable(self.value)?;
            let key = self.half(var, 0)?;
            let keys = self.flatten(key)?;
            let key = self.edit.tuple_expr(part, false, &keys)?;
            let reference = self.edit.tuple_field(var, 1)?;
            let value = self.call(compared, &[reference])?;
            let value = self.edit.clone_if_needed(value)?;
            let value = self.edit.tuple_expr(part, false, &[value])?;
            let pair = self.edit.tuple_expr(part, true, &[key, value])?;
            input = port(self.map_index(input, pair, var)?);
        }
        let ty = input_type(self.edit, input)?;
        let value = self
            .edit
            .type_id(self.edit.node(ty.into())?.edge("elementType")?)?;
        let key = self
            .edit
            .type_id(self.edit.node(ty.into())?.edge("keyType")?)?;
        let field = self.edit.tuple_field_type(value, 0)?;
        let operation = self
            .edit
            .node(part)?
            .data("operation")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::new("missing extremum operation"))?
            .to_owned();
        let mut post = None;
        let mut compared_type = value;
        let aggregation = match operation.as_str() {
            "Max" => "Max",
            "Min" if self.edit.nullable(field)? => {
                compared_type = self.edit.tuple_type(part, false, [field.into()])?;
                "MinSome1"
            }
            "Min" => "Min",
            "ArgMax" => {
                post = Some(self.post_pair(part, value, expected)?);
                "Max"
            }
            "ArgMin" => {
                let compared = self.edit.tuple_field_type(field, 0)?;
                if self.edit.nullable(compared)? {
                    let result = self.edit.tuple_field_type(expected, 0)?;
                    if self.edit.nullable(result)? {
                        let returned = self.edit.tuple_field_type(field, 1)?;
                        let ty = self.edit.tuple_type(part, false, [returned.into()])?;
                        let reference = self.edit.reference_type(ty)?;
                        let var = self.edit.variable(reference, self.next)?;
                        let row = self.edit.dereference(var)?;
                        let f = self.edit.tuple_field(row, 0)?;
                        let cast = self.edit.cast(part, f, result)?;
                        let owned = self.edit.clone_if_needed(cast)?;
                        let tuple = self.edit.tuple_expr(part, false, &[owned])?;
                        post = Some(self.edit.closure_from_variables(tuple, &[var])?);
                    }
                    "ArgMinSome"
                } else {
                    post = Some(self.post_pair(part, value, expected)?);
                    "Min"
                }
            }
            _ => return Err(Error::new("unsupported extremum operation")),
        };
        if matches!(operation.as_str(), "Min" | "Max")
            && !self.edit.same_type(compared_type, expected)?
        {
            let closure = self.caster(compared_type, expected, 0)?;
            let reduced = feldera_sql_expr::simplify_closure_with_warnings(
                self.edit,
                closure.into(),
                Some(self.warnings),
            )?;
            post = Some(self.edit.expression(reduced)?);
        }
        let result = self.edit.node(part)?.edge("type")?;
        let ty = self.edit.empty_origin_node(
            part,
            InnerKind::Type(Ty::Function),
            [
                ("resultType", link(result)),
                ("parameterTypes", Field::Array(vec![link(key)])),
                ("code", Field::Data("FUNCTION".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )?;
        let mut fields = vec![("type", link(ty))];
        if let Some(post) = post {
            fields.push(("postProcessing", link(post)));
        }
        fields.push(("aggregation", Field::Data(aggregation.into())));
        let function = self
            .edit
            .inner_at(part, InnerKind::Expression(Expr::MinMax), fields)?;
        let output = self
            .edit
            .indexed_zset_type(expected.into(), key, expected)?;
        self.operator(
            Op::StreamAggregate,
            input,
            [("outputType", link(output)), ("function", link(function))],
        )
    }
}
