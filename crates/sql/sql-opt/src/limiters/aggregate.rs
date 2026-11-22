//! Retain keys and extremal values for the primitive reduction inside a stateful aggregate.
use super::filter::{arg, bool_type, tuple_compare};
use super::retention::{delay, retain_keys};
use super::source::link;
use super::{Bound, Projection, apply_waterlines, pass::apply_bound};
use crate::rebuild::input_type;
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionKind as Expr, Field, NodeId, OperatorKind as Op,
    OutputPort,
};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
pub(super) struct AggregateFacts {
    primitive: Option<u64>,
    input_projection: Option<Projection>,
    result_projection: Option<Projection>,
}

pub(super) struct AggregateInput<'a> {
    pub site: NodeId,
    pub body: NodeId,
    pub data: OutputPort,
    pub waterline: Bound,
    pub following: Option<NodeId>,
    pub functions: &'a BTreeMap<u64, NodeId>,
}

impl AggregateInput<'_> {
    pub fn insert(
        &self,
        edit: &mut CircuitEditor,
        facts: &AggregateFacts,
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<Option<Bound>, Error> {
        let Some(primitive) = facts.primitive else {
            return Ok(None);
        };
        let body = self.body;
        if let Some(projection) = &facts.input_projection {
            self.retain_extremum(edit, projection, next, warnings)?;
        }
        let Some(projection) = &facts.result_projection else {
            return Ok(None);
        };
        let reduction = self
            .functions
            .get(&primitive)
            .ok_or_else(|| Error::new("missing primitive aggregate transfer"))?;
        let function = apply_waterlines(edit, *reduction, &[self.waterline.ty], next, warnings)?;
        let bound = apply_bound(edit, body, self.waterline, function)?;
        edit.place_before(body, bound.port.operator, self.site)?;
        let output = OutputPort {
            operator: self.site,
            number: 0,
        };
        let first = retain_keys(edit, body, self.site, self.data, projection, bound, next)?.first;
        retain_keys(edit, body, self.site, output, projection, bound, next)?;
        let mut result = None;
        let NodeId::Outer(number) = self.site else {
            return Err(Error::new("aggregate requires an operator site"));
        };
        if let Some(upsert) = self.functions.get(&number) {
            let function = apply_waterlines(edit, *upsert, &[bound.ty], next, warnings)?;
            result = Some(apply_bound(edit, body, bound, function)?);
        }
        if let Some(before) = self.following {
            edit.place_before(body, first, before)?;
        }
        Ok(result)
    }

    fn retain_extremum(
        &self,
        edit: &mut CircuitEditor,
        projection: &Projection,
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<(), Error> {
        let node = edit.node(self.site)?;
        let Some(function) = node.field("function").and_then(Field::node_id) else {
            return Ok(());
        };
        if !matches!(
            edit.node(function)?.inner_kind(),
            Some(feldera_sql_ir::InnerKind::Expression(Expr::MinMax))
        ) {
            return Ok(());
        }
        let Projection::Tuple { fields, .. } = projection else {
            return Err(Error::new("aggregate input needs a key/value projection"));
        };
        let [key, value] = fields.as_slice() else {
            return Err(Error::new("aggregate input projection requires two fields"));
        };
        if !value.monotone() {
            return Ok(());
        }
        if !matches!(value, Projection::Tuple { fields, .. } if fields.len() == 1) {
            return Err(Error::new("extremum input value must contain one field"));
        }
        let (which, n, keep_nulls) = match edit
            .node(function)?
            .data("aggregation")
            .and_then(|v| v.as_str())
        {
            Some("Min") => ("BottomN", 1, false),
            Some("Max") => ("TopN", 1, false),
            Some("MinSome1") => ("BottomN", 2, false),
            Some("ArgMinSome") => ("BottomN", 1, true),
            _ => return Err(Error::new("unknown extremum retention policy")),
        };
        let stream = input_type(edit, self.data)?;
        let row = edit.type_id(edit.node(stream.into())?.edge("elementType")?)?;
        let also_retain = if keep_nulls {
            let var = arg(edit, row, next)?;
            let read = edit.dereference(var)?;
            let pair = edit.tuple_field(read, 0)?;
            let compared = edit.tuple_field(pair, 0)?;
            let boolean = bool_type(edit, compared.into())?;
            let is_null = edit.make_expr(
                self.site,
                Expr::IsNull,
                boolean,
                [("expression", link(compared))],
            )?;
            Some(edit.closure_from_variables(is_null, &[var])?)
        } else {
            None
        };

        // The compacted input bound includes the key exactly when it has a monotone component.
        let var = arg(edit, self.waterline.ty, next)?;
        let read = edit.dereference(var)?;
        let values = edit.tuple_field(read, usize::from(key.monotone()))?;
        let field = edit.tuple_field(values, 0)?;
        let field = edit.clone_if_needed(field)?;
        let tuple = edit.tuple_expr(var.into(), false, &[field])?;
        let extract = edit.closure_from_variables(tuple, &[var])?;
        let extract = apply_waterlines(edit, extract.into(), &[self.waterline.ty], next, warnings)?;
        let body = self.body;
        let bound = apply_bound(edit, body, self.waterline, extract)?;
        let (_, control) = delay(edit, body, bound, next)?;

        let control_var = arg(edit, control.ty, next)?;
        let data_var = arg(edit, row, next)?;
        let read = edit.dereference(data_var)?;
        let project = value.project_data(edit, read)?;
        let right = edit.dereference(control_var)?;
        let mut compare = tuple_compare(edit, project, right)?;
        if let Some(predicate) = also_retain {
            let copy = edit.derive_inner(data_var.into(), [])?;
            let matched = feldera_sql_expr::apply_closure(
                edit,
                predicate.into(),
                &[edit.expression(copy)?],
                warnings,
            )?;
            let ty = edit.expression_type(compare)?;
            compare = edit.binary_expression(self.site, ty, BinaryOp::Or, compare, matched)?;
        }
        let data_param = edit.parameter_at(edit.expression_type(data_var)?.into(), data_var)?;
        let control_param = edit.parameter_at(control_var.into(), control_var)?;
        let predicate = edit.closure_with_parameters(compare, &[data_param, control_param])?;
        let retainer = edit.append_operator(
            body,
            self.site,
            Op::IntegrateTraceRetainNValues,
            &[self.data, control.port],
            [
                ("function", link(predicate)),
                ("outputType", link(stream)),
                ("n", Field::Data(n.into())),
                ("which", Field::Data(which.into())),
            ],
        )?;
        edit.move_origin_definition(self.site, retainer)?;
        edit.place_before(body, bound.port.operator, self.site)
    }
}
