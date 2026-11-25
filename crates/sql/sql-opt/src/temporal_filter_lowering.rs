//! Lower a clock predicate into ordinary filters, temporal windows and a residual join.
use crate::limiters::Stream;
use crate::temporal_comparisons::{ClockError, speculative_clock};
use crate::temporal_filters::checked_closure;
use crate::{
    ClockConsumer, ClockJoinInputs, ClockProjection, ClockRewrite, InitialBounds,
    TemporalPredicate, TemporalStreams, WindowComparison, WindowKey, WindowTerm, append_clock_join,
    append_temporal_window, apply_joined_clock, check_temporal_groups, scalar_clock_operators,
    temporal_window,
};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind, OutputPort, TypeId, TypeKind,
};

pub struct TemporalFilterLowering {
    pub site: NodeId,
    pub clock: Stream,
    pub empty_key: TypeId,
    pub timestamp: TypeId,
}

pub struct ClockFilter {
    pub function: ExpressionId,
    pub output: TypeId,
    pub indexed_clock: OutputPort,
    pub join_key: TypeId,
}

pub enum LoweredClockFilter {
    Windows,
    /// The residual predicate supplies the warning and join source positions.
    Joined(ExpressionId),
    /// Emit the residual warning before reporting the illegal clock use in the lambda.
    UnsupportedLambda {
        predicate: ExpressionId,
        lambda: ExpressionId,
    },
}

impl TemporalFilterLowering {
    /// Each window builds its own scalar waterline before its row index.
    pub fn append_window(
        &self,
        edit: &mut CircuitEditor,
        rows: Stream,
        key: WindowKey,
        initial: InitialBounds,
        comparisons: impl IntoIterator<Item = (WindowComparison, ExpressionId)>,
        next: &mut u64,
    ) -> Result<NodeId, Error> {
        let body = edit.circuit().root_id();
        let minimum = edit.minimum_value(self.timestamp)?;
        let clock = scalar_clock_operators(
            edit,
            body,
            self.clock.port,
            self.clock.ty,
            self.empty_key,
            minimum,
            next,
        )?;
        let key_type = key.nonnull_type;
        let functions = temporal_window(edit, key, initial, comparisons, next)?;
        append_temporal_window(
            edit,
            body,
            self.site,
            TemporalStreams { rows, clock },
            key_type,
            functions,
            next,
        )
    }

    /// An unknown expression or key domain uses the general lowering path.
    /// The caller supplies a private operator fragment and
    /// publishes it only when the complete filter has been lowered.
    pub fn lower_filter(
        &self,
        edit: &mut CircuitEditor,
        rows: Stream,
        filter: ClockFilter,
        next: &mut u64,
    ) -> Result<Option<LoweredClockFilter>, Error> {
        speculative_clock(next, |names| self.lower_checked_filter(edit, rows, filter, names))
    }

    /// A live caller can render a diagnostic using the operands of the failed
    /// predicate. Earlier classifications have already consumed their names.
    pub fn lower_checked_filter(
        &self,
        edit: &mut CircuitEditor,
        mut rows: Stream,
        filter: ClockFilter,
        next: &mut u64,
    ) -> Result<Option<LoweredClockFilter>, ClockError> {
        let Some((parameter, predicates)) = checked_closure(edit, filter.function, next)? else {
            return Ok(None);
        };
        let terms: Vec<_> = predicates
            .iter()
            .map(|predicate| WindowTerm::from_predicate(parameter, predicate))
            .collect();
        let Some(groups) = check_temporal_groups(edit, &terms)? else {
            return Ok(None);
        };
        let original_parameters = edit.node(filter.function.into())?.edges("parameters")?;
        let body = edit.circuit().root_id();
        for group in groups {
            let operator = if let Some(predicate) = group.predicate {
                let predicate = edit.wrap_bool_if_needed(predicate)?;
                if matches!(terms[group.first], WindowTerm::Residual(_)) {
                    let function = edit.closure_with_parameters(predicate, &[parameter])?;
                    let result = self.append_residual(edit, rows, &filter, function, next)?;
                    return Ok(Some(match result {
                        ClockRewrite::Rewritten(_) => LoweredClockFilter::Joined(predicate),
                        ClockRewrite::UnsupportedLambda(lambda) => {
                            LoweredClockFilter::UnsupportedLambda { predicate, lambda }
                        }
                    }));
                }
                let closure = edit.closure_with_parameters(predicate, &original_parameters)?;
                edit.append_operator(
                    body,
                    self.site,
                    OperatorKind::Filter,
                    &[rows.port],
                    [
                        ("outputType", Field::Reference(rows.ty.into())),
                        ("function", Field::Reference(closure.into())),
                    ],
                )?
            } else {
                let bounds = &predicates[group.first..group.first + group.count];
                let Some(window) = self.append_bounds(edit, rows, parameter, bounds, next)? else {
                    return Ok(None);
                };
                window
            };
            rows.port = OutputPort {
                operator,
                number: 0,
            };
            rows.ty = edit.type_id(edit.node(operator)?.edge("outputType")?)?;
        }
        Ok(Some(LoweredClockFilter::Windows))
    }

    fn append_residual(
        &self,
        edit: &mut CircuitEditor,
        rows: Stream,
        filter: &ClockFilter,
        function: ExpressionId,
        next: &mut u64,
    ) -> Result<ClockRewrite<NodeId>, Error> {
        let body = edit.circuit().root_id();
        let inputs = ClockJoinInputs {
            rows: rows.port,
            indexed_clock: filter.indexed_clock,
            row_type: edit.type_id(edit.node(rows.ty.into())?.edge("elementType")?)?,
        };
        let join = append_clock_join(
            edit,
            body,
            self.site,
            inputs,
            filter.join_key,
            self.timestamp,
            next,
        )?;
        let projection = ClockProjection {
            consumer: ClockConsumer::Filter,
            function,
            output: filter.output,
        };
        apply_joined_clock(edit, body, join, projection, self.timestamp, next)
    }

    fn append_bounds(
        &self,
        edit: &mut CircuitEditor,
        rows: Stream,
        parameter: NodeId,
        bounds: &[TemporalPredicate],
        next: &mut u64,
    ) -> Result<Option<NodeId>, Error> {
        let TemporalPredicate::Bound {
            key: expression, ..
        } = bounds[0]
        else {
            return Err(Error::new("window group requires a clock bound"));
        };
        let domain = edit.expression_type(expression)?;
        if !matches!(
            edit.type_kind(domain)?,
            TypeKind::Integer
                | TypeKind::Date
                | TypeKind::Time
                | TypeKind::Timestamp
                | TypeKind::TimestampTz
                | TypeKind::ShortInterval
                | TypeKind::LongInterval
                | TypeKind::Real
                | TypeKind::Double
                | TypeKind::Bool
                | TypeKind::Decimal
        ) {
            return Ok(None);
        }
        let nonnull_type = edit.with_nullability(domain, false)?;
        let key = WindowKey {
            parameter,
            row: edit.type_id(edit.node(rows.ty.into())?.edge("elementType")?)?,
            expression,
            nonnull_type,
        };
        let comparisons = bounds
            .iter()
            .map(|bound| match *bound {
                TemporalPredicate::Bound {
                    clock, comparison, ..
                } => Ok((comparison, clock)),
                _ => Err(Error::new("window group contains a residual predicate")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let upper_bounded = comparisons
            .iter()
            .any(|(op, _)| matches!(op, WindowComparison::Less | WindowComparison::LessEqual));
        let initial = InitialBounds {
            minimum_clock: edit.minimum_value(self.timestamp)?,
            lower: edit.minimum_value(nonnull_type)?,
            upper: if upper_bounded {
                edit.minimum_value(nonnull_type)?
            } else {
                edit.scalar_maximum(nonnull_type)?
            },
        };
        self.append_window(edit, rows, key, initial, comparisons, next)
            .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limiters::StreamKind;
    use feldera_sql_ir::{BinaryOp, BodyOrder, Circuit, ExpressionKind};
    use serde_json::json;

    #[test]
    fn different_window_keys_precede_the_residual_clock_join() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPTypeTimestamp","id":1,"code":"TIMESTAMP","mayBeNull":false,"singleton":true
        })])?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let timestamp = edit.type_id(roots[0])?;
        let join_key = edit.tuple_type(site, false, [])?;
        let empty_key = edit.tuple_type(site, true, [])?;
        let mut next = 0;
        let clock = crate::builtin_clock(&mut edit, site, timestamp, join_key, &mut next)?;
        let row = edit.tuple_type(site, false, [timestamp.into(), timestamp.into()])?;
        let collection = edit.zset_type(row)?;
        let source = edit.append_operator(
            site,
            site,
            OperatorKind::SourceMultiset,
            &[],
            [("outputType", Field::Reference(collection.into()))],
        )?;
        let port = |operator| OutputPort {
            operator,
            number: 0,
        };
        let rows = Stream {
            port: port(source),
            ty: collection,
            kind: StreamKind::Collection,
        };
        let lowering = TemporalFilterLowering {
            site,
            empty_key,
            timestamp,
            clock: Stream {
                port: port(clock.collection),
                ty: edit.type_id(edit.node(clock.collection)?.edge("outputType")?)?,
                kind: StreamKind::Collection,
            },
        };
        let reference = edit.reference_type(row)?;
        let variable = edit.named_variable(reference, "row")?;
        let value = edit.dereference(variable)?;
        let first = edit.tuple_field(value, 0)?;
        let second = edit.tuple_field(value, 1)?;
        let name = edit.function_path(site, "now")?;
        let now = edit.make_expr(
            site,
            ExpressionKind::Apply,
            timestamp,
            [
                ("function", Field::Reference(name.into())),
                ("arguments", Field::Array(vec![])),
            ],
        )?;
        let boolean = edit.bool_type(site, false)?;
        let lower = edit.binary_expression(site, boolean, BinaryOp::Gt, first, now)?;
        let upper = edit.binary_expression(site, boolean, BinaryOp::Lte, second, now)?;
        let residual = edit.binary_expression(site, boolean, BinaryOp::Neq, first, now)?;
        let bounds = edit.binary_expression(site, boolean, BinaryOp::And, lower, upper)?;
        let combined = edit.binary_expression(site, boolean, BinaryOp::And, bounds, residual)?;
        let filter = ClockFilter {
            function: edit.closure_from_variables(combined, &[variable])?,
            output: collection,
            indexed_clock: port(clock.indexed),
            join_key,
        };
        assert!(matches!(
            lowering.lower_filter(&mut edit, rows, filter, &mut next)?,
            Some(LoweredClockFilter::Joined(_))
        ));
        let circuit = edit.finish()?;
        let operations: Vec<_> = circuit
            .operator_order(BodyOrder::Postorder)?
            .into_iter()
            .filter_map(|id| circuit.node(id)?.operator_kind())
            .filter(|op| matches!(op, OperatorKind::Window | OperatorKind::StreamJoin))
            .collect();
        assert_eq!(
            operations,
            [
                OperatorKind::Window,
                OperatorKind::Window,
                OperatorKind::StreamJoin
            ]
        );
        Ok(())
    }
}
