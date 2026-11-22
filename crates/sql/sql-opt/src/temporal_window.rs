//! Build the functions of one SQL window from its compatible temporal comparisons.
use crate::limiters::{self, Bound, Stream, WindowBounds};
use crate::{temporal_window_bounds, window_projection, WindowEndpoint};
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind, Field, NodeId,
    OperatorKind as Op, OutputPort, TypeId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum WindowComparison {
    #[serde(rename = "GT")]
    Greater,
    #[serde(rename = "GTE")]
    GreaterEqual,
    #[serde(rename = "LT")]
    Less,
    #[serde(rename = "LTE")]
    LessEqual,
}

#[derive(Default)]
struct Endpoint {
    bound: Option<(ExpressionId, bool)>,
}

impl Endpoint {
    fn tighten(
        &mut self,
        edit: &mut CircuitEditor,
        value: ExpressionId,
        inclusive: bool,
        operation: BinaryOp,
    ) -> Result<(), Error> {
        let value = if let Some((previous, was_inclusive)) = self.bound {
            if was_inclusive != inclusive {
                return Err(Error::new(
                    "window bounds on the same side must agree on inclusivity",
                ));
            }
            edit.binary_expression(
                previous.into(),
                edit.expression_type(previous)?,
                operation,
                previous,
                value,
            )?
        } else {
            value
        };
        self.bound = Some((value, inclusive));
        Ok(())
    }

    fn inclusive(&self) -> bool {
        self.bound.is_none_or(|(_, inclusive)| inclusive)
    }

    fn endpoint(&self, initial: ExpressionId) -> WindowEndpoint {
        WindowEndpoint {
            expression: self.bound.map(|(value, _)| value),
            initial,
        }
    }
}

pub struct WindowKey {
    pub parameter: NodeId,
    pub row: TypeId,
    pub expression: ExpressionId,
    pub nonnull_type: TypeId,
}

pub struct InitialBounds {
    pub minimum_clock: ExpressionId,
    pub lower: ExpressionId,
    pub upper: ExpressionId,
}

pub struct WindowFunctions {
    pub bounds: ExpressionId,
    pub nonnull: Option<ExpressionId>,
    pub index: ExpressionId,
    pub restore: Option<ExpressionId>,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub lower_unbounded: bool,
}

pub struct TemporalStreams {
    pub rows: Stream,
    pub clock: Bound,
}

/// Apply temporal bounds to a collection and restore its original row layout.
/// The indexed window consumes deltas. Its caller continues to see a collection.
pub fn append_temporal_window(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    streams: TemporalStreams,
    key: TypeId,
    functions: WindowFunctions,
    next: &mut u64,
) -> Result<NodeId, Error> {
    let control = streams.clock.apply(edit, body, site, functions.bounds)?;
    let mut rows = streams.rows;
    let output = |operator| OutputPort {
        operator,
        number: 0,
    };
    let link = Field::Reference;
    if let Some(predicate) = functions.nonnull {
        rows.port = output(edit.append_operator(
            body,
            site,
            Op::Filter,
            &[rows.port],
            [
                ("outputType", link(rows.ty.into())),
                ("function", link(predicate.into())),
            ],
        )?);
    }

    let signature = edit.expression_type(functions.index)?;
    let pair = edit.node(signature.into())?.edge("resultType")?;
    let [_, value] = edit
        .node(pair)?
        .edges("tupFields")?
        .try_into()
        .map_err(|_| Error::new("temporal index must return a key and a value"))?;
    let value = edit.type_id(value)?;
    let indexed = edit.indexed_zset_type(site, key, value)?;
    let index = edit.append_operator(
        body,
        site,
        Op::MapIndex,
        &[rows.port],
        [
            ("outputType", link(indexed.into())),
            ("function", link(functions.index.into())),
        ],
    )?;
    let bounds = WindowBounds {
        lower_inclusive: functions.lower_inclusive,
        upper_inclusive: functions.upper_inclusive,
        lower_unbounded: functions.lower_unbounded,
    };
    let data = Stream {
        port: output(index),
        ty: indexed,
        kind: rows.kind,
    };
    let (_, result) = limiters::window(edit, body, site, data, control, bounds)?;
    let (kind, function, element) = if let Some(restore) = functions.restore {
        (
            Op::Map,
            restore,
            edit.type_id(edit.node(rows.ty.into())?.edge("elementType")?)?,
        )
    } else {
        // The window has no function of its own. Java constructs the deindex
        // closure at an empty source location, as for the closure signature.
        let deindex = crate::rebuild::deindex_function(edit, signature.into(), key, value, next)?;
        (Op::Deindex, deindex, value)
    };
    let collection = edit.zset_type(element)?;
    edit.append_operator(
        body,
        site,
        kind,
        &[result.port],
        [
            ("outputType", link(collection.into())),
            ("function", link(function.into())),
        ],
    )
}

pub fn temporal_window(
    edit: &mut CircuitEditor,
    key: WindowKey,
    initial: InitialBounds,
    comparisons: impl IntoIterator<Item = (WindowComparison, ExpressionId)>,
    next: &mut u64,
) -> Result<WindowFunctions, Error> {
    let mut lower = Endpoint::default();
    let mut upper = Endpoint::default();
    for (comparison, value) in comparisons {
        use WindowComparison::*;
        match comparison {
            Greater | GreaterEqual => lower.tighten(
                edit,
                value,
                matches!(comparison, GreaterEqual),
                BinaryOp::Max,
            )?,
            Less | LessEqual => {
                upper.tighten(edit, value, matches!(comparison, LessEqual), BinaryOp::Min)?
            }
        }
    }
    let bounds = temporal_window_bounds(
        edit,
        key.nonnull_type,
        initial.minimum_clock,
        lower.endpoint(initial.lower),
        upper.endpoint(initial.upper),
        next,
    )?;
    let nonnull = if edit.nullable(edit.expression_type(key.expression)?)? {
        let test = edit.null_test(key.expression)?;
        let predicate = edit.make_expr(
            test.into(),
            ExpressionKind::Unary,
            edit.expression_type(test)?,
            [
                ("source", Field::Reference(test.into())),
                ("opcode", Field::Data("NOT".into())),
            ],
        )?;
        Some(edit.closure_with_parameters(predicate, &[key.parameter])?)
    } else {
        None
    };
    let projection = window_projection(
        edit,
        key.parameter,
        key.row,
        key.expression,
        key.nonnull_type,
        next,
    )?;
    Ok(WindowFunctions {
        bounds,
        nonnull,
        index: projection.index,
        restore: projection.restore,
        lower_inclusive: lower.inclusive(),
        upper_inclusive: upper.inclusive(),
        lower_unbounded: lower.bound.is_none(),
    })
}
