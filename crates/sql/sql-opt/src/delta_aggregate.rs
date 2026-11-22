//! Expand aggregation and weight normalization on the graph used for monotonicity.

use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind as Op, OutputPort, TypeId,
};

pub enum UnaryDelta {
    Distinct {
        positive: bool,
    },
    Aggregate {
        output_type: TypeId,
        function: Option<ExpressionId>,
    },
}

pub struct ExpandedUnaryDelta {
    pub integral: NodeId,
    pub aggregate: Option<NodeId>,
    pub output: NodeId,
}

impl UnaryDelta {
    pub fn expand(
        self,
        edit: &mut CircuitEditor,
        body: NodeId,
        site: NodeId,
        delta: OutputPort,
        input_type: TypeId,
    ) -> Result<ExpandedUnaryDelta, Error> {
        let integral = edit.append_operator(
            body,
            site,
            Op::Integrate,
            &[delta],
            [("outputType", Field::Reference(input_type.into()))],
        )?;
        let integrated = OutputPort {
            operator: integral,
            number: 0,
        };
        let (aggregate, output) = match self {
            Self::Distinct { positive } => {
                let distinct = edit.append_operator(
                    body,
                    site,
                    Op::BinaryDistinct,
                    &[integrated, delta],
                    [
                        ("outputType", Field::Reference(input_type.into())),
                        ("positive", Field::Data(positive.into())),
                    ],
                )?;
                (None, distinct)
            }
            Self::Aggregate {
                output_type,
                function,
            } => {
                let ty = ("outputType", Field::Reference(output_type.into()));
                let function = function.map(|f| ("function", Field::Reference(f.into())));
                let aggregate = edit.append_operator(
                    body,
                    site,
                    Op::PrimitiveAggregate,
                    &[delta, integrated],
                    std::iter::once(ty.clone()).chain(function),
                )?;
                let updates = OutputPort {
                    operator: aggregate,
                    number: 0,
                };
                let upsert =
                    edit.append_operator(body, site, Op::UpsertFeedback, &[updates], [ty])?;
                (Some(aggregate), upsert)
            }
        };
        Ok(ExpandedUnaryDelta {
            integral,
            aggregate,
            output,
        })
    }
}
