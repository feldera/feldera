//! Check the roles of local expression fields after their IDs and slot counts
//! have been validated. Output type coercions belong to relational construction.
use crate::{
    ValidationError,
    expression_objects::{ExpressionObjects, ObjectKind},
    expressions::ExprId,
    input_producers::InputProducer,
    input_uses::InputUses,
    object_validation::require,
    relation_properties::{Properties, WindowBound},
    types::TypeKind,
};

pub(crate) fn check_slots(
    graph: &ExpressionObjects,
    inputs: &InputUses,
    producer: &InputProducer,
    properties: &Properties,
    roots: &[ExprId],
) -> Result<(), ValidationError> {
    let expression = |id: ExprId| &graph.expressions[id.0 as usize];
    match properties {
        Properties::Filter { .. } | Properties::Join { .. } => {
            for root in roots {
                require(
                    matches!(
                        inputs.type_table.types[expression(*root).ty.0 as usize].kind,
                        TypeKind::Boolean {}
                    ),
                    "relational condition requires boolean type",
                )?;
            }
        }
        Properties::Values { .. } => {
            for root in roots {
                require(
                    matches!(expression(*root).kind, ObjectKind::Literal { .. }),
                    "VALUES slot requires literal",
                )?;
            }
        }
        Properties::Window { groups } => {
            for (root, ty) in roots.iter().zip(&producer.constants) {
                let value = expression(*root);
                require(
                    matches!(value.kind, ObjectKind::Literal { .. }) && value.ty == *ty,
                    "window constant literal or type differs",
                )?;
            }
            let mut slot = producer.constants.len();
            for group in groups {
                for bound in [group.lower, group.upper] {
                    if matches!(bound, WindowBound::Preceding | WindowBound::Following) {
                        require(
                            !matches!(
                                expression(roots[slot]).kind,
                                ObjectKind::WindowAggregate { .. }
                            ),
                            "window offset cannot be an aggregate call",
                        )?;
                        slot += 1;
                    }
                }
                for _ in 0..group.calls {
                    require(
                        matches!(
                            expression(roots[slot]).kind,
                            ObjectKind::WindowAggregate { .. }
                        ),
                        "window aggregate slot requires extracted aggregate call",
                    )?;
                    slot += 1;
                }
            }
        }
        _ => {}
    }
    if !matches!(properties, Properties::Window { .. }) {
        for root in roots {
            require(
                !matches!(expression(*root).kind, ObjectKind::WindowAggregate { .. }),
                "extracted aggregate requires window owner",
            )?;
        }
    }
    Ok(())
}
