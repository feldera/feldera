use crate::ir::{exprs::visit::MapExprIds, ExprId, Function, Terminator};
use std::{collections::BTreeMap, mem::take};

#[allow(dead_code)]
pub(super) fn eliminate_redundant_jumps(func: &mut Function) {
    // Collect all block targets, ignoring them if there's more than one dominator
    // TODO: We could inline small blocks into each other
    let mut candidates = BTreeMap::new();
    for (&block_id, block) in func.blocks() {
        match block.terminator() {
            Terminator::Jump(jump) => {
                candidates
                    .entry(jump.target())
                    .and_modify(|dominator| *dominator = None)
                    .or_insert(Some(block_id));
            }

            Terminator::Branch(branch) => {
                for target in [branch.truthy(), branch.falsy()] {
                    candidates
                        .entry(target)
                        .and_modify(|dominator| *dominator = None)
                        .or_insert(None);
                }
            }

            Terminator::Return(_) | Terminator::Unreachable => {}
        }
    }

    // Remove all candidates that have more than one dominator
    let candidates: Vec<_> = candidates
        .into_iter()
        .filter_map(|(target, source)| source.map(|source| (source, target)))
        .collect();

    if candidates.is_empty() {
        return;
    }

    // Inline the redundant blocks
    tracing::debug!("replacing {} redundant jumps", candidates.len());

    // FIXME: Need to properly map block params across changing blocks
    for (source, target) in candidates {
        let mut target = if let Some(target) = func.blocks_mut().remove(&target) {
            target
        } else {
            continue;
        };
        let source = if let Some(source) = func.blocks_mut().get_mut(&source) {
            source
        } else {
            continue;
        };

        let mut subst = BTreeMap::new();
        subst.extend(
            target
                .params()
                .iter()
                .zip(source.terminator().as_jump().unwrap().params())
                .map(|(&(from, _), &to)| (from, to)),
        );

        let mut visitor = MapExprIds::new(|expr_id: &mut ExprId| {
            if let Some(&subst) = subst.get(expr_id) {
                *expr_id = subst;
            }
        });
        for (_, expr) in target.body_mut() {
            expr.apply_mut(&mut visitor);
        }

        source.body_vec_mut().append(target.body_vec_mut());
        *source.terminator_mut() = take(target.terminator_mut());
    }
}
