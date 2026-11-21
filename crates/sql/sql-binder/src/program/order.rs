//! S1 assigns relation ids while traversing all view roots together. A view
//! body is built in isolation, so its allocation order can differ once a second
//! root shares the queue. Table-scan construction order has a separate contract.

use crate::{Failure, unsupported};
use feldera_sql_rel_model::{input_uses::InputOwner, program::Program, statements::Statement};
use std::collections::VecDeque;

pub(super) fn number_relations(program: &mut Program) -> Result<(), Failure> {
    let relations = &program.producers.relations;
    let mut order = Vec::with_capacity(relations.len());
    let mut number = vec![None; relations.len()];
    let mut visited = vec![false; relations.len()];
    let mut queue: VecDeque<_> = program.producers.roots.iter().copied().collect();

    while let Some(id) = queue.pop_front() {
        let i = id as usize;
        if std::mem::replace(&mut visited[i], true) {
            continue;
        }
        // First encounter assigns an id to the operator and each direct input
        // before any queued sibling root is visited.
        for child in std::iter::once(&id).chain(&relations[i].inputs) {
            number[*child as usize].get_or_insert_with(|| {
                order.push(*child as usize);
                (order.len() - 1) as u32
            });
        }
        queue.extend(&relations[i].inputs);
    }
    if order.len() != relations.len() {
        return Err(unsupported(
            "bound program contains an unreachable relation",
        ));
    }
    if order.iter().copied().eq(0..order.len()) {
        return Ok(());
    }
    let remap = |value: &mut u32| {
        *value = number[*value as usize].expect("every reachable relation has an id");
    };
    reorder(&mut program.producers.relations, &order);
    reorder(&mut program.properties.relations, &order);
    for producer in &mut program.producers.relations {
        producer.inputs.iter_mut().for_each(remap);
    }
    program.producers.roots.iter_mut().for_each(remap);
    program.table_scan_order.iter_mut().for_each(remap);
    for scan in &mut program.catalog.scans {
        remap(&mut scan.relation);
    }
    for environment in &mut program.table.environments {
        match &mut environment.owner {
            InputOwner::Relations { inputs } => inputs.iter_mut().for_each(remap),
            InputOwner::Window { relation, inputs } => {
                remap(relation);
                inputs.iter_mut().for_each(remap);
            }
            InputOwner::Constant {} | InputOwner::Function { .. } => {}
        }
    }
    for site in &mut program.expressions.relation_uses {
        remap(&mut site.relation);
    }
    program
        .expressions
        .relation_uses
        .sort_by_key(|site| site.relation);
    for statement in &mut program.statements {
        match statement {
            Statement::CreateView { root, .. } => remap(&mut root.relation),
            Statement::ModifyTable {
                relation: Some(id), ..
            } => remap(id),
            _ => {}
        }
    }
    Ok(())
}

fn reorder<T>(items: &mut Vec<T>, order: &[usize]) {
    let mut slots: Vec<_> = std::mem::take(items).into_iter().map(Some).collect();
    items.extend(
        order
            .iter()
            .map(|i| slots[*i].take().expect("permutation of relation ids")),
    );
}
