//! Choose where pending lazy bindings become visible during the bottom-up CSE rewrite.
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct PendingBinding {
    owner: u64,
    dependencies: Vec<u64>,
}

#[derive(Deserialize)]
pub struct BindingScope {
    /// Block lets in source order, a single let expression, or closure parameters.
    declarations: Vec<u64>,
    closure: Option<u64>,
    outermost: bool,
}

impl BindingScope {
    /// A block inserts a binding after its last local dependency. A closure
    /// inserts all captured bindings at its entrance, in creation order.
    /// An unplaced binding travels out to the next enclosing scope.
    pub fn placements(&self, bindings: &[PendingBinding]) -> Vec<Option<usize>> {
        let positions: HashMap<_, _> = self.declarations.iter().copied().zip(0..).collect();
        bindings
            .iter()
            .map(|binding| {
                let mut local = binding
                    .dependencies
                    .iter()
                    .filter_map(|id| positions.get(id));
                match self.closure {
                    Some(owner) => {
                        (self.outermost || binding.owner == owner || local.next().is_some())
                            .then_some(0)
                    }
                    None => local.max().copied(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inner_scopes_consume_only_the_bindings_they_own() {
        let bindings = [
            PendingBinding {
                owner: 8,
                dependencies: vec![52, 4],
            },
            PendingBinding {
                owner: 9,
                dependencies: vec![],
            },
            PendingBinding {
                owner: 8,
                dependencies: vec![4],
            },
        ];
        let mut scope = BindingScope {
            declarations: vec![4, 52],
            closure: None,
            outermost: false,
        };
        assert_eq!(scope.placements(&bindings), [Some(1), None, Some(0)]);
        scope.closure = Some(9);
        scope.declarations = vec![52];
        assert_eq!(scope.placements(&bindings), [Some(0), Some(0), None]);
        scope.outermost = true;
        assert_eq!(scope.placements(&bindings), [Some(0); 3]);
    }
}
