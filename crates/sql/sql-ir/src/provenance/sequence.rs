//! Compose relational origins in execution order when adjacent operators fuse.
use super::{Origin, OriginId, OriginKind, Provenance, RelationId, SourceRange};
use crate::{Error, NodeId};
use serde_json::{json, Value};
use std::collections::BTreeSet;

impl Provenance<'_> {
    fn leaf(&self, id: OriginId) -> Result<(bool, RelationId), Error> {
        match self.origins[&id].kind {
            OriginKind::Final(rel) => Ok((true, rel)),
            OriginKind::Intermediate(rel) => Ok((false, rel)),
            _ => Err(Error::new("expected a relational leaf in a sequence")),
        }
    }

    /// Attach the composed origin to a fresh operator. The wire writer relocates
    /// the retained definitions to their first use after the original operators disappear.
    pub fn fuse_origins(
        &mut self,
        new: NodeId,
        earlier: NodeId,
        later: NodeId,
    ) -> Result<Option<Value>, Error> {
        let (a, b) = match (self.operator_origin(earlier), self.operator_origin(later)) {
            (Some(a), Some(b)) => (a, b),
            (None, None) => return Ok(None),
            _ => {
                return Err(Error::new(
                    "only one fused operator has relational provenance",
                ))
            }
        };
        let (id, wire) = self.after(a, b)?;
        self.operators.insert(new, id);
        Ok(Some(wire))
    }

    fn after(&mut self, earlier: OriginId, later: OriginId) -> Result<(OriginId, Value), Error> {
        use OriginKind::*;
        let reference = |id: OriginId| (id, json!({"ref":id.0}));
        let a = &self.origins[&earlier].kind;
        let b = &self.origins[&later].kind;
        if matches!(b, Intersection(_)) {
            return Err(Error::new("cannot append an intersection origin"));
        }
        if matches!(b, Empty) {
            return Ok(reference(earlier));
        }
        if matches!(a, Empty | PositionOnly { empty: true }) {
            return Ok(reference(later));
        }
        if matches!(b, PositionOnly { .. }) {
            return Ok(reference(earlier));
        }
        if matches!(a, Intersection(_)) {
            return Ok(reference(later));
        }
        let parts = |id| match &self.origins[&id].kind {
            Sequence(parts) => parts.clone(),
            _ => vec![id],
        };
        let mut merged = parts(earlier);
        let tail = parts(later);
        if matches!((a, b), (Sequence(_), Sequence(_))) {
            let mut seen = merged
                .iter()
                .map(|&id| self.leaf(id))
                .collect::<Result<BTreeSet<_>, _>>()?;
            for id in tail {
                if seen.insert(self.leaf(id)?) {
                    merged.push(id);
                }
            }
        } else {
            let tail_relations = tail
                .iter()
                .map(|&id| self.leaf(id).map(|(_, rel)| rel))
                .collect::<Result<BTreeSet<_>, _>>()?;
            let mut prefix = Vec::with_capacity(merged.len() + tail.len());
            for id in merged {
                if !tail_relations.contains(&self.leaf(id)?.1) {
                    prefix.push(id);
                }
            }
            if prefix.is_empty() && (!matches!(b, Sequence(_)) || matches!(a, Intermediate(_))) {
                return Ok(reference(later));
            }
            prefix.extend(tail);
            merged = prefix;
        }
        merged.dedup();
        let number = self
            .last_origin_number()
            .checked_add(1)
            .filter(|&n| n <= i32::MAX as u32)
            .ok_or_else(|| Error::new("relational origin identity exhausted"))?;
        let id = OriginId(number);
        let position = SourceRange::default();
        let children: Vec<_> = merged.iter().map(|id| json!({"ref":id.0})).collect();
        self.origins.insert(
            id,
            Origin {
                kind: Sequence(merged),
                position,
                positions: Vec::new(),
            },
        );
        Ok((
            id,
            json!({"id":number,"kind":"RelSequence","children":children,"position":position,"positions":[]}),
        ))
    }
}
