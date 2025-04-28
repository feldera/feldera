use std::collections::HashMap;

use crate::{Lir, LirNode, LirNodeId, Mir};

#[derive(Debug)]
enum Metric {
    Value(f64),
    Count(u64),
}

pub type Metrics = HashMap<String, Metric>;

#[derive(Default, Debug)]
pub struct Measurements {
    pub time: usize,
    pub measurements: HashMap<LirNodeId, Metrics>,
}

struct Profile {
    lir: Lir,
    mir: Mir,
    metrics: Vec<Measurements>,
}

impl Profile {
    fn new(lir: Lir, mir: Mir, metrics: Vec<Measurements>) -> Self {
        Self { lir, mir, metrics }
    }

    fn analyze_profile(&self) {
        eprintln!("{:?}", self.metrics);
    }
}

#[cfg(test)]
mod test {
    use crate::profile::*;
    use crate::*;
    use feldera_types::program_schema::SourcePosition;

    #[test]
    fn simple_profile() {
        let lir = Lir::new(
            vec![
                LirNode::new(
                    LirNodeId::new("lir_node_a"),
                    "map".to_string(),
                    vec!["mir_node_a".to_string()],
                ),
                LirNode::new(
                    LirNodeId::new("lir_node_b"),
                    "join".to_string(),
                    vec!["mir_node_b".to_string()],
                ),
            ],
            vec![LirEdge::new(
                Some(LirStreamId::new(0)),
                LirNodeId::new("lir_node_a"),
                LirNodeId::new("lir_node_b"),
            )],
        );

        let mut mir = Mir::new();
        let mut mir_node_a = MirNode::default();
        mir_node_a.persistent_id = Some("pid_a".to_string());
        mir_node_a.positions = vec![
            SourcePosition::new(0, 1, 2, 3),
            SourcePosition::new(4, 5, 6, 7),
        ];
        mir.insert("mir_node_a".to_string(), mir_node_a);

        let mut mir_node_b = MirNode::default();
        mir_node_b.persistent_id = Some("pid_b".to_string());
        mir_node_b.positions = vec![
            SourcePosition::new(8, 9, 10, 11),
            SourcePosition::new(12, 13, 14, 15),
        ];
        mir.insert("mir_node_b".to_string(), mir_node_b);

        let mut metrics = vec![Measurements::default(), Measurements::default()];
        metrics[0]
            .measurements
            .insert(LirNodeId::new("lir_node_a"), Metrics::default());
        metrics[0]
            .measurements
            .insert(LirNodeId::new("lir_node_b"), Metrics::default());

        metrics[0]
            .measurements
            .entry(LirNodeId::new("lir_node_a"))
            .or_insert_with(|| {
                let mut e = Metrics::default();
                e.insert("cpu".to_string(), Metric::Count(2));
                e.insert("rss".to_string(), Metric::Value(1.0));
                e
            });
        metrics[0]
            .measurements
            .entry(LirNodeId::new("lir_node_b"))
            .or_insert_with(|| {
                let mut e = Metrics::default();
                e.insert("cpu".to_string(), Metric::Count(1));
                e.insert("rss".to_string(), Metric::Value(2.0));
                e
            });

        metrics[1]
            .measurements
            .entry(LirNodeId::new("lir_node_a"))
            .or_insert_with(|| {
                let mut e = Metrics::default();
                e.insert("cpu".to_string(), Metric::Count(4));
                e.insert("rss".to_string(), Metric::Value(2.0));
                e
            });
        metrics[1]
            .measurements
            .entry(LirNodeId::new("lir_node_b"))
            .or_insert_with(|| {
                let mut e = Metrics::default();
                e.insert("cpu".to_string(), Metric::Count(2));
                e.insert("rss".to_string(), Metric::Value(4.0));
                e
            });

        let profile = Profile::new(lir, mir, metrics);

        profile.analyze_profile();
    }
}
