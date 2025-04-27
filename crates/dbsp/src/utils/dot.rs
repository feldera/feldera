//! Convert circuits to dot format for debugging.

use crate::{
    circuit::{
        circuit_builder::{CircuitBase, Edge, Node, StreamId},
        WithClock,
    },
    ChildCircuit,
};

pub struct DotNodeAttributes {
    label: Option<String>,
    fillcolor: Option<u32>,
}

impl Default for DotNodeAttributes {
    fn default() -> Self {
        Self::new()
    }
}

impl DotNodeAttributes {
    pub fn new() -> Self {
        Self {
            label: None,
            fillcolor: None,
        }
    }

    pub fn with_label(mut self, label: &str) -> Self {
        self.label = Some(label.to_string());
        self
    }

    pub fn with_color(mut self, color: Option<u32>) -> Self {
        self.fillcolor = color;
        self
    }

    pub fn to_string(&self) -> String {
        let mut attributes = Vec::new();
        if let Some(ref label) = self.label {
            attributes.push(format!("label=\"{}\"", label));
        }
        if let Some(color) = self.fillcolor {
            attributes.push(format!("fillcolor=\"#{:06x}\"", color));
            attributes.push("style=filled".to_string());
        }
        attributes.join(", ")
    }
}

pub struct DotEdgeAttributes {
    stream_id: Option<StreamId>,
    style: Option<String>,
    label: Option<String>,
}

impl DotEdgeAttributes {
    pub fn new(stream_id: Option<StreamId>) -> Self {
        Self {
            stream_id,
            style: None,
            label: None,
        }
    }

    pub fn with_label(mut self, label: Option<String>) -> Self {
        self.label = label;
        self
    }

    pub fn with_style(mut self, style: Option<String>) -> Self {
        self.style = style;
        self
    }

    pub fn to_string(&self) -> String {
        let mut attributes = Vec::new();
        if let Some(style) = &self.style {
            attributes.push(format!("style=\"{}\"", style));
        }
        let stream_id = if let Some(stream_id) = &self.stream_id {
            format!("{stream_id}\n")
        } else {
            String::new()
        };
        if let Some(label) = &self.label {
            attributes.push(format!("label=\"{stream_id}{}\"", label));
        }
        attributes.join(", ")
    }
}

impl<P> ChildCircuit<P>
where
    P: WithClock + Clone + 'static,
{
    /// Returns a dot representation of the circuit.
    pub fn to_dot<NF, EF>(&self, node_function: NF, edge_function: EF) -> String
    where
        NF: Fn(&dyn Node) -> Option<DotNodeAttributes>,
        EF: Fn(&Edge) -> Option<DotEdgeAttributes>,
    {
        let mut nodes = Vec::new();

        let _ = self.map_local_nodes(&mut |node| {
            if let Some(attributes) = node_function(node) {
                let node_id = node.local_id();
                nodes.push(format!("{} [{}];", node_id, attributes.to_string()));
            }
            Ok(())
        });

        let mut edges = Vec::new();

        for edge in self.edges().iter() {
            if let Some(attributes) = edge_function(edge) {
                edges.push(format!(
                    "{} -> {} [{}];",
                    edge.from,
                    edge.to,
                    attributes.to_string()
                ));
            }
        }

        format!(
            r#"digraph{{
    node [shape=box];
    {}
    {}
}}"#,
            nodes.join("\n"),
            edges.join("\n")
        )
    }

    pub fn to_dot_file(
        &self,
        node_function: impl Fn(&dyn Node) -> Option<DotNodeAttributes>,
        edge_function: impl Fn(&Edge) -> Option<DotEdgeAttributes>,
        filename: &str,
    ) {
        let dot = self.to_dot(node_function, edge_function);
        std::fs::write(filename, dot).expect("Unable to write file");
    }
}
