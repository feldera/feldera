use crate::ir::{
    graph::{GraphContext, Subgraph},
    DataflowNode, Function, Graph, GraphExt, LayoutId, Node, NodeId, NodeIdGen, RowLayout,
    RowLayoutCache, Terminator,
};
use petgraph::prelude::DiGraphMap;
use serde::{Deserialize, Serialize};
use std::{
    cmp::max,
    collections::BTreeMap,
    mem::{take, ManuallyDrop},
};

// TODO: Encapsulate this into a method on `Graph`
// TODO: Collect the highest block id and expression id for each function to allow
//       modifying (read: optimizing) functions

#[derive(Debug, Deserialize, Serialize)]
pub struct SqlGraph {
    #[serde(flatten)]
    graph: Graph,
    layouts: BTreeMap<LayoutId, RowLayout>,
}

impl SqlGraph {
    // TODO: Make sure all referenced nodes/layouts/blocks/expressions exist (verify
    // the generated graph)
    pub fn rematerialize(self) -> Graph {
        let Self { mut graph, layouts } = self;

        let (layout_cache, layout_mappings) = Self::rematerialize_layouts(layouts);

        // Find the highest node id and create a generator to pick up where it left off
        let highest_node_id = Self::collect_highest_id(graph.graph());
        let node_id_generator = highest_node_id
            .map(NodeIdGen::after_id)
            .unwrap_or_else(NodeIdGen::new);

        let context = GraphContext::from_parts(layout_cache, node_id_generator);

        let (mut inputs, mut functions) = (Vec::with_capacity(8), Vec::with_capacity(8));
        Self::rebuild_subgraph(graph.graph_mut(), context, &mut inputs, &mut functions);

        // Remap the graph's layouts
        graph.remap_layouts(&layout_mappings);

        graph
    }

    /// The input we get contains duplicated layouts so we have to deduplicate them
    fn rematerialize_layouts(
        layouts: BTreeMap<LayoutId, RowLayout>,
    ) -> (RowLayoutCache, BTreeMap<LayoutId, LayoutId>) {
        let layout_cache = RowLayoutCache::with_capacity(layouts.len());
        let mut mappings = BTreeMap::new();

        for (old_layout_id, layout) in layouts {
            let layout_id = layout_cache.add(layout);
            mappings.insert(old_layout_id, layout_id);
        }

        (layout_cache, mappings)
    }

    // Collect the highest id assigned to any node within the graph
    // TODO: If recursion becomes an issue we can either rewrite this in a
    // non-recursive form or use stacker
    fn collect_highest_id(graph: &Subgraph) -> Option<NodeId> {
        let mut highest = graph.nodes().last_key_value().map(|(&node_id, _)| node_id);

        for node in graph.nodes().values() {
            if let Node::Subgraph(subgraph) = node {
                if let Some(subgraph_highest) = Self::collect_highest_id(subgraph.subgraph()) {
                    if let Some(highest) = highest.as_mut() {
                        *highest = max(*highest, subgraph_highest);
                    } else {
                        highest = Some(subgraph_highest);
                    }
                }
            }
        }

        highest
    }

    fn rebuild_subgraph(
        graph: &mut Subgraph,
        context: GraphContext,
        inputs: &mut Vec<NodeId>,
        functions_buf: &mut Vec<*mut Function>,
    ) {
        debug_assert!(inputs.is_empty());

        let total_nodes = graph.nodes().len();
        let mut edges = DiGraphMap::with_capacity(total_nodes, total_nodes * 2);

        debug_assert!(functions_buf.is_empty());
        let mut functions = unsafe { buffer_to_usable(take(functions_buf)) };

        for (&node_id, node) in graph.nodes_mut() {
            // Collect the node's inputs
            node.inputs(inputs);

            // Add the node and all incoming edges to the graph
            edges.add_node(node_id);
            for input in inputs.drain(..) {
                edges.add_edge(input, node_id, ());
            }

            // If the node is a subgraph, recursively rebuild it
            if let Node::Subgraph(subgraph) = node {
                let mut functions_buf = unsafe { usable_to_buffer(functions) };

                Self::rebuild_subgraph(
                    subgraph.subgraph_mut(),
                    context.clone(),
                    inputs,
                    &mut functions_buf,
                );

                functions = unsafe { buffer_to_usable(functions_buf) };
            }

            // Rebuild function control flow graphs
            node.functions_mut(&mut functions);
            for function in functions.drain(..) {
                let blocks = function.blocks().len();
                let mut cfg = DiGraphMap::with_capacity(blocks, blocks + (blocks >> 1));

                for (&block_id, block) in function.blocks() {
                    match block.terminator() {
                        Terminator::Jump(jump) => {
                            cfg.add_edge(block_id, jump.target(), ());
                        }

                        Terminator::Branch(branch) => {
                            cfg.add_edge(block_id, branch.truthy(), ());
                            cfg.add_edge(block_id, branch.falsy(), ());
                        }

                        Terminator::Return(_) => {
                            cfg.add_node(block_id);
                        }
                    }
                }

                function.set_cfg(cfg);
            }
        }

        debug_assert!(inputs.is_empty());
        debug_assert!(functions.is_empty());
        *functions_buf = unsafe { usable_to_buffer(functions) };

        // Set the context and edges of the current subgraph
        graph.set_context(context);
        graph.set_edges(edges);
    }
}

#[inline]
unsafe fn buffer_to_usable<'a>(buffer: Vec<*mut Function>) -> Vec<&'a mut Function> {
    debug_assert!(buffer.is_empty());

    let mut functions = ManuallyDrop::new(buffer);
    let capacity = functions.capacity();
    let ptr = functions.as_mut_ptr().cast::<&mut Function>();

    unsafe { Vec::from_raw_parts(ptr, 0, capacity) }
}

#[inline]
unsafe fn usable_to_buffer(buffer: Vec<&mut Function>) -> Vec<*mut Function> {
    debug_assert!(buffer.is_empty());

    let mut functions = ManuallyDrop::new(buffer);
    let capacity = functions.capacity();
    let ptr = functions.as_mut_ptr().cast::<*mut Function>();

    unsafe { Vec::from_raw_parts(ptr, 0, capacity) }
}

impl From<Graph> for SqlGraph {
    fn from(graph: Graph) -> Self {
        let mut layouts = BTreeMap::new();
        graph.layout_cache().with_layouts(|layout_id, layout| {
            layouts.insert(layout_id, layout.clone());
        });

        Self { graph, layouts }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ir::{
            literal::{NullableConstant, RowLiteral},
            nodes::Fold,
            ColumnType, Constant, Graph, GraphExt, Node, RowLayoutBuilder,
        },
        sqljson::SqlGraph,
    };

    #[test]
    fn join_json() {
        crate::utils::test_logger();

        let mut graph = Graph::new();

        // let key = graph.layout_cache().add(
        //     RowLayoutBuilder::new()
        //         .with_column(ColumnType::U32, true)
        //         .build(),
        // );
        // let value = graph.layout_cache().add(
        //     RowLayoutBuilder::new()
        //         .with_column(ColumnType::String, false)
        //         .build(),
        // );

        // let output = graph.layout_cache().add(
        //     RowLayoutBuilder::new()
        //         .with_column(ColumnType::U32, true)
        //         .with_column(ColumnType::String, false)
        //         .with_column(ColumnType::String, false)
        //         .build(),
        // );
        // let unit = graph.layout_cache().unit();

        // let source = graph.source_map(key, value);
        // let source_index = graph.index_with(source, key, value, {
        //     let mut builder = graph.function_builder();
        //     let key_input = builder.add_input(key);
        //     let value_input = builder.add_input(value);
        //     let key_output = builder.add_output(key);
        //     let value_output = builder.add_output(value);

        //     let key = builder.load(key_input, 0);
        //     builder.store(key_output, 0, key);

        //     let value = builder.load(value_input, 0);
        //     let value = builder.copy_val(value);
        //     builder.store(value_output, 0, value);

        //     builder.ret_unit();

        //     builder.build()
        // });

        // let joined = graph.join_core(
        //     source_index,
        //     source_index,
        //     {
        //         let mut builder = graph.function_builder();
        //         let key = builder.add_input(key);
        //         let lhs_val = builder.add_input(value);
        //         let rhs_val = builder.add_input(value);
        //         let output = builder.add_output(output);
        //         let _unit_output = builder.add_output(unit);

        //         let key = builder.load(key, 0);
        //         builder.store(output, 0, key);

        //         let lhs_val = builder.load(lhs_val, 0);
        //         let lhs_val = builder.copy_val(lhs_val);
        //         builder.store(output, 1, lhs_val);

        //         let rhs_val = builder.load(rhs_val, 0);
        //         let rhs_val = builder.copy_val(rhs_val);
        //         builder.store(output, 2, rhs_val);

        //         builder.ret_unit();

        //         builder.build()
        //     },
        //     output,
        //     unit,
        //     StreamKind::Set,
        // );

        let null_i32 = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::I32, true)
                .build(),
        );
        let i32 = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::I32, false)
                .build(),
        );

        let source = graph.source(null_i32);
        let constant = graph.add_node(Node::Fold(Fold::new(
            source,
            RowLiteral::new(vec![NullableConstant::NonNull(Constant::U32(0))]),
            // Step
            {
                let mut builder = graph.function_builder();
                let acc = builder.add_input(i32);
                let input = builder.add_input(null_i32);
                let weight = builder.add_input(i32);
                let output = builder.add_output(i32);

                let acc = builder.load(acc, 0);
                let input_val = builder.load(input, 0);
                let input_null = builder.is_null(input, 0);
                let weight = builder.load(weight, 0);

                let zero = builder.constant(Constant::I32(0));
                let input_val = builder.select(input_null, zero, input_val);
                let input_mul_weight = builder.mul(input_val, weight);

                let acc_plus_input = builder.add(acc, input_mul_weight);
                builder.store(output, 0, acc_plus_input);

                builder.ret_unit();
                builder.build()
            },
            // Finish
            {
                let mut builder = graph.function_builder();
                let input = builder.add_input(i32);
                let output = builder.add_output(i32);

                builder.copy_row_to(input, output);
                builder.ret_unit();
                builder.build()
            },
            null_i32,
            i32,
            i32,
        )));

        graph.sink(constant);

        let graph = SqlGraph::from(graph);
        let json_graph = serde_json::to_string_pretty(&graph).unwrap();
        println!("{json_graph}");
    }
}
