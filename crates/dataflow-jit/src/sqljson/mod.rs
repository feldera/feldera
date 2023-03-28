use crate::ir::{
    graph::{GraphContext, Subgraph},
    nodes::{DataflowNode, Node},
    Function, Graph, GraphExt, LayoutId, NodeId, NodeIdGen, RowLayout, RowLayoutCache, Terminator,
};
use petgraph::prelude::DiGraphMap;
use serde::{Deserialize, Serialize};
use std::{
    cmp::max,
    collections::{BTreeMap, BTreeSet},
    mem::{take, ManuallyDrop},
};

// TODO: Encapsulate this into a method on `Graph`
// TODO: Collect the highest block id and expression id for each function to
// allow modifying (read: optimizing) functions

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

        // Collect all layouts used within the dataflow graph
        let mut used_layouts = BTreeSet::new();
        graph.map_layouts(|layout_id| {
            used_layouts.insert(layout_id);
        });

        // Deduplicate all layouts and remove any unused ones
        let (layout_cache, layout_mappings) = Self::rematerialize_layouts(layouts, used_layouts);

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

    /// The input we get contains duplicated layouts so we have to deduplicate
    /// them
    fn rematerialize_layouts(
        layouts: BTreeMap<LayoutId, RowLayout>,
        used_layouts: BTreeSet<LayoutId>,
    ) -> (RowLayoutCache, BTreeMap<LayoutId, LayoutId>) {
        let layout_cache = RowLayoutCache::with_capacity(layouts.len());
        let mut mappings = BTreeMap::new();

        for (old_layout_id, layout) in layouts {
            if used_layouts.contains(&old_layout_id) {
                let layout_id = layout_cache.add(layout);
                mappings.insert(old_layout_id, layout_id);
            }
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
        dataflow::CompiledDataflow,
        ir::{
            exprs::{ArgType, Call},
            nodes::{FilterMap, FlatMap, Node, StreamLayout},
            ColumnType, Constant, Graph, GraphExt, RowLayoutBuilder,
        },
        row::{Row, UninitRow},
        sqljson::SqlGraph,
    };
    use dbsp::{
        trace::{Batch, Batcher},
        OrdZSet, Runtime,
    };

    #[test]
    fn flat_map_set_set() {
        crate::utils::test_logger();

        let mut graph = Graph::new();

        let i32 = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::I32, false)
                .build(),
        );
        let row_vec_layout = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::Ptr, false)
                .with_column(ColumnType::Ptr, false)
                .build(),
        );

        let source = graph.source(i32);

        let flat_map = graph.add_node(Node::FlatMap(FlatMap::new(
            source,
            {
                let mut builder = graph.function_builder();
                let value = builder.add_input(i32);
                let vec = builder.add_input(row_vec_layout);

                for _ in 0..4 {
                    builder.add_expr(Call::new(
                        "dbsp.row.vec.push".into(),
                        vec![vec, value],
                        vec![ArgType::Row(row_vec_layout), ArgType::Row(i32)],
                        ColumnType::Unit,
                    ));
                }

                builder.ret_unit();
                builder.build()
            },
            StreamLayout::Set(i32),
        )));

        let sink = graph.sink(flat_map);

        let graph = SqlGraph::from(graph);
        let json_graph = serde_json::to_string_pretty(&graph).unwrap();
        println!("{json_graph}");

        let mut graph = serde_json::from_str::<SqlGraph>(&json_graph)
            .unwrap()
            .rematerialize();
        graph.optimize();

        let (dataflow, jit_handle, layout_cache) =
            CompiledDataflow::new(&graph, Default::default());
        let i32_offset = layout_cache.layout_of(i32).offset_of(0) as usize;
        let i32_vtable = unsafe { &*jit_handle.vtables()[&i32] };

        {
            let (mut runtime, (mut inputs, outputs)) =
                Runtime::init_circuit(1, move |circuit| dataflow.construct(circuit)).unwrap();

            let mut values = Vec::with_capacity(10 * 10);
            for k in 0..10 {
                for v in 0..10 {
                    let mut row = UninitRow::new(i32_vtable);
                    unsafe {
                        *row.as_mut_ptr().add(i32_offset).cast::<i32>() = k * v;
                    }

                    values.push((unsafe { row.assume_init() }, 1));
                }
            }
            inputs
                .get_mut(&source)
                .unwrap()
                .as_set_mut()
                .unwrap()
                .append(&mut values);

            runtime.step().unwrap();

            let output = outputs[&sink].as_set().unwrap().consolidate();

            let mut batch = Vec::with_capacity(10 * 10 * 4);
            for k in 0..10 {
                for v in 0..10 {
                    let row = unsafe {
                        let mut row = UninitRow::new(i32_vtable);
                        *row.as_mut_ptr().add(i32_offset).cast::<i32>() = k * v;
                        row.assume_init()
                    };

                    for _ in 0..4 {
                        batch.push((row.clone(), 1));
                    }
                }
            }

            let mut expected = <OrdZSet<Row, i32> as Batch>::Batcher::new_batcher(());
            expected.push_batch(&mut batch);
            let expected = expected.seal();
            assert_eq!(output, expected);

            runtime.kill().unwrap();
        }

        unsafe { jit_handle.free_memory() };
    }

    #[test]
    fn filter_map() {
        crate::utils::test_logger();

        let mut graph = Graph::new();

        let i32 = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::I32, false)
                .build(),
        );

        let source = graph.source(i32);

        let filter_map = graph.add_node(Node::FilterMap(FilterMap::new(
            source,
            {
                let mut builder = graph.function_builder().with_return_type(ColumnType::Bool);
                let input = builder.add_input(i32);
                let output = builder.add_output(i32);

                let value = builder.load(input, 0);
                builder.store(output, 0, input);
                let one_hundred = builder.constant(Constant::I32(100));
                let should_keep = builder.lt(value, one_hundred);
                builder.ret(should_keep);

                builder.build()
            },
            i32,
        )));

        graph.sink(filter_map);

        let graph = SqlGraph::from(graph);
        let json_graph = serde_json::to_string_pretty(&graph).unwrap();
        println!("{json_graph}");
    }
}
