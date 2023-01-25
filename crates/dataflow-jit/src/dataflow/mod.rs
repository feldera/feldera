mod nodes;

use cranelift_jit::JITModule;
pub use nodes::{DataflowNode, Filter, IndexWith, Map, Neg, Sink, Source, Sum};

use crate::{
    codegen::{Codegen, CodegenConfig, NativeLayoutCache, VTable},
    dataflow::nodes::{FilterIndex, MapIndex},
    ir::{DataflowNode as _, Graph, LayoutId, Node, NodeId, Stream as NodeStream},
    row::Row,
};
use dbsp::{
    operator::FilterMap,
    trace::{BatchReader, Cursor},
    Circuit, CollectionHandle, OrdIndexedZSet, OrdZSet, OutputHandle, Stream,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::transmute,
};

type Inputs = BTreeMap<NodeId, CollectionHandle<Row, i32>>;
type Outputs = BTreeMap<NodeId, RowOutput>;

// TODO: Change the weight to a `Row`? Toggle between `i32` and `i64`?
#[derive(Clone)]
pub enum RowStream<P> {
    Set(Stream<P, OrdZSet<Row, i32>>),
    Map(Stream<P, OrdIndexedZSet<Row, Row, i32>>),
}

#[derive(Clone)]
pub enum RowOutput {
    Set(OutputHandle<OrdZSet<Row, i32>>),
    Map(OutputHandle<OrdIndexedZSet<Row, Row, i32>>),
}

pub struct JitHandle {
    jit: JITModule,
    vtables: BTreeMap<LayoutId, *mut VTable>,
}

impl JitHandle {
    pub unsafe fn free_memory(mut self) {
        for &vtable in self.vtables.values() {
            drop(Box::from_raw(vtable));
        }
        self.vtables.clear();
        self.jit.free_memory();
    }
}

#[derive(Clone)]
pub struct CompiledDataflow {
    nodes: BTreeMap<NodeId, DataflowNode>,
}

impl CompiledDataflow {
    pub fn new(graph: &Graph, config: CodegenConfig) -> (Self, JitHandle, NativeLayoutCache) {
        let mut visited = BTreeSet::new();
        let mut node_kinds = BTreeMap::new();
        let mut node_streams: BTreeMap<NodeId, Option<_>> = BTreeMap::new();

        // Validate & optimize all nodes
        let mut queue: Vec<_> = graph.nodes().keys().copied().collect();
        let (mut inputs, mut input_nodes) = (Vec::with_capacity(16), Vec::with_capacity(16));
        while let Some(node_id) = queue.pop() {
            if visited.contains(&node_id) {
                continue;
            }

            let node = &graph.nodes()[&node_id];
            node.inputs(&mut input_nodes);

            if !input_nodes.iter().all(|node| visited.contains(node)) {
                queue.push(node_id);
                queue.append(&mut input_nodes);
                continue;
            }

            inputs.extend(input_nodes.iter().map(|input| node_streams[input].unwrap()));

            visited.insert(node_id);
            node_kinds.insert(node_id, node.output_kind(&inputs));
            node_streams.insert(node_id, node.output_stream(&inputs));

            inputs.extend(input_nodes.iter().map(|input| node_streams[input].unwrap()));

            // node.validate(&inputs, graph.layout_cache());
            // node.optimize(&inputs, graph.layout_cache());
            // node.validate(&inputs, graph.layout_cache());

            inputs.clear();
            input_nodes.clear();
        }
        drop((queue, inputs, input_nodes));

        // Run codegen over all nodes
        let mut codegen = Codegen::new(graph.layout_cache().clone(), config);
        // TODO: SmallVec
        let mut node_functions = BTreeMap::new();
        let mut vtables = BTreeMap::new();

        for (&node_id, node) in graph.nodes() {
            match node {
                Node::Map(map) => {
                    let map_fn = codegen.codegen_func(map.map_fn());
                    node_functions.insert(node_id, vec![map_fn]);

                    vtables
                        .entry(map.layout())
                        .or_insert_with(|| codegen.vtable_for(map.layout()));
                }

                Node::Fold(fold) => {
                    let step_fn = codegen.codegen_func(fold.step_fn());
                    let finish_fn = codegen.codegen_func(fold.finish_fn());
                    node_functions.insert(node_id, vec![step_fn, finish_fn]);
                }

                Node::Filter(filter) => {
                    let filter_fn = codegen.codegen_func(filter.filter_fn());
                    node_functions.insert(node_id, vec![filter_fn]);
                }

                Node::IndexWith(index_with) => {
                    let index_fn = codegen.codegen_func(index_with.index_fn());
                    node_functions.insert(node_id, vec![index_fn]);

                    vtables
                        .entry(index_with.key_layout())
                        .or_insert_with(|| codegen.vtable_for(index_with.key_layout()));
                    vtables
                        .entry(index_with.value_layout())
                        .or_insert_with(|| codegen.vtable_for(index_with.value_layout()));
                }

                Node::Source(source) => {
                    vtables
                        .entry(source.layout())
                        .or_insert_with(|| codegen.vtable_for(source.layout()));
                }

                Node::Neg(_) | Node::Sum(_) | Node::Differentiate(_) | Node::Sink(_) => {}
            }
        }

        let (jit, native_layout_cache) = codegen.finalize_definitions();
        let vtables: BTreeMap<_, _> = vtables
            .into_iter()
            .map(|(layout, vtable)| (layout, Box::into_raw(Box::new(vtable.marshalled(&jit)))))
            .collect();

        let mut nodes = BTreeMap::new();
        for (node_id, node) in graph.nodes() {
            let output = node_streams[node_id];

            match node {
                Node::Map(map) => {
                    let input = map.input();
                    let map_fn = jit.get_finalized_function(node_functions[node_id][0]);
                    let output_vtable = unsafe { &*vtables[&map.layout()] };

                    let node = match output.unwrap() {
                        NodeStream::Set(_) => DataflowNode::Map(Map {
                            input,
                            map_fn: unsafe {
                                transmute::<_, extern "C" fn(*const u8, *mut u8)>(map_fn)
                            },
                            output_vtable,
                        }),

                        NodeStream::Map(..) => DataflowNode::MapIndex(MapIndex {
                            input,
                            map_fn: unsafe {
                                transmute::<_, extern "C" fn(*const u8, *const u8, *mut u8)>(map_fn)
                            },
                            output_vtable,
                        }),
                    };

                    nodes.insert(*node_id, node);
                }

                Node::Neg(neg) => {
                    nodes.insert(*node_id, DataflowNode::Neg(Neg { input: neg.input() }));
                }

                Node::Sum(sum) => {
                    nodes.insert(
                        *node_id,
                        DataflowNode::Sum(Sum {
                            inputs: sum.inputs().to_vec(),
                        }),
                    );
                }

                Node::Fold(_) => todo!(),

                Node::Sink(sink) => {
                    nodes.insert(
                        *node_id,
                        DataflowNode::Sink(Sink {
                            input: sink.input(),
                        }),
                    );
                }

                Node::Source(source) => {
                    let output_vtable = unsafe { &*vtables[&source.layout()] };
                    nodes.insert(*node_id, DataflowNode::Source(Source { output_vtable }));
                }

                Node::Filter(filter) => {
                    let filter_fn = jit.get_finalized_function(node_functions[node_id][0]);
                    let input = filter.input();

                    let node = match output.unwrap() {
                        NodeStream::Set(_) => DataflowNode::Filter(Filter {
                            input,
                            filter_fn: unsafe {
                                transmute::<_, unsafe extern "C" fn(*const u8) -> bool>(filter_fn)
                            },
                        }),

                        NodeStream::Map(..) => DataflowNode::FilterIndex(FilterIndex {
                            input,
                            filter_fn: unsafe {
                                transmute::<_, unsafe extern "C" fn(*const u8, *const u8) -> bool>(
                                    filter_fn,
                                )
                            },
                        }),
                    };

                    nodes.insert(*node_id, node);
                }

                Node::IndexWith(index) => {
                    let input = index.input();
                    let index_fn = jit.get_finalized_function(node_functions[node_id][0]);
                    let key_vtable = unsafe { &*vtables[&index.key_layout()] };
                    let value_vtable = unsafe { &*vtables[&index.value_layout()] };

                    let node = match node_streams[&input].unwrap() {
                        NodeStream::Set(_) => DataflowNode::IndexWith(IndexWith {
                            input,
                            index_fn: unsafe {
                                transmute::<_, unsafe extern "C" fn(*const u8, *mut u8, *mut u8)>(
                                    index_fn,
                                )
                            },
                            key_vtable,
                            value_vtable,
                        }),

                        NodeStream::Map(..) => todo!(),
                    };

                    nodes.insert(*node_id, node);
                }

                Node::Differentiate(_) => todo!(),
            }
        }

        (
            Self { nodes },
            JitHandle { jit, vtables },
            native_layout_cache,
        )
    }

    pub fn construct(&self, circuit: &mut Circuit<()>) -> (Inputs, Outputs) {
        let mut streams = BTreeMap::<NodeId, RowStream<Circuit<()>>>::new();

        let mut inputs = BTreeMap::new();
        let mut outputs = BTreeMap::new();

        // FIXME: Instead of this we should make `nodes` be an ordered
        // `Vec<(NodeId, DataflowNode)>` where all of a node's predecessors
        // occur before it
        let mut queue: Vec<_> = self.nodes.keys().copied().collect();
        while let Some(node_id) = queue.pop() {
            if streams.contains_key(&node_id) {
                continue;
            }

            let node = &self.nodes[&node_id];
            match node {
                DataflowNode::Map(map) => {
                    if let Some(input) = streams.get(&map.input) {
                        let mapped = match input {
                            RowStream::Set(input) => {
                                let (map_fn, vtable) = (map.map_fn, map.output_vtable);
                                RowStream::Set(input.map(move |input| {
                                    let mut output = unsafe { Row::uninit(vtable) };
                                    unsafe { map_fn(input.as_ptr(), output.as_mut_ptr()) };
                                    // println!("Map: {input:?} -> {output:?}");
                                    output
                                }))
                            }

                            RowStream::Map(_) => todo!(),
                        };

                        streams.insert(node_id, mapped);
                    } else {
                        queue.extend([node_id, map.input]);
                    }
                }

                DataflowNode::MapIndex(map) => {
                    if let Some(input) = streams.get(&map.input) {
                        let mapped = match input {
                            RowStream::Map(input) => {
                                let (map_fn, vtable) = (map.map_fn, map.output_vtable);
                                RowStream::Set(input.map(move |(key, value)| {
                                    let mut output = unsafe { Row::uninit(vtable) };
                                    unsafe {
                                        map_fn(key.as_ptr(), value.as_ptr(), output.as_mut_ptr());
                                    }
                                    output
                                }))
                            }

                            RowStream::Set(_) => todo!(),
                        };

                        streams.insert(node_id, mapped);
                    } else {
                        queue.extend([node_id, map.input]);
                    }
                }

                DataflowNode::Filter(filter) => {
                    if let Some(input) = streams.get(&filter.input) {
                        let filter_fn = filter.filter_fn;
                        let filtered = match input {
                            RowStream::Set(input) => RowStream::Set(
                                input.filter(move |input| unsafe { filter_fn(input.as_ptr()) }),
                            ),
                            RowStream::Map(_) => todo!(),
                        };

                        streams.insert(node_id, filtered);
                    } else {
                        queue.extend([node_id, filter.input]);
                    }
                }

                DataflowNode::FilterIndex(filter) => {
                    if let Some(input) = streams.get(&filter.input) {
                        let filter_fn = filter.filter_fn;
                        let filtered = match input {
                            RowStream::Map(input) => {
                                RowStream::Map(input.filter(move |(key, value)| unsafe {
                                    filter_fn(key.as_ptr(), value.as_ptr())
                                }))
                            }
                            RowStream::Set(_) => todo!(),
                        };

                        streams.insert(node_id, filtered);
                    } else {
                        queue.extend([node_id, filter.input]);
                    }
                }

                DataflowNode::IndexWith(index_with) => {
                    if let Some(input) = streams.get(&index_with.input) {
                        let (index_fn, key_vtable, value_vtable) = (
                            index_with.index_fn,
                            index_with.key_vtable,
                            index_with.value_vtable,
                        );

                        let indexed = match input {
                            RowStream::Set(input) => input.index_with(move |input| {
                                let (mut key_output, mut value_output) =
                                    unsafe { (Row::uninit(key_vtable), Row::uninit(value_vtable)) };

                                unsafe {
                                    index_fn(
                                        input.as_ptr(),
                                        key_output.as_mut_ptr(),
                                        value_output.as_mut_ptr(),
                                    );
                                }

                                // println!(
                                //     "IndexWith: {input:?} -> {key_output:?}: {value_output:?}",
                                // );
                                (key_output, value_output)
                            }),

                            // FIXME: `.index_with()` requires that `Key` is a `()`
                            RowStream::Map(_) => todo!(),
                        };

                        streams.insert(node_id, RowStream::Map(indexed));
                    } else {
                        queue.extend([node_id, index_with.input]);
                    }
                }

                DataflowNode::Sum(sum) => {
                    if !sum.inputs.iter().all(|input| streams.contains_key(input)) {
                        queue.push(node_id);
                        queue.extend(sum.inputs.iter().copied());
                        continue;
                    }

                    let sum = match &streams[&sum.inputs[0]] {
                        RowStream::Set(first) => {
                            RowStream::Set(first.sum(sum.inputs[1..].iter().map(|input| {
                                if let RowStream::Set(input) = &streams[input] {
                                    input
                                } else {
                                    unreachable!()
                                }
                            })))
                        }
                        RowStream::Map(first) => {
                            RowStream::Map(first.sum(sum.inputs[1..].iter().map(|input| {
                                if let RowStream::Map(input) = &streams[input] {
                                    input
                                } else {
                                    unreachable!()
                                }
                            })))
                        }
                    };
                    streams.insert(node_id, sum);
                }

                DataflowNode::Neg(neg) => {
                    if let Some(input) = streams.get(&neg.input) {
                        let negated = match input {
                            RowStream::Set(input) => RowStream::Set(input.neg()),
                            RowStream::Map(input) => RowStream::Map(input.neg()),
                        };

                        streams.insert(node_id, negated);
                    } else {
                        queue.extend([node_id, neg.input]);
                    }
                }

                DataflowNode::Sink(sink) => {
                    if let Some(input) = streams.get(&sink.input) {
                        let output = match input {
                            RowStream::Set(input) => RowOutput::Set(input.output()),
                            RowStream::Map(input) => RowOutput::Map(input.output()),
                        };

                        outputs.insert(node_id, output);
                    } else {
                        queue.extend([node_id, sink.input]);
                    }
                }

                // TODO: Map inputs
                DataflowNode::Source(source) => {
                    let (stream, handle) = circuit.add_input_zset::<Row, i32>();

                    if cfg!(debug_assertions) {
                        stream.inspect(|row| {
                            let mut cursor = row.cursor();
                            while cursor.key_valid() {
                                let key = cursor.key();
                                // println!("Source: {key:?}");
                                assert_eq!(key.vtable().layout_id, source.output_vtable.layout_id);
                                cursor.step_key();
                            }
                        });
                    }

                    streams.insert(node_id, RowStream::Set(stream));
                    inputs.insert(node_id, handle);
                }
            }
        }

        (inputs, outputs)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        codegen::CodegenConfig,
        dataflow::{CompiledDataflow, RowOutput},
        ir::{
            ColumnType, Constant, FunctionBuilder, Graph, IndexWith, Map, RowLayoutBuilder, Sink,
            Source,
        },
        row::Row,
    };
    use dbsp::{
        trace::{BatchReader, Cursor},
        Runtime,
    };

    #[test]
    fn compiled_dataflow() {
        let mut graph = Graph::new();

        let unit_layout = graph.layout_cache().unit();
        let xy_layout = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_row(ColumnType::U32, false)
                .with_row(ColumnType::U32, false)
                .build(),
        );
        let x_layout = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_row(ColumnType::U32, false)
                .build(),
        );

        let source = graph.add_node(Source::new(xy_layout));

        let mul = graph.add_node(Map::new(
            source,
            {
                let mut func = FunctionBuilder::new();
                let input = func.add_input(xy_layout);
                let output = func.add_output(x_layout);

                let x = func.load(input, 0);
                let y = func.load(input, 1);
                let xy = func.mul(x, y);
                func.store(output, 0, xy);

                func.ret_unit();
                func.build()
            },
            x_layout,
        ));

        let y_index = graph.add_node(IndexWith::new(
            source,
            {
                let mut func = FunctionBuilder::new();
                let input = func.add_input(xy_layout);
                let key = func.add_output(unit_layout);
                let value = func.add_output(x_layout);

                func.store(key, 0, Constant::Unit);

                let y = func.load(input, 0);
                func.store(value, 0, y);

                func.ret_unit();
                func.build()
            },
            unit_layout,
            x_layout,
        ));
        let y_squared = graph.add_node(Map::new(
            y_index,
            {
                let mut func = FunctionBuilder::new();
                let _key = func.add_input(unit_layout);
                let value = func.add_input(x_layout);
                let output = func.add_output(x_layout);

                let y = func.load(value, 0);
                let y_squared = func.mul(y, y);
                func.store(output, 0, y_squared);

                func.ret_unit();
                func.build()
            },
            x_layout,
        ));

        let mul_sink = graph.add_node(Sink::new(mul));
        let y_squared_sink = graph.add_node(Sink::new(y_squared));

        // let mut validator = Validator::new();
        // validator.validate_graph(&graph);

        graph.optimize();

        // validator.validate_graph(&graph);

        let (dataflow, jit_handle, mut layout_cache) =
            CompiledDataflow::new(&graph, CodegenConfig::debug());

        let (mut runtime, (mut inputs, outputs)) =
            Runtime::init_circuit(1, move |circuit| dataflow.construct(circuit)).unwrap();

        let mut values = Vec::new();
        let layout = layout_cache.compute(xy_layout);
        for (x, y) in [(1, 2), (0, 0), (1000, 2000), (12, 12)] {
            unsafe {
                let mut row = Row::uninit(&*jit_handle.vtables[&xy_layout]);
                row.as_mut_ptr()
                    .add(layout.offset_of(0) as usize)
                    .cast::<u32>()
                    .write(x);
                row.as_mut_ptr()
                    .add(layout.offset_of(1) as usize)
                    .cast::<u32>()
                    .write(y);

                values.push((row, 1i32));
            }
        }
        inputs.get_mut(&source).unwrap().append(&mut values);

        runtime.step().unwrap();

        let mul_sink = &outputs[&mul_sink];
        if let RowOutput::Set(output) = mul_sink {
            let output = output.consolidate();
            let mut cursor = output.cursor();

            println!("mul_sink:");
            while cursor.key_valid() {
                let weight = cursor.weight();
                let key = cursor.key();
                println!("  {key:?}: {weight}");

                cursor.step_key();
            }
        } else {
            unreachable!()
        }

        let y_squared_sink = &outputs[&y_squared_sink];
        if let RowOutput::Set(output) = y_squared_sink {
            let output = output.consolidate();
            let mut cursor = output.cursor();

            println!("y_squared_sink:");
            while cursor.key_valid() {
                let weight = cursor.weight();
                let key = cursor.key();
                println!("  {key:?}: {weight}");

                cursor.step_key();
            }
        } else {
            unreachable!()
        }

        runtime.kill().unwrap();

        unsafe { jit_handle.free_memory() };
    }
}
