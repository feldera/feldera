mod nodes;

use crate::{
    codegen::{Codegen, CodegenConfig, LayoutVTable, NativeLayoutCache, VTable},
    dataflow::nodes::{
        DataflowSubgraph, DelayedFeedback, Delta0, Differentiate, Distinct, Export, FilterFn,
        FilterMap, FilterMapIndex, Integrate, JoinCore, MapFn, Min, Minus, Noop,
    },
    ir::{
        graph, DataflowNode as _, Graph, GraphExt, LayoutId, Node, NodeId, StreamKind,
        StreamLayout, Subgraph as SubgraphNode,
    },
    row::{Row, UninitRow},
};
use cranelift_jit::JITModule;
use cranelift_module::FuncId;
use dbsp::{
    operator::FilterMap as _,
    time::NestedTimestamp32,
    trace::{BatchReader, Cursor, Spine},
    Circuit, CollectionHandle, OrdIndexedZSet, OrdZSet, OutputHandle, Stream,
};
use derive_more::{IsVariant, Unwrap};
use nodes::{
    DataflowNode, Filter, IndexWith, Map, MonotonicJoin, Neg, Sink, Source, SourceMap, Sum,
};
use petgraph::{algo, prelude::DiGraphMap};
use std::{collections::BTreeMap, iter, mem::transmute, ptr::NonNull};

// TODO: Keep layout ids in dataflow nodes so we can do assertions that types
// are correct

type Inputs = BTreeMap<NodeId, RowInput>;
type Outputs = BTreeMap<NodeId, RowOutput>;

#[derive(Clone, IsVariant, Unwrap)]
pub enum RowInput {
    Set(CollectionHandle<Row, i32>),
    Map(CollectionHandle<Row, (Row, i32)>),
}

impl RowInput {
    pub fn as_set_mut(&mut self) -> Option<&mut CollectionHandle<Row, i32>> {
        if let Self::Set(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub fn as_map_mut(&mut self) -> Option<&mut CollectionHandle<Row, (Row, i32)>> {
        if let Self::Map(handle) = self {
            Some(handle)
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub enum RowOutput {
    Set(OutputHandle<OrdZSet<Row, i32>>),
    Map(OutputHandle<OrdIndexedZSet<Row, Row, i32>>),
}

impl RowOutput {
    pub const fn as_set(&self) -> Option<&OutputHandle<OrdZSet<Row, i32>>> {
        if let Self::Set(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub fn as_set_mut(&mut self) -> Option<&mut OutputHandle<OrdZSet<Row, i32>>> {
        if let Self::Set(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub const fn as_map(&self) -> Option<&OutputHandle<OrdIndexedZSet<Row, Row, i32>>> {
        if let Self::Map(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub fn as_map_mut(&mut self) -> Option<&mut OutputHandle<OrdIndexedZSet<Row, Row, i32>>> {
        if let Self::Map(handle) = self {
            Some(handle)
        } else {
            None
        }
    }
}

// TODO: Change the weight to a `Row`? Toggle between `i32` and `i64`?
#[derive(Clone, IsVariant, Unwrap)]
pub enum RowStream<P> {
    Set(Stream<P, OrdZSet<Row, i32>>),
    Map(Stream<P, OrdIndexedZSet<Row, Row, i32>>),
}

#[derive(Clone, IsVariant, Unwrap)]
pub enum RowTrace<P> {
    Set(Stream<P, Spine<OrdZSet<Row, i32>>>),
    Map(Stream<P, Spine<OrdIndexedZSet<Row, Row, i32>>>),
}

pub struct JitHandle {
    jit: JITModule,
    vtables: BTreeMap<LayoutId, *mut VTable>,
}

impl JitHandle {
    pub fn vtables(&self) -> &BTreeMap<LayoutId, *mut VTable> {
        &self.vtables
    }

    /// Free all memory associated with the JIT compiled code, including vtables
    /// and the functions themselves
    ///
    /// # Safety
    ///
    /// Cannot call this while any vtable or function pointers are still live or
    /// in use, any attempt to use them after calling this function is UB
    pub unsafe fn free_memory(mut self) {
        for &vtable in self.vtables.values() {
            drop(Box::from_raw(vtable));
        }

        self.vtables.clear();
        self.jit.free_memory();
    }
}

#[derive(Debug, Clone)]
pub struct CompiledDataflow {
    nodes: BTreeMap<NodeId, DataflowNode>,
    edges: DiGraphMap<NodeId, ()>,
}

impl CompiledDataflow {
    pub fn new(graph: &Graph, config: CodegenConfig) -> (Self, JitHandle, NativeLayoutCache) {
        let mut node_kinds = BTreeMap::new();
        let mut node_streams: BTreeMap<NodeId, Option<_>> = BTreeMap::new();

        let (mut inputs, mut input_nodes) = (Vec::with_capacity(16), Vec::with_capacity(16));
        let order = algo::toposort(graph.edges(), None).unwrap();
        for node_id in order {
            if !graph.nodes().contains_key(&node_id) {
                continue;
            }

            let node = &graph.nodes()[&node_id];

            node.inputs(&mut input_nodes);
            inputs.extend(input_nodes.iter().filter_map(|input| node_streams[input]));

            node_kinds.insert(node_id, node.output_kind(&inputs));
            node_streams.insert(node_id, node.output_stream(&inputs));

            inputs.clear();
            input_nodes.clear();

            if let Node::Subgraph(subgraph) = node {
                collect_subgraph_inputs(
                    subgraph,
                    &mut inputs,
                    &mut input_nodes,
                    &mut node_kinds,
                    &mut node_streams,
                );
            }
        }
        drop((inputs, input_nodes));

        fn collect_subgraph_inputs(
            graph: &SubgraphNode,
            inputs: &mut Vec<StreamLayout>,
            input_nodes: &mut Vec<NodeId>,
            node_kinds: &mut BTreeMap<NodeId, Option<StreamKind>>,
            node_streams: &mut BTreeMap<NodeId, Option<StreamLayout>>,
        ) {
            let order = algo::toposort(graph.edges(), None).unwrap();
            for node_id in order {
                if graph.input_nodes().contains_key(&node_id) {
                    continue;
                }

                let node = &graph.nodes()[&node_id];
                node.inputs(input_nodes);
                inputs.extend(input_nodes.iter().filter_map(|input| node_streams[input]));

                node_kinds.insert(node_id, node.output_kind(inputs));
                node_streams.insert(node_id, node.output_stream(inputs));

                inputs.clear();
                input_nodes.clear();

                if let Node::Subgraph(subgraph) = node {
                    collect_subgraph_inputs(
                        subgraph,
                        inputs,
                        input_nodes,
                        node_kinds,
                        node_streams,
                    );
                }
            }
        }

        fn collect_functions(
            codegen: &mut Codegen,
            functions: &mut BTreeMap<NodeId, Vec<FuncId>>,
            vtables: &mut BTreeMap<LayoutId, LayoutVTable>,
            graph: &graph::Subgraph,
        ) {
            for (&node_id, node) in graph.nodes() {
                match node {
                    Node::Map(map) => {
                        let map_fn = codegen.codegen_func("map_fn", map.map_fn());
                        functions.insert(node_id, vec![map_fn]);

                        vtables
                            .entry(map.layout())
                            .or_insert_with(|| codegen.vtable_for(map.layout()));
                    }

                    Node::Filter(filter) => {
                        let filter_fn = codegen.codegen_func("filter_fn", filter.filter_fn());
                        functions.insert(node_id, vec![filter_fn]);
                    }

                    Node::FilterMap(filter_map) => {
                        let fmap_fn =
                            codegen.codegen_func("filter_map_fn", filter_map.filter_map());
                        functions.insert(node_id, vec![fmap_fn]);

                        vtables
                            .entry(filter_map.layout())
                            .or_insert_with(|| codegen.vtable_for(filter_map.layout()));
                    }

                    Node::Fold(fold) => {
                        let step_fn = codegen.codegen_func("fold_step_fn", fold.step_fn());
                        let finish_fn = codegen.codegen_func("fold_finish_fn", fold.finish_fn());
                        functions.insert(node_id, vec![step_fn, finish_fn]);
                    }

                    Node::IndexWith(index_with) => {
                        let index_fn = codegen.codegen_func("index_fn", index_with.index_fn());
                        functions.insert(node_id, vec![index_fn]);

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

                    Node::SourceMap(source) => {
                        vtables
                            .entry(source.key())
                            .or_insert_with(|| codegen.vtable_for(source.key()));
                        vtables
                            .entry(source.value())
                            .or_insert_with(|| codegen.vtable_for(source.value()));
                    }

                    Node::JoinCore(join) => {
                        let join_fn = codegen.codegen_func("join_fn", join.join_fn());
                        functions.insert(node_id, vec![join_fn]);

                        vtables
                            .entry(join.key_layout())
                            .or_insert_with(|| codegen.vtable_for(join.key_layout()));
                        vtables
                            .entry(join.value_layout())
                            .or_insert_with(|| codegen.vtable_for(join.value_layout()));
                    }

                    Node::MonotonicJoin(join) => {
                        let join_fn = codegen.codegen_func("monotonic_join_fn", join.join_fn());
                        functions.insert(node_id, vec![join_fn]);

                        vtables
                            .entry(join.key_layout())
                            .or_insert_with(|| codegen.vtable_for(join.key_layout()));
                    }

                    Node::Subgraph(subgraph) => {
                        collect_functions(codegen, functions, vtables, subgraph.subgraph());
                    }

                    Node::Min(_)
                    | Node::Distinct(_)
                    | Node::Delta0(_)
                    | Node::DelayedFeedback(_)
                    | Node::Neg(_)
                    | Node::Sum(_)
                    | Node::Differentiate(_)
                    | Node::Integrate(_)
                    | Node::Sink(_)
                    | Node::Export(_)
                    | Node::ExportedNode(_)
                    | Node::Minus(_) => {}
                }
            }
        }

        // Run codegen over all nodes
        let mut codegen = Codegen::new(graph.layout_cache().clone(), config);
        // TODO: SmallVec
        let mut node_functions = BTreeMap::new();
        let mut vtables = BTreeMap::new();
        collect_functions(
            &mut codegen,
            &mut node_functions,
            &mut vtables,
            graph.graph(),
        );

        fn compile_nodes(
            graph: &graph::Subgraph,
            vtables: &BTreeMap<LayoutId, *mut VTable>,
            jit: &JITModule,
            node_streams: &BTreeMap<NodeId, Option<StreamLayout>>,
            node_functions: &BTreeMap<NodeId, Vec<FuncId>>,
        ) -> BTreeMap<NodeId, DataflowNode> {
            let mut nodes = BTreeMap::new();
            for (node_id, node) in graph.nodes() {
                let output = node_streams[node_id];

                match node {
                    Node::Map(map) => {
                        let input = map.input();
                        let map_fn = jit.get_finalized_function(node_functions[node_id][0]);
                        let output_vtable = unsafe { &*vtables[&map.layout()] };

                        let map_fn = unsafe {
                            match output.unwrap() {
                                StreamLayout::Set(_) => MapFn::Set(transmute(map_fn)),
                                StreamLayout::Map(..) => MapFn::Map(transmute(map_fn)),
                            }
                        };
                        let map = DataflowNode::Map(Map {
                            input,
                            map_fn,
                            output_vtable,
                        });

                        nodes.insert(*node_id, map);
                    }

                    Node::Filter(filter) => {
                        let filter_fn = jit.get_finalized_function(node_functions[node_id][0]);
                        let input = filter.input();

                        let (filter_fn, layout) = unsafe {
                            match output.unwrap() {
                                StreamLayout::Set(key) => {
                                    (FilterFn::Set(transmute(filter_fn)), StreamLayout::Set(key))
                                }
                                StreamLayout::Map(key, value) => (
                                    FilterFn::Map(transmute(filter_fn)),
                                    StreamLayout::Map(key, value),
                                ),
                            }
                        };
                        let filter = DataflowNode::Filter(Filter {
                            input,
                            filter_fn,
                            layout,
                        });

                        nodes.insert(*node_id, filter);
                    }

                    Node::FilterMap(filter_map) => {
                        let fmap_fn = jit.get_finalized_function(node_functions[node_id][0]);
                        let input = filter_map.input();
                        let output_vtable = unsafe { &*vtables[&filter_map.layout()] };

                        let node = match output.unwrap() {
                            StreamLayout::Set(_) => DataflowNode::FilterMap(FilterMap {
                                input,
                                filter_map: unsafe {
                                    transmute::<_, unsafe extern "C" fn(*const u8, *mut u8) -> bool>(
                                        fmap_fn,
                                    )
                                },
                                output_vtable,
                            }),

                            StreamLayout::Map(..) => DataflowNode::FilterMapIndex(FilterMapIndex {
                                input,
                                filter_map: unsafe {
                                    transmute::<
                                        _,
                                        unsafe extern "C" fn(*const u8, *const u8, *mut u8) -> bool,
                                    >(fmap_fn)
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

                    Node::Minus(minus) => {
                        nodes.insert(
                            *node_id,
                            DataflowNode::Minus(Minus {
                                lhs: minus.lhs(),
                                rhs: minus.rhs(),
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

                    Node::SourceMap(source) => {
                        let key_vtable = unsafe { &*vtables[&source.key()] };
                        let value_vtable = unsafe { &*vtables[&source.value()] };
                        nodes.insert(
                            *node_id,
                            DataflowNode::SourceMap(SourceMap {
                                key_vtable,
                                value_vtable,
                            }),
                        );
                    }

                    Node::IndexWith(index) => {
                        let input = index.input();
                        let index_fn = jit.get_finalized_function(node_functions[node_id][0]);
                        let key_vtable = unsafe { &*vtables[&index.key_layout()] };
                        let value_vtable = unsafe { &*vtables[&index.value_layout()] };

                        let node = match node_streams[&input].unwrap() {
                            StreamLayout::Set(_) => DataflowNode::IndexWith(IndexWith {
                                input,
                                index_fn: unsafe {
                                    transmute::<_, unsafe extern "C" fn(*const u8, *mut u8, *mut u8)>(
                                        index_fn,
                                    )
                                },
                                key_vtable,
                                value_vtable,
                            }),

                            StreamLayout::Map(..) => todo!(),
                        };

                        nodes.insert(*node_id, node);
                    }

                    Node::Differentiate(diff) => {
                        nodes.insert(
                            *node_id,
                            DataflowNode::Differentiate(Differentiate {
                                input: diff.input(),
                            }),
                        );
                    }

                    Node::Integrate(integrate) => {
                        nodes.insert(
                            *node_id,
                            DataflowNode::Integrate(Integrate {
                                input: integrate.input(),
                            }),
                        );
                    }

                    Node::Delta0(delta) => {
                        nodes.insert(
                            *node_id,
                            DataflowNode::Delta0(Delta0 {
                                input: delta.input(),
                            }),
                        );
                    }

                    Node::DelayedFeedback(_) => {
                        nodes.insert(*node_id, DataflowNode::DelayedFeedback(DelayedFeedback {}));
                    }

                    Node::Min(min) => {
                        nodes.insert(*node_id, DataflowNode::Min(Min { input: min.input() }));
                    }

                    Node::Distinct(distinct) => {
                        nodes.insert(
                            *node_id,
                            DataflowNode::Distinct(Distinct {
                                input: distinct.input(),
                            }),
                        );
                    }

                    Node::JoinCore(join) => {
                        let (lhs, rhs) = (join.lhs(), join.rhs());
                        let join_fn = jit.get_finalized_function(node_functions[node_id][0]);
                        let key_vtable = unsafe { &*vtables[&join.key_layout()] };
                        let value_vtable = unsafe { &*vtables[&join.value_layout()] };

                        let node = match (node_streams[&lhs].unwrap(), node_streams[&rhs].unwrap())
                        {
                            (StreamLayout::Map(..), StreamLayout::Map(..)) => {
                                DataflowNode::JoinCore(JoinCore {
                                    lhs,
                                    rhs,
                                    join_fn: unsafe {
                                        transmute::<
                                            _,
                                            unsafe extern "C" fn(
                                                *const u8,
                                                *const u8,
                                                *const u8,
                                                *mut u8,
                                                *mut u8,
                                            ),
                                        >(join_fn)
                                    },
                                    key_vtable,
                                    value_vtable,
                                    output_kind: join.result_kind(),
                                })
                            }

                            _ => todo!(),
                        };

                        nodes.insert(*node_id, node);
                    }

                    Node::MonotonicJoin(join) => {
                        let (lhs, rhs) = (join.lhs(), join.rhs());
                        let join_fn = jit.get_finalized_function(node_functions[node_id][0]);
                        let key_vtable = unsafe { &*vtables[&join.key_layout()] };

                        let node = DataflowNode::MonotonicJoin(MonotonicJoin {
                            lhs,
                            rhs,
                            join_fn: unsafe {
                                transmute::<
                                    _,
                                    unsafe extern "C" fn(*const u8, *const u8, *const u8, *mut u8),
                                >(join_fn)
                            },
                            key_vtable,
                        });
                        nodes.insert(*node_id, node);
                    }

                    Node::Subgraph(subgraph) => {
                        let node = DataflowNode::Subgraph(DataflowSubgraph {
                            edges: subgraph.edges().clone(),
                            inputs: subgraph.input_nodes().clone(),
                            nodes: compile_nodes(
                                subgraph.subgraph(),
                                vtables,
                                jit,
                                node_streams,
                                node_functions,
                            ),
                            feedback_connections: subgraph.feedback_connections().clone(),
                        });
                        nodes.insert(*node_id, node);
                    }

                    Node::Export(export) => {
                        let node = DataflowNode::Export(Export {
                            input: export.input(),
                        });
                        nodes.insert(*node_id, node);
                    }

                    Node::ExportedNode(exported) => {
                        let node = DataflowNode::Noop(Noop {
                            input: exported.input(),
                        });
                        nodes.insert(*node_id, node);
                    }
                }
            }

            nodes
        }

        let (jit, native_layout_cache) = codegen.finalize_definitions();
        let vtables = vtables
            .into_iter()
            .map(|(layout, vtable)| (layout, Box::into_raw(Box::new(vtable.marshalled(&jit)))))
            .collect();
        let nodes = compile_nodes(
            graph.graph(),
            &vtables,
            &jit,
            &node_streams,
            &node_functions,
        );

        (
            Self {
                nodes,
                edges: graph.edges().clone(),
            },
            JitHandle { jit, vtables },
            native_layout_cache,
        )
    }

    pub fn construct(self, circuit: &mut Circuit<()>) -> (Inputs, Outputs) {
        let mut streams = BTreeMap::<NodeId, RowStream<Circuit<()>>>::new();

        let mut inputs = BTreeMap::new();
        let mut outputs = BTreeMap::new();

        let order = algo::toposort(&self.edges, None).unwrap();
        for node_id in order {
            let node = &self.nodes[&node_id];
            match node {
                DataflowNode::Map(map) => {
                    let input = &streams[&map.input];
                    let vtable = map.output_vtable;

                    let mapped = match (input, map.map_fn) {
                        (RowStream::Set(input), MapFn::Set(map_fn)) => {
                            RowStream::Set(input.map(move |input| {
                                let mut output = UninitRow::new(vtable);
                                unsafe {
                                    map_fn(input.as_ptr(), output.as_mut_ptr());
                                    output.assume_init()
                                }
                            }))
                        }

                        (RowStream::Map(input), MapFn::Map(map_fn)) => {
                            RowStream::Set(input.map(move |(key, value)| {
                                let mut output = UninitRow::new(vtable);
                                unsafe {
                                    map_fn(key.as_ptr(), value.as_ptr(), output.as_mut_ptr());
                                    output.assume_init()
                                }
                            }))
                        }

                        _ => unreachable!(),
                    };

                    streams.insert(node_id, mapped);
                }

                DataflowNode::Filter(filter) => {
                    let input = &streams[&filter.input()];

                    let filtered = match (filter.filter_fn, input) {
                        (FilterFn::Set(filter_fn), RowStream::Set(input)) => {
                            let filtered =
                                input.filter(move |input| unsafe { filter_fn(input.as_ptr()) });
                            RowStream::Set(filtered)
                        }

                        (FilterFn::Map(filter_fn), RowStream::Map(input)) => {
                            let filtered = input.filter(move |(key, value)| unsafe {
                                filter_fn(key.as_ptr(), value.as_ptr())
                            });
                            RowStream::Map(filtered)
                        }

                        _ => unreachable!(),
                    };

                    streams.insert(node_id, filtered);
                }

                DataflowNode::FilterMap(map) => {
                    let input = &streams[&map.input];
                    let mapped = match input {
                        RowStream::Set(input) => {
                            let (fmap_fn, vtable) = (map.filter_map, map.output_vtable);

                            let mut output = None;
                            RowStream::Set(input.flat_map(move |input| {
                                let mut out =
                                    output.take().unwrap_or_else(|| UninitRow::new(vtable));

                                if unsafe { fmap_fn(input.as_ptr(), out.as_mut_ptr()) } {
                                    Some(unsafe { out.assume_init() })
                                } else {
                                    output = Some(out);
                                    None
                                }
                            }))
                        }

                        RowStream::Map(_) => todo!(),
                    };

                    streams.insert(node_id, mapped);
                }

                DataflowNode::FilterMapIndex(map) => {
                    let input = &streams[&map.input];
                    let mapped = match input {
                        RowStream::Map(input) => {
                            let (fmap_fn, vtable) = (map.filter_map, map.output_vtable);

                            let mut output = None;
                            RowStream::Set(input.flat_map(move |(key, value)| {
                                let mut out =
                                    output.take().unwrap_or_else(|| UninitRow::new(vtable));

                                let keep_element = unsafe {
                                    fmap_fn(key.as_ptr(), value.as_ptr(), out.as_mut_ptr())
                                };
                                if keep_element {
                                    Some(unsafe { out.assume_init() })
                                } else {
                                    output = Some(out);
                                    None
                                }
                            }))
                        }

                        RowStream::Set(_) => todo!(),
                    };

                    streams.insert(node_id, mapped);
                }

                DataflowNode::IndexWith(index_with) => {
                    let input = &streams[&index_with.input];
                    let (index_fn, key_vtable, value_vtable) = (
                        index_with.index_fn,
                        index_with.key_vtable,
                        index_with.value_vtable,
                    );

                    let indexed = match input {
                        RowStream::Set(input) => input.index_with(move |input| {
                            let (mut key_output, mut value_output) =
                                (UninitRow::new(key_vtable), UninitRow::new(value_vtable));

                            unsafe {
                                index_fn(
                                    input.as_ptr(),
                                    key_output.as_mut_ptr(),
                                    value_output.as_mut_ptr(),
                                );

                                (key_output.assume_init(), value_output.assume_init())
                            }
                        }),

                        // FIXME: `.index_with()` requires that `Key` is a `()`
                        RowStream::Map(_) => todo!(),
                    };

                    streams.insert(node_id, RowStream::Map(indexed));
                }

                DataflowNode::Sum(sum) => {
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

                DataflowNode::Minus(minus) => {
                    let lhs = &streams[&minus.lhs];
                    let rhs = streams[&minus.rhs].clone();
                    let difference = match lhs {
                        RowStream::Set(first) => RowStream::Set(first.minus(&rhs.unwrap_set())),
                        RowStream::Map(first) => RowStream::Map(first.minus(&rhs.unwrap_map())),
                    };
                    streams.insert(node_id, difference);
                }

                DataflowNode::Neg(neg) => {
                    let input = &streams[&neg.input];
                    let negated = match input {
                        RowStream::Set(input) => RowStream::Set(input.neg()),
                        RowStream::Map(input) => RowStream::Map(input.neg()),
                    };

                    streams.insert(node_id, negated);
                }

                DataflowNode::Differentiate(diff) => {
                    let input = &streams[&diff.input];
                    let differentiated = match input {
                        RowStream::Set(input) => RowStream::Set(input.differentiate()),
                        RowStream::Map(input) => RowStream::Map(input.differentiate()),
                    };

                    streams.insert(node_id, differentiated);
                }

                DataflowNode::Integrate(integrate) => {
                    let input = &streams[&integrate.input];
                    // TODO: It'd be better if we used `.integrate_trace()`
                    let integrated = match input {
                        RowStream::Set(input) => RowStream::Set(input.integrate()),
                        RowStream::Map(input) => RowStream::Map(input.integrate()),
                    };

                    streams.insert(node_id, integrated);
                }

                DataflowNode::Sink(sink) => {
                    let input = &streams[&sink.input];
                    let output = match input {
                        RowStream::Set(input) => RowOutput::Set(input.output()),
                        RowStream::Map(input) => RowOutput::Map(input.output()),
                    };

                    outputs.insert(node_id, output);
                }

                DataflowNode::Source(source) => {
                    let (stream, handle) = circuit.add_input_zset::<Row, i32>();

                    if cfg!(debug_assertions) {
                        stream.inspect(|row| {
                            let mut cursor = row.cursor();
                            while cursor.key_valid() {
                                let key = cursor.key();
                                assert_eq!(key.vtable().layout_id, source.output_vtable.layout_id);
                                cursor.step_key();
                            }
                        });
                    }

                    streams.insert(node_id, RowStream::Set(stream));
                    inputs.insert(node_id, RowInput::Set(handle));
                }

                DataflowNode::SourceMap(_source) => {
                    let (stream, handle) = circuit.add_input_indexed_zset::<Row, Row, i32>();
                    streams.insert(node_id, RowStream::Map(stream));
                    inputs.insert(node_id, RowInput::Map(handle));
                }

                DataflowNode::Delta0(_) => todo!(),

                DataflowNode::DelayedFeedback(_) => todo!(),

                DataflowNode::Min(_) => todo!(),

                DataflowNode::Distinct(_) => todo!(),

                DataflowNode::JoinCore(join) => {
                    let lhs = streams[&join.lhs].clone();
                    let rhs = streams[&join.rhs].clone();
                    let (join_fn, key_vtable, _value_vtable) =
                        (join.join_fn, join.key_vtable, join.value_vtable);

                    let joined = match join.output_kind {
                        StreamKind::Set => {
                            RowStream::Set(lhs.unwrap_map().join_generic::<(), _, _, _, _>(
                                &rhs.unwrap_map(),
                                move |key, lhs_val, rhs_val| {
                                    let mut output = UninitRow::new(key_vtable);
                                    unsafe {
                                        join_fn(
                                            key.as_ptr(),
                                            lhs_val.as_ptr(),
                                            rhs_val.as_ptr(),
                                            output.as_mut_ptr(),
                                            NonNull::<u8>::dangling().as_ptr(),
                                        );
                                    }

                                    iter::once((unsafe { output.assume_init() }, ()))
                                },
                            ))
                        }

                        StreamKind::Map => todo!(),
                    };
                    streams.insert(node_id, joined);
                }

                DataflowNode::MonotonicJoin(join) => {
                    let lhs = &streams[&join.lhs];
                    let rhs = streams[&join.rhs].clone();
                    let (join_fn, key_vtable) = (join.join_fn, join.key_vtable);

                    let joined = match lhs {
                        RowStream::Set(lhs) => {
                            RowStream::Set(lhs.monotonic_stream_join::<_, _, _>(
                                &rhs.unwrap_set(),
                                move |key, lhs_val, rhs_val| {
                                    let mut output = UninitRow::new(key_vtable);
                                    unsafe {
                                        join_fn(
                                            key.as_ptr(),
                                            lhs_val as *const () as *const u8,
                                            rhs_val as *const () as *const u8,
                                            output.as_mut_ptr(),
                                        );

                                        output.assume_init()
                                    }
                                },
                            ))
                        }

                        RowStream::Map(lhs) => {
                            RowStream::Set(lhs.monotonic_stream_join::<_, _, _>(
                                &rhs.unwrap_map(),
                                move |key, lhs_val, rhs_val| {
                                    let mut output = UninitRow::new(key_vtable);
                                    unsafe {
                                        join_fn(
                                            key.as_ptr(),
                                            lhs_val.as_ptr(),
                                            rhs_val.as_ptr(),
                                            output.as_mut_ptr(),
                                        );

                                        output.assume_init()
                                    }
                                },
                            ))
                        }
                    };
                    streams.insert(node_id, joined);
                }

                DataflowNode::Export(_) => todo!(),

                DataflowNode::Subgraph(subgraph) => {
                    let mut needs_consolidate = BTreeMap::new();

                    circuit
                        .fixedpoint(|subcircuit| {
                            let mut substreams = BTreeMap::new();
                            let mut feedbacks = BTreeMap::new();

                            let nodes = algo::toposort(&subgraph.edges, None).unwrap();
                            for node_id in nodes {
                                if subgraph.inputs.contains_key(&node_id) {
                                    continue;
                                }

                                match &subgraph.nodes[&node_id] {
                                    DataflowNode::Map(map) => {
                                        let input = &substreams[&map.input];
                                        let vtable = map.output_vtable;

                                        let mapped = match (input, map.map_fn) {
                                            (RowStream::Set(input), MapFn::Set(map_fn)) => {
                                                RowStream::Set(input.map(move |input| {
                                                    let mut output = UninitRow::new(vtable);
                                                    unsafe {
                                                        map_fn(input.as_ptr(), output.as_mut_ptr());
                                                        output.assume_init()
                                                    }
                                                }))
                                            }

                                            (RowStream::Map(input), MapFn::Map(map_fn)) => {
                                                RowStream::Set(input.map(move |(key, value)| {
                                                    let mut output = UninitRow::new(vtable);
                                                    unsafe {
                                                        map_fn(
                                                            key.as_ptr(),
                                                            value.as_ptr(),
                                                            output.as_mut_ptr(),
                                                        );
                                                        output.assume_init()
                                                    }
                                                }))
                                            }

                                            _ => unreachable!(),
                                        };

                                        substreams.insert(node_id, mapped);
                                    }

                                    DataflowNode::Filter(filter) => {
                                        let input = &substreams[&filter.input()];
                                        let filtered = match (filter.filter_fn, input) {
                                            (FilterFn::Set(filter_fn), RowStream::Set(input)) => {
                                                let filtered = input.filter(move |input| unsafe {
                                                    filter_fn(input.as_ptr())
                                                });
                                                RowStream::Set(filtered)
                                            }

                                            (FilterFn::Map(filter_fn), RowStream::Map(input)) => {
                                                let filtered =
                                                    input.filter(move |(key, value)| unsafe {
                                                        filter_fn(key.as_ptr(), value.as_ptr())
                                                    });
                                                RowStream::Map(filtered)
                                            }

                                            _ => unreachable!(),
                                        };

                                        substreams.insert(node_id, filtered);
                                    }

                                    DataflowNode::FilterMap(map) => {
                                        let input = &substreams[&map.input];
                                        let mapped = match input {
                                            RowStream::Set(input) => {
                                                let (fmap_fn, vtable) =
                                                    (map.filter_map, map.output_vtable);

                                                let mut output = None;
                                                RowStream::Set(input.flat_map(move |input| {
                                                    let mut out = output
                                                        .take()
                                                        .unwrap_or_else(|| UninitRow::new(vtable));
                                                    unsafe {
                                                        if fmap_fn(input.as_ptr(), out.as_mut_ptr())
                                                        {
                                                            Some(out.assume_init())
                                                        } else {
                                                            output = Some(out);
                                                            None
                                                        }
                                                    }
                                                }))
                                            }

                                            RowStream::Map(_) => todo!(),
                                        };

                                        substreams.insert(node_id, mapped);
                                    }

                                    DataflowNode::FilterMapIndex(map) => {
                                        let input = &substreams[&map.input];
                                        let mapped = match input {
                                            RowStream::Map(input) => {
                                                let (fmap_fn, vtable) =
                                                    (map.filter_map, map.output_vtable);

                                                let mut output = None;
                                                RowStream::Set(input.flat_map(
                                                    move |(key, value)| {
                                                        let mut out =
                                                            output.take().unwrap_or_else(|| {
                                                                UninitRow::new(vtable)
                                                            });

                                                        unsafe {
                                                            if fmap_fn(
                                                                key.as_ptr(),
                                                                value.as_ptr(),
                                                                out.as_mut_ptr(),
                                                            ) {
                                                                Some(out.assume_init())
                                                            } else {
                                                                output = Some(out);
                                                                None
                                                            }
                                                        }
                                                    },
                                                ))
                                            }

                                            RowStream::Set(_) => todo!(),
                                        };

                                        substreams.insert(node_id, mapped);
                                    }

                                    DataflowNode::IndexWith(index_with) => {
                                        let input = &substreams[&index_with.input];
                                        let (index_fn, key_vtable, value_vtable) = (
                                            index_with.index_fn,
                                            index_with.key_vtable,
                                            index_with.value_vtable,
                                        );

                                        let indexed = match input {
                                            RowStream::Set(input) => {
                                                input.index_with(move |input| {
                                                    let (mut key_output, mut value_output) = (
                                                        UninitRow::new(key_vtable),
                                                        UninitRow::new(value_vtable),
                                                    );

                                                    unsafe {
                                                        index_fn(
                                                            input.as_ptr(),
                                                            key_output.as_mut_ptr(),
                                                            value_output.as_mut_ptr(),
                                                        );

                                                        (
                                                            key_output.assume_init(),
                                                            value_output.assume_init(),
                                                        )
                                                    }
                                                })
                                            }

                                            // FIXME: `.index_with()` requires that `Key` is a `()`
                                            RowStream::Map(_) => todo!(),
                                        };

                                        substreams.insert(node_id, RowStream::Map(indexed));
                                    }

                                    DataflowNode::Sum(sum) => {
                                        let sum = match &substreams[&sum.inputs[0]] {
                                            RowStream::Set(first) => RowStream::Set(first.sum(
                                                sum.inputs[1..].iter().map(|input| {
                                                    if let RowStream::Set(input) =
                                                        &substreams[input]
                                                    {
                                                        input
                                                    } else {
                                                        unreachable!()
                                                    }
                                                }),
                                            )),
                                            RowStream::Map(first) => RowStream::Map(first.sum(
                                                sum.inputs[1..].iter().map(|input| {
                                                    if let RowStream::Map(input) =
                                                        &substreams[input]
                                                    {
                                                        input
                                                    } else {
                                                        unreachable!()
                                                    }
                                                }),
                                            )),
                                        };
                                        substreams.insert(node_id, sum);
                                    }

                                    DataflowNode::Minus(minus) => {
                                        let lhs = &substreams[&minus.lhs];
                                        let rhs = substreams[&minus.rhs].clone();
                                        let difference = match lhs {
                                            RowStream::Set(first) => {
                                                RowStream::Set(first.minus(&rhs.unwrap_set()))
                                            }
                                            RowStream::Map(first) => {
                                                RowStream::Map(first.minus(&rhs.unwrap_map()))
                                            }
                                        };
                                        substreams.insert(node_id, difference);
                                    }

                                    DataflowNode::Neg(neg) => {
                                        let input = &substreams[&neg.input];
                                        let negated = match input {
                                            RowStream::Set(input) => RowStream::Set(input.neg()),
                                            RowStream::Map(input) => RowStream::Map(input.neg()),
                                        };

                                        substreams.insert(node_id, negated);
                                    }

                                    DataflowNode::Sink(_)
                                    | DataflowNode::Source(_)
                                    | DataflowNode::SourceMap(_) => todo!(),

                                    DataflowNode::Delta0(delta) => {
                                        let input = &streams[&delta.input];
                                        let delta0 = match input {
                                            RowStream::Set(input) => {
                                                RowStream::Set(input.delta0(subcircuit))
                                            }
                                            RowStream::Map(input) => {
                                                RowStream::Map(input.delta0(subcircuit))
                                            }
                                        };

                                        substreams.insert(node_id, delta0);
                                    }

                                    DataflowNode::DelayedFeedback(_feedback) => {
                                        let feedback = dbsp::operator::DelayedFeedback::<
                                            _,
                                            OrdZSet<Row, i32>,
                                        >::new(
                                            subcircuit
                                        );
                                        let stream = feedback.stream().clone();
                                        substreams.insert(node_id, RowStream::Set(stream));
                                        feedbacks.insert(node_id, feedback);
                                    }

                                    DataflowNode::Min(min) => {
                                        let min = match &substreams[&min.input] {
                                            RowStream::Set(_) => todo!(),
                                            RowStream::Map(input) => {
                                                RowStream::Map(input.aggregate_generic::<(), _, _>(
                                                    dbsp::operator::Min,
                                                ))
                                            }
                                        };
                                        substreams.insert(node_id, min);
                                    }

                                    DataflowNode::Distinct(distinct) => {
                                        let distinct = match &substreams[&distinct.input] {
                                            RowStream::Set(input) => RowStream::Set(
                                                input.distinct::<NestedTimestamp32>(),
                                            ),
                                            RowStream::Map(_input) => todo!(),
                                        };
                                        substreams.insert(node_id, distinct);
                                    }

                                    DataflowNode::JoinCore(join) => {
                                        let lhs = substreams[&join.lhs].clone();
                                        let rhs = substreams[&join.rhs].clone();
                                        let (join_fn, key_vtable, _value_vtable) =
                                            (join.join_fn, join.key_vtable, join.value_vtable);

                                        let joined = match join.output_kind {
                                            StreamKind::Set => RowStream::Set(
                                                lhs.unwrap_map()
                                                    .join_generic::<NestedTimestamp32, _, _, _, _>(
                                                        &rhs.unwrap_map(),
                                                        move |key, lhs_val, rhs_val| {
                                                            let mut output =
                                                                UninitRow::new(key_vtable);
                                                            unsafe {
                                                                join_fn(
                                                                    key.as_ptr(),
                                                                    lhs_val.as_ptr(),
                                                                    rhs_val.as_ptr(),
                                                                    output.as_mut_ptr(),
                                                                    NonNull::<u8>::dangling()
                                                                        .as_ptr(),
                                                                );
                                                            }

                                                            iter::once((
                                                                unsafe { output.assume_init() },
                                                                (),
                                                            ))
                                                        },
                                                    ),
                                            ),

                                            StreamKind::Map => todo!(),
                                        };
                                        substreams.insert(node_id, joined);
                                    }

                                    DataflowNode::MonotonicJoin(join) => {
                                        let lhs = &substreams[&join.lhs];
                                        let rhs = substreams[&join.rhs].clone();
                                        let (join_fn, key_vtable) = (join.join_fn, join.key_vtable);

                                        let joined = match lhs {
                                            RowStream::Set(lhs) => RowStream::Set(
                                                lhs.monotonic_stream_join::<_, _, _>(
                                                    &rhs.unwrap_set(),
                                                    move |key, lhs_val, rhs_val| {
                                                        let mut output = UninitRow::new(key_vtable);
                                                        unsafe {
                                                            join_fn(
                                                                key.as_ptr(),
                                                                lhs_val as *const () as *const u8,
                                                                rhs_val as *const () as *const u8,
                                                                output.as_mut_ptr(),
                                                            );

                                                            output.assume_init()
                                                        }
                                                    },
                                                ),
                                            ),

                                            RowStream::Map(lhs) => RowStream::Set(
                                                lhs.monotonic_stream_join::<_, _, _>(
                                                    &rhs.unwrap_map(),
                                                    move |key, lhs_val, rhs_val| {
                                                        let mut output = UninitRow::new(key_vtable);
                                                        unsafe {
                                                            join_fn(
                                                                key.as_ptr(),
                                                                lhs_val.as_ptr(),
                                                                rhs_val.as_ptr(),
                                                                output.as_mut_ptr(),
                                                            );

                                                            output.assume_init()
                                                        }
                                                    },
                                                ),
                                            ),
                                        };
                                        substreams.insert(node_id, joined);
                                    }

                                    DataflowNode::Export(export) => {
                                        let exported = match &substreams[&export.input] {
                                            RowStream::Set(input) => {
                                                RowTrace::Set(input.integrate_trace().export())
                                            }
                                            RowStream::Map(input) => {
                                                RowTrace::Map(input.integrate_trace().export())
                                            }
                                        };

                                        needs_consolidate.insert(node_id, exported);
                                    }

                                    DataflowNode::Differentiate(diff) => {
                                        let input = &streams[&diff.input];
                                        let differentiated = match input {
                                            RowStream::Set(input) => {
                                                RowStream::Set(input.differentiate_nested())
                                            }
                                            RowStream::Map(input) => {
                                                RowStream::Map(input.differentiate_nested())
                                            }
                                        };

                                        streams.insert(node_id, differentiated);
                                    }

                                    DataflowNode::Integrate(diff) => {
                                        let input = &streams[&diff.input];
                                        let integrated = match input {
                                            RowStream::Set(input) => {
                                                RowStream::Set(input.integrate_nested())
                                            }
                                            RowStream::Map(input) => {
                                                RowStream::Map(input.integrate_nested())
                                            }
                                        };

                                        streams.insert(node_id, integrated);
                                    }

                                    DataflowNode::Subgraph(_) => todo!(),

                                    DataflowNode::Noop(_) => {}
                                }
                            }

                            // Connect all feedback nodes
                            for (source, feedback) in subgraph.feedback_connections.iter() {
                                let source = substreams[source].clone().unwrap_set();
                                let feedback = feedbacks.remove(feedback).unwrap();
                                feedback.connect(&source);
                            }

                            Ok(())
                        })
                        .unwrap();

                    for (node_id, stream) in needs_consolidate {
                        let consolidated = match stream {
                            RowTrace::Set(stream) => RowStream::Set(stream.consolidate()),
                            RowTrace::Map(stream) => RowStream::Map(stream.consolidate()),
                        };
                        streams.insert(node_id, consolidated);
                    }
                }

                DataflowNode::Noop(_) => {}
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
            graph::GraphExt, ColumnType, Constant, Distinct, FunctionBuilder, Graph, IndexWith,
            JoinCore, Map, Min, Minus, MonotonicJoin, RowLayoutBuilder, Sink, Source, SourceMap,
            StreamKind, StreamLayout, Sum,
        },
        row::UninitRow,
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
                .with_column(ColumnType::U32, false)
                .with_column(ColumnType::U32, false)
                .build(),
        );
        let x_layout = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::U32, false)
                .build(),
        );

        let source = graph.add_node(Source::new(xy_layout));

        let mul = graph.add_node(Map::new(
            source,
            {
                let mut func = FunctionBuilder::new(graph.layout_cache().clone());
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
                let mut func = FunctionBuilder::new(graph.layout_cache().clone());
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
                let mut func = FunctionBuilder::new(graph.layout_cache().clone());
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

        let (dataflow, jit_handle, layout_cache) =
            CompiledDataflow::new(&graph, CodegenConfig::debug());

        let (mut runtime, (mut inputs, outputs)) =
            Runtime::init_circuit(1, move |circuit| dbg!(dataflow).construct(circuit)).unwrap();

        let mut values = Vec::new();
        let layout = layout_cache.layout_of(xy_layout);
        for (x, y) in [(1, 2), (0, 0), (1000, 2000), (12, 12)] {
            unsafe {
                let mut row = UninitRow::new(&*jit_handle.vtables[&xy_layout]);
                row.as_mut_ptr()
                    .add(layout.offset_of(0) as usize)
                    .cast::<u32>()
                    .write(x);
                row.as_mut_ptr()
                    .add(layout.offset_of(1) as usize)
                    .cast::<u32>()
                    .write(y);

                values.push((row.assume_init(), 1i32));
            }
        }
        inputs
            .get_mut(&source)
            .unwrap()
            .as_set_mut()
            .unwrap()
            .append(&mut values);

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

    #[test]
    fn bfs() {
        crate::utils::test_logger();

        let mut graph = Graph::new();

        let unit = graph.layout_cache().unit();

        // `{ u64 }`
        let u64x1 = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::U64, false)
                .build(),
        );
        // `{ u64, u64 }`
        let u64x2 = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::U64, false)
                .with_column(ColumnType::U64, false)
                .build(),
        );

        let roots = graph.add_node(Source::new(u64x2));
        let edges = graph.add_node(SourceMap::new(u64x1, u64x1));
        let vertices = graph.add_node(Source::new(u64x1));

        let (_recursive, distances) = graph.subgraph(|subgraph| {
            let nodes = subgraph.delayed_feedback(u64x2);

            let roots = subgraph.delta0(roots);
            let edges = subgraph.delta0(edges);

            let nodes_index = subgraph.add_node(IndexWith::new(
                nodes,
                {
                    let mut func = FunctionBuilder::new(subgraph.layout_cache().clone());
                    let input = func.add_input(u64x2);
                    let key = func.add_output(u64x1);
                    let value = func.add_output(u64x1);

                    let node = func.load(input, 0);
                    let dist = func.load(input, 1);
                    func.store(key, 0, node);
                    func.store(value, 0, dist);

                    func.ret_unit();
                    func.build()
                },
                u64x1,
                u64x1,
            ));
            let nodes_join_edges = subgraph.add_node(JoinCore::new(
                nodes_index,
                edges,
                {
                    let mut func = FunctionBuilder::new(subgraph.layout_cache().clone());
                    let _key = func.add_input(u64x1);
                    let node_value = func.add_input(u64x1);
                    let edge_value = func.add_input(u64x1);
                    let output_key = func.add_output(u64x2);
                    let _output_value = func.add_output(unit);

                    let dist = func.load(node_value, 0);
                    let dest = func.load(edge_value, 0);
                    let one = func.constant(Constant::U64(1));
                    let dist_plus_one = func.add(dist, one);
                    func.store(output_key, 0, dest);
                    func.store(output_key, 1, dist_plus_one);

                    func.ret_unit();
                    func.build()
                },
                u64x2,
                unit,
                StreamKind::Set,
            ));

            let joined_plus_roots = subgraph.add_node(Sum::new(vec![nodes_join_edges, roots]));
            let joined_plus_roots = subgraph.add_node(IndexWith::new(
                joined_plus_roots,
                {
                    let mut func = FunctionBuilder::new(subgraph.layout_cache().clone());
                    let input = func.add_input(u64x2);
                    let key = func.add_output(u64x1);
                    let value = func.add_output(u64x1);

                    let node = func.load(input, 0);
                    let dist = func.load(input, 1);
                    func.store(key, 0, node);
                    func.store(value, 0, dist);

                    func.ret_unit();
                    func.build()
                },
                u64x1,
                u64x1,
            ));

            let min = subgraph.add_node(Min::new(joined_plus_roots));
            let min_set = subgraph.add_node(Map::new(
                min,
                {
                    let mut func = FunctionBuilder::new(subgraph.layout_cache().clone());
                    let key = func.add_input(u64x1);
                    let value = func.add_input(u64x1);
                    let output = func.add_output(u64x2);

                    let key = func.load(key, 0);
                    let value = func.load(value, 0);
                    func.store(output, 0, key);
                    func.store(output, 1, value);

                    func.ret_unit();
                    func.build()
                },
                u64x2,
            ));
            let min_set_distinct = subgraph.add_node(Distinct::new(min_set));
            subgraph.connect_feedback(min_set_distinct, nodes);
            subgraph.export(min_set_distinct, StreamLayout::Set(u64x2))
        });

        let reachable_nodes = graph.add_node(Map::new(
            distances,
            {
                let mut func = FunctionBuilder::new(graph.layout_cache().clone());
                let distance = func.add_input(u64x2);
                let output = func.add_output(u64x1);

                let node = func.load(distance, 0);
                func.store(output, 0, node);

                func.ret_unit();
                func.build()
            },
            u64x1,
        ));

        let reachable_nodes = graph.add_node(MonotonicJoin::new(
            vertices,
            reachable_nodes,
            {
                let mut func = FunctionBuilder::new(graph.layout_cache().clone());
                let key = func.add_input(u64x1);
                let _lhs_val = func.add_input(unit);
                let _rhs_val = func.add_input(unit);
                let output = func.add_output(u64x1);

                let key = func.load(key, 0);
                func.store(output, 0, key);

                func.ret_unit();
                func.build()
            },
            u64x1,
        ));
        let unreachable_nodes = graph.add_node(Minus::new(vertices, reachable_nodes));
        let unreachable_nodes = graph.add_node(Map::new(
            unreachable_nodes,
            {
                let mut func = FunctionBuilder::new(graph.layout_cache().clone());
                let node = func.add_input(u64x1);
                let output = func.add_output(u64x2);

                let node = func.load(node, 0);
                func.store(output, 0, node);

                let weight = func.constant(Constant::U64(i64::MAX as u64));
                func.store(output, 1, weight);

                func.ret_unit();
                func.build()
            },
            u64x2,
        ));

        let distances = graph.add_node(Sum::new(vec![distances, unreachable_nodes]));
        let sink = graph.add_node(Sink::new(distances));

        let (dataflow, jit_handle, layout_cache) =
            CompiledDataflow::new(&graph, CodegenConfig::debug());
        let (mut runtime, (mut inputs, outputs)) =
            Runtime::init_circuit(1, move |circuit| dataflow.construct(circuit)).unwrap();

        {
            let u64x1_vtable = unsafe { &*jit_handle.vtables()[&u64x1] };
            let u64x1_offset = layout_cache.layout_of(u64x1).offset_of(0) as usize;
            let u64x2_vtable = unsafe { &*jit_handle.vtables()[&u64x2] };
            let u64x2_layout = layout_cache.layout_of(u64x2);

            let roots = inputs.get_mut(&roots).unwrap().as_set_mut().unwrap();
            let mut source_vertex = UninitRow::new(u64x2_vtable);
            unsafe {
                source_vertex
                    .as_mut_ptr()
                    .add(u64x2_layout.offset_of(0) as usize)
                    .cast::<u64>()
                    .write(1);
                source_vertex
                    .as_mut_ptr()
                    .add(u64x2_layout.offset_of(1) as usize)
                    .cast::<u64>()
                    .write(0);

                roots.push(source_vertex.assume_init(), 1);
            }

            let vertices_data = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            let vertices = inputs.get_mut(&vertices).unwrap().as_set_mut().unwrap();
            for &vertex in vertices_data {
                let mut key = UninitRow::new(u64x1_vtable);
                unsafe {
                    key.as_mut_ptr()
                        .add(u64x1_offset)
                        .cast::<u64>()
                        .write(vertex);

                    vertices.push(key.assume_init(), 1);
                }
            }

            let edge_data = &[
                (1, 3),
                (1, 5),
                (2, 4),
                (2, 5),
                (2, 10),
                (3, 1),
                (3, 5),
                (3, 8),
                (3, 10),
                (5, 3),
                (5, 4),
                (5, 8),
                (6, 3),
                (6, 4),
                (7, 4),
                (8, 1),
                (9, 4),
            ];
            let edges = inputs.get_mut(&edges).unwrap().as_map_mut().unwrap();
            for &(src, dest) in edge_data {
                let (mut key, mut value) =
                    (UninitRow::new(u64x1_vtable), UninitRow::new(u64x1_vtable));

                unsafe {
                    key.as_mut_ptr().add(u64x1_offset).cast::<u64>().write(src);
                    value
                        .as_mut_ptr()
                        .add(u64x1_offset)
                        .cast::<u64>()
                        .write(dest);

                    edges.push(key.assume_init(), (value.assume_init(), 1));
                }
            }
        }

        runtime.dump_profile("../../target").unwrap();
        runtime.step().unwrap();
        runtime.dump_profile("../../target").unwrap();
        runtime.kill().unwrap();

        {
            let u64x2_layout = layout_cache.layout_of(u64x2);
            let mut produced = Vec::new();

            let outputs = outputs[&sink].as_set().unwrap().consolidate();
            let mut cursor = outputs.cursor();
            while cursor.key_valid() {
                let weight = cursor.weight();
                let key = cursor.key();
                println!("Output: {key:?}: {weight}");

                unsafe {
                    let node = *key
                        .as_ptr()
                        .add(u64x2_layout.offset_of(0) as usize)
                        .cast::<u64>();
                    let distance = *key
                        .as_ptr()
                        .add(u64x2_layout.offset_of(1) as usize)
                        .cast::<u64>();
                    produced.push((node, distance));
                }

                cursor.step_key();
            }

            produced.sort_by_key(|&(node, _)| node);

            let expected = &[
                (1, 0),
                (2, 9223372036854775807),
                (3, 1),
                (4, 2),
                (5, 1),
                (6, 9223372036854775807),
                (7, 9223372036854775807),
                (8, 2),
                (9, 9223372036854775807),
                (10, 2),
            ];
            assert_eq!(produced, expected);
        }

        unsafe { jit_handle.free_memory() };
    }
}
