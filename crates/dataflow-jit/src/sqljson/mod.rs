#![allow(dead_code, unused_imports, clippy::type_complexity)]

use crate::{
    codegen::{CodegenConfig, NativeLayout},
    dataflow::CompiledDataflow,
    ir::{
        graph::{GraphContext, Subgraph},
        ColumnType, Constant, ConstantStream, DataflowNode, Function, FunctionBuilder, Graph,
        GraphExt, LayoutId, Node, NodeId, NodeIdGen, NullableConstant, RowLayout, RowLayoutBuilder,
        RowLayoutCache, RowLiteral, Sink, StreamKind, StreamLayout, Terminator, Validator,
    },
    row::{Row, UninitRow},
};
use dbsp::{
    trace::{BatchReader, Cursor},
    Runtime,
};
use petgraph::prelude::DiGraphMap;
use serde::{Deserialize, Serialize};
use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    mem::{take, ManuallyDrop},
};

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

#[test]
fn join_json() {
    crate::utils::test_logger();

    let mut graph = Graph::new();

    let key = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::U32, true)
            .build(),
    );
    let value = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::String, false)
            .build(),
    );

    let output = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::U32, true)
            .with_column(ColumnType::String, false)
            .with_column(ColumnType::String, false)
            .build(),
    );
    let unit = graph.layout_cache().unit();

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

    let constant = graph.add_node(Node::Constant(ConstantStream::new(
        crate::ir::StreamLiteral::Set(vec![
            (
                RowLiteral::new(vec![
                    NullableConstant::NonNull(Constant::U32(10)),
                    NullableConstant::Nullable(Some(Constant::String(String::from("foo")))),
                    NullableConstant::Nullable(Some(Constant::String(String::from("bar")))),
                ]),
                10,
            ),
            (
                RowLiteral::new(vec![
                    NullableConstant::NonNull(Constant::U32(10)),
                    NullableConstant::Nullable(None),
                    NullableConstant::Nullable(Some(Constant::String(String::from("bing")))),
                ]),
                -5,
            ),
        ]),
        StreamLayout::Set(output),
    )));

    graph.sink(constant);

    let graph = SqlGraph::from(graph);
    let json_graph = serde_json::to_string_pretty(&graph).unwrap();
    println!("{json_graph}");
}

#[test]
#[ignore]
fn test_parse_sql_output() {
    const SQL: &str = include_str!("simple_select.json");
    // const SQL: &str = include_str!("green_tripdata_count.json");

    crate::utils::test_logger();

    let (graph, sources, sinks) = parse_sql_output(SQL);

    let graph = SqlGraph::from(graph);
    let json_graph = serde_json::to_string_pretty(&graph).unwrap();
    println!("{json_graph}");

    let mut graph = serde_json::from_str::<SqlGraph>(&json_graph)
        .unwrap()
        .rematerialize();
    println!("{graph:#?}");

    graph.optimize();
    // Validator::new().validate_graph(&graph).unwrap();

    let (dataflow, jit_handle, layout_cache) =
        CompiledDataflow::new(&graph, CodegenConfig::debug());

    let (mut runtime, (mut inputs, outputs)) =
        Runtime::init_circuit(1, move |circuit| dataflow.construct(circuit)).unwrap();

    // runtime.step().unwrap();
    // runtime.kill().unwrap();
    // unsafe { jit_handle.free_memory() };
    // return;

    let (input_node, input_layout) = sources["T"];
    let (output_node, _output_layout) = sinks["V"];

    let mut values = Vec::new();
    let layout = layout_cache.layout_of(input_layout);
    for (x, y) in [
        (Some(1000), Some(0)),
        (Some(1001), Some(1)),
        (Some(10), Some(2)),
        (None, Some(3)),
        (None, None),
        (Some(2000), None),
    ] {
        let mut row = UninitRow::new(unsafe { &*jit_handle.vtables()[&input_layout] });

        row.set_column_null(0, &layout, x.is_none());
        if let Some(x) = x {
            unsafe {
                row.as_mut_ptr()
                    .add(layout.offset_of(0) as usize)
                    .cast::<i32>()
                    .write(x)
            };
        }

        row.set_column_null(1, &layout, y.is_none());
        if let Some(y) = y {
            unsafe {
                row.as_mut_ptr()
                    .add(layout.offset_of(1) as usize)
                    .cast::<i32>()
                    .write(y)
            };
        }

        values.push((unsafe { row.assume_init() }, 1i32));
    }
    inputs
        .get_mut(&input_node)
        .unwrap()
        .as_set_mut()
        .unwrap()
        .append(&mut values);

    runtime.step().unwrap();

    if let Some(output) = outputs[&output_node].as_set() {
        let output = output.consolidate();
        let mut cursor = output.cursor();

        println!("output:");
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

/// Returns the dataflow graph, sources and sinks
fn parse_sql_output(
    output: &str,
) -> (
    Graph,
    HashMap<String, (NodeId, LayoutId)>,
    HashMap<String, (NodeId, LayoutId)>,
) {
    let raw = serde_json::from_str::<serde_json::Value>(output).unwrap();

    let mut graph = Graph::new();
    let mut functions = HashMap::new();

    for func in raw["functions"].as_array().unwrap() {
        let name = func["variable"].as_str().unwrap();

        let mut builder = FunctionBuilder::new(graph.layout_cache().clone());

        let mut arg_layouts = Vec::new();
        let args = match func["type"]["argumentTypes"][1].as_array() {
            Some(args) => args,
            None => continue,
        };
        for arg in args {
            debug_assert_eq!(
                arg["class"].as_str().unwrap(),
                "org.dbsp.sqlCompiler.ir.type.DBSPTypeRef",
            );
            debug_assert_eq!(
                arg["type"]["class"].as_str().unwrap(),
                "org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple",
            );

            let layout = graph.layout_cache().add(sql_layout(&arg["type"]));
            let arg = if arg["mutable"].as_bool().unwrap() {
                builder.add_input_output(layout)
            } else {
                builder.add_input(layout)
            };
            arg_layouts.push(arg);
        }

        let mut outputs = Vec::new();
        let ret_ty = func["type"]["resultType"]["class"].as_str().unwrap();
        if ret_ty == "org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool" {
            builder.set_return_type(ColumnType::Bool);
        } else if ret_ty == "org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple" {
            let layout = graph
                .layout_cache()
                .add(sql_layout(&func["type"]["resultType"]));
            outputs.push(builder.add_output(layout));
        } else if ret_ty == "org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple" {
            let elems = func["type"]["resultType"]["tupFields"][1]
                .as_array()
                .unwrap();
            for elem in elems {
                if elem["class"].as_str().unwrap()
                    == "org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple"
                {
                    let mut layout_builder = RowLayoutBuilder::new();
                    for elem in elem["tupFields"][1].as_array().unwrap() {
                        if elem["class"] == "org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple" {
                            sql_layout_into(elem, &mut layout_builder);
                        } else {
                            let (column_type, nullable) = sql_type(elem);
                            layout_builder.add_column(column_type, nullable);
                        }
                    }

                    outputs
                        .push(builder.add_output(graph.layout_cache().add(layout_builder.build())));
                } else if elem["class"] == "org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple" {
                    let layout = graph.layout_cache().add(sql_layout(elem));
                    outputs.push(builder.add_output(layout));
                } else {
                    let (column_type, nullable) = sql_type(elem);
                    let layout = graph.layout_cache().add(
                        RowLayoutBuilder::new()
                            .with_column(column_type, nullable)
                            .build(),
                    );
                    outputs.push(builder.add_output(layout));
                }
            }
        } else {
            todo!("unknown return type: {ret_ty}");
        }

        let func = &func["initializer"];
        let mut variables = HashMap::new();
        let mut variables_null = HashMap::new();

        let params = func["parameters"][1].as_array().unwrap();
        for (param, param_id) in params.iter().zip(arg_layouts) {
            variables.insert(param["pattern"]["identifier"].as_str().unwrap(), param_id);
        }

        let body = func["body"]["contents"][1].as_array().unwrap();

        let mut tuples = HashMap::new();
        for expr in body {
            let var = expr["variable"].as_str().unwrap();

            let initializer = &expr["initializer"];
            let kind = initializer["class"].as_str().unwrap();

            // Loads
            // TODO: Maybe stores to depending on context?
            if kind == "org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression" {
                let target = initializer["expression"]["variable"].as_str().unwrap();
                let target = variables[target];
                let column = initializer["fieldNo"].as_u64().unwrap() as usize;
                let load = builder.load(target, column);
                variables.insert(var, load);

                if initializer["type"]["mayBeNull"].as_bool().unwrap() {
                    let is_null = builder.is_null(target, column);
                    variables_null.insert(var, is_null);
                }

            // Casts
            } else if kind == "org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression" {
                let src = &initializer["source"];
                let src_var = src["variable"].as_str().unwrap();
                let src_value = variables[src_var];
                let (_src_ty, src_nullable) = sql_type(&src["type"]);
                let (dest_ty, dest_nullable) = sql_type(&initializer["destinationType"]);
                debug_assert_eq!(src_nullable, dest_nullable);

                let cast = if src_nullable {
                    let src_is_null = variables_null[src_var];
                    variables_null.insert(var, src_is_null);
                    builder.cast(src_value, dest_ty)
                } else {
                    builder.cast(src_value, dest_ty)
                };

                variables.insert(var, cast);

            // Binops
            } else if kind == "org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression" {
                let lhs_var = initializer["left"]["variable"].as_str().unwrap();
                let rhs_var = initializer["right"]["variable"].as_str().unwrap();
                let (lhs, rhs) = (variables[lhs_var], variables[rhs_var]);

                let operation = initializer["operation"].as_str().unwrap();
                let binop = match operation {
                    "==" => builder.eq(lhs, rhs),
                    "!=" => builder.neq(lhs, rhs),
                    "<" => builder.lt(lhs, rhs),
                    ">" => builder.gt(lhs, rhs),
                    "<=" => builder.le(lhs, rhs),
                    ">=" => builder.ge(lhs, rhs),
                    op => todo!("unknown binop: {op}"),
                };
                variables.insert(var, binop);

                match (variables_null.get(lhs_var), variables_null.get(rhs_var)) {
                    (Some(&lhs_null), Some(&rhs_null)) => {
                        let null = builder.or(lhs_null, rhs_null);
                        variables_null.insert(var, null);
                    }
                    (Some(&null), None) | (None, Some(&null)) => {
                        variables_null.insert(var, null);
                    }
                    (None, None) => {}
                }

            // i32 constants
            // TODO: Handle null constants
            } else if kind == "org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal" {
                let value = initializer["value"].as_i64().unwrap() as i32;
                let constant = builder.constant(Constant::I32(value));
                variables.insert(var, constant);

            // i64 constants
            // TODO: Handle null constants
            } else if kind == "org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal" {
                let value = initializer["value"].as_i64().unwrap();
                let constant = builder.constant(Constant::I64(value));
                variables.insert(var, constant);

            // boolean constants
            // TODO: Handle null constants
            } else if kind == "org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral" {
                let value = initializer["value"].as_bool().unwrap();
                let constant = builder.constant(Constant::Bool(value));
                variables.insert(var, constant);

            // Function calls
            } else if kind == "org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression" {
                let func = initializer["function"]["path"]["components"][1][0]["identifier"]
                    .as_str()
                    .unwrap();

                if func == "extract_Timestamp_epoch" {
                    // timestamp.milliseconds() / 1000
                    let timestamp = initializer["arguments"][1][0]["variable"].as_str().unwrap();
                    let timestamp = variables[timestamp];

                    // Timestamp is represented by milliseconds
                    let millis = builder.cast(timestamp, ColumnType::I64);
                    let one_thousand = builder.constant(Constant::I64(1000));
                    let epoch = builder.div(millis, one_thousand);
                    variables.insert(var, epoch);
                } else if func == "extract_Timestamp_epochN" {
                    // timestamp.milliseconds() / 1000
                    let timestamp = initializer["arguments"][1][0]["variable"].as_str().unwrap();
                    let timestamp = variables[timestamp];

                    // Timestamp is represented by milliseconds
                    let millis = builder.cast(timestamp, ColumnType::I64);
                    let one_thousand = builder.constant(Constant::I64(1000));
                    let epoch = builder.div(millis, one_thousand);
                    variables.insert(var, epoch);

                    // FIXME: Ideally we'd do this via branching, need basic block params first
                    let is_null = variables_null[var];
                    variables_null.insert(var, is_null);
                } else {
                    todo!("unknown function: {func}")
                }
            } else if kind == "org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression" {
                let layout = graph.layout_cache().add(sql_layout(&initializer["type"]));
                let row = builder.uninit_row(layout);

                for (idx, col) in initializer["fields"][1]
                    .as_array()
                    .unwrap()
                    .iter()
                    .enumerate()
                {
                    let col = col["variable"].as_str().unwrap();
                    let value = variables[col];

                    if let Some(&is_null) = variables_null.get(col) {
                        builder.set_null(row, idx, is_null);
                        // TODO: Make branching happen for non-scalar values
                        builder.store(row, idx, value);
                    } else {
                        builder.store(row, idx, value);
                    }
                }

                variables.insert(var, row);
            } else if kind == "org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression" {
                let elements: Vec<_> = initializer["fields"][1]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|field| {
                        let ty = &field["type"];

                        let layout = if ty["class"].as_str().unwrap()
                            == "org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple"
                        {
                            graph.layout_cache().add(sql_layout(ty))
                        } else {
                            let (column_type, nullable) = sql_type(ty);
                            graph.layout_cache().add(
                                RowLayoutBuilder::new()
                                    .with_column(column_type, nullable)
                                    .build(),
                            )
                        };

                        let elem = field["variable"].as_str().unwrap();
                        (elem, layout)
                    })
                    .collect();

                tuples.insert(var, elements);
            } else if kind == "org.dbsp.sqlCompiler.ir.expression.literal.DBSPCloneExpression" {
                // FIXME: Clone values
                let src = variables[initializer["expression"]["variable"].as_str().unwrap()];
                variables.insert(var, src);
            } else {
                todo!("unknown expression body: {expr} (kind: {kind})")
            }
        }

        let last_expr = &func["body"]["lastExpression"];
        let class = last_expr["class"].as_str().unwrap();
        if class == "org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression" {
            let func = last_expr["function"]["path"]["components"][1][0]["identifier"]
                .as_str()
                .unwrap();

            if func == "wrap_bool" {
                let arg_name = last_expr["arguments"][1][0]["variable"].as_str().unwrap();
                let arg = variables[arg_name];
                let arg_is_null = variables_null[arg_name];

                let return_bool = builder.create_block();
                let return_null = builder.create_block();
                builder.branch(arg_is_null, return_null, return_bool);
                builder.seal_current();

                // TODO: BB args to a single block instead of making two return blocks
                builder.move_to(return_bool);
                builder.ret(arg);
                builder.seal_current();

                builder.move_to(return_null);
                builder.ret(false);
                builder.seal_current();
            } else {
                todo!("unknown last expression function: {last_expr}")
            }
        } else if class == "org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression" {
            let output = outputs[0];

            let elements = last_expr["fields"][1]
                .as_array()
                .unwrap()
                .iter()
                .map(|elem| {
                    let var_name = elem["variable"].as_str().unwrap();
                    let value = variables[var_name];
                    let is_null = variables_null.get(var_name).copied();
                    (value, is_null)
                })
                .enumerate();
            for (column, (value, is_null)) in elements {
                builder.store(output, column, value);

                if let Some(is_null) = is_null {
                    builder.set_null(output, column, is_null);
                }
            }

            builder.ret_unit();
            builder.seal_current();
        } else if class == "org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression" {
            // let mut idx = 0;
            // for elem in last_expr["fields"][1].as_array().unwrap() {
            //     let class = elem["type"]["class"].as_str().unwrap();

            //     if class == "org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression" {
            //     } else if class ==
            // "org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression" {
            //         for (column, (value, is_null)) in elements {
            //             builder.store(output, column, value);

            //             if let Some(is_null) = is_null {
            //                 builder.set_null(output, column, is_null);
            //             }
            //         }
            //         let var_name = elem["variable"].as_str().unwrap();
            //         let value = variables[var_name];
            //         let is_null = variables_null.get(var_name).copied();

            //         let output = outputs[idx];
            //         builder.store(output, idx, value);
            //         if let Some(is_null) = is_null {
            //             builder.set_null(output, idx, is_null);
            //         }

            //         idx += 1;
            //     } else {
            //         todo!("unknown tuple element: {last_expr} (class: {class})")
            //     }
            // }

            builder.ret_unit();
            builder.seal_current();
        } else {
            todo!("unknown last expression: {last_expr}")
        }

        let function = builder.build();
        println!("{function:#?}");
        functions.insert(name, function);
    }

    dbg!(&functions);

    let mut sources = HashMap::new();
    let mut sinks = HashMap::new();

    let mut streams = HashMap::new();
    for operator in raw["operators"].as_array().unwrap() {
        let operation = operator["operation"].as_str().unwrap();

        // Source nodes
        if operation.is_empty() {
            let output = operator["output"].as_str().unwrap();

            // TODO: Support other input types
            debug_assert_eq!(
                operator["type"]["class"].as_str().unwrap(),
                "org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet",
            );
            debug_assert!(operator["inputs"].as_array().unwrap().is_empty());

            let key_layout = graph
                .layout_cache()
                .add(sql_layout(&operator["type"]["elementType"]));

            let source = graph.source(key_layout);
            streams.insert(output, source);
            sources.insert(output.to_owned(), (source, key_layout));
        } else if operation == "filter" {
            let input = operator["inputs"][0].as_str().unwrap();
            let input_stream = streams[input];
            let filter_fn = operator["function"]["variable"].as_str().unwrap();
            let output = operator["output"].as_str().unwrap();

            let filter = graph.filter(input_stream, functions[filter_fn].clone());
            streams.insert(output, filter);
        } else if operation == "map" || operation == "map_index" {
            let input = operator["inputs"][0].as_str().unwrap();
            let input_stream = streams[input];
            let layout = graph
                .layout_cache()
                .add(sql_layout(&operator["type"]["elementType"]));
            let map_fn = operator["function"]["variable"].as_str().unwrap();
            let output = operator["output"].as_str().unwrap();

            let map = graph.map(input_stream, layout, functions[map_fn].clone());
            streams.insert(output, map);
        } else if operation == "index_with" {
            let input = operator["inputs"][0].as_str().unwrap();
            let input_stream = streams[input];
            let key_layout = graph
                .layout_cache()
                .add(sql_layout(&operator["type"]["keyType"]));
            let value_layout = graph
                .layout_cache()
                .add(sql_layout(&operator["type"]["elementType"]));
            let map_fn = operator["function"]["variable"].as_str().unwrap();
            let output = operator["output"].as_str().unwrap();

            let index_with = graph.index_with(
                input_stream,
                key_layout,
                value_layout,
                functions[map_fn].clone(),
            );
            streams.insert(output, index_with);
        } else if operation == "differentiate" {
            let input = operator["inputs"][0].as_str().unwrap();
            let input_stream = streams[input];
            let differentiated = graph.differentiate(input_stream);
            streams.insert(output, differentiated);
        } else if operation == "inspect" {
            let input = operator["inputs"][0].as_str().unwrap();
            let input_stream = streams[input];
            let sink = graph.sink(input_stream);
            let layout = graph
                .layout_cache()
                .add(sql_layout(&operator["type"]["elementType"]));
            let output = operator["output"].as_str().unwrap();
            sinks.insert(output.to_owned(), (sink, layout));
        } else {
            todo!("unknown operator: {operator}")
        }
    }

    dbg!(&graph);
    (graph, sources, sinks)
}

fn sql_type(ty: &serde_json::Value) -> (ColumnType, bool) {
    let class = ty["class"].as_str().unwrap();
    let nullable = ty["mayBeNull"].as_bool().unwrap();

    let column_ty = if class == "org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger" {
        let width = ty["width"].as_u64().unwrap();
        match width {
            8 => ColumnType::I8,
            16 => ColumnType::I16,
            32 => ColumnType::I32,
            64 => ColumnType::I64,
            width => todo!("unknown integer width {width}"),
        }
    } else if class == "org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool" {
        ColumnType::Bool
    } else {
        todo!("unknown type: {ty}")
    };

    (column_ty, nullable)
}

fn sql_layout(fields: &serde_json::Value) -> RowLayout {
    let mut layout = RowLayoutBuilder::new();
    sql_layout_into(fields, &mut layout);
    dbg!(layout.build())
}

fn sql_layout_into(fields: &serde_json::Value, layout: &mut RowLayoutBuilder) {
    let fields = fields["tupFields"][1].as_array().unwrap();
    for elem in fields {
        let kind = elem["class"].as_str().unwrap();
        let nullable = elem["mayBeNull"].as_bool().unwrap();

        if kind == "org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger" {
            let ty = match elem["width"].as_u64().unwrap() {
                16 => ColumnType::I16,
                32 => ColumnType::I32,
                64 => ColumnType::I64,
                width => todo!("unknown integer width: {width}"),
            };
            layout.add_column(ty, nullable);
        } else if kind == "org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble" {
            layout.add_column(ColumnType::F64, nullable);
        } else if kind == "org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool" {
            layout.add_column(ColumnType::Bool, nullable);
        } else if kind == "org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString" {
            layout.add_column(ColumnType::String, nullable);
        } else if kind == "org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp" {
            layout.add_column(ColumnType::Timestamp, nullable);
        } else {
            todo!("unknown element type: {kind}")
        }
    }
}
