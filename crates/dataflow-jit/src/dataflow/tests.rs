#![cfg(test)]

use crate::{
    codegen::CodegenConfig,
    dataflow::{CompiledDataflow, RowOutput},
    ir::{
        graph::GraphExt,
        nodes::{Min, Minus, MonotonicJoin, StreamKind, StreamLayout, Sum},
        ColumnType, Constant, FunctionBuilder, Graph, RowLayoutBuilder,
    },
    row::UninitRow,
    utils,
};
use dbsp::{
    trace::{BatchReader, Cursor},
    Runtime,
};

#[test]
fn compiled_dataflow() {
    utils::test_logger();

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

    let source = graph.source(xy_layout);

    let mul = graph.map(
        source,
        StreamLayout::Set(xy_layout),
        StreamLayout::Set(x_layout),
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
    );

    let y_index = graph.index_with(source, unit_layout, x_layout, {
        let mut func = FunctionBuilder::new(graph.layout_cache().clone());
        let input = func.add_input(xy_layout);
        let key = func.add_output(unit_layout);
        let value = func.add_output(x_layout);

        func.store(key, 0, Constant::Unit);

        let y = func.load(input, 0);
        func.store(value, 0, y);

        func.ret_unit();
        func.build()
    });
    let y_squared = graph.map(
        y_index,
        StreamLayout::Map(unit_layout, x_layout),
        StreamLayout::Set(x_layout),
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
    );

    let mul_sink = graph.sink(mul);
    let y_squared_sink = graph.sink(y_squared);

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
        .0
        .as_set_mut()
        .unwrap()
        .append(&mut values);

    runtime.step().unwrap();

    let (mul_sink, _) = &outputs[&mul_sink];
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

    let (y_squared_sink, _) = &outputs[&y_squared_sink];
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

    let roots = graph.source(u64x2);
    let edges = graph.source_map(u64x1, u64x1);
    let vertices = graph.source(u64x1);

    let (_recursive, distances) = graph.subgraph(|subgraph| {
        let nodes = subgraph.delayed_feedback(u64x2);

        let roots = subgraph.delta0(roots);
        let edges = subgraph.delta0(edges);

        let nodes_index = subgraph.index_with(nodes, u64x1, u64x1, {
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
        });
        let nodes_join_edges = subgraph.join_core(
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
        );

        let joined_plus_roots = subgraph.add_node(Sum::new(vec![nodes_join_edges, roots]));
        let joined_plus_roots = subgraph.index_with(joined_plus_roots, u64x1, u64x1, {
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
        });

        let min = subgraph.add_node(Min::new(joined_plus_roots));
        let min_set = subgraph.map(
            min,
            StreamLayout::Map(u64x1, u64x1),
            StreamLayout::Set(u64x2),
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
        );
        let min_set_distinct = subgraph.distinct(min_set);
        subgraph.connect_feedback(min_set_distinct, nodes);
        subgraph.export(min_set_distinct, StreamLayout::Set(u64x2))
    });

    let reachable_nodes = graph.map(
        distances,
        StreamLayout::Set(u64x2),
        StreamLayout::Set(u64x1),
        {
            let mut func = FunctionBuilder::new(graph.layout_cache().clone());
            let distance = func.add_input(u64x2);
            let output = func.add_output(u64x1);

            let node = func.load(distance, 0);
            func.store(output, 0, node);

            func.ret_unit();
            func.build()
        },
    );

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
    let unreachable_nodes = graph.map(
        unreachable_nodes,
        StreamLayout::Set(u64x1),
        StreamLayout::Set(u64x2),
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
    );

    let distances = graph.add_node(Sum::new(vec![distances, unreachable_nodes]));
    let sink = graph.sink(distances);

    let (dataflow, jit_handle, layout_cache) =
        CompiledDataflow::new(&graph, CodegenConfig::debug());
    let (mut runtime, (mut inputs, outputs)) =
        Runtime::init_circuit(1, move |circuit| dataflow.construct(circuit)).unwrap();

    {
        let u64x1_vtable = unsafe { &*jit_handle.vtables()[&u64x1] };
        let u64x1_offset = layout_cache.layout_of(u64x1).offset_of(0) as usize;
        let u64x2_vtable = unsafe { &*jit_handle.vtables()[&u64x2] };
        let u64x2_layout = layout_cache.layout_of(u64x2);

        let roots = inputs.get_mut(&roots).unwrap().0.as_set_mut().unwrap();
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
        let vertices = inputs.get_mut(&vertices).unwrap().0.as_set_mut().unwrap();
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
        let edges = inputs.get_mut(&edges).unwrap().0.as_map_mut().unwrap();
        for &(src, dest) in edge_data {
            let (mut key, mut value) = (UninitRow::new(u64x1_vtable), UninitRow::new(u64x1_vtable));

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

    runtime.step().unwrap();
    runtime.dump_profile("../../target/bfs").unwrap();
    runtime.kill().unwrap();

    {
        let u64x2_layout = layout_cache.layout_of(u64x2);
        let mut produced = Vec::new();

        let outputs = outputs[&sink].0.as_set().unwrap().consolidate();
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
