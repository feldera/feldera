#![cfg(test)]

use crate::{
    codegen::{Codegen, CodegenConfig},
    dataflow::CompiledDataflow,
    ir::{
        exprs::Constant,
        function::FunctionBuilder,
        graph::{Graph, GraphExt},
        literal::{NullableConstant, RowLiteral},
        nodes::{Differentiate, Fold, IndexWith, Neg, Source, StreamLayout, Sum},
        row_layout::{RowLayout, RowLayoutBuilder},
        types::ColumnType,
        validate::Validator,
    },
    row::{Row, UninitRow},
};
use dbsp::{
    trace::{Batch, BatchReader, Builder, Cursor},
    OrdZSet, Runtime,
};

// ```sql
// CREATE VIEW V AS SELECT Sum(r.COL1 * r.COL5)
// FROM T r
// WHERE 0.5 * (SELECT Sum(r1.COL5) FROM T r1)
//     = (SELECT Sum(r2.COL5) FROM T r2 WHERE r2.COL1 = r.COL1)
// ```
#[test]
fn complex() {
    let mut graph = Graph::new();

    let unit_layout = graph.layout_cache().unit();
    let weight_layout = graph.layout_cache().add(RowLayout::weight());
    let nullable_i32 = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::I32, true)
            .build(),
    );

    // let T = circuit.add_source(T);
    let source_row = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::I32, false)
            .with_column(ColumnType::F64, false)
            .with_column(ColumnType::Bool, false)
            .with_column(ColumnType::String, false)
            .with_column(ColumnType::I32, true)
            .with_column(ColumnType::F64, true)
            .build(),
    );
    let source = graph.add_node(Source::new(source_row, None));

    // ```
    // let stream7850: Stream<_, OrdZSet<Tuple1<Option<i32>>, Weight>> = T.map(
    //     move |t: &Tuple6<i32, F64, bool, String, Option<i32>, Option<F64>>| -> Tuple1<Option<i32>> {
    //         Tuple1::new(t.4)
    //     },
    // );
    // ```
    let stream7850 = graph.map(
        source,
        StreamLayout::Set(source_row),
        StreamLayout::Set(nullable_i32),
        {
            let mut func = FunctionBuilder::new(graph.layout_cache().clone());
            let input = func.add_input(source_row);
            let output = func.add_output(nullable_i32);

            // Set the output row to null if the input value is null
            let value_is_null = func.is_null(input, 4);
            func.set_null(output, 0, value_is_null);

            // Copy the value to the output row
            let value = func.load(input, 4);
            func.store(output, 0, value);

            func.ret_unit();
            func.build()
        },
    );

    // ```
    // let stream7856: Stream<_, OrdIndexedZSet<(), Tuple1<Option<i32>>, Weight>> = stream7850
    //     .index_with(
    //         move |t: &Tuple1<Option<i32>>| -> ((), Tuple1<Option<i32>>) { ((), Tuple1::new(t.0)) },
    //     );
    // ```
    let stream7856 = graph.add_node(IndexWith::new(
        stream7850,
        {
            let mut func = FunctionBuilder::new(graph.layout_cache().clone());
            let input = func.add_input(nullable_i32);
            let key = func.add_output(unit_layout);
            let value = func.add_output(nullable_i32);

            func.store(key, 0, Constant::Unit);

            // Set the output row to null if the input value is null
            let value_is_null = func.is_null(input, 0);
            func.set_null(value, 0, value_is_null);

            // Copy the value to the output row
            let input_val = func.load(input, 0);
            func.store(value, 0, input_val);

            func.ret_unit();
            func.build()
        },
        unit_layout,
        nullable_i32,
    ));

    // ```
    // let stream7861: Stream<_, OrdIndexedZSet<(), Tuple1<Option<i32>>, Weight>> = stream7856
    //     .aggregate::<(), _>(
    //         Fold::<_, UnimplementedSemigroup<Tuple1<Option<i32>>>, _, _>::with_output(
    //             (None::<i32>,),
    //             move |a: &mut (Option<i32>,), v: &Tuple1<Option<i32>>, w: Weight| {
    //                 *a = (move |a: Option<i32>,
    //                             v: &Tuple1<Option<i32>>,
    //                             w: Weight|
    //                       -> Option<i32> {
    //                     agg_plus_N_N(a, v.0.mul_by_ref(&w))
    //                 }(a.0, v, w),)
    //             },
    //             move |a: (Option<i32>,)| -> Tuple1<Option<i32>> {
    //                 Tuple1::new(identity::<Option<i32>>(a.0))
    //             },
    //         ),
    //     );
    // ```
    let stream7861 = graph.add_node(Fold::new(
        stream7856,
        RowLiteral::new(vec![NullableConstant::null()]),
        // FIXME: Fully move over to mutable `acc` instead of a return value
        // Equivalent to
        // ```rust
        // fn(mut acc, current, weight) {
        //     set_null(acc, is_null(acc) & is_null(current));
        //
        //     if is_null(current) {
        //         return;
        //     } else {
        //         let diff = current * weight;
        //         if is_null(acc) {
        //             insert diff into acc;
        //             return;
        //         } else {
        //             let sum = acc + diff;
        //             insert sum into acc;
        //             return
        //         }
        //     }
        // }
        // ```
        {
            let mut func = FunctionBuilder::new(graph.layout_cache().clone());
            let accumulator = func.add_output(nullable_i32);
            let current = func.add_input(nullable_i32);
            let weight = func.add_input(weight_layout);

            let acc_is_null = func.is_null(accumulator, 0);
            let current_is_null = func.is_null(current, 0);
            let acc_or_current_null = func.and(acc_is_null, current_is_null);
            func.set_null(accumulator, 0, acc_or_current_null);

            let current_null = func.create_block();
            let current_non_null = func.create_block();
            func.branch(current_is_null, current_null, [], current_non_null, []);

            func.move_to(current_null);
            func.ret_unit();
            func.seal_current();

            let acc_null = func.create_block();
            let acc_non_null = func.create_block();

            func.move_to(current_non_null);
            let current = func.load(current, 0);
            let weight = func.load(weight, 0);
            let diff = func.mul(current, weight);
            func.branch(acc_is_null, acc_null, [], acc_non_null, []);
            func.seal_current();

            func.move_to(acc_null);
            func.store(accumulator, 0, diff);
            func.ret_unit();
            func.seal_current();

            func.move_to(acc_non_null);
            let acc = func.load(accumulator, 0);
            let sum = func.add(acc, diff);
            func.store(accumulator, 0, sum);
            func.ret_unit();
            func.seal_current();

            func.build()
        },
        // Just a unit closure
        {
            let mut func = FunctionBuilder::new(graph.layout_cache().clone());
            let input = func.add_input(nullable_i32);
            let output = func.add_output(nullable_i32);

            // Set the output row to null if the input value is null
            let value_is_null = func.is_null(input, 0);
            func.set_null(output, 0, value_is_null);

            // Unconditionally copy the value into the output row
            let value = func.load(input, 0);
            func.store(output, 0, value);

            func.ret_unit();
            func.seal_current();

            func.build()
        },
        nullable_i32,
        nullable_i32,
        nullable_i32,
    ));

    // ```
    // let stream7866: Stream<_, OrdZSet<Tuple1<Option<i32>>, Weight>> = stream7861.map(
    //     move |(k, v): (&(), &Tuple1<Option<i32>>)| -> Tuple1<Option<i32>> { Tuple1::new(v.0) },
    // );
    // ```
    let stream7866_layout = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::Unit, false)
            .with_column(ColumnType::I32, true)
            .build(),
    );
    let stream7866 = graph.map(
        stream7861,
        StreamLayout::Set(stream7866_layout),
        StreamLayout::Set(nullable_i32),
        {
            let mut func = FunctionBuilder::new(graph.layout_cache().clone());
            let input = func.add_input(stream7866_layout);
            let output = func.add_output(nullable_i32);

            // Set the output row to null if the input value is null
            let value_is_null = func.is_null(input, 1);
            func.set_null(output, 0, value_is_null);

            // Unconditionally copy the value into the output row
            let value = func.load(input, 1);
            func.store(output, 0, value);

            func.ret_unit();
            func.seal_current();

            func.build()
        },
    );

    // ```
    // let stream7874: Stream<_, OrdZSet<Tuple1<Option<i32>>, Weight>> =
    //     stream7866.map(move |_t: _| -> Tuple1<Option<i32>> { Tuple1::new(None::<i32>) });
    // ```
    let stream7874 = graph.map(
        stream7866,
        StreamLayout::Set(nullable_i32),
        StreamLayout::Set(nullable_i32),
        {
            let mut func = FunctionBuilder::new(graph.layout_cache().clone());
            let _input = func.add_input(nullable_i32);
            let output = func.add_output(nullable_i32);

            func.set_null(output, 0, Constant::Bool(true));
            func.ret_unit();
            func.build()
        },
    );

    // ```
    // let stream7879: Stream<_, OrdZSet<Tuple1<Option<i32>>, Weight>> = stream7874.neg();
    // ```
    let stream7879 = graph.add_node(Neg::new(stream7874, StreamLayout::Set(nullable_i32)));

    // TODO: Constant sources/generators
    // ```
    // let stream7130 = circuit.add_source(Generator::new(|| zset!(
    //     Tuple1::new(None::<i32>) => 1,
    // )));
    // ```
    let stream7130 = graph.add_node(Source::new(nullable_i32, None));

    // ```
    // let stream7883: Stream<_, OrdZSet<Tuple1<Option<i32>>, Weight>> = stream7130.differentiate();
    // ```
    let stream7883 = graph.add_node(Differentiate::new(
        stream7130,
        StreamLayout::Set(nullable_i32),
    ));

    // ```
    // let stream7887: Stream<_, OrdZSet<Tuple1<Option<i32>>, Weight>> =
    //     stream7883.sum([&stream7879, &stream7866]);
    // ```
    let stream7887 = graph.add_node(Sum::new(
        vec![stream7883, stream7879, stream7866],
        StreamLayout::Set(nullable_i32),
    ));

    // ```
    // let stream7892: Stream<_, OrdIndexedZSet<(), Tuple6<i32, F64, bool, String, Option<i32>, Option<F64>>, Weight>> =
    //     T.index_with(move |l: &Tuple6<i32, F64, bool, String, Option<i32>, Option<F64>>| -> ((), Tuple6<i32, F64, bool, String, Option<i32>, Option<F64>>) {
    //         ((), Tuple6::new(l.0, l.1, l.2, l.3.clone(), l.4, l.5))
    //     });
    // ```
    let _stream7892 = graph.add_node(IndexWith::new(
        source,
        {
            let mut func = FunctionBuilder::new(graph.layout_cache().clone());
            let input = func.add_input(source_row);
            let key = func.add_output(unit_layout);
            let value = func.add_output(source_row);

            func.store(key, 0, Constant::Unit);
            func.copy_row_to(input, value);
            func.ret_unit();
            func.build()
        },
        unit_layout,
        source_row,
    ));

    // ```
    // let stream7897: Stream<_, OrdIndexedZSet<(), Tuple1<Option<i32>>, Weight>> = stream7887
    //     .index_with(
    //         move |r: &Tuple1<Option<i32>>| -> ((), Tuple1<Option<i32>>) { ((), Tuple1::new(r.0)) },
    //     );
    // ```
    let _stream7897 = graph.add_node(IndexWith::new(
        stream7887,
        {
            let mut func = FunctionBuilder::new(graph.layout_cache().clone());
            let input = func.add_input(nullable_i32);
            let key = func.add_output(unit_layout);
            let value = func.add_output(nullable_i32);

            func.store(key, 0, Constant::Unit);
            func.copy_row_to(input, value);
            func.ret_unit();
            func.build()
        },
        unit_layout,
        nullable_i32,
    ));

    println!("Pre-opt: {graph:#?}");
    graph.optimize();
    println!("Post-opt: {graph:#?}");

    {
        let mut codegen = Codegen::new(graph.layout_cache().clone(), CodegenConfig::debug());

        let mut functions = Vec::new();
        graph.functions(&mut functions);

        // TODO: Deduplicate functions
        for func in functions {
            codegen.codegen_func("fn", func);
        }
    }
}

#[test]
fn mapping() {
    let mut graph = Graph::new();

    let xy_layout = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::U32, false)
            .with_column(ColumnType::U32, false)
            .build(),
    );
    let source = graph.add_node(Source::new(xy_layout, None));

    let x_layout = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::U32, false)
            .build(),
    );
    let map = graph.map(
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

    let sink = graph.sink(map, "X", StreamLayout::Set(x_layout));

    let mut validator = Validator::new(graph.layout_cache().clone());
    validator.validate_graph(&graph).unwrap();

    graph.optimize();

    validator.validate_graph(&graph).unwrap();

    let (dataflow, jit_handle, layout_cache) =
        CompiledDataflow::new(&graph, CodegenConfig::debug(), |_| ());

    {
        let (mut runtime, (mut inputs, outputs)) =
            Runtime::init_circuit(1, move |circuit| dataflow.construct(circuit)).unwrap();

        let (xy_x_offset, xy_y_offset) = {
            let xy_layout = layout_cache.layout_of(xy_layout);
            (
                xy_layout.offset_of(0) as usize,
                xy_layout.offset_of(1) as usize,
            )
        };

        let mut values = Vec::new();
        for (x, y) in [(1, 2), (0, 0), (1000, 2000), (12, 12)] {
            unsafe {
                let mut row = UninitRow::new(&*jit_handle.vtables()[&xy_layout]);
                row.as_mut_ptr().add(xy_x_offset).cast::<u32>().write(x);
                row.as_mut_ptr().add(xy_y_offset).cast::<u32>().write(y);

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

        let output = outputs
            .get(&sink)
            .unwrap()
            .0
            .as_set()
            .unwrap()
            .consolidate();
        let mut cursor = output.cursor();
        while cursor.key_valid() {
            let weight = cursor.weight();
            let key = cursor.key();
            println!("{key:?}: {weight}");

            cursor.step_key();
        }

        let x_x_offset = layout_cache.layout_of(x_layout).offset_of(0) as usize;

        let mut expected = <OrdZSet<Row, i32> as Batch>::Builder::new_builder(());
        for (key, weight) in [(0, 1), (2, 1), (144, 1), (2_000_000, 1)] {
            unsafe {
                let mut row = UninitRow::new(&*jit_handle.vtables()[&x_layout]);
                row.as_mut_ptr().add(x_x_offset).cast::<u32>().write(key);

                expected.push((row.assume_init(), weight));
            }
        }
        assert_eq!(output, expected.done());

        runtime.kill().unwrap();
    }

    unsafe { jit_handle.free_memory() }
}
