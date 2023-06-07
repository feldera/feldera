use crate::{
    codegen::CodegenConfig,
    facade::Demands,
    ir::{
        exprs::{Call, RowOrScalar},
        literal::{NullableConstant, RowLiteral, StreamCollection},
        nodes::StreamLayout,
        ColumnType, Constant, Graph, GraphExt, RowLayoutBuilder,
    },
    utils, DbspCircuit,
};

#[test]
fn issue_195() {
    utils::test_logger();

    let mut graph = Graph::new();

    let layout = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::I32, false)
            .with_column(ColumnType::U32, false)
            .with_column(ColumnType::F32, false)
            .with_column(ColumnType::F64, false)
            .with_column(ColumnType::Bool, false)
            .with_column(ColumnType::String, false)
            .build(),
    );

    let source = graph.source(layout);

    let printed = graph.map(
        source,
        StreamLayout::Set(layout),
        StreamLayout::Set(layout),
        {
            let mut builder = graph.function_builder();
            let input = builder.add_input(layout);
            let output = builder.add_output(layout);

            let capacity = builder.constant(Constant::Usize(256));
            let mut target_string = builder.add_expr(Call::new(
                "dbsp.str.with.capacity".into(),
                vec![capacity],
                vec![RowOrScalar::Scalar(ColumnType::Usize)],
                ColumnType::String,
            ));

            let space = builder.constant(Constant::String(" ".into()));

            for (column, ty) in [
                ColumnType::I32,
                ColumnType::U32,
                ColumnType::F32,
                ColumnType::F64,
                ColumnType::Bool,
                ColumnType::String,
            ]
            .into_iter()
            .enumerate()
            {
                let value = builder.load(input, column);

                let string = builder.add_expr(Call::new(
                    "dbsp.str.write".into(),
                    vec![target_string, value],
                    vec![
                        RowOrScalar::Scalar(ColumnType::String),
                        RowOrScalar::Scalar(ty),
                    ],
                    ColumnType::String,
                ));

                let string = builder.add_expr(Call::new(
                    "dbsp.str.write".into(),
                    vec![string, space],
                    vec![
                        RowOrScalar::Scalar(ColumnType::String),
                        RowOrScalar::Scalar(ColumnType::String),
                    ],
                    ColumnType::String,
                ));

                target_string = string;
            }

            builder.add_expr(Call::new(
                "dbsp.io.str.print".into(),
                vec![target_string],
                vec![RowOrScalar::Scalar(ColumnType::String)],
                ColumnType::Unit,
            ));

            builder.copy_row_to(input, output);
            builder.ret_unit();
            builder.build()
        },
    );

    let _sink = graph.sink(printed, StreamLayout::Set(layout));

    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());

    circuit.append_input(
        source,
        &StreamCollection::Set(vec![(
            RowLiteral::new(vec![
                NullableConstant::NonNull(Constant::I32(-10)),
                NullableConstant::NonNull(Constant::U32(10)),
                NullableConstant::NonNull(Constant::F32(f32::MIN)),
                NullableConstant::NonNull(Constant::F64(12.0)),
                NullableConstant::NonNull(Constant::Bool(true)),
                NullableConstant::NonNull(Constant::String("foobar".into())),
            ]),
            1,
        )]),
    );

    circuit.step().unwrap();
    circuit.kill().unwrap();
}
