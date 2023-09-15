#![cfg(test)]

use crate::{
    codegen::CodegenConfig,
    facade::Demands,
    ir::{
        literal::{NullableConstant, RowLiteral, StreamCollection},
        nodes::{IndexByColumn, StreamKind, StreamLayout},
        ColumnType, Constant, Graph, GraphExt, NodeId, RowLayoutBuilder,
    },
    sql_graph::SqlGraph,
    utils, DbspCircuit,
};
use std::path::Path;

#[test]
fn time_series_enrich_e2e() {
    utils::test_logger();

    // Deserialize the graph from json
    let graph = serde_json::from_str::<SqlGraph>(TIME_SERIES_ENRICH_SRC)
        .unwrap()
        .rematerialize();

    let transactions_layout = graph.nodes()[&TRANSACTIONS_ID]
        .clone()
        .unwrap_source()
        .layout();
    let demographics_layout = graph.nodes()[&DEMOGRAPHICS_ID]
        .clone()
        .unwrap_source()
        .layout();

    let mut demands = Demands::new();
    demands.add_csv_deserialize(transactions_layout, transaction_mappings());
    demands.add_csv_deserialize(demographics_layout, demographic_mappings());

    // Create the circuit
    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), demands);

    // Ingest data
    circuit.append_csv_input(
        TRANSACTIONS_ID,
        &Path::new(PATH).join("transactions_20K.csv"),
    );
    circuit.append_csv_input(DEMOGRAPHICS_ID, &Path::new(PATH).join("demographics.csv"));

    // Step the circuit
    circuit.step().unwrap();

    // TODO: Inspect outputs
    let _output = circuit.consolidate_output(SINK_ID);

    // Shut down the circuit
    circuit.kill().unwrap();
}

#[test]
fn time_series_enrich_e2e_2() {
    utils::test_logger();

    // Deserialize the graph from json
    let mut graph = Graph::new();

    let unit_layout = graph.layout_cache().unit();
    let demographics_layout = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::F64, false)
            .with_column(ColumnType::String, true)
            .with_column(ColumnType::String, true)
            .with_column(ColumnType::String, true)
            .with_column(ColumnType::String, true)
            .with_column(ColumnType::String, true)
            .with_column(ColumnType::I32, true)
            .with_column(ColumnType::F64, true)
            .with_column(ColumnType::F64, true)
            .with_column(ColumnType::I32, true)
            .with_column(ColumnType::String, true)
            .with_column(ColumnType::Date, true)
            .build(),
    );
    let transactions_layout = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::Timestamp, false)
            .with_column(ColumnType::F64, false)
            .with_column(ColumnType::String, true)
            .with_column(ColumnType::String, true)
            .with_column(ColumnType::F64, true)
            .with_column(ColumnType::String, true)
            .with_column(ColumnType::I32, true)
            .with_column(ColumnType::F64, true)
            .with_column(ColumnType::F64, true)
            .with_column(ColumnType::I32, true)
            .build(),
    );
    let key_layout = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::F64, false)
            .build(),
    );
    let culled_demographics_layout = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::String, true)
            .with_column(ColumnType::String, true)
            .build(),
    );
    let culled_transactions_layout = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::Timestamp, false)
            .build(),
    );
    let output_layout = graph.layout_cache().add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::Timestamp, false)
            .with_column(ColumnType::F64, false)
            .with_column(ColumnType::String, true)
            .with_column(ColumnType::String, true)
            .build(),
    );

    let demographics_src = graph.source(demographics_layout);
    let transactions_src = graph.source(transactions_layout);

    let indexed_demographics = graph.add_node(IndexByColumn::new(
        demographics_src,
        demographics_layout,
        0,
        vec![2, 3, 5, 6, 7, 8, 9, 10, 11],
        key_layout,
        culled_demographics_layout,
    ));
    let indexed_transactions = graph.add_node(IndexByColumn::new(
        transactions_src,
        transactions_layout,
        1,
        vec![2, 3, 4, 5, 6, 7, 8, 9],
        key_layout,
        culled_transactions_layout,
    ));

    let transactions_join_demographics = graph.join_core(
        indexed_transactions,
        indexed_demographics,
        {
            let mut builder = graph.function_builder();

            let key = builder.add_input(key_layout);
            let transaction = builder.add_input(culled_transactions_layout);
            let demographic = builder.add_input(culled_demographics_layout);
            let output = builder.add_output(output_layout);
            let _unit_output = builder.add_output(unit_layout);

            let trans_date_trans_time = builder.load(transaction, 0);
            let cc_num = builder.load(key, 0);
            builder.store(output, 0, trans_date_trans_time);
            builder.store(output, 1, cc_num);

            {
                let first_not_null = builder.create_block();
                let after = builder.create_block();

                let first_null = builder.is_null(demographic, 0);
                builder.set_null(output, 2, first_null);
                builder.branch(first_null, after, [], first_not_null, []);

                builder.move_to(first_not_null);
                let first = builder.load(demographic, 0);
                let first = builder.copy(first);
                builder.store(output, 2, first);
                builder.jump(after, []);

                builder.move_to(after);
            }

            {
                let city_not_null = builder.create_block();
                let after = builder.create_block();

                let city_null = builder.is_null(demographic, 1);
                builder.set_null(output, 3, city_null);
                builder.branch(city_null, after, [], city_not_null, []);

                builder.move_to(city_not_null);
                let city = builder.load(demographic, 1);
                let city = builder.copy(city);
                builder.store(output, 3, city);
                builder.jump(after, []);

                builder.move_to(after);
            }

            builder.ret_unit();
            builder.build()
        },
        output_layout,
        unit_layout,
        StreamKind::Set,
    );

    let sink = graph.sink(
        transactions_join_demographics,
        "transactions_join_demographics",
        StreamLayout::Set(output_layout),
    );

    let mut demands = Demands::new();
    demands.add_csv_deserialize(transactions_layout, transaction_mappings());
    demands.add_csv_deserialize(demographics_layout, demographic_mappings());

    // Create the circuit
    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), demands);

    // Ingest data
    circuit.append_csv_input(
        transactions_src,
        &Path::new(PATH).join("transactions_20K.csv"),
    );
    circuit.append_csv_input(demographics_src, &Path::new(PATH).join("demographics.csv"));

    // Step the circuit
    circuit.step().unwrap();

    // TODO: Inspect outputs
    let _output = circuit.consolidate_output(sink);

    // Shut down the circuit
    circuit.kill().unwrap();
}

const PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../demo/project_demo01-TimeSeriesEnrich",
);

fn transaction_mappings() -> Vec<(usize, usize, Option<String>)> {
    vec![
        (0, 0, Some("%F %T".into())),
        (1, 1, None),
        (2, 2, None),
        (3, 3, None),
        (4, 4, None),
        (5, 5, None),
        (6, 6, None),
        (7, 7, None),
        (8, 8, None),
        (9, 9, None),
    ]
}

fn demographic_mappings() -> Vec<(usize, usize, Option<String>)> {
    vec![
        (0, 0, None),
        (1, 1, None),
        (2, 2, None),
        (3, 3, None),
        (4, 4, None),
        (5, 5, None),
        (6, 6, None),
        (7, 7, None),
        (8, 8, None),
        (9, 9, None),
        (10, 10, None),
        (11, 11, Some("%F".into())),
    ]
}

const TRANSACTIONS_ID: NodeId = NodeId::new(54);
const DEMOGRAPHICS_ID: NodeId = NodeId::new(68);
const SINK_ID: NodeId = NodeId::new(273);

static TIME_SERIES_ENRICH_SRC: &str = include_str!("time_series_enrich.json");
static CONSTANT_STREAM_TEST: &str = include_str!("constant_stream.json");
static UNUSED_SOURCE: &str = include_str!("unused_source.json");

#[test]
fn constant_stream() {
    utils::test_logger();

    // Deserialize the graph from json
    let graph = serde_json::from_str::<SqlGraph>(CONSTANT_STREAM_TEST)
        .unwrap()
        .rematerialize();

    // Create the circuit
    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());

    // Step the circuit
    circuit.step().unwrap();

    // Inspect outputs
    let output = circuit.consolidate_output(NodeId::new(2));

    // Shut down the circuit
    circuit.kill().unwrap();

    // Ensure the output is correct
    let expected = StreamCollection::Set(vec![(
        RowLiteral::new(vec![NullableConstant::NonNull(Constant::U32(1))]),
        1,
    )]);
    assert_eq!(output, expected);
}

#[test]
fn append_unused_source() {
    utils::test_logger();

    // Deserialize the graph from json
    let graph = serde_json::from_str::<SqlGraph>(UNUSED_SOURCE)
        .unwrap()
        .rematerialize();

    // Create the circuit
    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());

    // Feed data to our unused input
    circuit.append_input(
        NodeId::new(1),
        &StreamCollection::Set(vec![(
            RowLiteral::new(vec![
                NullableConstant::NonNull(Constant::I32(1)),
                NullableConstant::NonNull(Constant::F64(1.0)),
                NullableConstant::NonNull(Constant::Bool(true)),
                NullableConstant::NonNull(Constant::String("foobar".into())),
                NullableConstant::Nullable(Some(Constant::I32(1))),
                NullableConstant::Nullable(Some(Constant::F64(1.0))),
            ]),
            1,
        )]),
    );

    // Step the circuit
    circuit.step().unwrap();

    let output = circuit.consolidate_output(NodeId::new(3));

    // Kill the circuit
    circuit.kill().unwrap();

    // Ensure the output is correct
    let expected = StreamCollection::Set(vec![(
        RowLiteral::new(vec![NullableConstant::NonNull(Constant::I32(0))]),
        1,
    )]);
    assert_eq!(output, expected);
}
