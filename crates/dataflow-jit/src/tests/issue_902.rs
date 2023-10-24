use crate::{
    codegen::CodegenConfig,
    facade::Demands,
    ir::{literal::StreamCollection, NodeId},
    sql_graph::SqlGraph,
    tests::must_equal_sc,
    utils, DbspCircuit,
};

#[test]
fn issue_902() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(include_str!("issue_902.json"))
        .unwrap()
        .rematerialize();

    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());
    circuit.step().unwrap();

    let result = circuit.consolidate_output(NodeId::new(1323));
    #[rustfmt::skip]
    let expected = vec![
        (row![std::f64::INFINITY, std::f64::INFINITY, ?std::f64::NAN], 1),
        (row![1.0f64, std::f64::INFINITY, ?0.0f64], 1),
        (row![std::f64::INFINITY, 1.0f64, ?std::f64::INFINITY], 1),
        (row![1.0f64, 1.0f64, ?1.0f64], 1),
    ];
    assert!(must_equal_sc(&result, &StreamCollection::Set(expected)));

    circuit.kill().unwrap();
}
