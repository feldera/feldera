//! Test for https://github.com/feldera/dbsp/issues/288

use chrono::{NaiveDate, NaiveDateTime};

use crate::{
    codegen::CodegenConfig,
    facade::Demands,
    ir::{
        literal::{NullableConstant, RowLiteral, StreamCollection},
        Constant, NodeId,
    },
    sql_graph::SqlGraph,
    tests::must_equal_sc,
    utils, DbspCircuit,
};

const CIRCUIT: &str = r#"{
    "nodes": {
        "1": {
            "ConstantStream": {
                "layout": {
                    "Set": 1
                },
                "value": {
                    "layout": {
                        "Set": 1
                    },
                    "value": {
                        "Set": [
                            [
                                {
                                    "rows": [
                                        {
                                            "NonNull": {
                                                "Date": "2001-07-08"
                                            }
                                        },
                                        {
                                            "NonNull": {
                                                "Timestamp": "2001-07-08T00:34:59.026+09:30"
                                            }
                                        }
                                    ]
                                },
                                1
                            ]
                        ]
                    }
                }
            }
        },
        "2": {
            "Sink": {
                "input": 1,
                "input_layout": {
                    "Set": 1
                }
            }
        }
    },
    "layouts": {
        "1": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "Date"
                },
                {
                    "nullable": false,
                    "ty": "Timestamp"
                }
            ]
        }
    }
}"#;

#[test]
fn issue_288() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());

    circuit.step().unwrap();

    let result = circuit.consolidate_output(NodeId::new(2));
    assert!(must_equal_sc(
        &result,
        &StreamCollection::Set(vec![(
            RowLiteral::new(vec![
                NullableConstant::NonNull(Constant::Date(
                    NaiveDate::parse_from_str("2001-07-08", "%F").unwrap(),
                )),
                NullableConstant::NonNull(Constant::Timestamp(
                    NaiveDateTime::parse_from_str("2001-07-08T00:34:59.026+09:30", "%+").unwrap(),
                )),
            ]),
            1,
        )]),
    ));

    circuit.kill().unwrap();
}
