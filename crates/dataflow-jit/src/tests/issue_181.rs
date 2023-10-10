//! Test for https://github.com/feldera/feldera/issues/181

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
        "6934": {
            "Source": {
                "layout": { "Set": 1 },
                "kind": "ZSet",
                "table": "T"
            }
        },
        "6936": {
            "Map": {
                "input": 6934,
                "map_fn": {
                    "args": [
                        {
                            "id": 1,
                            "layout": 1,
                            "flags": "input"
                        },
                        {
                            "id": 2,
                            "layout": 2,
                            "flags": "output"
                        }
                    ],
                    "ret": "Unit",
                    "entry_block": 1,
                    "blocks": {
                        "1": {
                            "id": 1,
                            "body": [
                                [
                                    3,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 1,
                                            "column": 3,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    4,
                                    {
                                        "Copy": {
                                            "value": 3,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    5,
                                    {
                                        "Constant": {
                                            "String": " "
                                        }
                                    }
                                ],
                                [
                                    6,
                                    {
                                        "Call": {
                                            "function": "dbsp.str.concat_clone",
                                            "args": [
                                                4,
                                                5
                                            ],
                                            "arg_types": [
                                                {
                                                    "Scalar": "String"
                                                },
                                                {
                                                    "Scalar": "String"
                                                }
                                            ],
                                            "ret_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    7,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 1,
                                            "column": 3,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    8,
                                    {
                                        "Copy": {
                                            "value": 7,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    9,
                                    {
                                        "Call": {
                                            "function": "dbsp.str.concat_clone",
                                            "args": [
                                                6,
                                                8
                                            ],
                                            "arg_types": [
                                                {
                                                    "Scalar": "String"
                                                },
                                                {
                                                    "Scalar": "String"
                                                }
                                            ],
                                            "ret_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    10,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 2,
                                            "column": 0,
                                            "value": {
                                                "Expr": 9
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Return": {
                                    "value": {
                                        "Imm": "Unit"
                                    }
                                }
                            },
                            "params": []
                        }
                    }
                },
                "input_layout": {
                    "Set": 1
                },
                "output_layout": {
                    "Set": 2
                }
            }
        },
        "6939": {
            "Sink": {
                "input": 6936,
                "view": "V",
                "comment": "CREATE VIEW V AS SELECT T.COL4 || ' ' || T.COL4 FROM T",
                "input_layout": {
                    "Set": 2
                }
            }
        }
    },
    "layouts": {
        "1": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "I32"
                },
                {
                    "nullable": false,
                    "ty": "F64"
                },
                {
                    "nullable": false,
                    "ty": "Bool"
                },
                {
                    "nullable": false,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                }
            ]
        },
        "2": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "String"
                }
            ]
        }
    }
}"#;

#[test]
fn issue_181() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());

    circuit.append_input(
        NodeId::new(6934),
        &StreamCollection::Set(vec![
            (
                RowLiteral::new(vec![
                    NullableConstant::NonNull(Constant::I32(10)),
                    NullableConstant::NonNull(Constant::F64(1.0)),
                    NullableConstant::NonNull(Constant::Bool(false)),
                    NullableConstant::NonNull(Constant::String(String::from("Hi"))),
                    NullableConstant::Nullable(Some(Constant::I32(1))),
                    NullableConstant::Nullable(Some(Constant::F64(0.0))),
                ]),
                1,
            ),
            (
                RowLiteral::new(vec![
                    NullableConstant::NonNull(Constant::I32(10)),
                    NullableConstant::NonNull(Constant::F64(12.0)),
                    NullableConstant::NonNull(Constant::Bool(true)),
                    NullableConstant::NonNull(Constant::String(String::from("Hello"))),
                    NullableConstant::null(),
                    NullableConstant::null(),
                ]),
                1,
            ),
        ]),
    );

    circuit.step().unwrap();

    let result = circuit.consolidate_output(NodeId::new(6939));
    assert!(must_equal_sc(
        &result,
        &StreamCollection::Set(vec![
            (
                RowLiteral::new(vec![NullableConstant::NonNull(Constant::String(
                    "Hello Hello".into(),
                ))]),
                1,
            ),
            (
                RowLiteral::new(vec![NullableConstant::NonNull(Constant::String(
                    "Hi Hi".into(),
                ))]),
                1,
            )
        ])
    ));

    circuit.kill().unwrap();
}
