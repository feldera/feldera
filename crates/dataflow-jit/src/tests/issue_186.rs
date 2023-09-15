//! Test for https://github.com/feldera/feldera/issues/186

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
    "6949": {
      "Source": {
        "layout": 1,
        "table": "T"
      }
    },
    "6951": {
      "Map": {
        "input": 6949,
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
                      "String": ""
                    }
                  }
                ],
                [
                  6,
                  {
                    "Constant": {
                      "Bool": true
                    }
                  }
                ]
              ],
              "terminator": {
                "Branch": {
                  "cond": {
                    "Expr": 6
                  },
                  "true_params": [],
                  "false_params": [],
                  "falsy": 3,
                  "truthy": 2
                }
              },
              "params": []
            },
            "2": {
              "id": 2,
              "body": [
                [
                  9,
                  {
                    "Constant": {
                      "String": ""
                    }
                  }
                ]
              ],
              "terminator": {
                "Jump": {
                  "target": 4,
                  "params": [
                    9
                  ]
                }
              },
              "params": []
            },
            "3": {
              "id": 3,
              "body": [
                [
                  7,
                  {
                    "Call": {
                      "function": "dbsp.str.concat",
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
                ]
              ],
              "terminator": {
                "Jump": {
                  "target": 4,
                  "params": [
                    7
                  ]
                }
              },
              "params": []
            },
            "4": {
              "id": 4,
              "body": [
                [
                  10,
                  {
                    "Store": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "value": {
                        "Expr": 8
                      },
                      "value_type": "String"
                    }
                  }
                ],
                [
                  11,
                  {
                    "SetNull": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "is_null": {
                        "Expr": 6
                      }
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
              "params": [
                [
                  8,
                  {
                    "Scalar": "String"
                  }
                ]
              ]
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
    "6954": {
      "Sink": {
        "input": 6951,
        "view": "V",
        "comment": "CREATE VIEW V AS SELECT T.COL4 || NULL FROM T",
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
          "nullable": true,
          "ty": "String"
        }
      ]
    }
  }
}"#;

#[test]
fn issue_186() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());

    circuit.append_input(
        NodeId::new(6949),
        &StreamCollection::Set(vec![
            (
                RowLiteral::new(vec![
                    NullableConstant::NonNull(Constant::I32(10)),
                    NullableConstant::NonNull(Constant::F64(12.0)),
                    NullableConstant::NonNull(Constant::Bool(true)),
                    NullableConstant::NonNull(Constant::String(String::from("Hi"))),
                    NullableConstant::null(),
                    NullableConstant::null(),
                ]),
                1,
            ),
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
        ]),
    );

    circuit.step().unwrap();

    let output = circuit.consolidate_output(NodeId::new(6954));
    assert!(must_equal_sc(
        &output,
        &StreamCollection::Set(vec![(RowLiteral::new(vec![NullableConstant::null()]), 2)])
    ));

    circuit.kill().unwrap();
}
