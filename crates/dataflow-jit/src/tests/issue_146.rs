//! Test for https://github.com/feldera/dbsp/issues/146

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
    "6941": {
      "Source": {
        "layout": 1,
        "table": "T"
      }
    },
    "6943": {
      "Map": {
        "input": 6941,
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
                      "column": 5,
                      "column_type": "F64"
                    }
                  }
                ],
                [
                  4,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 1,
                      "column": 5
                    }
                  }
                ],
                [
                  5,
                  {
                    "Load": {
                      "source": 1,
                      "source_layout": 1,
                      "column": 5,
                      "column_type": "F64"
                    }
                  }
                ],
                [
                  6,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 1,
                      "column": 5
                    }
                  }
                ],
                [
                  7,
                  {
                    "BinOp": {
                      "lhs": 3,
                      "rhs": 5,
                      "kind": "Div",
                      "operand_ty": "F64"
                    }
                  }
                ],
                [
                  8,
                  {
                    "BinOp": {
                      "lhs": 4,
                      "rhs": 6,
                      "kind": "Or",
                      "operand_ty": "Bool"
                    }
                  }
                ],
                [
                  9,
                  {
                    "Store": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "value": {
                        "Expr": 7
                      },
                      "value_type": "F64"
                    }
                  }
                ],
                [
                  10,
                  {
                    "SetNull": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "is_null": {
                        "Expr": 8
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
    "6946": {
      "Sink": {
        "input": 6943,
        "comment": "CREATE VIEW V AS SELECT T.COL6 / T.COL6 FROM T",
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
          "ty": "F64"
        }
      ]
    }
  }
}"#;

#[test]
fn issue_146() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());

    circuit.append_input(
        NodeId::new(6941),
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

    let result = circuit.consolidate_output(NodeId::new(6946));
    assert!(must_equal_sc(
        &result,
        &StreamCollection::Set(vec!(
            (RowLiteral::new(vec!(NullableConstant::null())), 1),
            (
                RowLiteral::new(vec!(NullableConstant::Nullable(Some(Constant::F64(
                    f64::NAN
                ))))),
                1,
            ),
        ))
    ));

    circuit.kill().unwrap();
}
