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
use rust_decimal::Decimal;
use std::str::FromStr;

static CIRCUIT: &str = r#"{
  "nodes": {
    "168": {
      "Source": {
        "layout": { "Set": 1 },
        "kind": "ZSet",
        "table": "T"
      }
    },
    "289": {
      "Map": {
        "input": 168,
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
                      "column": 0,
                      "column_type": "I32"
                    }
                  }
                ],
                [
                  4,
                  {
                    "Cast": {
                      "value": 3,
                      "from": "I32",
                      "to": "F64"
                    }
                  }
                ],
                [
                  5,
                  {
                    "Load": {
                      "source": 1,
                      "source_layout": 1,
                      "column": 1,
                      "column_type": "F64"
                    }
                  }
                ],
                [
                  6,
                  {
                    "BinOp": {
                      "lhs": 4,
                      "rhs": 5,
                      "kind": "Add",
                      "operand_ty": "F64"
                    }
                  }
                ],
                [
                  7,
                  {
                    "Load": {
                      "source": 1,
                      "source_layout": 1,
                      "column": 2,
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
                      "function": "dbsp.str.parse",
                      "args": [
                        8
                      ],
                      "arg_types": [
                        {
                          "Scalar": "String"
                        }
                      ],
                      "ret_ty": "F64"
                    }
                  }
                ],
                [
                  10,
                  {
                    "BinOp": {
                      "lhs": 6,
                      "rhs": 9,
                      "kind": "Add",
                      "operand_ty": "F64"
                    }
                  }
                ],
                [
                  11,
                  {
                    "Load": {
                      "source": 1,
                      "source_layout": 1,
                      "column": 4,
                      "column_type": "Decimal"
                    }
                  }
                ],
                [
                  12,
                  {
                    "Copy": {
                      "value": 11,
                      "value_ty": "Decimal"
                    }
                  }
                ],
                [
                  13,
                  {
                    "Cast": {
                      "value": 12,
                      "from": "Decimal",
                      "to": "F64"
                    }
                  }
                ],
                [
                  14,
                  {
                    "BinOp": {
                      "lhs": 10,
                      "rhs": 13,
                      "kind": "Add",
                      "operand_ty": "F64"
                    }
                  }
                ],
                [
                  15,
                  {
                    "Store": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "value": {
                        "Expr": 14
                      },
                      "value_type": "F64"
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
    "293": {
      "Sink": {
        "input": 289,
        "view": "V",
        "comment": "CREATE VIEW V AS SELECT T.COL1 + T.COL2 + T.COL3 + T.COL5 FROM T",
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
          "ty": "String"
        },
        {
          "nullable": true,
          "ty": "Decimal"
        },
        {
          "nullable": false,
          "ty": "Decimal"
        }
      ]
    },
    "2": {
      "columns": [
        {
          "nullable": false,
          "ty": "F64"
        }
      ]
    }
  }
}"#;

#[test]
fn issue_338() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit =
        DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new()).unwrap();

    circuit
        .append_input(
            NodeId::new(168),
            &StreamCollection::Set(vec![(
                RowLiteral::new(vec![
                    NullableConstant::NonNull(Constant::I32(10)),
                    NullableConstant::NonNull(Constant::F64(12.0)),
                    NullableConstant::NonNull(Constant::String(String::from("100100"))),
                    NullableConstant::null(),
                    NullableConstant::NonNull(Constant::Decimal(
                        Decimal::from_str("100103123").unwrap(),
                    )),
                ]),
                1i32,
            )]),
        )
        .unwrap();

    circuit.step().unwrap();

    let output = circuit.consolidate_output(NodeId::new(293)).unwrap();
    assert!(must_equal_sc(
        &output,
        &StreamCollection::Set(vec![(
            RowLiteral::new(vec![NullableConstant::NonNull(Constant::F64(1.00203245E8))]),
            1,
        )])
    ));

    circuit.kill().unwrap();
}
