//! Test for https://github.com/feldera/feldera/issues/147

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
    "14710": {
      "Source": {
        "layout": { "Set": 1 },
        "kind": "ZSet",
        "table": "T"
      }
    },
    "14712": {
      "Map": {
        "input": 14710,
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
                      "column": 4,
                      "column_type": "I32"
                    }
                  }
                ],
                [
                  4,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 1,
                      "column": 4
                    }
                  }
                ],
                [
                  5,
                  {
                    "Store": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "value": {
                        "Expr": 3
                      },
                      "value_type": "I32"
                    }
                  }
                ],
                [
                  6,
                  {
                    "SetNull": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "is_null": {
                        "Expr": 4
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
    "14715": {
      "IndexWith": {
        "input": 14712,
        "index_fn": {
          "args": [
            {
              "id": 1,
              "layout": 2,
              "flags": "input"
            },
            {
              "id": 2,
              "layout": 3,
              "flags": "output"
            },
            {
              "id": 3,
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
                  4,
                  {
                    "Load": {
                      "source": 1,
                      "source_layout": 2,
                      "column": 0,
                      "column_type": "I32"
                    }
                  }
                ],
                [
                  5,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 2,
                      "column": 0
                    }
                  }
                ],
                [
                  6,
                  {
                    "Store": {
                      "target": 3,
                      "target_layout": 2,
                      "column": 0,
                      "value": {
                        "Expr": 4
                      },
                      "value_type": "I32"
                    }
                  }
                ],
                [
                  7,
                  {
                    "SetNull": {
                      "target": 3,
                      "target_layout": 2,
                      "column": 0,
                      "is_null": {
                        "Expr": 5
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
        "key_layout": 3,
        "value_layout": 2
      }
    },
    "14722": {
      "Fold": {
        "input": 14715,
        "acc_layout": 2,
        "step_layout": 2,
        "output_layout": 2,
        "finish_fn": {
          "args": [
            {
              "id": 1,
              "layout": 2,
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
                      "source_layout": 2,
                      "column": 0,
                      "column_type": "I32"
                    }
                  }
                ],
                [
                  4,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 2,
                      "column": 0
                    }
                  }
                ],
                [
                  5,
                  {
                    "Store": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "value": {
                        "Expr": 3
                      },
                      "value_type": "I32"
                    }
                  }
                ],
                [
                  6,
                  {
                    "SetNull": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "is_null": {
                        "Expr": 4
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
        "step_fn": {
          "args": [
            {
              "id": 1,
              "layout": 2,
              "flags": "inout"
            },
            {
              "id": 2,
              "layout": 2,
              "flags": "input"
            },
            {
              "id": 3,
              "layout": 4,
              "flags": "input"
            }
          ],
          "ret": "Unit",
          "entry_block": 1,
          "blocks": {
            "1": {
              "id": 1,
              "body": [
                [
                  5,
                  {
                    "Load": {
                      "source": 3,
                      "source_layout": 4,
                      "column": 0,
                      "column_type": "I32"
                    }
                  }
                ]
              ],
              "terminator": {
                "Jump": {
                  "target": 2,
                  "params": []
                }
              },
              "params": []
            },
            "2": {
              "id": 2,
              "body": [
                [
                  6,
                  {
                    "Load": {
                      "source": 1,
                      "source_layout": 2,
                      "column": 0,
                      "column_type": "I32"
                    }
                  }
                ],
                [
                  7,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 2,
                      "column": 0
                    }
                  }
                ],
                [
                  8,
                  {
                    "Load": {
                      "source": 2,
                      "source_layout": 2,
                      "column": 0,
                      "column_type": "I32"
                    }
                  }
                ],
                [
                  9,
                  {
                    "IsNull": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0
                    }
                  }
                ],
                [
                  10,
                  {
                    "BinOp": {
                      "lhs": 8,
                      "rhs": 5,
                      "kind": "Mul",
                      "operand_ty": "I32"
                    }
                  }
                ],
                [
                  11,
                  {
                    "BinOp": {
                      "lhs": 6,
                      "rhs": 10,
                      "kind": "Add",
                      "operand_ty": "I32"
                    }
                  }
                ],
                [
                  12,
                  {
                    "BinOp": {
                      "lhs": 7,
                      "rhs": 9,
                      "kind": "And",
                      "operand_ty": "Bool"
                    }
                  }
                ]
              ],
              "terminator": {
                "Jump": {
                  "target": 3,
                  "params": []
                }
              },
              "params": []
            },
            "3": {
              "id": 3,
              "body": [
                [
                  13,
                  {
                    "Store": {
                      "target": 1,
                      "target_layout": 2,
                      "column": 0,
                      "value": {
                        "Expr": 11
                      },
                      "value_type": "I32"
                    }
                  }
                ],
                [
                  14,
                  {
                    "SetNull": {
                      "target": 1,
                      "target_layout": 2,
                      "column": 0,
                      "is_null": {
                        "Expr": 12
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
        "init": {
          "rows": [
            {
              "Nullable": null
            }
          ]
        }
      }
    },
    "14724": {
      "Map": {
        "input": 14722,
        "map_fn": {
          "args": [
            {
              "id": 1,
              "layout": 3,
              "flags": "input"
            },
            {
              "id": 2,
              "layout": 2,
              "flags": "input"
            },
            {
              "id": 3,
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
                  4,
                  {
                    "Load": {
                      "source": 2,
                      "source_layout": 2,
                      "column": 0,
                      "column_type": "I32"
                    }
                  }
                ],
                [
                  5,
                  {
                    "IsNull": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0
                    }
                  }
                ],
                [
                  6,
                  {
                    "Store": {
                      "target": 3,
                      "target_layout": 2,
                      "column": 0,
                      "value": {
                        "Expr": 4
                      },
                      "value_type": "I32"
                    }
                  }
                ],
                [
                  7,
                  {
                    "SetNull": {
                      "target": 3,
                      "target_layout": 2,
                      "column": 0,
                      "is_null": {
                        "Expr": 5
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
          "Map": [
            3,
            2
          ]
        },
        "output_layout": {
          "Set": 2
        }
      }
    },
    "14729": {
      "Map": {
        "input": 14724,
        "map_fn": {
          "args": [
            {
              "id": 1,
              "layout": 2,
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
                    "Constant": {
                      "I32": 0
                    }
                  }
                ],
                [
                  4,
                  {
                    "Constant": {
                      "Bool": true
                    }
                  }
                ],
                [
                  5,
                  {
                    "Store": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "value": {
                        "Expr": 3
                      },
                      "value_type": "I32"
                    }
                  }
                ],
                [
                  6,
                  {
                    "SetNull": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "is_null": {
                        "Expr": 4
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
          "Set": 2
        },
        "output_layout": {
          "Set": 2
        }
      }
    },
    "14732": {
      "Neg": {
        "input": 14729,
        "layout": {
          "Set": 2
        }
      }
    },
    "14735": {
      "ConstantStream": {
        "comment": "zset!(\n    Tuple1::new(None::<i32>) => 1,\n)",
        "layout": {
          "Set": 2
        },
        "value": {
          "layout": {
            "Set": 2
          },
          "value": {
            "Set": [
              [
                {
                  "rows": [
                    {
                      "Nullable": null
                    }
                  ]
                },
                1
              ]
            ]
          }
        },
        "consolidated": false
      }
    },
    "14737": {
      "Sum": {
        "layout": {
          "Set": 2
        },
        "inputs": [
          14735,
          14732,
          14724
        ]
      }
    },
    "14739": {
      "Sink": {
        "input": 14737,
        "view": "V",
        "comment": "CREATE VIEW V AS SELECT SUM(T.COL5) FROM T",
        "input_layout": {
          "Set": 2
        }
      }
    }
  },
  "layouts": {
    "3": {
      "columns": [
        {
          "nullable": false,
          "ty": "Unit"
        }
      ]
    },
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
          "ty": "I32"
        }
      ]
    },
    "4": {
      "columns": [
        {
          "nullable": false,
          "ty": "I32"
        }
      ]
    }
  }
}"#;

#[test]
#[ignore = "currently produces undefined output values"]
fn issue_147() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit =
        DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new()).unwrap();

    circuit
        .append_input(
            NodeId::new(14710),
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
                        NullableConstant::NonNull(Constant::String(String::from("Hi"))),
                        NullableConstant::null(),
                        NullableConstant::null(),
                    ]),
                    1,
                ),
            ]),
        )
        .unwrap();

    circuit.step().unwrap();

    let result = circuit.consolidate_output(NodeId::new(14739)).unwrap();
    assert!(must_equal_sc(
        &result,
        &StreamCollection::Set(vec!((
            RowLiteral::new(vec!(NullableConstant::Nullable(Some(Constant::I32(1))))),
            1,
        )))
    ));

    circuit.kill().unwrap();
}
