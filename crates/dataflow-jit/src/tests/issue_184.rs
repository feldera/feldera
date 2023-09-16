//! Test for https://github.com/feldera/feldera/issues/184

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
    "7051": {
      "Source": {
        "layout": 1,
        "table": "T"
      }
    },
    "7053": {
      "Map": {
        "input": 7051,
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
                      "column": 1,
                      "column_type": "F64"
                    }
                  }
                ],
                [
                  4,
                  {
                    "Store": {
                      "target": 2,
                      "target_layout": 2,
                      "column": 0,
                      "value": {
                        "Expr": 3
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
    "7056": {
      "IndexWith": {
        "input": 7053,
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
                      "column_type": "F64"
                    }
                  }
                ],
                [
                  5,
                  {
                    "Store": {
                      "target": 3,
                      "target_layout": 2,
                      "column": 0,
                      "value": {
                        "Expr": 4
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
        "key_layout": 3,
        "value_layout": 2
      }
    },
    "7063": {
      "Fold": {
        "input": 7056,
        "acc_layout": 4,
        "step_layout": 2,
        "output_layout": 4,
        "finish_fn": {
          "args": [
            {
              "id": 1,
              "layout": 4,
              "flags": "input"
            },
            {
              "id": 2,
              "layout": 4,
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
                      "source_layout": 4,
                      "column": 0,
                      "column_type": "F64"
                    }
                  }
                ],
                [
                  4,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 4,
                      "column": 0
                    }
                  }
                ],
                [
                  5,
                  {
                    "Store": {
                      "target": 2,
                      "target_layout": 4,
                      "column": 0,
                      "value": {
                        "Expr": 3
                      },
                      "value_type": "F64"
                    }
                  }
                ],
                [
                  6,
                  {
                    "SetNull": {
                      "target": 2,
                      "target_layout": 4,
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
              "layout": 4,
              "flags": "inout"
            },
            {
              "id": 2,
              "layout": 2,
              "flags": "input"
            },
            {
              "id": 3,
              "layout": 5,
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
                  4,
                  {
                    "Load": {
                      "source": 3,
                      "source_layout": 5,
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
                  5,
                  {
                    "Load": {
                      "source": 1,
                      "source_layout": 4,
                      "column": 0,
                      "column_type": "F64"
                    }
                  }
                ],
                [
                  6,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 4,
                      "column": 0
                    }
                  }
                ],
                [
                  7,
                  {
                    "Load": {
                      "source": 2,
                      "source_layout": 2,
                      "column": 0,
                      "column_type": "F64"
                    }
                  }
                ],
                [
                  8,
                  {
                    "Cast": {
                      "value": 4,
                      "from": "I32",
                      "to": "F64"
                    }
                  }
                ],
                [
                  9,
                  {
                    "BinOp": {
                      "lhs": 7,
                      "rhs": 8,
                      "kind": "Mul",
                      "operand_ty": "F64"
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
                  "falsy": 4,
                  "truthy": 3
                }
              },
              "params": []
            },
            "3": {
              "id": 3,
              "body": [
                [
                  10,
                  {
                    "Constant": {
                      "Bool": false
                    }
                  }
                ]
              ],
              "terminator": {
                "Jump": {
                  "target": 5,
                  "params": [
                    9,
                    10
                  ]
                }
              },
              "params": []
            },
            "4": {
              "id": 4,
              "body": [
                [
                  11,
                  {
                    "BinOp": {
                      "lhs": 5,
                      "rhs": 9,
                      "kind": "Add",
                      "operand_ty": "F64"
                    }
                  }
                ],
                [
                  12,
                  {
                    "Constant": {
                      "Bool": false
                    }
                  }
                ]
              ],
              "terminator": {
                "Jump": {
                  "target": 5,
                  "params": [
                    11,
                    12
                  ]
                }
              },
              "params": []
            },
            "5": {
              "id": 5,
              "body": [],
              "terminator": {
                "Jump": {
                  "target": 6,
                  "params": []
                }
              },
              "params": [
                [
                  13,
                  {
                    "Scalar": "F64"
                  }
                ],
                [
                  14,
                  {
                    "Scalar": "Bool"
                  }
                ]
              ]
            },
            "6": {
              "id": 6,
              "body": [
                [
                  15,
                  {
                    "Store": {
                      "target": 1,
                      "target_layout": 4,
                      "column": 0,
                      "value": {
                        "Expr": 13
                      },
                      "value_type": "F64"
                    }
                  }
                ],
                [
                  16,
                  {
                    "SetNull": {
                      "target": 1,
                      "target_layout": 4,
                      "column": 0,
                      "is_null": {
                        "Expr": 14
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
    "7065": {
      "Map": {
        "input": 7063,
        "map_fn": {
          "args": [
            {
              "id": 1,
              "layout": 3,
              "flags": "input"
            },
            {
              "id": 2,
              "layout": 4,
              "flags": "input"
            },
            {
              "id": 3,
              "layout": 4,
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
                      "source_layout": 4,
                      "column": 0,
                      "column_type": "F64"
                    }
                  }
                ],
                [
                  5,
                  {
                    "IsNull": {
                      "target": 2,
                      "target_layout": 4,
                      "column": 0
                    }
                  }
                ],
                [
                  6,
                  {
                    "Store": {
                      "target": 3,
                      "target_layout": 4,
                      "column": 0,
                      "value": {
                        "Expr": 4
                      },
                      "value_type": "F64"
                    }
                  }
                ],
                [
                  7,
                  {
                    "SetNull": {
                      "target": 3,
                      "target_layout": 4,
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
            4
          ]
        },
        "output_layout": {
          "Set": 4
        }
      }
    },
    "7070": {
      "Map": {
        "input": 7065,
        "map_fn": {
          "args": [
            {
              "id": 1,
              "layout": 4,
              "flags": "input"
            },
            {
              "id": 2,
              "layout": 4,
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
                      "F64": 0.0
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
                      "target_layout": 4,
                      "column": 0,
                      "value": {
                        "Expr": 3
                      },
                      "value_type": "F64"
                    }
                  }
                ],
                [
                  6,
                  {
                    "SetNull": {
                      "target": 2,
                      "target_layout": 4,
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
          "Set": 4
        },
        "output_layout": {
          "Set": 4
        }
      }
    },
    "7073": {
      "Neg": {
        "input": 7070,
        "layout": {
          "Set": 4
        }
      }
    },
    "7076": {
      "ConstantStream": {
        "comment": "zset!(\n    Tuple1::new(None::<F64>) => 1,\n)",
        "layout": {
          "Set": 4
        },
        "value": {
          "layout": {
            "Set": 4
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
    "7078": {
      "Sum": {
        "layout": {
          "Set": 4
        },
        "inputs": [
          7076,
          7073,
          7065
        ]
      }
    },
    "7080": {
      "Sink": {
        "input": 7078,
        "view": "V",
        "comment": "CREATE VIEW V AS SELECT SUM(T.COL2) FROM T",
        "input_layout": {
          "Set": 4
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
    "4": {
      "columns": [
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
          "ty": "F64"
        }
      ]
    },
    "5": {
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
fn issue_184() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());

    circuit.append_input(
        NodeId::new(7051),
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

    let output = circuit.consolidate_output(NodeId::new(7080));
    assert!(must_equal_sc(
        &output,
        &StreamCollection::Set(vec![(
            RowLiteral::new(vec!(NullableConstant::Nullable(Some(Constant::F64(13.0))))),
            1,
        )])
    ));

    circuit.kill().unwrap();
}
