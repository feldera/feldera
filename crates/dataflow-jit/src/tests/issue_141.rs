//! Test for https://github.com/feldera/feldera/issues/141

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
    "7070": {
      "Source": {
        "layout": 1,
        "table": "T"
      }
    },
    "7072": {
      "Map": {
        "input": 7070,
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
    "7075": {
      "IndexWith": {
        "input": 7072,
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
    "7082": {
      "Fold": {
        "input": 7075,
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
                      "column_type": "I32"
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
                      "value_type": "I32"
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
              "layout": 2,
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
                      "source_layout": 2,
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
                      "column_type": "I32"
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
                      "column_type": "I32"
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
                  8,
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
                    7,
                    8
                  ]
                }
              },
              "params": []
            },
            "4": {
              "id": 4,
              "body": [
                [
                  9,
                  {
                    "BinOp": {
                      "lhs": 5,
                      "rhs": 7,
                      "kind": "Max",
                      "operand_ty": "I32"
                    }
                  }
                ],
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
                  11,
                  {
                    "Scalar": "I32"
                  }
                ],
                [
                  12,
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
                  13,
                  {
                    "Store": {
                      "target": 1,
                      "target_layout": 4,
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
                      "target_layout": 4,
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
    "7084": {
      "Map": {
        "input": 7082,
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
                      "column_type": "I32"
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
                      "value_type": "I32"
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
    "7089": {
      "Map": {
        "input": 7084,
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
                      "target_layout": 4,
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
    "7092": {
      "Neg": {
        "input": 7089,
        "layout": {
          "Set": 4
        }
      }
    },
    "7095": {
      "ConstantStream": {
        "comment": "zset!(\n    Tuple1::new(None::<i32>) => 1,\n)",
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
    "7097": {
      "Sum": {
        "layout": {
          "Set": 4
        },
        "inputs": [
          7095,
          7092,
          7084
        ]
      }
    },
    "7099": {
      "Sink": {
        "input": 7097,
        "comment": "CREATE VIEW V AS SELECT MAX(T.COL1) FROM T",
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
    "2": {
      "columns": [
        {
          "nullable": false,
          "ty": "I32"
        }
      ]
    },
    "4": {
      "columns": [
        {
          "nullable": true,
          "ty": "I32"
        }
      ]
    }
  }
}"#;

#[test]
fn issue_141() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());

    circuit.append_input(
        NodeId::new(7070),
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

    let result = circuit.consolidate_output(NodeId::new(7099));
    assert!(must_equal_sc(
        &result,
        &StreamCollection::Set(vec!((
            RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::I32(10)))]),
            1,
        )))
    ));

    circuit.kill().unwrap();
}
