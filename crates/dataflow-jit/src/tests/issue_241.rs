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
    "6764": {
      "SourceMap": {
        "key_layout": 3,
        "value_layout": 2
      }
    },
    "7172": {
      "Fold": {
        "input": 6764,
        "acc_layout": 4,
        "step_layout": 2,
        "output_layout": 4,
        "finish_fn": {
          "args": [
            {
              "id": 1,
              "layout": 5,
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
              "body": [],
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
                  3,
                  {
                    "Load": {
                      "source": 1,
                      "source_layout": 5,
                      "column": 0,
                      "column_type": "I64"
                    }
                  }
                ],
                [
                  4,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 5,
                      "column": 0
                    }
                  }
                ],
                [
                  5,
                  {
                    "Load": {
                      "source": 1,
                      "source_layout": 5,
                      "column": 1,
                      "column_type": "I64"
                    }
                  }
                ],
                [
                  6,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 5,
                      "column": 1
                    }
                  }
                ],
                [
                  7,
                  {
                    "Constant": {
                      "I64": 0
                    }
                  }
                ],
                [
                  8,
                  {
                    "BinOp": {
                      "lhs": 7,
                      "rhs": 5,
                      "kind": "Eq",
                      "operand_ty": "I64"
                    }
                  }
                ]
              ],
              "terminator": {
                "Branch": {
                  "cond": {
                    "Expr": 8
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
                  9,
                  {
                    "Constant": {
                      "Bool": true
                    }
                  }
                ]
              ],
              "terminator": {
                "Jump": {
                  "target": 5,
                  "params": [
                    9,
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
                    "BinOp": {
                      "lhs": 4,
                      "rhs": 6,
                      "kind": "Or",
                      "operand_ty": "Bool"
                    }
                  }
                ],
                [
                  11,
                  {
                    "BinOp": {
                      "lhs": 3,
                      "rhs": 5,
                      "kind": "Div",
                      "operand_ty": "I64"
                    }
                  }
                ]
              ],
              "terminator": {
                "Jump": {
                  "target": 5,
                  "params": [
                    10,
                    11
                  ]
                }
              },
              "params": []
            },
            "5": {
              "id": 5,
              "body": [
                [
                  14,
                  {
                    "Cast": {
                      "value": 13,
                      "from": "I64",
                      "to": "I32"
                    }
                  }
                ]
              ],
              "terminator": {
                "Jump": {
                  "target": 6,
                  "params": []
                }
              },
              "params": [
                [
                  12,
                  {
                    "Scalar": "Bool"
                  }
                ],
                [
                  13,
                  {
                    "Scalar": "I64"
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
                      "target": 2,
                      "target_layout": 4,
                      "column": 0,
                      "value": {
                        "Expr": 14
                      },
                      "value_type": "I32"
                    }
                  }
                ],
                [
                  16,
                  {
                    "SetNull": {
                      "target": 2,
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
        "step_fn": {
          "args": [
            {
              "id": 1,
              "layout": 5,
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
                ],
                [
                  5,
                  {
                    "Uninit": {
                      "value": {
                        "Row": 6
                      }
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
                      "source_layout": 5,
                      "column": 0,
                      "column_type": "I64"
                    }
                  }
                ],
                [
                  7,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 5,
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
                    "Cast": {
                      "value": 8,
                      "from": "I32",
                      "to": "I64"
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
                ],
                [
                  11,
                  {
                    "Cast": {
                      "value": 4,
                      "from": "I32",
                      "to": "I64"
                    }
                  }
                ],
                [
                  12,
                  {
                    "BinOp": {
                      "lhs": 9,
                      "rhs": 11,
                      "kind": "Mul",
                      "operand_ty": "I64"
                    }
                  }
                ],
                [
                  13,
                  {
                    "BinOp": {
                      "lhs": 10,
                      "rhs": 10,
                      "kind": "Or",
                      "operand_ty": "Bool"
                    }
                  }
                ]
              ],
              "terminator": {
                "Branch": {
                  "cond": {
                    "Expr": 7
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
              "body": [],
              "terminator": {
                "Jump": {
                  "target": 5,
                  "params": [
                    12,
                    13
                  ]
                }
              },
              "params": []
            },
            "4": {
              "id": 4,
              "body": [],
              "terminator": {
                "Branch": {
                  "cond": {
                    "Expr": 13
                  },
                  "true_params": [],
                  "false_params": [],
                  "falsy": 7,
                  "truthy": 6
                }
              },
              "params": []
            },
            "5": {
              "id": 5,
              "body": [
                [
                  18,
                  {
                    "Load": {
                      "source": 1,
                      "source_layout": 5,
                      "column": 1,
                      "column_type": "I64"
                    }
                  }
                ],
                [
                  19,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 5,
                      "column": 1
                    }
                  }
                ],
                [
                  20,
                  {
                    "Cast": {
                      "value": 4,
                      "from": "I32",
                      "to": "I64"
                    }
                  }
                ]
              ],
              "terminator": {
                "Branch": {
                  "cond": {
                    "Expr": 19
                  },
                  "true_params": [],
                  "false_params": [],
                  "falsy": 9,
                  "truthy": 8
                }
              },
              "params": [
                [
                  16,
                  {
                    "Scalar": "I64"
                  }
                ],
                [
                  17,
                  {
                    "Scalar": "Bool"
                  }
                ]
              ]
            },
            "6": {
              "id": 6,
              "body": [],
              "terminator": {
                "Jump": {
                  "target": 5,
                  "params": [
                    6,
                    7
                  ]
                }
              },
              "params": []
            },
            "7": {
              "id": 7,
              "body": [
                [
                  14,
                  {
                    "BinOp": {
                      "lhs": 6,
                      "rhs": 12,
                      "kind": "Add",
                      "operand_ty": "I64"
                    }
                  }
                ],
                [
                  15,
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
                    14,
                    15
                  ]
                }
              },
              "params": []
            },
            "8": {
              "id": 8,
              "body": [
                [
                  21,
                  {
                    "Constant": {
                      "Bool": false
                    }
                  }
                ]
              ],
              "terminator": {
                "Jump": {
                  "target": 10,
                  "params": [
                    20,
                    21
                  ]
                }
              },
              "params": []
            },
            "9": {
              "id": 9,
              "body": [
                [
                  22,
                  {
                    "BinOp": {
                      "lhs": 18,
                      "rhs": 20,
                      "kind": "Add",
                      "operand_ty": "I64"
                    }
                  }
                ],
                [
                  23,
                  {
                    "Constant": {
                      "Bool": false
                    }
                  }
                ]
              ],
              "terminator": {
                "Jump": {
                  "target": 10,
                  "params": [
                    22,
                    23
                  ]
                }
              },
              "params": []
            },
            "10": {
              "id": 10,
              "body": [],
              "terminator": {
                "Jump": {
                  "target": 11,
                  "params": []
                }
              },
              "params": [
                [
                  24,
                  {
                    "Scalar": "I64"
                  }
                ],
                [
                  25,
                  {
                    "Scalar": "Bool"
                  }
                ]
              ]
            },
            "11": {
              "id": 11,
              "body": [
                [
                  26,
                  {
                    "Load": {
                      "source": 5,
                      "source_layout": 6,
                      "column": 0,
                      "column_type": "I64"
                    }
                  }
                ],
                [
                  27,
                  {
                    "IsNull": {
                      "target": 5,
                      "target_layout": 6,
                      "column": 0
                    }
                  }
                ],
                [
                  28,
                  {
                    "Store": {
                      "target": 1,
                      "target_layout": 5,
                      "column": 0,
                      "value": {
                        "Expr": 26
                      },
                      "value_type": "I64"
                    }
                  }
                ],
                [
                  29,
                  {
                    "SetNull": {
                      "target": 1,
                      "target_layout": 5,
                      "column": 0,
                      "is_null": {
                        "Expr": 27
                      }
                    }
                  }
                ],
                [
                  30,
                  {
                    "Load": {
                      "source": 5,
                      "source_layout": 6,
                      "column": 1,
                      "column_type": "I64"
                    }
                  }
                ],
                [
                  31,
                  {
                    "IsNull": {
                      "target": 5,
                      "target_layout": 6,
                      "column": 1
                    }
                  }
                ],
                [
                  32,
                  {
                    "Store": {
                      "target": 1,
                      "target_layout": 5,
                      "column": 1,
                      "value": {
                        "Expr": 30
                      },
                      "value_type": "I64"
                    }
                  }
                ],
                [
                  33,
                  {
                    "SetNull": {
                      "target": 1,
                      "target_layout": 5,
                      "column": 1,
                      "is_null": {
                        "Expr": 31
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
    "7194": {
      "Map": {
        "input": 7172,
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
    "7209": {
      "Map": {
        "input": 7194,
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
    "7213": {
      "Neg": {
        "input": 7209,
        "layout": {
          "Set": 4
        }
      }
    },
    "7221": {
      "ConstantStream": {
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
    "7224": {
      "Sum": {
        "layout": {
          "Set": 4
        },
        "inputs": [
          7221,
          7213,
          7194
        ]
      }
    },
    "7227": {
      "Sink": {
        "input": 7224,
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
    "5": {
      "columns": [
        {
          "nullable": true,
          "ty": "I64"
        },
        {
          "nullable": true,
          "ty": "I64"
        }
      ]
    },
    "6": {
      "columns": [
        {
          "nullable": true,
          "ty": "I64"
        },
        {
          "nullable": true,
          "ty": "I64"
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
#[ignore = "original code is broken, it depends on uninit data"]
fn issue_241() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());

    circuit.append_input(
        NodeId::new(6764),
        &StreamCollection::Map(vec![
            (
                RowLiteral::new(vec![NullableConstant::NonNull(Constant::Unit)]),
                RowLiteral::new(vec![NullableConstant::NonNull(Constant::I32(10))]),
                1,
            ),
            (
                RowLiteral::new(vec![NullableConstant::NonNull(Constant::Unit)]),
                RowLiteral::new(vec![NullableConstant::NonNull(Constant::I32(10))]),
                1,
            ),
        ]),
    );

    circuit.step().unwrap();

    let output = circuit.consolidate_output(NodeId::new(7227));
    assert!(must_equal_sc(
        &output,
        &StreamCollection::Set(vec![(
            RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::I32(10)))]),
            1,
        )]),
    ));

    circuit.kill().unwrap();
}
