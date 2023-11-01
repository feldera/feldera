//! Test case for https://github.com/feldera/feldera/pull/314#issuecomment-1622786239

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

static CIRCUIT: &str = r#"{
    "nodes": {
        "6766": {
            "Source": {
                "layout": { "Set": 1 },
                "kind": "ZSet",
                "table": "T"
            }
        },
        "7091": {
            "Map": {
                "input": 6766,
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
        "7112": {
            "IndexWith": {
                "input": 7091,
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
        "7174": {
            "Fold": {
                "input": 7112,
                "acc_layout": 5,
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
                                        "Store": {
                                            "target": 5,
                                            "target_layout": 6,
                                            "column": 0,
                                            "value": {
                                                "Expr": 16
                                            },
                                            "value_type": "I64"
                                        }
                                    }
                                ],
                                [
                                    19,
                                    {
                                        "SetNull": {
                                            "target": 5,
                                            "target_layout": 6,
                                            "column": 0,
                                            "is_null": {
                                                "Expr": 17
                                            }
                                        }
                                    }
                                ],
                                [
                                    20,
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
                                    21,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 5,
                                            "column": 1
                                        }
                                    }
                                ],
                                [
                                    22,
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
                                        "Expr": 21
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
                        "9": {
                            "id": 9,
                            "body": [
                                [
                                    24,
                                    {
                                        "BinOp": {
                                            "lhs": 20,
                                            "rhs": 22,
                                            "kind": "Add",
                                            "operand_ty": "I64"
                                        }
                                    }
                                ],
                                [
                                    25,
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
                                        24,
                                        25
                                    ]
                                }
                            },
                            "params": []
                        },
                        "10": {
                            "id": 10,
                            "body": [
                                [
                                    28,
                                    {
                                        "Store": {
                                            "target": 5,
                                            "target_layout": 6,
                                            "column": 1,
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
                                            "target": 5,
                                            "target_layout": 6,
                                            "column": 1,
                                            "is_null": {
                                                "Expr": 27
                                            }
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Jump": {
                                    "target": 11,
                                    "params": []
                                }
                            },
                            "params": [
                                [
                                    26,
                                    {
                                        "Scalar": "I64"
                                    }
                                ],
                                [
                                    27,
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
                                    30,
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
                                    31,
                                    {
                                        "IsNull": {
                                            "target": 5,
                                            "target_layout": 6,
                                            "column": 0
                                        }
                                    }
                                ],
                                [
                                    32,
                                    {
                                        "Store": {
                                            "target": 1,
                                            "target_layout": 5,
                                            "column": 0,
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
                                            "column": 0,
                                            "is_null": {
                                                "Expr": 31
                                            }
                                        }
                                    }
                                ],
                                [
                                    34,
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
                                    35,
                                    {
                                        "IsNull": {
                                            "target": 5,
                                            "target_layout": 6,
                                            "column": 1
                                        }
                                    }
                                ],
                                [
                                    36,
                                    {
                                        "Store": {
                                            "target": 1,
                                            "target_layout": 5,
                                            "column": 1,
                                            "value": {
                                                "Expr": 34
                                            },
                                            "value_type": "I64"
                                        }
                                    }
                                ],
                                [
                                    37,
                                    {
                                        "SetNull": {
                                            "target": 1,
                                            "target_layout": 5,
                                            "column": 1,
                                            "is_null": {
                                                "Expr": 35
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
                        },
                        {
                            "Nullable": null
                        }
                    ]
                }
            }
        },
        "7196": {
            "Map": {
                "input": 7174,
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
        "7211": {
            "Map": {
                "input": 7196,
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
        "7215": {
            "Neg": {
                "input": 7211,
                "layout": {
                    "Set": 4
                }
            }
        },
        "7223": {
            "ConstantStream": {
                "comment": "zset!(Tuple1::new((i32?)null) => 1,)",
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
        "7226": {
            "Sum": {
                "layout": {
                    "Set": 4
                },
                "inputs": [
                    7223,
                    7215,
                    7196
                ]
            }
        },
        "7229": {
            "Sink": {
                "input": 7226,
                "view": "V",
                "comment": "CREATE VIEW V AS SELECT AVG(T.COL1) FROM T",
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
fn issue_314() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit =
        DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new()).unwrap();

    circuit
        .append_input(
            NodeId::new(6766),
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

    let output = circuit.consolidate_output(NodeId::new(7229)).unwrap();
    assert!(must_equal_sc(
        &output,
        &StreamCollection::Set(vec![(
            RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::I32(10)))]),
            1,
        )])
    ));

    circuit.kill().unwrap();
}
