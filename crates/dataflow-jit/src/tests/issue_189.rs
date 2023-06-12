//! Test for https://github.com/feldera/dbsp/issues/189

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
        "7131": {
            "Source": {
                "layout": 1,
                "table": "T"
            }
        },
        "7133": {
            "Map": {
                "input": 7131,
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
                                ],
                                [
                                    5,
                                    {
                                        "Constant": {
                                            "I32": 1
                                        }
                                    }
                                ],
                                [
                                    6,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 2,
                                            "column": 1,
                                            "value": {
                                                "Expr": 5
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    7,
                                    {
                                        "Constant": {
                                            "I32": 2
                                        }
                                    }
                                ],
                                [
                                    8,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 2,
                                            "column": 2,
                                            "value": {
                                                "Expr": 7
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
        "7136": {
            "IndexWith": {
                "input": 7133,
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
                                            "target": 2,
                                            "target_layout": 3,
                                            "column": 0,
                                            "value": {
                                                "Expr": 4
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
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
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 0,
                                            "value": {
                                                "Expr": 6
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    8,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 2,
                                            "column": 1,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    9,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 1,
                                            "value": {
                                                "Expr": 8
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    10,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 2,
                                            "column": 2,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    11,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 2,
                                            "value": {
                                                "Expr": 10
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
                "key_layout": 4,
                "value_layout": 2
            }
        },
        "7147": {
            "Fold": {
                "input": 7136,
                "acc_layout": 5,
                "step_layout": 2,
                "output_layout": 5,
                "finish_fn": {
                    "args": [
                        {
                            "id": 1,
                            "layout": 6,
                            "flags": "input"
                        },
                        {
                            "id": 3,
                            "layout": 5,
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
                                            "source_layout": 6,
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
                                            "target_layout": 6,
                                            "column": 0
                                        }
                                    }
                                ],
                                [
                                    8,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 5,
                                            "column": 0,
                                            "value": {
                                                "Expr": 4
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    9,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 5,
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
                            "layout": 3,
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
                                            "source_layout": 3,
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
                                            "source_layout": 5,
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
                                            "target_layout": 5,
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
                                            "column": 1,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    8,
                                    {
                                        "BinOp": {
                                            "lhs": 7,
                                            "rhs": 4,
                                            "kind": "Mul",
                                            "operand_ty": "I32"
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
                                    9,
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
                                        8,
                                        9
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
                                            "lhs": 5,
                                            "rhs": 8,
                                            "kind": "Add",
                                            "operand_ty": "I32"
                                        }
                                    }
                                ],
                                [
                                    11,
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
                                        10,
                                        11
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
                                    12,
                                    {
                                        "Scalar": "I32"
                                    }
                                ],
                                [
                                    13,
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
                                    "target": 7,
                                    "params": []
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
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 5,
                                            "column": 1,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    15,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 5,
                                            "column": 1
                                        }
                                    }
                                ],
                                [
                                    16,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 2,
                                            "column": 2,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    17,
                                    {
                                        "BinOp": {
                                            "lhs": 16,
                                            "rhs": 4,
                                            "kind": "Mul",
                                            "operand_ty": "I32"
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Branch": {
                                    "cond": {
                                        "Expr": 15
                                    },
                                    "true_params": [],
                                    "false_params": [],
                                    "falsy": 9,
                                    "truthy": 8
                                }
                            },
                            "params": []
                        },
                        "8": {
                            "id": 8,
                            "body": [
                                [
                                    18,
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
                                        17,
                                        18
                                    ]
                                }
                            },
                            "params": []
                        },
                        "9": {
                            "id": 9,
                            "body": [
                                [
                                    19,
                                    {
                                        "BinOp": {
                                            "lhs": 14,
                                            "rhs": 17,
                                            "kind": "Add",
                                            "operand_ty": "I32"
                                        }
                                    }
                                ],
                                [
                                    20,
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
                                        19,
                                        20
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
                                    21,
                                    {
                                        "Scalar": "I32"
                                    }
                                ],
                                [
                                    22,
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
                                    23,
                                    {
                                        "Store": {
                                            "target": 1,
                                            "target_layout": 5,
                                            "column": 0,
                                            "value": {
                                                "Expr": 12
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    24,
                                    {
                                        "SetNull": {
                                            "target": 1,
                                            "target_layout": 5,
                                            "column": 0,
                                            "is_null": {
                                                "Expr": 13
                                            }
                                        }
                                    }
                                ],
                                [
                                    25,
                                    {
                                        "Store": {
                                            "target": 1,
                                            "target_layout": 5,
                                            "column": 1,
                                            "value": {
                                                "Expr": 21
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    26,
                                    {
                                        "SetNull": {
                                            "target": 1,
                                            "target_layout": 5,
                                            "column": 1,
                                            "is_null": {
                                                "Expr": 22
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
        "7149": {
            "Map": {
                "input": 7147,
                "map_fn": {
                    "args": [
                        {
                            "id": 1,
                            "layout": 4,
                            "flags": "input"
                        },
                        {
                            "id": 2,
                            "layout": 5,
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
                                            "source": 1,
                                            "source_layout": 4,
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
                                ],
                                [
                                    6,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 5,
                                            "column": 0,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    7,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 5,
                                            "column": 0
                                        }
                                    }
                                ],
                                [
                                    8,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 1,
                                            "value": {
                                                "Expr": 6
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    9,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 5,
                                            "column": 1,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    10,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 5,
                                            "column": 1
                                        }
                                    }
                                ],
                                [
                                    11,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 2,
                                            "value": {
                                                "Expr": 9
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
                    "Map": [
                        4,
                        5
                    ]
                },
                "output_layout": {
                    "Set": 2
                }
            }
        },
        "7154": {
            "Map": {
                "input": 7149,
                "map_fn": {
                    "args": [
                        {
                            "id": 1,
                            "layout": 2,
                            "flags": "input"
                        },
                        {
                            "id": 2,
                            "layout": 5,
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
                                            "I32": 20
                                        }
                                    }
                                ],
                                [
                                    4,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 2,
                                            "column": 1,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    5,
                                    {
                                        "Constant": {
                                            "I32": 0
                                        }
                                    }
                                ],
                                [
                                    6,
                                    {
                                        "BinOp": {
                                            "lhs": 5,
                                            "rhs": 4,
                                            "kind": "Eq",
                                            "operand_ty": "I32"
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
                                    7,
                                    {
                                        "Constant": {
                                            "Bool": true
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Jump": {
                                    "target": 4,
                                    "params": [
                                        7,
                                        5
                                    ]
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
                                ],
                                [
                                    9,
                                    {
                                        "BinOp": {
                                            "lhs": 3,
                                            "rhs": 4,
                                            "kind": "Div",
                                            "operand_ty": "I32"
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Jump": {
                                    "target": 4,
                                    "params": [
                                        8,
                                        9
                                    ]
                                }
                            },
                            "params": []
                        },
                        "4": {
                            "id": 4,
                            "body": [
                                [
                                    12,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 5,
                                            "column": 0,
                                            "value": {
                                                "Expr": 11
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    13,
                                    {
                                        "SetNull": {
                                            "target": 2,
                                            "target_layout": 5,
                                            "column": 0,
                                            "is_null": {
                                                "Expr": 10
                                            }
                                        }
                                    }
                                ],
                                [
                                    14,
                                    {
                                        "Constant": {
                                            "I32": 20
                                        }
                                    }
                                ],
                                [
                                    15,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 2,
                                            "column": 2,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    16,
                                    {
                                        "Constant": {
                                            "I32": 0
                                        }
                                    }
                                ],
                                [
                                    17,
                                    {
                                        "BinOp": {
                                            "lhs": 16,
                                            "rhs": 15,
                                            "kind": "Eq",
                                            "operand_ty": "I32"
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Branch": {
                                    "cond": {
                                        "Expr": 17
                                    },
                                    "true_params": [],
                                    "false_params": [],
                                    "falsy": 6,
                                    "truthy": 5
                                }
                            },
                            "params": [
                                [
                                    10,
                                    {
                                        "Scalar": "Bool"
                                    }
                                ],
                                [
                                    11,
                                    {
                                        "Scalar": "I32"
                                    }
                                ]
                            ]
                        },
                        "5": {
                            "id": 5,
                            "body": [
                                [
                                    18,
                                    {
                                        "Constant": {
                                            "Bool": true
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Jump": {
                                    "target": 7,
                                    "params": [
                                        18,
                                        16
                                    ]
                                }
                            },
                            "params": []
                        },
                        "6": {
                            "id": 6,
                            "body": [
                                [
                                    19,
                                    {
                                        "Constant": {
                                            "Bool": false
                                        }
                                    }
                                ],
                                [
                                    20,
                                    {
                                        "BinOp": {
                                            "lhs": 14,
                                            "rhs": 15,
                                            "kind": "Div",
                                            "operand_ty": "I32"
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Jump": {
                                    "target": 7,
                                    "params": [
                                        19,
                                        20
                                    ]
                                }
                            },
                            "params": []
                        },
                        "7": {
                            "id": 7,
                            "body": [
                                [
                                    23,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 5,
                                            "column": 1,
                                            "value": {
                                                "Expr": 22
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    24,
                                    {
                                        "SetNull": {
                                            "target": 2,
                                            "target_layout": 5,
                                            "column": 1,
                                            "is_null": {
                                                "Expr": 21
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
                                    21,
                                    {
                                        "Scalar": "Bool"
                                    }
                                ],
                                [
                                    22,
                                    {
                                        "Scalar": "I32"
                                    }
                                ]
                            ]
                        }
                    }
                },
                "input_layout": {
                    "Set": 2
                },
                "output_layout": {
                    "Set": 5
                }
            }
        },
        "7157": {
            "Sink": {
                "input": 7154,
                "comment": "CREATE VIEW V AS SELECT 20 / SUM(1), 20 / SUM(2) FROM T GROUP BY COL1",
                "input_layout": {
                    "Set": 5
                }
            }
        }
    },
    "layouts": {
        "5": {
            "columns": [
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "I32"
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
                },
                {
                    "nullable": false,
                    "ty": "I32"
                },
                {
                    "nullable": false,
                    "ty": "I32"
                }
            ]
        },
        "3": {
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
                    "nullable": false,
                    "ty": "I32"
                }
            ]
        },
        "6": {
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
#[ignore = "performs invalid division operations"]
fn issue_189() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new());

    circuit.append_input(
        NodeId::new(7131),
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

    let output = circuit.consolidate_output(NodeId::new(7157));
    assert!(must_equal_sc(
        &output,
        &StreamCollection::Set(vec!((
            RowLiteral::new(vec!(
                NullableConstant::Nullable(Some(Constant::I32(10))),
                NullableConstant::Nullable(Some(Constant::I32(5))),
            )),
            1,
        )))
    ));

    circuit.kill().unwrap();
}
