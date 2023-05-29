//! Test for https://github.com/feldera/dbsp/issues/145

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
        "15748": {
            "Source": {
                "layout": 1,
                "table": "T"
            }
        },
        "15750": {
            "IndexWith": {
                "input": 15748,
                "index_fn": {
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
                        },
                        {
                            "id": 3,
                            "layout": 1,
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
                                            "source_layout": 1,
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
                                            "source": 1,
                                            "source_layout": 1,
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
                                            "target_layout": 1,
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
                                            "source_layout": 1,
                                            "column": 1,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    9,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 1,
                                            "value": {
                                                "Expr": 8
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    10,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 1,
                                            "column": 2,
                                            "column_type": "Bool"
                                        }
                                    }
                                ],
                                [
                                    11,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 2,
                                            "value": {
                                                "Expr": 10
                                            },
                                            "value_type": "Bool"
                                        }
                                    }
                                ],
                                [
                                    12,
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
                                    1000,
                                    {
                                        "Copy": {
                                            "value": 12,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    13,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 3,
                                            "value": {
                                                "Expr": 1000
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    14,
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
                                    15,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 1,
                                            "column": 4
                                        }
                                    }
                                ],
                                [
                                    16,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 4,
                                            "value": {
                                                "Expr": 14
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    17,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 4,
                                            "is_null": {
                                                "Expr": 15
                                            }
                                        }
                                    }
                                ],
                                [
                                    18,
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
                                    19,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 1,
                                            "column": 5
                                        }
                                    }
                                ],
                                [
                                    20,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 5,
                                            "value": {
                                                "Expr": 18
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    21,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 5,
                                            "is_null": {
                                                "Expr": 19
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
                "value_layout": 1
            }
        },
        "15752": {
            "IndexWith": {
                "input": 15748,
                "index_fn": {
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
                        },
                        {
                            "id": 3,
                            "layout": 1,
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
                                            "source_layout": 1,
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
                                            "source": 1,
                                            "source_layout": 1,
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
                                            "target_layout": 1,
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
                                            "source_layout": 1,
                                            "column": 1,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    9,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 1,
                                            "value": {
                                                "Expr": 8
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    10,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 1,
                                            "column": 2,
                                            "column_type": "Bool"
                                        }
                                    }
                                ],
                                [
                                    11,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 2,
                                            "value": {
                                                "Expr": 10
                                            },
                                            "value_type": "Bool"
                                        }
                                    }
                                ],
                                [
                                    12,
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
                                    2000,
                                    {
                                        "Copy": {
                                            "value": 12,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    13,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 3,
                                            "value": {
                                                "Expr": 2000
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    14,
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
                                    15,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 1,
                                            "column": 4
                                        }
                                    }
                                ],
                                [
                                    16,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 4,
                                            "value": {
                                                "Expr": 14
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    17,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 4,
                                            "is_null": {
                                                "Expr": 15
                                            }
                                        }
                                    }
                                ],
                                [
                                    18,
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
                                    19,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 1,
                                            "column": 5
                                        }
                                    }
                                ],
                                [
                                    20,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 5,
                                            "value": {
                                                "Expr": 18
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    21,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 5,
                                            "is_null": {
                                                "Expr": 19
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
                "value_layout": 1
            }
        },
        "15754": {
            "JoinCore": {
                "lhs": 15750,
                "rhs": 15752,
                "join_fn": {
                    "args": [
                        {
                            "id": 1,
                            "layout": 3,
                            "flags": "input"
                        },
                        {
                            "id": 2,
                            "layout": 1,
                            "flags": "input"
                        },
                        {
                            "id": 3,
                            "layout": 1,
                            "flags": "input"
                        },
                        {
                            "id": 4,
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
                                    5,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 1,
                                            "column": 0,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    6,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 0,
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
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 1,
                                            "column": 1,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    8,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 1,
                                            "value": {
                                                "Expr": 7
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    9,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 1,
                                            "column": 2,
                                            "column_type": "Bool"
                                        }
                                    }
                                ],
                                [
                                    10,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 2,
                                            "value": {
                                                "Expr": 9
                                            },
                                            "value_type": "Bool"
                                        }
                                    }
                                ],
                                [
                                    11,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 1,
                                            "column": 3,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    200,
                                    {
                                        "Copy": {
                                            "value": 11,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    12,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 3,
                                            "value": {
                                                "Expr": 200
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    13,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 1,
                                            "column": 4,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    14,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 1,
                                            "column": 4
                                        }
                                    }
                                ],
                                [
                                    15,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 4,
                                            "value": {
                                                "Expr": 13
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    16,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 4,
                                            "is_null": {
                                                "Expr": 14
                                            }
                                        }
                                    }
                                ],
                                [
                                    17,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 1,
                                            "column": 5,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    18,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 1,
                                            "column": 5
                                        }
                                    }
                                ],
                                [
                                    19,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 5,
                                            "value": {
                                                "Expr": 17
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    20,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 5,
                                            "is_null": {
                                                "Expr": 18
                                            }
                                        }
                                    }
                                ],
                                [
                                    21,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 1,
                                            "column": 0,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    22,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 6,
                                            "value": {
                                                "Expr": 21
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    23,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 1,
                                            "column": 1,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    24,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 7,
                                            "value": {
                                                "Expr": 23
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    25,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 1,
                                            "column": 2,
                                            "column_type": "Bool"
                                        }
                                    }
                                ],
                                [
                                    26,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 8,
                                            "value": {
                                                "Expr": 25
                                            },
                                            "value_type": "Bool"
                                        }
                                    }
                                ],
                                [
                                    27,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 1,
                                            "column": 3,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    500,
                                    {
                                        "Copy": {
                                            "value": 27,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    28,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 9,
                                            "value": {
                                                "Expr": 500
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    29,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 1,
                                            "column": 4,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    30,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 4
                                        }
                                    }
                                ],
                                [
                                    31,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 10,
                                            "value": {
                                                "Expr": 29
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    32,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 10,
                                            "is_null": {
                                                "Expr": 30
                                            }
                                        }
                                    }
                                ],
                                [
                                    33,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 1,
                                            "column": 5,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    34,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 5
                                        }
                                    }
                                ],
                                [
                                    35,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 11,
                                            "value": {
                                                "Expr": 33
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    36,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 4,
                                            "column": 11,
                                            "is_null": {
                                                "Expr": 34
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
                "value_layout": 5,
                "key_layout": 4,
                "output_kind": "Set"
            }
        },
        "15756": {
            "Map": {
                "input": 15754,
                "map_fn": {
                    "args": [
                        {
                            "id": 1,
                            "layout": 4,
                            "flags": "input"
                        },
                        {
                            "id": 2,
                            "layout": 6,
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
                                            "column": 2,
                                            "column_type": "Bool"
                                        }
                                    }
                                ],
                                [
                                    4,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 6,
                                            "column": 0,
                                            "value": {
                                                "Expr": 3
                                            },
                                            "value_type": "Bool"
                                        }
                                    }
                                ],
                                [
                                    5,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 8,
                                            "column_type": "Bool"
                                        }
                                    }
                                ],
                                [
                                    6,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 6,
                                            "column": 1,
                                            "value": {
                                                "Expr": 5
                                            },
                                            "value_type": "Bool"
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
                    "Set": 6
                }
            }
        },
        "15759": {
            "Sink": {
                "input": 15756,
                "comment": "CREATE VIEW V AS SELECT T1.COL3, T2.COL3 FROM T AS T1 JOIN T AS T2 ON T1.COL1 = T2.COL1",
                "input_layout": {
                    "Set": 6
                }
            }
        }
    },
    "layouts": {
        "5": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "Unit"
                }
            ]
        },
        "6": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "Bool"
                },
                {
                    "nullable": false,
                    "ty": "Bool"
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
                },
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
        "3": {
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
pub fn issue_145() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit = DbspCircuit::new(graph, true, 1usize, CodegenConfig::debug(), Demands::new());

    circuit.append_input(
        NodeId::new(15748u32),
        &StreamCollection::Set(vec![
            (
                RowLiteral::new(vec![
                    NullableConstant::NonNull(Constant::I32(10i32)),
                    NullableConstant::NonNull(Constant::F64(1.0)),
                    NullableConstant::NonNull(Constant::Bool(false)),
                    NullableConstant::NonNull(Constant::String(String::from("Hi"))),
                    NullableConstant::Nullable(Some(Constant::I32(1i32))),
                    NullableConstant::Nullable(Some(Constant::F64(0.0))),
                ]),
                1i32,
            ),
            (
                RowLiteral::new(vec![
                    NullableConstant::NonNull(Constant::I32(10i32)),
                    NullableConstant::NonNull(Constant::F64(12.0)),
                    NullableConstant::NonNull(Constant::Bool(true)),
                    NullableConstant::NonNull(Constant::String(String::from("Hi"))),
                    NullableConstant::null(),
                    NullableConstant::null(),
                ]),
                1i32,
            ),
        ]),
    );

    circuit.step().unwrap();

    let result = circuit.consolidate_output(NodeId::new(15759u32));
    assert!(must_equal_sc(
        &result,
        &StreamCollection::Set(vec!(
            (
                RowLiteral::new(vec!(
                    NullableConstant::NonNull(Constant::Bool(false)),
                    NullableConstant::NonNull(Constant::Bool(false)),
                )),
                1i32,
            ),
            (
                RowLiteral::new(vec!(
                    NullableConstant::NonNull(Constant::Bool(true)),
                    NullableConstant::NonNull(Constant::Bool(true)),
                )),
                1i32,
            ),
            (
                RowLiteral::new(vec!(
                    NullableConstant::NonNull(Constant::Bool(true)),
                    NullableConstant::NonNull(Constant::Bool(false)),
                )),
                1i32,
            ),
            (
                RowLiteral::new(vec!(
                    NullableConstant::NonNull(Constant::Bool(false)),
                    NullableConstant::NonNull(Constant::Bool(true)),
                )),
                1i32,
            ),
        ))
    ));

    circuit.kill().unwrap();
}
