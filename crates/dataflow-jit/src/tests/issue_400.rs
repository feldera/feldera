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
use chrono::NaiveDate;

const CIRCUIT: &str = r#"{
  "nodes": {
    "594": {
      "Source": {
        "layout": { "Set": 1 },
        "kind": "ZSet",
        "table": "DATE_TBL"
      }
    },
    "657": {
      "Filter": {
        "input": 594,
        "filter_fn": {
          "args": [
            {
              "id": 1,
              "layout": 1,
              "flags": "input"
            }
          ],
          "ret": "Bool",
          "entry_block": 1,
          "blocks": {
            "1": {
              "id": 1,
              "body": [
                [
                  2,
                  {
                    "Load": {
                      "source": 1,
                      "source_layout": 1,
                      "column": 0,
                      "column_type": "Date"
                    }
                  }
                ],
                [
                  3,
                  {
                    "IsNull": {
                      "target": 1,
                      "target_layout": 1,
                      "column": 0
                    }
                  }
                ],
                [
                  4,
                  {
                    "Constant": {
                      "Date": "2000-01-01"
                    }
                  }
                ],
                [
                  5,
                  {
                    "BinOp": {
                      "lhs": 2,
                      "rhs": 4,
                      "kind": "LessThan",
                      "operand_ty": "Date"
                    }
                  }
                ],
                [
                  6,
                  {
                    "Constant": {
                      "Bool": false
                    }
                  }
                ],
                [
                  7,
                  {
                    "Select": {
                      "cond": 3,
                      "if_true": 6,
                      "if_false": 5,
                      "value_type": {
                        "Scalar": "Bool"
                      }
                    }
                  }
                ]
              ],
              "terminator": {
                "Return": {
                  "value": {
                    "Expr": 7
                  }
                }
              },
              "params": []
            }
          }
        }
      }
    },
    "660": {
      "Sink": {
        "input": 657,
        "view": "VV",
        "comment": "CREATE VIEW VV AS SELECT f1 FROM DATE_TBL WHERE f1 < '2000-01-01'",
        "input_layout": {
          "Set": 1
        }
      }
    }
  },
  "layouts": {
    "1": {
      "columns": [
        {
          "nullable": true,
          "ty": "Date"
        }
      ]
    }
  }
}"#;

#[test]
fn issue_400() {
    utils::test_logger();

    let graph = serde_json::from_str::<SqlGraph>(CIRCUIT)
        .unwrap()
        .rematerialize();

    let mut circuit =
        DbspCircuit::new(graph, true, 1, CodegenConfig::debug(), Demands::new()).unwrap();

    circuit
        .append_input(
            NodeId::new(594),
            &StreamCollection::Set(vec![
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("2038-04-08", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("1996-03-01", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("1996-02-28", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("1957-04-09", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("1997-02-28", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("1996-03-02", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("2000-04-03", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("1996-02-29", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("2000-04-01", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (RowLiteral::new(vec![NullableConstant::null()]), 1i32),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("1997-03-01", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("1957-06-13", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("2039-04-09", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("2000-04-02", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("1997-03-02", "%F").unwrap(),
                    )))]),
                    1,
                ),
                (
                    RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                        NaiveDate::parse_from_str("2040-04-10", "%F").unwrap(),
                    )))]),
                    1,
                ),
            ]),
        )
        .unwrap();

    circuit.step().unwrap();

    let output = circuit.consolidate_output(NodeId::new(660)).unwrap();
    assert!(must_equal_sc(
        &output,
        &StreamCollection::Set(vec![
            (
                RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                    NaiveDate::parse_from_str("1957-04-09", "%F").unwrap()
                )))]),
                1,
            ),
            (
                RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                    NaiveDate::parse_from_str("1996-03-01", "%F").unwrap()
                )))]),
                1,
            ),
            (
                RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                    NaiveDate::parse_from_str("1997-03-02", "%F").unwrap()
                )))]),
                1,
            ),
            (
                RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                    NaiveDate::parse_from_str("1957-06-13", "%F").unwrap()
                )))]),
                1,
            ),
            (
                RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                    NaiveDate::parse_from_str("1996-02-29", "%F").unwrap()
                )))]),
                1,
            ),
            (
                RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                    NaiveDate::parse_from_str("1996-02-28", "%F").unwrap()
                )))]),
                1,
            ),
            (
                RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                    NaiveDate::parse_from_str("1997-02-28", "%F").unwrap()
                )))]),
                1,
            ),
            (
                RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                    NaiveDate::parse_from_str("1996-03-02", "%F").unwrap()
                )))]),
                1,
            ),
            (
                RowLiteral::new(vec![NullableConstant::Nullable(Some(Constant::Date(
                    NaiveDate::parse_from_str("1997-03-01", "%F").unwrap()
                )))]),
                1,
            ),
        ])
    ));

    circuit.kill().unwrap();
}
