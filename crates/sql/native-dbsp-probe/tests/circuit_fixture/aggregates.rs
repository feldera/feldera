use super::circuit_fixture::{body, class, field, items, links, number};
use feldera_sql_difftest::{compare_circuits, parse_json};
use serde_json::Value;
use std::{env, fs};

pub(super) fn case_from_file(variable: &str, test: &str) -> Value {
    let path = env::var_os(variable).expect("frozen corpus case path");
    let case = parse_json(&fs::read(path).unwrap()).expect("strict JSON case");
    assert_eq!(
        case["source_test"],
        format!("org.dbsp.sqlCompiler.compiler.sql.simple.OptimizedIncrementalTests#{test}"),
    );
    assert_eq!(case["case_index"], 0);
    assert_eq!(case["step_semantics"], "delta");
    compare_circuits(&case["circuit"], &case["circuit"]).expect("strict exported circuit");
    case
}

pub(super) fn operator_graph<'a>(case: &'a Value, graph: &[(&str, &[usize])]) -> &'a [Value] {
    let operators = items(&case["circuit"]["allOperators"]);
    assert_eq!(operators.len(), graph.len());
    for (operator, (name, sources)) in operators.iter().zip(graph) {
        class(operator, name);
        let sources: Vec<_> = sources.iter().map(|&index| &operators[index]).collect();
        links(operator, &sources);
    }
    assert_eq!(operators[0]["tableName"]["name"], "t");
    assert_eq!(operators[operators.len() - 3]["viewName"]["name"], "v");
    assert_eq!(operators.last().unwrap()["viewName"]["name"], "error_view");
    let empty = class(
        &operators[operators.len() - 2]["function"],
        "DBSPZSetExpression",
    );
    assert!(items(&empty["data"]).is_empty() && items(&empty["weights"]).is_empty());
    operators
}

pub(super) fn tuple(expression: &Value) -> &[Value] {
    items(&class(expression, "DBSPTupleExpression")["fields"])
}

pub(super) fn constant_tuple(expression: &Value) -> &[Value] {
    let static_value = class(
        &class(expression, "DBSPCloneExpression")["expression"],
        "DBSPStaticExpression",
    );
    tuple(&static_value["initializer"])
}

pub(super) fn count_filter(case: &Value) -> (usize, i32) {
    let operators = operator_graph(
        case,
        &[
            ("DBSPSourceMultisetOperator", &[]),
            ("DBSPFlatMapIndexOperator", &[0]),
            ("DBSPAggregateLinearPostprocessOperator", &[1]),
            ("DBSPMapIndexOperator", &[2]),
            ("DBSPAggregateLinearPostprocessOperator", &[3]),
            ("DBSPMapOperator", &[4]),
            ("DBSPMapOperator", &[4]),
            ("DBSPNegateOperator", &[6]),
            ("DBSPIncrementalConstantOperator", &[]),
            ("DBSPAtomicSumOperator", &[8, 7, 5]),
            ("DBSPSinkOperator", &[9]),
            ("DBSPIncrementalConstantOperator", &[]),
            ("DBSPSinkOperator", &[11]),
        ],
    );
    let ([input], index) = body(&operators[1]) else {
        panic!("expected one index parameter");
    };
    let [filter, binding] = items(&class(index, "DBSPBlockExpression")["contents"]) else {
        panic!("expected a filter and a key");
    };
    let guard = class(
        &class(filter, "DBSPExpressionStatement")["expression"],
        "DBSPIfExpression",
    );
    let negated = class(&guard["condition"], "DBSPUnaryExpression");
    assert_eq!(negated["opcode"], "NOT");
    let compare = class(&negated["source"], "DBSPBinaryExpression");
    assert_eq!(compare["opcode"], "GT");
    let column = field(&compare["left"], input);
    let threshold = number(&compare["right"], "DBSPI32Literal").expect("threshold");
    let [key, value] = items(
        &class(
            &class(binding, "DBSPLetStatement")["initializer"],
            "DBSPRawTupleExpression",
        )["fields"],
    ) else {
        panic!("expected index key and value");
    };
    let [key] = tuple(key) else {
        panic!("expected singleton key");
    };
    assert_eq!(field(key, input), column);
    assert!(constant_tuple(value).is_empty());
    let (_, per_key) = body(&operators[2]);
    assert_eq!(number(per_key, "DBSPI64Literal"), Some(1_i64));
    let (_, global) = body(&operators[4]);
    let [rows, distinct] = tuple(global) else {
        panic!("expected row count and distinct count");
    };
    let [row_count, row_presence] = tuple(rows) else {
        panic!("expected row-count contribution");
    };
    let cast = class(
        &class(row_count, "DBSPHandleErrorExpression")["source"],
        "DBSPCastExpression",
    );
    assert_eq!(cast["safe"], "RustCast");
    assert_eq!(cast["source"]["fieldNo"], 0);
    assert_eq!(number(row_presence, "DBSPI64Literal"), Some(1_i64));
    let [key_count, key_presence] = constant_tuple(distinct) else {
        panic!("expected distinct-count contribution");
    };
    assert_eq!(number(key_count, "DBSPI64Literal"), Some(1_i64));
    assert_eq!(number(key_presence, "DBSPI64Literal"), Some(1_i64));
    let (_, default) = body(&operators[6]);
    let [rows, keys] = constant_tuple(default) else {
        panic!("expected two count defaults");
    };
    assert_eq!(number(rows, "DBSPI64Literal"), Some(0_i64));
    assert_eq!(number(keys, "DBSPI64Literal"), Some(0_i64));
    (column, threshold)
}

pub(super) fn sum_column(case: &Value) -> usize {
    let operators = operator_graph(
        case,
        &[
            ("DBSPSourceMultisetOperator", &[]),
            ("DBSPMapIndexOperator", &[0]),
            ("DBSPAggregateLinearPostprocessOperator", &[1]),
            ("DBSPMapOperator", &[2]),
            ("DBSPMapOperator", &[2]),
            ("DBSPNegateOperator", &[4]),
            ("DBSPIncrementalConstantOperator", &[]),
            ("DBSPAtomicSumOperator", &[6, 5, 3]),
            ("DBSPSinkOperator", &[7]),
            ("DBSPIncrementalConstantOperator", &[]),
            ("DBSPSinkOperator", &[9]),
        ],
    );
    let ([input], index) = body(&operators[1]) else {
        panic!("expected one index parameter");
    };
    let [key, value] = items(&class(index, "DBSPRawTupleExpression")["fields"]) else {
        panic!("expected global index key and value");
    };
    assert!(constant_tuple(key).is_empty());
    let ([value_parameter], contribution) = body(&operators[2]) else {
        panic!("expected one aggregate parameter");
    };
    let [sum, count, presence] = tuple(contribution) else {
        panic!("expected sum, non-NULL count and group presence");
    };
    for conditional in [sum, count] {
        let branch = class(conditional, "DBSPIfExpression");
        let condition = class(&branch["condition"], "DBSPUnaryExpression");
        assert_eq!(condition["opcode"], "NOT");
        let null_test = class(&condition["source"], "DBSPIsNullExpression");
        assert_eq!(field(&null_test["expression"], value_parameter), 0);
        assert_eq!(number(&branch["negative"], "DBSPI64Literal"), Some(0_i64));
    }
    let cast = class(
        &class(&sum["positive"], "DBSPHandleErrorExpression")["source"],
        "DBSPCastExpression",
    );
    assert_eq!(cast["safe"], "RustCast");
    assert_eq!(field(&cast["source"], value_parameter), 0);
    assert_eq!(number(&count["positive"], "DBSPI64Literal"), Some(1_i64));
    assert_eq!(number(presence, "DBSPI64Literal"), Some(1_i64));
    let postprocess = class(&operators[2]["postProcess"], "DBSPClosureExpression");
    let [result] = tuple(&postprocess["body"]) else {
        panic!("expected one aggregate output");
    };
    let choose_sum = class(result, "DBSPIfExpression");
    assert_eq!(choose_sum["condition"]["opcode"], "NEQ");
    assert_eq!(choose_sum["condition"]["left"]["fieldNo"], 1);
    assert_eq!(
        number(&choose_sum["condition"]["right"], "DBSPI64Literal"),
        Some(0_i64)
    );
    assert_eq!(
        number::<i32>(&choose_sum["negative"], "DBSPI32Literal"),
        None
    );
    let (_, default) = body(&operators[4]);
    let [default] = constant_tuple(default) else {
        panic!("expected singleton default");
    };
    assert_eq!(number::<i32>(default, "DBSPI32Literal"), None);
    let [value] = tuple(value) else {
        panic!("expected singleton index value");
    };
    field(value, input)
}
