//! The two frozen Unicode cases, lowered from exported circuits and run on DBSP.

use dbsp::{
    Circuit, DBData, OrdZSet, Runtime,
    circuit::{CircuitConfig, StepSize},
    operator::ConstantGenerator,
    utils::{Tup1, Tup2, Tup5},
};
use feldera_native_dbsp_probe::{
    ErrorRow, ProjectedStringRow, StringProjection, StringProjectionSpec, StringRow,
    build_string_projection, build_string_range,
};
use feldera_native_probe::{
    CompiledStringArchive, CompiledStringRange, Error, StringArchiveLayout,
};
use feldera_sql_difftest::{compare_circuits, parse_json};
use feldera_sqllib::SqlString;
use serde_json::Value;
use std::{env, fs, sync::Arc};

#[path = "circuit_fixture/text_input.rs"]
mod text_input;
use text_input::{ArchivedStringBatch, text_archive};

fn class<'a>(node: &'a Value, expected: &str) -> &'a Value {
    assert_eq!(node["class"], expected, "unexpected IR node");
    node
}

fn one(values: &Value) -> &Value {
    let [value] = values.as_array().expect("IR list").as_slice() else {
        panic!("expected one child in the native string filter subset");
    };
    value
}

fn operator<'a>(operators: &'a [Value], name: &str) -> &'a Value {
    let mut matches = operators.iter().filter(|node| node["class"] == name);
    let first = matches.next().unwrap_or_else(|| panic!("missing {name}"));
    assert!(matches.next().is_none(), "more than one {name}");
    first
}

fn connects(consumer: &Value, producer: &Value) -> bool {
    let input = one(&consumer["inputs"]);
    input["outputNumber"] == 0 && input["operator"]["node"] == producer["id"]
}

fn string_literal(node: &Value) -> &str {
    assert_eq!(class(node, "DBSPStringLiteral")["charset"], "UTF-8");
    node["value"].as_str().expect("non-null UTF-8 literal")
}

fn check_field(node: &Value, parameter: &Value) {
    let field = class(
        &class(node, "DBSPCloneExpression")["expression"],
        "DBSPFieldExpression",
    );
    assert_eq!(field["fieldNo"], 0);
    let input = class(&field["expression"], "DBSPDerefExpression");
    assert_eq!(
        class(&input["expression"], "DBSPVariablePath")["variable"],
        *parameter
    );
}

fn static_value(node: &Value) -> &Value {
    let clone = class(node, "DBSPCloneExpression");
    &class(&clone["expression"], "DBSPStaticExpression")["initializer"]
}

fn comparison_bound<'a>(node: &'a Value, opcode: &str, parameter: &Value) -> &'a str {
    assert_eq!(class(node, "DBSPBinaryExpression")["opcode"], opcode);
    check_field(&node["left"], parameter);
    string_literal(static_value(&node["right"]))
}

struct StringGraph<'a> {
    input: &'a str,
    view: &'a str,
    error_view: &'a str,
    output_type: &'a Value,
    function: &'a Value,
}

fn plan<'a>(circuit: &'a Value, transform: &str) -> StringGraph<'a> {
    compare_circuits(circuit, circuit).expect("strict exported circuit");
    let operators = circuit["allOperators"].as_array().expect("operator list");
    assert_eq!(operators.len(), 5);
    let source = operator(operators, "DBSPSourceMultisetOperator");
    let transform = operator(operators, transform);
    let constant = operator(operators, "DBSPConstantOperator");
    assert!(source["inputs"].as_array().unwrap().is_empty());
    assert!(constant["inputs"].as_array().unwrap().is_empty());
    assert!(connects(transform, source));
    let sinks: Vec<_> = operators
        .iter()
        .filter(|node| node["class"] == "DBSPSinkOperator")
        .collect();
    let [first, second] = sinks.as_slice() else {
        panic!("expected both sinks")
    };
    let (view, errors) = if connects(first, transform) {
        (*first, *second)
    } else {
        (*second, *first)
    };
    assert!(connects(view, transform) && connects(errors, constant));

    let source_type = class(&source["outputType"], "DBSPTypeZSet");
    let source_row = class(&source_type["elementType"], "DBSPTypeTuple");
    assert_eq!(source_row["mayBeNull"], false);
    let field = class(one(&source_row["tupFields"]), "DBSPTypeString");
    assert_eq!(field["mayBeNull"], true);
    assert_eq!(field["fixed"], false);
    assert_eq!(field["precision"], -1);
    let output_type = if transform["outputType"]["node"] == source_type["id"] {
        source_type
    } else {
        class(&transform["outputType"], "DBSPTypeZSet")
    };
    assert_eq!(view["outputType"]["node"], output_type["id"]);

    let error_type = class(&constant["outputType"], "DBSPTypeZSet");
    assert_eq!(errors["outputType"]["node"], error_type["id"]);
    let error_row = class(&error_type["elementType"], "DBSPTypeTuple");
    let error_fields = error_row["tupFields"]
        .as_array()
        .expect("error tuple fields");
    assert_eq!(error_fields.len(), 3);
    for field in error_fields {
        assert_eq!(class(field, "DBSPTypeString")["mayBeNull"], false);
    }
    let empty = class(&constant["function"], "DBSPZSetExpression");
    assert!(empty["data"].as_array().unwrap().is_empty());
    assert!(empty["weights"].as_array().unwrap().is_empty());

    StringGraph {
        input: source["tableName"]["name"].as_str().unwrap(),
        view: view["viewName"]["name"].as_str().unwrap(),
        error_view: errors["viewName"]["name"].as_str().unwrap(),
        output_type,
        function: class(&transform["function"], "DBSPClosureExpression"),
    }
}

fn closure(function: &Value) -> (&Value, &Value) {
    (
        &class(one(&function["parameters"]), "DBSPParameter")["name"],
        &function["body"],
    )
}

fn range(function: &Value) -> (&str, &str) {
    let (parameter, body) = closure(function);
    let condition = class(body, "DBSPUnaryExpression");
    assert_eq!(condition["opcode"], "WRAP_BOOL");
    let range = class(&condition["source"], "DBSPBinaryExpression");
    assert_eq!(range["opcode"], "AND");
    (
        comparison_bound(&range["left"], "GT", parameter),
        comparison_bound(&range["right"], "LT", parameter),
    )
}

fn decode_bag<T>(zset: &Value, decode_row: impl Fn(&Value) -> T) -> Vec<Tup2<T, i64>> {
    class(zset, "DBSPZSetExpression");
    let data = zset["data"].as_array().expect("bag data");
    let weights = zset["weights"].as_array().expect("bag weights");
    assert_eq!(data.len(), weights.len());
    data.iter()
        .zip(weights)
        .map(|(row, weight)| {
            Tup2(
                decode_row(&class(row, "DBSPTupleExpression")["fields"]),
                weight.as_i64().expect("signed weight"),
            )
        })
        .collect()
}

fn decode_input_row(fields: &Value) -> StringRow {
    let cell = one(fields);
    let literal = if cell["class"] == "DBSPCastExpression" {
        assert_eq!(cell["safe"], "SqlUnsafe");
        let target = &cell["type"];
        assert!(target["node"].is_u64() || target["class"] == "DBSPTypeString");
        &cell["source"]
    } else {
        cell
    };
    Tup1(Some(SqlString::from_ref(string_literal(literal))))
}

fn read_case(variable: &str, index: u64) -> Value {
    let path = env::var_os(variable).expect("frozen corpus case path");
    let raw = fs::read(path).expect("read frozen case");
    let case = parse_json(&raw).expect("strict JSON case");
    assert_eq!(
        case["source_test"],
        "org.dbsp.sqlCompiler.compiler.sql.simple.StringTests#testNonBmpCastsAndRanges"
    );
    assert_eq!(case["case_index"], index);
    assert_eq!(case["step_semantics"], "whole_relation");
    case
}

fn steps<'a>(case: &'a Value, graph: &StringGraph<'_>) -> (&'a Value, &'a Value) {
    assert_eq!(one(&case["circuit_inputs"])["text"], graph.input);
    let [view, error] = case["circuit_outputs"].as_array().unwrap().as_slice() else {
        panic!("two circuit outputs")
    };
    assert_eq!(view["name"]["text"], graph.view);
    assert_eq!(view["system"], false);
    assert_eq!(error["name"]["text"], graph.error_view);
    assert_eq!(error["system"], true);
    let command = one(&case["commands"]);
    assert_eq!(command["kind"], "step");
    let input = one(&command["inputs"]);
    assert_eq!(input["name"]["text"], graph.input);
    (&input["zset_ir"], &one(&command["outputs"])["zset_ir"])
}

#[test]
#[ignore = "requires P0_STRING_RANGE_CASE from the frozen native corpus archive"]
fn frozen_range_filter_executes_from_ir() {
    let case = read_case("P0_STRING_RANGE_CASE", 1);
    let graph = plan(&case["circuit"], "DBSPFilterOperator");
    let (input, output) = steps(&case, &graph);
    let mut rows = decode_bag(input, decode_input_row);
    let expected = decode_bag(output, decode_input_row);
    let (lower, upper) = range(graph.function);
    let lower = lower.to_owned();
    let upper = upper.to_owned();
    let (mut runtime, ports) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_step_size(StepSize::FullSteps),
        move |circuit| Ok(build_string_range(circuit, &lower, &upper)?),
    )
    .expect("build native range circuit");
    ports.input.append(&mut rows);
    runtime.transaction().expect("execute frozen input");
    assert_eq!(
        ports.filtered.consolidate(),
        OrdZSet::from_keys((), expected)
    );
    assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
}

fn arguments<'a>(node: &'a Value, name: &str) -> &'a Value {
    let apply = class(node, "DBSPApplyExpression");
    let function = class(&apply["function"], "DBSPPathExpression");
    let path = class(&function["path"], "DBSPPath");
    let component = class(one(&path["components"]), "DBSPSimplePathSegment");
    assert_eq!(component["identifier"], name);
    assert!(component["genericArgs"].as_array().unwrap().is_empty());
    &apply["arguments"]
}

fn cast(node: &Value, fixed: bool, width: i64) -> &Value {
    let handle = class(node, "DBSPHandleErrorExpression");
    assert_eq!(handle["runtimeBehavior"], "PanicWithSource");
    let conversion = class(&handle["source"], "DBSPCastExpression");
    assert_eq!(conversion["safe"], "RustCast");
    let result = class(&conversion["type"], "DBSPTypeSqlResult");
    let text = class(one(&result["typeArgs"]), "DBSPTypeString");
    assert_eq!(text["fixed"], fixed);
    assert_eq!(text["mayBeNull"], true);
    assert_eq!(text["precision"], width);
    &conversion["source"]
}

fn integer_literal(node: &Value) -> i32 {
    class(node, "DBSPI32Literal")["value"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

fn projection(graph: &StringGraph<'_>) -> StringProjectionSpec {
    let output = class(&graph.output_type["elementType"], "DBSPTypeTuple");
    let output_fields = output["tupFields"].as_array().unwrap();
    let [first, literal, dynamic_length, static_length, trimmed] = output_fields.as_slice() else {
        panic!("expected a five-field Unicode projection")
    };
    for (field, nullable, precision) in [(first, true, 1), (literal, false, 1), (trimmed, true, -1)]
    {
        assert_eq!(class(field, "DBSPTypeString")["mayBeNull"], nullable);
        assert_eq!(field["fixed"], false);
        assert_eq!(field["precision"], precision);
    }
    for (field, nullable) in [(dynamic_length, true), (static_length, false)] {
        assert_eq!(class(field, "DBSPTypeInteger")["mayBeNull"], nullable);
        assert_eq!(field["width"], 32);
        assert_eq!(field["signed"], true);
    }
    let (parameter, body) = closure(graph.function);
    let fields = class(body, "DBSPTupleExpression")["fields"]
        .as_array()
        .unwrap();
    let [first, literal, dynamic_length, static_length, trimmed] = fields.as_slice() else {
        panic!("expected five projection expressions")
    };
    check_field(cast(first, false, 1), parameter);
    let left = cast(one(arguments(dynamic_length, "char_lengthN")), true, 2);
    let [value, width] = arguments(left, "left_sN_i32")
        .as_array()
        .unwrap()
        .as_slice()
    else {
        panic!("expected LEFT with a constant width")
    };
    check_field(value, parameter);
    assert_eq!(integer_literal(width), 1);
    let literal_char = string_literal(one(arguments(static_value(static_length), "char_length_")));
    let [pattern, text] = arguments(trimmed, "trim_leading_s_sN")
        .as_array()
        .unwrap()
        .as_slice()
    else {
        panic!("expected trim pattern and input text")
    };
    check_field(text, parameter);
    StringProjectionSpec {
        varchar_literal: string_literal(static_value(literal)).to_owned(),
        padded_width: 2,
        padded_literal: literal_char.to_owned(),
        trim_pattern: string_literal(static_value(pattern)).to_owned(),
    }
}

fn decode_projection(fields: &Value) -> ProjectedStringRow {
    let [first, literal, length, literal_length, trimmed] = fields.as_array().unwrap().as_slice()
    else {
        panic!("expected five output values")
    };
    Tup5(
        Some(SqlString::from_ref(string_literal(first))),
        SqlString::from_ref(string_literal(literal)),
        Some(integer_literal(length)),
        integer_literal(literal_length),
        Some(SqlString::from_ref(string_literal(trimmed))),
    )
}

#[test]
#[ignore = "requires P0_STRING_PROJECTION_CASE from the frozen native corpus archive"]
fn frozen_unicode_projection_executes_from_ir() {
    let case = read_case("P0_STRING_PROJECTION_CASE", 0);
    let graph = plan(&case["circuit"], "DBSPMapOperator");
    let spec = projection(&graph);
    let (input, output) = steps(&case, &graph);
    let mut rows = decode_bag(input, decode_input_row);
    let expected = decode_bag(output, decode_projection);
    let (mut runtime, ports) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_step_size(StepSize::FullSteps),
        move |circuit| Ok(build_string_projection(circuit, &spec)?),
    )
    .expect("build native Unicode projection");
    ports.input.append(&mut rows);
    runtime.transaction().expect("execute frozen input");
    assert_eq!(ports.output.consolidate(), OrdZSet::from_keys((), expected));
    assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
}

fn run_owned_rows<T, F>(
    rows: Vec<Tup2<StringRow, i64>>,
    expected: Vec<Tup2<T, i64>>,
    compile: impl FnOnce() -> Result<F, Error> + Clone + Send + 'static,
) where
    T: DBData,
    F: Fn(&ArchivedStringBatch) -> OrdZSet<T> + 'static,
{
    let batch = Arc::new(ArchivedStringBatch::new(&rows));
    let owner = batch.owner();
    let (mut runtime, (input, output, errors)) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_step_size(StepSize::FullSteps),
        move |circuit| {
            let evaluate = compile()?;
            let (source, input) = circuit.add_input_stream::<Option<Arc<ArchivedStringBatch>>>();
            let output = source.apply(move |rows| {
                rows.as_deref()
                    .map(&evaluate)
                    .unwrap_or_else(OrdZSet::empty)
            });
            let errors = circuit.add_source(ConstantGenerator::new(OrdZSet::<ErrorRow>::empty()));
            Ok((input, output.output(), errors.output()))
        },
    )
    .unwrap();
    input.set_for_worker(0, Some(batch));
    runtime.transaction().unwrap();
    assert_eq!(output.consolidate(), OrdZSet::from_keys((), expected));
    assert_eq!(errors.consolidate(), OrdZSet::empty());
    runtime.kill().unwrap();
    drop(input);
    assert!(owner.upgrade().is_none());
}

#[test]
fn descriptor_reads_nullable_inline_and_external_utf8() {
    let layout = text_archive();
    let values = [
        None,
        Some(""),
        Some("é🍉"),
        Some("é: longer than fifteen bytes🍉"),
    ];
    let archive = layout.encode(&values).unwrap();
    for ((bytes, root), expected) in archive.rows().zip(values) {
        assert_eq!(layout.decode_single(bytes, root).unwrap(), expected);
        assert!(
            layout
                .decode_single(&bytes[..bytes.len() - 1], root)
                .is_err()
        );
        assert!(layout.decode_single(bytes, usize::MAX).is_err());
    }

    let plain =
        CompiledStringArchive::compile(StringArchiveLayout::new(16, 0, None).unwrap()).unwrap();
    let archive = plain
        .encode(&[Some("a"), Some("a long out-of-line string")])
        .unwrap();
    let mut rows = archive.rows();
    let (inline, root) = rows.next().unwrap();
    let mut invalid_inline = inline.to_vec();
    invalid_inline[root] = 0xff;
    assert!(plain.decode_single(&invalid_inline, root).is_err());
    let (outline, root) = rows.next().unwrap();
    let mut invalid_outline = outline.to_vec();
    invalid_outline[root + 8..root + 16].copy_from_slice(&i64::MIN.to_le_bytes());
    assert!(plain.decode_single(&invalid_outline, root).is_err());
}

#[test]
#[ignore = "requires P0_STRING_RANGE_CASE from the frozen native corpus archive"]
fn frozen_range_filter_reads_descriptor_owned_input() {
    let case = read_case("P0_STRING_RANGE_CASE", 1);
    let graph = plan(&case["circuit"], "DBSPFilterOperator");
    let (input, output) = steps(&case, &graph);
    let (lower, upper) = range(graph.function);
    let (lower, upper) = (lower.to_owned(), upper.to_owned());
    run_owned_rows(
        decode_bag(input, decode_input_row),
        decode_bag(output, decode_input_row),
        move || {
            let predicate = CompiledStringRange::compile(&lower, &upper)?;
            Ok(move |batch: &ArchivedStringBatch| {
                let (strings, weights): (Vec<_>, Vec<_>) = batch.values().unzip();
                let keep = predicate.filter(&strings).unwrap();
                OrdZSet::from_keys(
                    (),
                    strings
                        .into_iter()
                        .zip(weights)
                        .zip(keep)
                        .filter(|(_, keep)| *keep != 0)
                        .map(|((text, weight), _)| {
                            Tup2(Tup1(text.map(SqlString::from_ref)), weight)
                        })
                        .collect(),
                )
            })
        },
    );
}

#[test]
#[ignore = "requires P0_STRING_PROJECTION_CASE from the frozen native corpus archive"]
fn frozen_unicode_projection_reads_descriptor_owned_input() {
    let case = read_case("P0_STRING_PROJECTION_CASE", 0);
    let graph = plan(&case["circuit"], "DBSPMapOperator");
    let (input, output) = steps(&case, &graph);
    let spec = projection(&graph);
    run_owned_rows(
        decode_bag(input, decode_input_row),
        decode_bag(output, decode_projection),
        move || {
            let projection = StringProjection::compile(&spec)?;
            Ok(move |batch: &ArchivedStringBatch| {
                projection.evaluate(
                    batch
                        .values()
                        .map(|(text, weight)| (text.map(SqlString::from_ref), weight)),
                )
            })
        },
    );
}
