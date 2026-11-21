//! Replay the nested-row JSON program and its different field-name policies.

use dbsp::{
    Circuit, OrdZSet, Runtime,
    circuit::{CircuitConfig, StepSize},
    operator::Generator,
    utils::{Tup1, Tup2, Tup3},
};
use feldera_native_dbsp_probe::{
    ErrorRow, JsonAddressInput, JsonAddressProjection, JsonAddressRow, JsonAddressSpec,
    build_json_address,
};
use feldera_sql_difftest::{compare_circuits, parse_json};
use feldera_sqllib::SqlString;
use serde_json::Value;
use std::{env, fs, sync::Arc};

mod circuit_fixture;
#[path = "circuit_fixture/text_input.rs"]
mod text_input;
use circuit_fixture::{bag, body, class, field, items, links, number, one};
use text_input::ArchivedStringBatch;

fn function_name(call: &Value) -> &str {
    let apply = class(call, "DBSPApplyExpression");
    let function = class(&apply["function"], "DBSPPathExpression");
    let path = class(&function["path"], "DBSPPath");
    class(one(&path["components"]), "DBSPSimplePathSegment")["identifier"]
        .as_str()
        .unwrap()
}

fn input_field(call: &Value, parameter: &Value) -> usize {
    let argument = class(one(&call["arguments"]), "DBSPCloneExpression");
    field(&argument["expression"], parameter)
}

fn plan(case: &Value) -> JsonAddressSpec {
    let circuit = &case["circuit"];
    compare_circuits(circuit, circuit).expect("strict exported circuit");
    let [
        source,
        parsed,
        parsed_sink,
        decoded,
        decoded_sink,
        constant,
        errors,
    ] = items(&circuit["allOperators"])
    else {
        panic!("expected two JSON conversions and an empty system output");
    };
    for (operator, name, inputs) in [
        (source, "DBSPSourceMultisetOperator", &[][..]),
        (parsed, "DBSPMapOperator", &[source]),
        (parsed_sink, "DBSPSinkOperator", &[parsed]),
        (decoded, "DBSPMapOperator", &[source]),
        (decoded_sink, "DBSPSinkOperator", &[decoded]),
        (constant, "DBSPConstantOperator", &[]),
        (errors, "DBSPSinkOperator", &[constant]),
    ] {
        links(class(operator, name), inputs);
    }
    assert_eq!(source["tableName"]["name"], "data");
    assert_eq!(parsed_sink["viewName"]["name"], "decoded0");
    assert_eq!(decoded_sink["viewName"]["name"], "decoded1");
    assert_eq!(errors["viewName"]["name"], "error_view");
    let empty = class(&constant["function"], "DBSPZSetExpression");
    assert!(items(&empty["data"]).is_empty() && items(&empty["weights"]).is_empty());

    let ([input], parsed_body) = body(parsed) else {
        panic!("expected one JSON source parameter");
    };
    let lazy_input = class(parsed_body, "DBSPLetExpression");
    let json = &class(&lazy_input["initializer"], "DBSPLazyExpression")["expression"];
    assert_eq!(function_name(json), "parse_json_fv_sN");
    let source_column = input_field(json, input);
    let result = one(&class(&lazy_input["consumer"], "DBSPTupleExpression")["fields"]);
    let nullable = class(result, "DBSPIfExpression");
    let null_test = class(&nullable["condition"], "DBSPIsNullExpression");
    class(&null_test["expression"], "DBSPDerefExpression");
    assert!(class(&nullable["positive"], "DBSPTupleExpression")["fields"].is_null());
    let recover = class(&nullable["negative"], "DBSPHandleErrorExpression");
    assert_eq!(recover["runtimeBehavior"], "ReturnNone");
    let build_tuple = &recover["source"];
    assert_eq!(function_name(build_tuple), "Tup3::from_results");
    let [city, street, number] = items(&build_tuple["arguments"]) else {
        panic!("expected three address field casts");
    };
    let variant_fields = [city, street, number].map(|expression| {
        let cast = class(expression, "DBSPCastExpression");
        assert_eq!(cast["safe"], "RustCast");
        let access = class(
            &class(&cast["source"], "DBSPCloneExpression")["expression"],
            "DBSPBinaryExpression",
        );
        assert_eq!(access["opcode"], "VARIANT_INDEX");
        let static_field = class(
            &class(&access["right"], "DBSPCloneExpression")["expression"],
            "DBSPStaticExpression",
        );
        string(&static_field["initializer"])
            .unwrap()
            .str()
            .to_owned()
    });

    let ([input], decoded_body) = body(decoded) else {
        panic!("expected one JSON source parameter");
    };
    let decoded = one(&class(decoded_body, "DBSPTupleExpression")["fields"]);
    assert_eq!(function_name(decoded), "jsonstring_as_address2");
    assert_eq!(input_field(decoded, input), source_column);
    let [record, function] = items(&circuit["declarations"]) else {
        panic!("expected record and conversion declarations");
    };
    let record = class(&class(record, "DBSPDeclaration")["item"], "DBSPStructItem");
    let fields = items(&class(&record["type"], "DBSPTypeStruct")["fields"]);
    assert_eq!(fields.len(), 3);
    for (index, (field, (name, ty))) in fields
        .iter()
        .zip([
            ("city", "DBSPTypeString"),
            ("street", "DBSPTypeString"),
            ("number", "DBSPTypeInteger"),
        ])
        .enumerate()
    {
        assert_eq!(class(field, "Field")["index"], index);
        assert_eq!(field["name"]["name"], name);
        assert_eq!(field["name"]["isQuoted"], false);
        assert_eq!(class(&field["type"], ty)["mayBeNull"], true);
    }
    let function = class(
        &class(function, "DBSPDeclaration")["item"],
        "DBSPFunctionItem",
    );
    let function = &class(&function["function"], "DBSPFunction")["body"];
    let [_, call] = items(&class(function, "DBSPBlockExpression")["contents"]) else {
        panic!("expected nullable input and decoder");
    };
    assert_eq!(
        function_name(&class(call, "DBSPLetStatement")["initializer"]),
        "from_json_string"
    );

    JsonAddressSpec {
        source_column,
        variant_fields,
    }
}

fn string(literal: &Value) -> Option<SqlString> {
    let string = class(literal, "DBSPStringLiteral");
    assert_eq!(string["charset"], "UTF-8");
    string["value"].as_str().map(SqlString::from_ref)
}

fn address(fields: &[Value]) -> JsonAddressRow {
    let [row] = fields else {
        panic!("expected singleton nested row")
    };
    let [city, street, house] = items(&class(row, "DBSPTupleExpression")["fields"]) else {
        panic!("expected city, street and number");
    };
    Tup1(Some(Tup3(
        string(city),
        string(street),
        number(house, "DBSPI32Literal"),
    )))
}

fn read_case() -> Value {
    let path = env::var_os("P0_JSON_ADDRESS_CASE").expect("frozen corpus case path");
    let case = parse_json(&fs::read(path).unwrap()).expect("strict JSON case");
    assert_eq!(
        case["source_test"],
        "org.dbsp.sqlCompiler.compiler.sql.simple.SerdeTest#jsonStructTest2"
    );
    assert_eq!(case["case_index"], 0);
    assert_eq!(case["step_semantics"], "whole_relation");
    case
}

#[test]
#[ignore = "requires P0_JSON_ADDRESS_CASE from the frozen native corpus archive"]
fn frozen_nested_json_address_executes_from_ir() {
    let case = read_case();
    let spec = plan(&case);
    let (mut runtime, ports) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_step_size(StepSize::FullSteps),
        move |circuit| Ok(build_json_address(circuit, &spec)?),
    )
    .unwrap();
    for command in items(&case["commands"]) {
        let mut input = bag(one(&command["inputs"]), "data", |fields| {
            let [text] = fields else {
                panic!("expected a JSON string")
            };
            Tup1(string(text))
        });
        let [parsed, decoded] = items(&command["outputs"]) else {
            panic!("expected both JSON views");
        };
        ports.input.append(&mut input);
        runtime.transaction().unwrap();
        assert_eq!(
            ports.parsed.consolidate(),
            OrdZSet::from_keys((), bag(parsed, "decoded0", address))
        );
        assert_eq!(
            ports.decoded.consolidate(),
            OrdZSet::from_keys((), bag(decoded, "decoded1", address))
        );
        assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
    }

    ports.input.append(&mut vec![Tup2(Tup1(None), 2)]);
    runtime.transaction().unwrap();
    let null_rows = OrdZSet::from_keys((), vec![Tup2(Tup1(None), 2)]);
    assert_eq!(ports.parsed.consolidate(), null_rows);
    assert_eq!(ports.decoded.consolidate(), null_rows);
    assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
}

#[test]
#[ignore = "requires P0_JSON_ADDRESS_CASE from the frozen native corpus archive"]
fn frozen_nested_json_address_reads_descriptor_owned_input() {
    let case = read_case();
    let spec = plan(&case);
    let (mut runtime, (input, parsed, decoded, errors)) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_step_size(StepSize::FullSteps),
        move |circuit| {
            let (parse, decode) = JsonAddressProjection::compile(&spec)?;
            let (source, input) = circuit.add_input_stream::<Option<Arc<ArchivedStringBatch>>>();
            let parsed = source.apply(move |batch| {
                parse.evaluate(batch.iter().flat_map(|batch| {
                    batch
                        .values()
                        .map(|(text, weight)| (text.map(SqlString::from_ref), weight))
                }))
            });
            let decoded = source.apply(move |batch| {
                decode.evaluate(batch.iter().flat_map(|batch| {
                    batch
                        .values()
                        .map(|(text, weight)| (text.map(SqlString::from_ref), weight))
                }))
            });
            let errors = circuit.add_source(Generator::new(OrdZSet::<ErrorRow>::empty));
            Ok((input, parsed.output(), decoded.output(), errors.output()))
        },
    )
    .unwrap();
    let mut owners = Vec::new();
    let mut step = |rows: Vec<Tup2<JsonAddressInput, i64>>, expected_parsed, expected_decoded| {
        let batch = Arc::new(ArchivedStringBatch::new(&rows));
        owners.push(batch.owner());
        input.set_for_worker(0, Some(batch));
        runtime.transaction().unwrap();
        assert_eq!(
            parsed.consolidate(),
            OrdZSet::from_keys((), expected_parsed)
        );
        assert_eq!(
            decoded.consolidate(),
            OrdZSet::from_keys((), expected_decoded)
        );
        assert_eq!(errors.consolidate(), OrdZSet::empty());
    };
    for command in items(&case["commands"]) {
        let input = bag(one(&command["inputs"]), "data", |fields| {
            let [text] = fields else {
                panic!("expected a JSON string")
            };
            Tup1(string(text))
        });
        let [parsed, decoded] = items(&command["outputs"]) else {
            panic!("expected both JSON views");
        };
        step(
            input,
            bag(parsed, "decoded0", address),
            bag(decoded, "decoded1", address),
        );
    }
    step(
        vec![Tup2(Tup1(None), 2)],
        vec![Tup2(Tup1(None), 2)],
        vec![Tup2(Tup1(None), 2)],
    );
    runtime.kill().unwrap();
    drop(input);
    assert!(owners.iter().all(|owner| owner.upgrade().is_none()));
}
