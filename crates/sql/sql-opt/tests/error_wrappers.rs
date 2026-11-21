use feldera_sql_ir::{Circuit, NodeId};
use feldera_sql_opt::create_runtime_error_wrappers;
use serde_json::{json, Value};

fn position(start: u32, end: u32) -> Value {
    json!({"start_line_number": 1, "start_column": start, "end_line_number": 1, "end_column": end})
}

#[test]
fn error_indices_follow_first_use_within_each_operator() {
    let result_type = json!({"class":"DBSPTypeSqlResult","id":10,"code":"RESULT",
        "mayBeNull":false,"typeArgs":[{"class":"DBSPTypeInteger","id":11,
            "code":"INT32","width":32,"signed":true,"mayBeNull":true}]});
    let source = |id, location, ty| {
        json!({"class":"DBSPVariablePath","id":id,
        "variable":format!("value_{id}"),"position":location,"type":ty})
    };
    let cast = |id, safe: bool, location, value| {
        json!({"class":"DBSPCastExpression","id":id,
        "safe":if safe { "UnwrapResultSafe" } else { "UnwrapResultUnsafe" },
        "position":location,"type":{"node":11},"source":value})
    };
    let tuple = json!({"class":"DBSPRawTupleExpression","id":20,"type":{
        "class":"DBSPTypeRawTuple","id":21,"mayBeNull":false,"code":"RAW_TUPLE",
        "tupFields":vec![json!({"node":11}); 5]},"fields":[
        cast(30,false,position(2,3),source(31,position(2,3),result_type)),
        cast(32,true,position(7,9),source(33,position(7,9),json!({"node":10}))),
        cast(34,false,position(7,12),json!({"node":33})),
        cast(35,false,position(0,0),json!({"node":31})),
        cast(36,false,position(12,15),source(37,position(0,0),json!({"node":10})))
    ]});
    let graph = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],
        "allOperators":[{"class":"DBSPConstantOperator","id":1,"inputs":[],"function":tuple},
            {"class":"DBSPConstantOperator","id":2,"inputs":[],"function":{"node":34}}]}))
    .unwrap();
    let circuit = create_runtime_error_wrappers(graph, &mut 0).unwrap();
    let handlers = |id| {
        let node = circuit.node(id).unwrap();
        assert_eq!(node.class(), "DBSPHandleErrorExpression");
        assert_eq!(node.data("hasSourcePosition"), Some(&json!(true)));
        (
            node.data("index").unwrap().as_u64().unwrap(),
            node.data("runtimeBehavior").unwrap().as_str().unwrap(),
        )
    };
    let function = |id| {
        circuit
            .node(NodeId::Outer(id))
            .unwrap()
            .edge("function")
            .unwrap()
    };
    let first = circuit.node(function(1)).unwrap().edges("fields").unwrap();
    assert_eq!(
        first.into_iter().map(handlers).collect::<Vec<_>>(),
        [
            (0, "PanicWithSource"),
            (1, "ReturnNone"),
            (1, "PanicWithSource"),
            (0, "Panic"),
            (0, "PanicWithSource")
        ]
    );
    assert_eq!(handlers(function(2)), (0, "PanicWithSource"));
}
