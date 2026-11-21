use feldera_sql_ir::Circuit;
use feldera_sql_opt::adjust_sql_index;
use serde_json::json;

#[test]
fn sql_and_safe_offsets_keep_their_own_origins_and_share_the_offset_type() {
    let position =
        json!({"start_line_number":1,"start_column":12,"end_line_number":1,"end_column":16});
    let binary = |id, opcode: &str, left, right| {
        json!({
            "class":"DBSPBinaryExpression","id":id,"sourceEmpty":false,"position":position,
            "type":{"node":10},"opcode":opcode,"left":left,"right":right
        })
    };
    let integer = json!({"class":"DBSPTypeInteger","id":10,"sourceEmpty":true,
        "code":"INT32","width":32,"signed":true,"mayBeNull":false});
    let offset =
        json!({"class":"DBSPI32Literal","id":11,"sourceEmpty":true,"type":integer,"value":"2"});
    let array = json!({"class":"DBSPVariablePath","id":12,"variable":"a","type":{
        "class":"DBSPTypeArray","id":13,"typeArgs":[{"node":10}],"mayBeNull":false,"code":"ARRAY"}});
    let graph = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
        {"class":"DBSPConstantOperator","id":1,"inputs":[],"function":binary(20,"SQL_INDEX",array,offset)},
        {"class":"DBSPConstantOperator","id":2,"inputs":[],"function":binary(21,"SAFE_RUST_INDEX",json!({"node":12}),json!({"node":11}))}
    ]})).unwrap();
    let result = adjust_sql_index(graph).unwrap();
    let offsets: Vec<_> = result
        .operators()
        .map(|(_, node)| {
            let access = result.node(node.edge("function").unwrap()).unwrap();
            assert_eq!(access.data("opcode"), Some(&json!("RUST_INDEX")));
            result.node(access.edge("right").unwrap()).unwrap()
        })
        .collect();
    assert_eq!(offsets[0].data("value"), Some(&json!("1")));
    assert!(offsets[0].field("position").is_some());
    assert_eq!(offsets[1].data("value"), Some(&json!("2")));
    assert!(offsets[1].field("position").is_none());
    assert_eq!(
        offsets[0].edge("type").unwrap(),
        offsets[1].edge("type").unwrap()
    );
    assert!(result
        .node(offsets[0].edge("type").unwrap())
        .unwrap()
        .field("singleton")
        .is_none());
}
