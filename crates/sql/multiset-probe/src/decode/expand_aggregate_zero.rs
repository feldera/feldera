//! Expand the empty-input row of a global aggregate in a flat snapshot circuit.

use super::{
    array, encode_rewrite, field, inner_reference,
    wire_edits::{copy_intermediate_origin, copy_origin, operator, port, WireEdits},
};
use crate::{provenance::Provenance, Error, Result};
use serde_json::{json, Value};

fn tagged(mut operator: Value, region: usize) -> Value {
    operator["annotations"] = json!([{"class": "GlobalAggregate", "id": region}]);
    operator
}

pub(crate) fn rewrite(circuit: &Value) -> Result<Value> {
    let original = array(circuit, "allOperators")?;
    let mut edits = WireEdits::new(circuit)?;
    let mut provenance = Provenance::read(original)?;
    let mut operators = Vec::with_capacity(original.len());
    let mut region = 0;
    for node in original {
        let id = edits.remember_type(node)?;
        if node["class"] == "DBSPNestedOperator" {
            return Err(Error(
                "aggregate-zero expansion requires a flat circuit".into(),
            ));
        }
        if node["class"] != "DBSPAggregateZeroOperator" {
            operators.push(edits.retain(node, id)?);
            continue;
        }
        let [input] = array(node, "inputs")? else {
            return Err(Error("aggregate-zero expansion requires one input".into()));
        };
        let body = edits.definitions().resolve(field(node, "function")?)?;
        let body_type = edits.definitions().resolve(field(body, "type")?)?;
        let body = inner_reference(body)?;
        let row = inner_reference(body_type)?;
        let output = inner_reference(field(node, "outputType")?)?;
        let map_type = json!({
            "class": "DBSPTypeZSet", "id": edits.inner_id()?, "sourceEmpty": true,
            "elementType": row, "name": "WSet", "code": "ZSET", "mayBeNull": false
        });
        let map_type_ref = inner_reference(&map_type)?;
        let map_id = edits.id()?;
        let mut map = operator("DBSPMapOperator", map_id, vec![input.clone()], map_type);
        map["function"] = edits.constant_function(body_type, body.clone())?;
        copy_intermediate_origin(node, &mut map, &mut provenance)?;
        operators.push(tagged(map, region));

        let neg_id = edits.id()?;
        let mut neg = operator(
            "DBSPNegateOperator",
            neg_id,
            vec![port(map_id)],
            map_type_ref,
        );
        copy_intermediate_origin(node, &mut neg, &mut provenance)?;
        operators.push(tagged(neg, region));

        let zero_id = edits.id()?;
        let mut zero = operator("DBSPConstantOperator", zero_id, Vec::new(), output.clone());
        zero["function"] = json!({
            "class": "DBSPZSetExpression", "id": edits.inner_id()?, "sourceEmpty": true,
            "elementType": row, "data": [body], "type": output, "weights": [1]
        });
        copy_intermediate_origin(node, &mut zero, &mut provenance)?;
        operators.push(tagged(zero, region));

        // Sum the real aggregate, the empty-row seed and the seed's retraction.
        let mut sum = operator(
            "DBSPAtomicSumOperator",
            id,
            vec![port(zero_id), port(neg_id), input.clone()],
            output,
        );
        copy_origin(node, &mut sum);
        operators.push(tagged(sum, region));
        edits.replaced(id);
        region += 1;
    }
    encode_rewrite(circuit, &edits.finish(circuit, operators))
}
