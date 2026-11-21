use super::circuit_fixture::{class, number};
use dbsp::utils::Tup7;
use feldera_native_dbsp_probe::IntegerCaseRow;
use feldera_sqllib::{F64, SqlString};
use serde_json::Value;

pub(super) fn input_row(fields: &[Value]) -> IntegerCaseRow {
    let [id, double, flag, text, key, decimal, unsigned] = fields else {
        panic!("expected seven input fields")
    };
    let bits = class(double, "DBSPDoubleLiteral")["value"]
        .as_u64()
        .expect("double bits");
    let text = class(text, "DBSPStringLiteral");
    assert_eq!(text["charset"], "UTF-8");
    Tup7(
        number(id, "DBSPI32Literal").expect("required INTEGER"),
        F64::new(f64::from_bits(bits)),
        class(flag, "DBSPBoolLiteral")["value"]
            .as_bool()
            .expect("required BOOLEAN"),
        SqlString::from_ref(text["value"].as_str().expect("required VARCHAR")),
        number(key, "DBSPI32Literal"),
        number(decimal, "DBSPDecimalLiteral"),
        number(unsigned, "DBSPU32Literal"),
    )
}
