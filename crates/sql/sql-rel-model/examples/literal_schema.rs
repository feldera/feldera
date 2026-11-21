//! Print the draft typed-literal wire schema. Type/source references still need validation.

fn main() {
    let schema = schemars::schema_for!(feldera_sql_rel_model::literals::Literal);
    println!("{}", serde_json::to_string_pretty(&schema).unwrap());
}
