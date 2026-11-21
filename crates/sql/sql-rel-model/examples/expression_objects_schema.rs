fn main() {
    println!(
        "{}",
        serde_json::to_string_pretty(&schemars::schema_for!(
            feldera_sql_rel_model::expression_objects::ExpressionObjects
        ))
        .unwrap()
    );
}
