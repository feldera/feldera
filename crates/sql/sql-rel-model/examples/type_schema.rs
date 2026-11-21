//! Write the type/source fragment; it is not a complete S1 schema.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = schemars::schema_for!(feldera_sql_rel_model::TypeTable);
    serde_json::to_writer_pretty(std::io::stdout().lock(), &schema)?;
    println!();
    Ok(())
}
