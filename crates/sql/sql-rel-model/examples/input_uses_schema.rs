//! Write the shared input-reference/use contract, not the full S1 schema.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = schemars::schema_for!(feldera_sql_rel_model::input_uses::InputUses);
    serde_json::to_writer_pretty(std::io::stdout().lock(), &schema)?;
    println!();
    Ok(())
}
