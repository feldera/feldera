//! Write the producer topology contract, not full relational semantics.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = schemars::schema_for!(feldera_sql_rel_model::input_producers::InputProducers);
    serde_json::to_writer_pretty(std::io::stdout().lock(), &schema)?;
    println!();
    Ok(())
}
