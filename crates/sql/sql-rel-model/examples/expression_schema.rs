//! Write the experimental resolved-expression fragment, not complete S1.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = schemars::schema_for!(feldera_sql_rel_model::expressions::ExpressionTable);
    serde_json::to_writer_pretty(std::io::stdout().lock(), &schema)?;
    println!();
    Ok(())
}
