fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=FELDERA_RUNTIME_OVERRIDE");
    Ok(())
}
