use vergen_gitcl::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gitcl = GitclBuilder::default().sha(false).dirty(false).build()?;
    Emitter::default().add_instructions(&gitcl)?.emit()?;

    println!("cargo:rerun-if-env-changed=FELDERA_RUNTIME_OVERRIDE");
    Ok(())
}
