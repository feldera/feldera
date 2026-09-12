use vergen_gitcl::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gitcl = GitclBuilder::default().sha(false).dirty(false).build()?;
    Emitter::default()
        .add_instructions(&gitcl)?
        .quiet()
        .emit()?;

    // Read via option_env! in controller::stats; without these, a cached build
    // would keep reporting the version it was first compiled with.
    println!("cargo:rerun-if-env-changed=FELDERA_PLATFORM_VERSION");
    println!("cargo:rerun-if-env-changed=FELDERA_RUNTIME_VERSION");
    println!("cargo:rerun-if-env-changed=FELDERA_RUNTIME_OVERRIDE");
    Ok(())
}
