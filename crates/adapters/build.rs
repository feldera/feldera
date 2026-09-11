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

    // The compiler server sets FELDERA_RUNTIME_VERSION alongside FELDERA_RUNTIME_OVERRIDE.
    // Without the version, controller::stats would fall back to this crate's version,
    // which is wrong for an overridden runtime; fail here instead of reporting it.
    if std::env::var_os("FELDERA_RUNTIME_OVERRIDE").is_some()
        && std::env::var_os("FELDERA_RUNTIME_VERSION").is_none()
    {
        return Err("FELDERA_RUNTIME_OVERRIDE is set but FELDERA_RUNTIME_VERSION is not".into());
    }
    Ok(())
}
