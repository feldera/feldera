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

    // A pipeline-manager that sets FELDERA_PLATFORM_VERSION also sets
    // FELDERA_RUNTIME_VERSION; a manager that predates both sets neither.
    // Missing only the runtime version is a manager bug that controller::stats
    // would hide by reporting a fallback version.
    if std::env::var_os("FELDERA_PLATFORM_VERSION").is_some()
        && std::env::var_os("FELDERA_RUNTIME_VERSION").is_none()
    {
        return Err("FELDERA_PLATFORM_VERSION is set but FELDERA_RUNTIME_VERSION is not".into());
    }
    Ok(())
}
