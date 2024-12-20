use change_detection::ChangeDetection;
use static_files::{resource_dir, NpmBuild};
use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

// These are touched during the build, so it would re-build every time if we
// don't exclude them from change detection:
const EXCLUDE_LIST: [&str; 4] = [
    "../../web-console/node_modules",
    "../../web-console/build",
    "../../web-console/.svelte-kit",
    "../../web-console/pipeline-manager-",
];

/// The build script has two features:
/// 1. Generate the static files of the Web Console, which are served by the manager.
/// 2. Identify the git commit hash, which is used in the platform version.
///
/// **Web Console build**
///
/// The Web Console build has two modes:
/// - If `WEBCONSOLE_BUILD_DIR` we use that to serve in the manager
/// - Otherwise, we build it from the ../../web-console directory and serve that
///
/// The first mode is useful in CI builds to cache the website build so it
/// doesn't get built many times due to changing rustc flags.
///
/// **Identify git commit hash**
///
/// The platform version must include the git commit hash, such that automatic
/// upgrades take place not only between releases, but also between individual commits.
///
/// Identification has two modes:
/// - If `FELDERA_BUILD_GIT_COMMIT_HASH` is set, use this
/// - Otherwise, use `git rev-parse HEAD`
///
/// The first mode is useful when the crate was copied directly without the parent `.git` folder.
fn main() {
    // Web Console build
    if let Ok(webui_out_folder) = env::var("WEBCONSOLE_BUILD_DIR") {
        ChangeDetection::path(&webui_out_folder)
            .path("build.rs")
            .generate();
        println!("cargo:rerun-if-changed=build.rs");
        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
        let mut resource_dir = resource_dir(webui_out_folder);
        let _ = resource_dir.with_generated_filename(out_dir.join("generated.rs"));
        resource_dir
            .build()
            .expect("Could not use WEBCONSOLE_BUILD_DIR as a website location")
    } else {
        ChangeDetection::exclude(|path: &Path| {
            EXCLUDE_LIST
                .iter()
                .any(|exclude| path.to_str().unwrap().starts_with(exclude))
                // Also exclude web-console folder itself because we mutate things inside
                // of it
                || path.to_str().unwrap() == "../../web-console/"
        })
        .path("../../web-console/")
        .path("build.rs")
        .generate();

        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
        let out_dir_parts = out_dir.iter().collect::<Vec<_>>();
        let rel_build_dir = out_dir_parts[out_dir_parts.len() - 2..]
            .iter()
            .collect::<PathBuf>();
        env::set_var("BUILD_DIR", rel_build_dir.clone());
        let asset_path: PathBuf = Path::new("../../web-console/").join(rel_build_dir);
        let mut resource_dir = NpmBuild::new("../../web-console")
            .executable("bun")
            .install()
            .expect(
                "Could not run `bun install`. Follow set-up instructions in web-console/README.md",
            )
            .run("build")
            .expect("Could not run `bun run build`. Run it manually in web-console/ to debug.")
            .target(asset_path.clone())
            .to_resource_dir();
        let _ = resource_dir.with_generated_filename(out_dir.join("generated.rs"));
        resource_dir.build().expect("SvelteKit app failed to build")
    };

    // Identify git commit hash
    let git_commit_hash = env::var("FELDERA_BUILD_GIT_COMMIT_HASH").unwrap_or({
        let output = Command::new("git")
            .arg("rev-parse")
            .arg("HEAD")
            .stdout(Stdio::piped())
            .output()
            .expect("failed to execute 'git rev-parse HEAD'");
        if !output.status.success() {
            panic!(
                "Command 'git rev-parse HEAD' returned unsuccessful status: {}",
                output.status
            )
        }
        String::from_utf8(output.stdout)
            .expect("git commit hash should be valid UTF-8")
            .trim()
            .to_string()
    });
    assert_eq!(
        git_commit_hash.len(),
        40,
        "expected git commit hash to be 40 characters long: {git_commit_hash}"
    );
    for c in git_commit_hash.chars() {
        assert!(
            c.is_ascii_hexdigit() && (c.is_ascii_digit() || c.is_lowercase()),
            "Invalid character '{c}' in git commit hash"
        );
    }
    // TODO: is below check for .git necessary / too slow?
    // println!("cargo:rerun-if-changed=../../.git/");
    println!("cargo:rerun-if-env-changed=FELDERA_BUILD_GIT_COMMIT_HASH");
    println!("cargo:rustc-env=FELDERA_BUILD_GIT_COMMIT_HASH={git_commit_hash}");
}
