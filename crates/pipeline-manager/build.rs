use change_detection::ChangeDetection;
use static_files::{resource_dir, NpmBuild};
use std::env;
use std::path::{Path, PathBuf};

// These are touched during the build, so it would re-build every time if we
// don't exclude them from change detection:
const EXCLUDE_LIST: [&str; 4] = [
    "../../web-console/node_modules",
    "../../web-console/build",
    "../../web-console/.svelte-kit",
    "../../web-console/pipeline-manager-",
];

/// The build script has two modes:
///
/// - if `WEBCONSOLE_BUILD_DIR` we use that to serve in the manager
/// - otherwise we build the web-console from web-console and serve it from the
///   manager
///
/// The first mode is useful in CI builds to cache the website build so it
/// doesn't get built many times due to changing rustc flags.
fn main() {
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

        // This should be safe because the build-script is single-threaded
        unsafe {
            env::set_var("BUILD_DIR", rel_build_dir.clone());
        }
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

    // Determine whether the platform version includes a suffix
    let platform_version_suffix =
        env::var("FELDERA_PLATFORM_VERSION_SUFFIX").unwrap_or("".to_string());
    println!("cargo:rerun-if-env-changed=FELDERA_PLATFORM_VERSION_SUFFIX");
    println!("cargo:rustc-env=FELDERA_PLATFORM_VERSION_SUFFIX={platform_version_suffix}");

    // Determine the enterprise version
    let enterprise_version = env::var("FELDERA_ENTERPRISE_VERSION").unwrap_or(
        {
            if cfg!(feature = "feldera-enterprise") {
                "unknown"
            } else {
                "not-applicable"
            }
        }
        .to_string(),
    );
    println!("cargo:rerun-if-env-changed=FELDERA_ENTERPRISE_VERSION");
    println!("cargo:rustc-env=FELDERA_ENTERPRISE_VERSION={enterprise_version}");
}
