use change_detection::ChangeDetection;
use static_files::{resource_dir, NpmBuild};
use std::env;
use std::path::Path;

// These are touched during the build, so it would re-build every time if we
// don't exclude them from change detection:
const EXCLUDE_LIST: [&str; 3] = [
    "../../web-ui/node_modules",
    "../../web-ui/out",
    "../../web-ui/.next",
];

/// The build script has two modes:
///
/// - if `WEBUI_BUILD_DIR` we use that to serve in the manager
/// - otherwise we build the web-ui from web-ui and serve it from the manager
///
/// The first mode is useful in CI builds to cache the website build so it
/// doesn't get built many times due to changing rustc flags.
fn main() {
    if let Ok(webui_out_folder) = env::var("WEBUI_BUILD_DIR") {
        ChangeDetection::path(&webui_out_folder).path("build.rs").generate();
        println!("cargo:rerun-if-changed=build.rs");
        resource_dir(webui_out_folder)
            .build()
            .expect("Could not use WEBUI_BUILD_DIR as a website location")
    } else {
        ChangeDetection::exclude(|path: &Path| {
            EXCLUDE_LIST
            .iter()
            .any(|exclude| path.to_str().unwrap().starts_with(exclude))
            // Also exclude web-ui folder itself because we mutate things inside
            // of it
            || path.to_str().unwrap() == "../../web-ui/"
        })
        .path("../../web-ui/")
        .path("build.rs")
        .generate();
        println!("cargo:rerun-if-env-changed=NEXT_PUBLIC_MUIX_PRO_KEY");

        NpmBuild::new("../../web-ui")
            .executable("yarn")
            .install()
            .expect("Could not run `yarn install`. Follow set-up instructions in web-ui/README.md")
            .run("build")
            .expect("Could not run `yarn build`. Run it manually in web-ui/ to debug.")
            .run("export-to-out")
            .expect("Could not run `yarn export-to-out`. Run it manually in web-ui/ to debug.")
            .target(env::var("OUT_DIR").unwrap())
            .to_resource_dir()
            .build()
            .unwrap();
    }
}
