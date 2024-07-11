use change_detection::ChangeDetection;
use static_files::{resource_dir, NpmBuild};
use std::path::{Path, PathBuf};
use std::{env /* fs */};

// These are touched during the build, so it would re-build every time if we
// don't exclude them from change detection:
const EXCLUDE_LIST: [&str; 4] = [
    "../../web-console/node_modules",
    "../../web-console/out",
    "../../web-console/.next",
    "../../web-console/pipeline-manager-",
];
// const SVELTEKIT_EXCLUDE_LIST: [&str; 4] = [
//     "../../web-console-sveltekit/node_modules",
//     "../../web-console-sveltekit/build",
//     "../../web-console-sveltekit/.svelte-kit",
//     "../../web-console-sveltekit/pipeline-manager-",
// ];

/// The build script has two modes:
///
/// - if `WEBUI_BUILD_DIR` we use that to serve in the manager
/// - otherwise we build the web-console from web-console and serve it from the
///   manager
///
/// The first mode is useful in CI builds to cache the website build so it
/// doesn't get built many times due to changing rustc flags.
fn main() {
    if let Ok(webui_out_folder) = env::var("WEBUI_BUILD_DIR") {
        ChangeDetection::path(&webui_out_folder)
            .path("build.rs")
            .generate();
        println!("cargo:rerun-if-changed=build.rs");
        resource_dir(webui_out_folder)
            .build()
            .expect("Could not use WEBUI_BUILD_DIR as a website location")
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
        println!("cargo:rerun-if-env-changed=NEXT_PUBLIC_MUIX_PRO_KEY");

        // sets distDir in next.config.js, unfortunately it's not possible to
        // set it to OUT_DIR because it has to be inside of the web-console directory
        // so we take the last two parts of OUT_DIR and use them as a relative path
        // inside of web-console/. This ensures that parallel builds don't collide
        // with each other since the hash part of OUT_DIR is always unique.
        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
        let out_dir_parts = out_dir.iter().collect::<Vec<_>>();
        let rel_build_dir = out_dir_parts[out_dir_parts.len() - 2..]
            .iter()
            .collect::<PathBuf>();
        env::set_var("BUILD_DIR", rel_build_dir.clone());
        let asset_path = Path::new("../../web-console/").join(rel_build_dir);

        NpmBuild::new("../../web-console")
            .executable("yarn")
            .install()
            .expect(
                "Could not run `yarn install`. Follow set-up instructions in web-console/README.md",
            )
            .run("build")
            .expect("Could not run `yarn build`. Run it manually in web-console/ to debug.")
            .target(asset_path)
            .to_resource_dir()
            .build()
            .unwrap();
    }

    /*{
        // sveltekit
        if let Ok(webui_out_folder) = env::var("WEBCONSOLE_BUILD_DIR") {
            ChangeDetection::path(&webui_out_folder)
                .path("build.rs")
                .generate();
            println!("cargo:rerun-if-changed=build.rs");
            let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
            fs::create_dir(out_dir.join("v2")).ok();
            let mut resource_dir = resource_dir(webui_out_folder);
            let _ = resource_dir.with_generated_filename(out_dir.join("v2").join("generated.rs"));
            resource_dir
                .build()
                .expect("Could not use WEBCONSOLE_BUILD_DIR as a website location")
        } else {
            ChangeDetection::exclude(|path: &Path| {
                SVELTEKIT_EXCLUDE_LIST
                .iter()
                .any(|exclude| path.to_str().unwrap().starts_with(exclude))
                // Also exclude web-console folder itself because we mutate things inside
                // of it
                || path.to_str().unwrap() == "../../web-console-sveltekit/"
            })
            .path("../../web-console-sveltekit/")
            .path("build.rs")
            .generate();

            let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
            fs::create_dir(out_dir.join("v2")).ok();
            let out_dir_parts = out_dir.iter().collect::<Vec<_>>();
            let rel_build_dir = out_dir_parts[out_dir_parts.len() - 2..]
                .iter()
                .collect::<PathBuf>();
            env::set_var("BUILD_DIR", rel_build_dir.clone());
            let asset_path: PathBuf = Path::new("../../web-console-sveltekit/").join("build");
            let mut resource_dir = NpmBuild::new("../../web-console-sveltekit")
                .executable("bun")
                .install()
                .expect("Could not run `bun install`. Follow set-up instructions in web-console-sveltekit/README.md")
                .run("build")
                .expect("Could not run `bun run build`. Run it manually in web-console-sveltekit/ to debug.")
                .target(asset_path.clone())
                .to_resource_dir();
            let _ = resource_dir.with_generated_filename(out_dir.join("v2").join("generated.rs"));
            resource_dir.build().expect("SvelteKit app failed to build");
        };
    }*/
}
