use progenitor::{GenerationSettings, InterfaceStyle};
use std::{env, fs, path::Path};

fn main() {
    let openapi = include_bytes!("bench_openapi.json");
    println!("cargo:rerun-if-changed=bench_openapi.json");

    let spec = serde_json::from_reader(&openapi[..]).unwrap();
    let mut settings = GenerationSettings::new();
    settings.with_interface(InterfaceStyle::Builder);
    let mut generator = progenitor::Generator::new(&settings);

    let tokens = generator.generate_tokens(&spec).unwrap();
    let ast = syn::parse2(tokens).unwrap();
    let content = prettyplease::unparse(&ast);
    let content = content.replace(
        "impl Client",
        "#[rustversion::attr(since(1.89), allow(mismatched_lifetime_syntaxes))]\nimpl Client",
    );

    let mut out_file = Path::new(&env::var("OUT_DIR").unwrap()).to_path_buf();
    out_file.push("codegen.rs");

    fs::write(out_file, content).unwrap();
}
