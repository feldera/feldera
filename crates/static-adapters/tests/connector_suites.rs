//! The connector suites of `dbsp_adapters`, each registered case one test, on the static
//! engine.

fn main() {
    dbsp_adapters::test::run_suites(static_adapters::test::StaticTestEngine::new());
}
