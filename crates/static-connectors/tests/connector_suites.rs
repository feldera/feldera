//! The connector suites of `dbsp_connectors`, each registered case one test, on the static
//! engine.

fn main() {
    dbsp_connectors::test::run_suites(static_connectors::test::StaticTestEngine::new());
}
