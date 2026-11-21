use feldera_native_probe::CompiledNullableStringMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[test]
fn callback_only_visits_present_rows_in_order() {
    let map = CompiledNullableStringMap::compile().unwrap();
    let mut visited = Vec::new();
    let mapped = map
        .evaluate(
            &[None, Some("🐍"), Some(""), None, Some("東京")],
            |text| {
                visited.push(text.to_owned());
                (!text.is_empty()).then(|| text.chars().count())
            },
        )
        .unwrap();
    assert_eq!(visited, ["🐍", "", "東京"]);
    assert_eq!(mapped, [None, Some(1), None, None, Some(2)]);
    assert!(
        map.evaluate::<(), _>(&[], |_| panic!("empty batch"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn callback_panics_resume_after_the_native_batch_returns() {
    let map = CompiledNullableStringMap::compile().unwrap();
    let mut visited = Vec::new();
    let result = catch_unwind(AssertUnwindSafe(|| {
        map.evaluate(&[Some("first"), Some("failed"), Some("later")], |text| {
            visited.push(text.to_owned());
            assert_ne!(text, "failed", "callback failure");
            Some(())
        })
    }));
    assert!(result.is_err());
    assert_eq!(visited, ["first", "failed"]);
    assert_eq!(
        map.evaluate(&[Some("reusable")], |text| Some(text.len()))
            .unwrap(),
        [Some(8)]
    );
}
