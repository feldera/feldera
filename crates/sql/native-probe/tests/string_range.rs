use feldera_native_probe::CompiledStringRange;

#[test]
fn native_utf8_range_preserves_null_and_strict_prefix_order() {
    let values = [
        None,
        Some(""),
        Some("a"),
        Some("a\0"),
        Some("aa"),
        Some("aaz"),
        Some("z"),
        Some("ｚ"),
        Some("𝔸"),
        Some("🐍"),
        Some("🐍a"),
        Some("한"),
    ];
    for (lower, upper) in [("", "z"), ("a", "aa"), ("ｚ", "🐍"), ("a", "a"), ("z", "a")] {
        let code = CompiledStringRange::compile(lower, upper).unwrap();
        let expected: Vec<_> = values
            .iter()
            .map(|value| u8::from(value.is_some_and(|value| value > lower && value < upper)))
            .collect();
        assert_eq!(code.filter(&values).unwrap(), expected);
        assert!(code.filter(&[]).unwrap().is_empty());
    }
}
