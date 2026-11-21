use feldera_native_probe::{CompiledStringProjection, StringSlices};

#[test]
fn native_utf8_slices_match_rust_char_and_prefix_semantics() {
    let values = [
        None,
        Some(""),
        Some("az"),
        Some("aaa"),
        Some("a\0a"),
        Some("\0\0x"),
        Some("ééx"),
        Some("한글"),
        Some("🐍🐍a"),
        Some("🐍x"),
        Some("𝔸"),
    ];
    for pattern in ["a", "a\0", "\0", "é", "🐍"] {
        let code = CompiledStringProjection::compile(pattern).unwrap();
        let expected: Vec<_> = values
            .iter()
            .map(|value| {
                let text = value.unwrap_or("");
                StringSlices {
                    first_end: text.chars().next().map_or(0, char::len_utf8),
                    trimmed_start: text.len() - text.trim_start_matches(pattern).len(),
                }
            })
            .collect();
        assert_eq!(code.project(&values).unwrap(), expected);
    }
    assert!(CompiledStringProjection::compile("").is_err());
}
