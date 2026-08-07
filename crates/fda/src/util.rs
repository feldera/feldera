//! Shared helpers for rendering what the API returns.

/// Render text the API returned so a terminal displays it rather than obeys it.
///
/// A table cell goes straight to stdout, and much of what fills one is written
/// by somebody else: a member's name and email come from the identity provider,
/// a tenant's name and a trust's description come from whoever created them. An
/// escape sequence in that text is acted on by the terminal instead of shown,
/// which lets one user redraw what an administrator sees, or reach whatever
/// else that terminal binds to a control sequence.
///
/// Every control character is replaced, along with the bidirectional overrides
/// that reorder a line without leaving a visible trace. Ordinary text, accents
/// and scripts of every direction included, is left alone. `--format json`
/// bypasses this and reports exactly what the server said, escaped by the JSON
/// encoder.
///
/// Not for pipeline logs or query results, which are the program's own output
/// and are meant to arrive verbatim.
pub fn terminal_safe(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            // C0, DEL and C1. `char::is_control` covers all three.
            c if c.is_control() => char::REPLACEMENT_CHARACTER,
            // Explicit bidirectional embedding, override and isolate
            // (Unicode Annex #9): they change the reading order of what
            // follows, which is how a name can misrepresent another one.
            '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' => char::REPLACEMENT_CHARACTER,
            c => c,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::terminal_safe;

    /// Text the API returns is displayed, never obeyed: control and
    /// bidirectional-override characters must not reach the terminal.
    #[test]
    fn table_text_cannot_carry_terminal_control_sequences() {
        // A colour change, a cursor move, a clipboard write, and a line the
        // renderer would otherwise split into two.
        for hostile in [
            "\u{1b}[31mroot\u{1b}[0m",
            "\u{1b}[2J\u{1b}[H",
            "\u{1b}]52;c;bWFsaWNl\u{7}",
            "admin\r\nfake row",
            "bell\u{7}",
            "nul\u{0}byte",
            "c1\u{9b}31m",
            "\u{202e}moc.live@nimda",
        ] {
            let safe = terminal_safe(hostile);
            assert!(
                !safe.chars().any(|c| c.is_control()
                    || ('\u{202A}'..='\u{202E}').contains(&c)
                    || ('\u{2066}'..='\u{2069}').contains(&c)),
                "{safe:?} still carries a control character"
            );
            assert_eq!(safe.chars().count(), hostile.chars().count());
        }
    }

    /// Ordinary names survive intact, whatever script they are written in.
    #[test]
    fn table_text_leaves_ordinary_names_alone() {
        for benign in [
            "Ada Lovelace",
            "ada@example.com",
            "Gerd Zellweger",
            "Ólafur Þórðarson",
            "\u{5f20}\u{4f1f}",
            "\u{645}\u{62d}\u{645}\u{62f}",
            "tenant-1_a.b",
            "",
        ] {
            assert_eq!(terminal_safe(benign), benign);
        }
    }
}
