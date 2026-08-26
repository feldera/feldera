//! Sanitization for server-provided text.

/// Replace terminal controls and bidirectional overrides so that
/// server-provided names can never repaint or reorder the screen.
pub fn terminal_safe(text: &str) -> String {
    text.chars()
        .map(|character| match character {
            character if character.is_control() => char::REPLACEMENT_CHARACTER,
            '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' => char::REPLACEMENT_CHARACTER,
            character => character,
        })
        .collect()
}

/// Remove ANSI escape sequences (colors, cursor moves) so log lines carry
/// only their text; the console applies its own styling.
pub fn strip_ansi(text: &str) -> String {
    let mut cleaned = String::with_capacity(text.len());
    let mut characters = text.chars().peekable();
    while let Some(character) = characters.next() {
        if character != '\u{1b}' {
            cleaned.push(character);
            continue;
        }
        match characters.peek() {
            // CSI: `ESC [` parameters, ended by a byte in `@`..=`~`.
            Some('[') => {
                characters.next();
                for ended in characters.by_ref() {
                    if ('@'..='~').contains(&ended) {
                        break;
                    }
                }
            }
            // OSC: `ESC ]` payload, ended by BEL or `ESC \`.
            Some(']') => {
                characters.next();
                while let Some(ended) = characters.next() {
                    if ended == '\u{7}' {
                        break;
                    }
                    if ended == '\u{1b}' && characters.peek() == Some(&'\\') {
                        characters.next();
                        break;
                    }
                }
            }
            // Two-character escapes.
            Some(_) => {
                characters.next();
            }
            None => {}
        }
    }
    cleaned
}

/// Sanitize multi-line text, keeping newlines but nothing else that controls
/// the terminal.
pub fn terminal_safe_multiline(text: &str) -> String {
    text.lines()
        .map(terminal_safe)
        .collect::<Vec<_>>()
        .join("\n")
}

/// Word-wrap `text` to `width` columns, keeping its line breaks and the
/// spacing inside each line; a word wider than a line breaks mid-word.
///
/// ```
/// use f5a::model::text::wrap_words;
///
/// assert_eq!(wrap_words("the quick brown fox", 9), ["the quick", "brown fox"]);
/// assert_eq!(wrap_words("abcdef", 4), ["abcd", "ef"]);
/// ```
pub fn wrap_words(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut wrapped = Vec::new();
    for line in text.lines() {
        let mut current = String::new();
        let mut columns = 0;
        let mut is_first_word = true;
        for word in line.split(' ') {
            let word_columns = word.chars().count();
            if !is_first_word && columns + 1 + word_columns > width {
                wrapped.push(std::mem::take(&mut current));
                columns = 0;
                is_first_word = true;
            }
            if !is_first_word {
                current.push(' ');
                columns += 1;
            }
            for character in word.chars() {
                if columns == width {
                    wrapped.push(std::mem::take(&mut current));
                    columns = 0;
                }
                current.push(character);
                columns += 1;
            }
            is_first_word = false;
        }
        wrapped.push(current);
    }
    wrapped
}

#[cfg(test)]
mod tests {
    use super::{strip_ansi, terminal_safe, terminal_safe_multiline, wrap_words};

    #[test]
    fn words_wrap_at_the_width_and_long_words_break() {
        assert_eq!(
            wrap_words("the quick brown fox", 9),
            vec!["the quick", "brown fox"]
        );
        assert_eq!(wrap_words("abcdefghij", 4), vec!["abcd", "efgh", "ij"]);
        assert_eq!(wrap_words("a abcdefgh", 4), vec!["a", "abcd", "efgh"]);
        assert_eq!(wrap_words("fits", 80), vec!["fits"]);
    }

    #[test]
    fn wrapping_keeps_line_breaks_and_indentation() {
        assert_eq!(
            wrap_words("one\n\n    two three", 8),
            vec!["one", "", "    two", "three"]
        );
        assert!(wrap_words("", 8).is_empty());
        assert_eq!(wrap_words("ab", 0), vec!["a", "b"]);
    }

    #[test]
    fn ansi_sequences_vanish_but_text_survives() {
        assert_eq!(strip_ansi("\u{1b}[32mINFO\u{1b}[0m ready"), "INFO ready");
        assert_eq!(strip_ansi("\u{1b}]0;title\u{7}text"), "text");
        assert_eq!(strip_ansi("\u{1b}]8;;x\u{1b}\\link"), "link");
        assert_eq!(strip_ansi("\u{1b}Mplain"), "plain");
        assert_eq!(strip_ansi("no escapes"), "no escapes");
        assert_eq!(strip_ansi("dangling\u{1b}"), "dangling");
    }

    #[test]
    fn control_sequences_are_neutralized() {
        assert_eq!(terminal_safe("one\u{1b}[2J\u{202e}two"), "one�[2J�two");
        assert_eq!(terminal_safe("张伟"), "张伟");
    }

    #[test]
    fn multiline_keeps_newlines_but_not_escapes() {
        assert_eq!(
            terminal_safe_multiline("a\nb\u{1b}c\nd"),
            "a\nb�c\nd".to_string()
        );
    }
}
