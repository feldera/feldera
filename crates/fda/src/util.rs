//! Shared helpers for rendering what the API returns.

use reqwest::header::{HeaderName, HeaderValue};

/// Header names whose value is a credential, used to warn before one travels
/// over an unencrypted connection.
const SENSITIVE_HEADERS: [&str; 4] = [
    "authorization",
    "cookie",
    "proxy-authorization",
    "x-api-key",
];

/// Is this a header whose value should not cross an unencrypted connection?
pub fn is_sensitive_header(name: &HeaderName) -> bool {
    SENSITIVE_HEADERS.contains(&name.as_str())
}

/// Parse one `--header` argument into the name and value it carries.
///
/// The syntax is the one `curl -H` uses: a name, a colon, and the value, with
/// surrounding whitespace removed from both. The value is marked sensitive, so
/// that a cookie or token passed this way is not printed by the logs and not
/// indexed into an HTTP/2 header table.
pub fn parse_header(spec: &str) -> Result<(HeaderName, HeaderValue), String> {
    let (name, value) = spec
        .split_once(':')
        .ok_or_else(|| format!("Invalid --header `{spec}`: expected `Name: Value`"))?;
    let name = HeaderName::from_bytes(name.trim().as_bytes())
        .map_err(|e| format!("Invalid --header `{spec}`: {e}"))?;
    let mut value = HeaderValue::from_str(value.trim())
        .map_err(|e| format!("Invalid --header `{spec}`: {e}"))?;
    value.set_sensitive(true);
    Ok((name, value))
}

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
    use super::{HeaderName, is_sensitive_header, parse_header, terminal_safe};

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

    /// `Name: Value`, as `curl -H` spells it, with the whitespace around each
    /// part removed.
    #[test]
    fn header_arguments_are_parsed_like_curl_spells_them() {
        for (spec, name, value) in [
            ("Cookie: session=abc", "cookie", "session=abc"),
            ("  X-Trace-Id:42  ", "x-trace-id", "42"),
            (
                "Authorization: Bearer tok.en",
                "authorization",
                "Bearer tok.en",
            ),
            // A value may itself contain colons, and may be empty.
            ("X-Range: bytes:0:10", "x-range", "bytes:0:10"),
            ("X-Empty:", "x-empty", ""),
        ] {
            let (parsed_name, parsed_value) = parse_header(spec).expect(spec);
            assert_eq!(parsed_name.as_str(), name);
            assert_eq!(parsed_value.to_str().expect("printable value"), value);
            assert!(
                parsed_value.is_sensitive(),
                "a header passed on the command line may carry a credential"
            );
        }
    }

    /// What cannot become a header is reported with the argument that produced
    /// it, rather than sent or silently dropped.
    #[test]
    fn unparsable_header_arguments_are_rejected() {
        for spec in [
            "no-colon-here",
            ":empty-name",
            "Bad Name: value",
            "X-Newline: one\r\ntwo",
        ] {
            let err = parse_header(spec).expect_err(spec);
            assert!(
                err.contains(spec),
                "the report must quote the argument: {err}"
            );
        }
    }

    /// Only the headers that carry a credential deserve a warning when the
    /// connection is unencrypted.
    #[test]
    fn credential_headers_are_told_apart_from_the_rest() {
        for name in [
            "authorization",
            "cookie",
            "proxy-authorization",
            "x-api-key",
        ] {
            assert!(is_sensitive_header(&HeaderName::from_static(name)));
        }
        for name in ["feldera-tenant", "user-agent", "x-trace-id"] {
            assert!(!is_sensitive_header(&HeaderName::from_static(name)));
        }
    }
}
