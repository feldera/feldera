//! The window around the frames. The window has a title bar, the frame rows,
//! the tagline and a panel, all inside box lines.

use super::color::{CLAY, ColorMode, INK, LICHEN, LICHEN_TINT, Rgb, STEEL};
use super::smooth;

pub(super) const TAGLINE: [&str; 3] = ["Precisely compute ", "only what changed", "."];

/// A row of the window while `Window` builds it. `width` counts the visible
/// characters of `text`, without the escape sequences.
struct Row {
    text: String,
    width: usize,
    /// The escape sequence that resets the text style.
    reset: &'static str,
}

impl Row {
    fn new(reset: &'static str) -> Self {
        Row {
            text: String::new(),
            width: 0,
            reset,
        }
    }

    /// Appends `s`. `style` is the escape sequence that styles `s`, or an empty
    /// string for plain text.
    fn push(mut self, s: &str, style: &str) -> Self {
        if style.is_empty() {
            self.text.push_str(s);
        } else {
            self.text.push_str(&format!("{style}{s}{}", self.reset));
        }
        self.width += s.chars().count();
        self
    }

    /// Appends spaces until the row is `col` columns wide.
    fn pad_to(mut self, col: usize) -> Self {
        let pad = col.saturating_sub(self.width);
        self.text.push_str(&" ".repeat(pad));
        self.width += pad;
        self
    }
}

/// Returns `s` cut to `max` characters. A cut string ends in an ellipsis.
fn fit(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

/// Returns `host` without its scheme and without a trailing slash, in the form
/// that people type.
fn display_host(host: &str) -> String {
    let h = host.split_once("://").map_or(host, |(_, rest)| rest);
    h.trim_end_matches('/').to_string()
}

/// The window around the frames.
///
/// ```text
/// ╭─ fda v0.1.0 ──────────────── feldera.com ─╮  title bar
/// │ frame rows                                │
/// │                                           │
/// │  Precisely compute only what changed.     │  tagline
/// │                                           │
/// ├───────────────────────────────────────────┤  divider
/// │                                           │
/// │   GET STARTED           CONNECTION        │  panel, 3 rows
/// │   ■ fda --help ...      ■ Host ...        │
/// │   ■ Web console ...     ■ Docs ...        │
/// ╰───────────────────────────────────────────╯
/// ```
pub(super) struct Window {
    /// The width between the side borders, in columns.
    pub(super) inner: usize,
    pub(super) color: ColorMode,
    /// The Feldera host that `fda` connects to.
    pub(super) host: String,
    /// The color of secondary text for the theme of the terminal.
    pub(super) muted: Rgb,
}

impl Window {
    /// The number of rows above the frame rows. The title bar is the only one.
    pub(super) const FILM_TOP: usize = 1;
    /// The number of rows below the frame rows. The diagram above shows them.
    pub(super) const BELOW_FILM: usize = 9;
    /// The number of columns between the left border and the tagline.
    pub(super) const TAGLINE_INDENT: usize = 2;

    /// Returns `s` in the color of the box lines.
    fn line(&self, s: &str) -> String {
        format!("{}{s}{}", self.color.fg(STEEL), self.color.reset())
    }

    /// Returns `row` between the side borders. `row` must be `inner` columns wide.
    pub(super) fn wrap(&self, row: &str) -> String {
        format!("{}{row}{}", self.line("│"), self.line("│"))
    }

    /// Returns `row` between the side borders, with spaces added up to `inner` columns.
    fn close(&self, row: Row) -> String {
        let row = row.pad_to(self.inner);
        self.wrap(&row.text)
    }

    /// Returns the title bar. It shows `fda` and its version on the left, and
    /// feldera.com on the right.
    fn top(&self) -> String {
        let c = self.color;
        let reset = c.reset();
        let bold = if c == ColorMode::Off { "" } else { "\x1b[1m" };
        let version = format!("v{}", env!("CARGO_PKG_VERSION"));
        let left = Row::new(reset)
            .push(" ", "")
            .push("fda", bold)
            .push(" ", "")
            .push(&version, &c.fg(self.muted))
            .push(" ", "");
        let site = " feldera.com ";
        // A box line fills the columns between the two labels. The two corners
        // take 4 columns.
        let fill = (self.inner + 2).saturating_sub(4 + left.width + site.len());
        format!(
            "{}{}{}{}{site}{reset}{}",
            self.line("╭─"),
            left.text,
            self.line(&"─".repeat(fill)),
            c.fg(self.muted),
            self.line("─╮"),
        )
    }

    fn blank(&self) -> String {
        self.wrap(&" ".repeat(self.inner))
    }

    /// Returns the style of the highlighted words of the tagline, which is Ink
    /// text on a Lichen tint background.
    pub(super) fn band(&self) -> String {
        format!("{}{}", self.color.bg(LICHEN_TINT), self.color.fg(INK))
    }

    fn tagline(&self) -> String {
        let c = self.color;
        let band = self.band();
        self.close(
            Row::new(c.reset())
                .pad_to(Self::TAGLINE_INDENT)
                .push(TAGLINE[0], "")
                .push(TAGLINE[1], &band)
                .push(TAGLINE[2], ""),
        )
    }

    /// Returns the three panel rows. GET STARTED is on the left and CONNECTION
    /// is on the right.
    fn panel(&self) -> [String; 3] {
        let c = self.color;
        let reset = c.reset();
        let (muted, lichen, clay) = (c.fg(self.muted), c.fg(LICHEN), c.fg(CLAY));
        // The start columns of the commands, their descriptions, the connection labels and
        // their values. `right` is at least 16 columns after `desc`, so that the descriptions fit.
        let (left, desc) = (3, 18);
        let right = (self.inner / 2 + 3).max(desc + 16);
        let value = right + 8;
        let host = display_host(&self.host);
        let desc_max = right.saturating_sub(desc + 2);
        let value_max = self.inner.saturating_sub(value + 1);
        [
            self.close(
                Row::new(reset)
                    .pad_to(left)
                    .push("GET STARTED", &muted)
                    .pad_to(right)
                    .push("CONNECTION", &muted),
            ),
            self.close(
                Row::new(reset)
                    .pad_to(left)
                    .push("■", &lichen)
                    .push(" fda --help", "")
                    .pad_to(desc)
                    .push(&fit("every command", desc_max), &muted)
                    .pad_to(right)
                    .push("■", &clay)
                    .push(" Host", "")
                    .pad_to(value)
                    .push(&fit(&host, value_max), ""),
            ),
            self.close(
                Row::new(reset)
                    .pad_to(left)
                    .push("■", &lichen)
                    .push(" Web console", "")
                    .pad_to(desc)
                    .push(&fit(&host, desc_max), &muted)
                    .pad_to(right)
                    .push("■", &clay)
                    .push(" Docs", "")
                    .pad_to(value)
                    .push(&fit("docs.feldera.com", value_max), ""),
            ),
        ]
    }

    /// Returns all rows of the window around the rows of `frame`. The tagline
    /// row is empty if `tagline` is false.
    pub(super) fn rows(&self, frame: &[String], tagline: bool) -> Vec<String> {
        let mut rows = vec![self.top()];
        rows.extend(frame.iter().map(|l| self.wrap(l)));
        rows.push(self.blank());
        rows.push(if tagline {
            self.tagline()
        } else {
            self.blank()
        });
        rows.push(self.blank());
        rows.push(self.line(&format!("├{}┤", "─".repeat(self.inner))));
        rows.push(self.blank());
        rows.extend(self.panel());
        rows.push(self.line(&format!("╰{}╯", "─".repeat(self.inner))));
        rows
    }

    /// Returns the rows of the intro at the progress `p`, from 0 to 1, for `film_h` frame rows.
    /// Registration marks appear in the corners. Box lines grow from the corners until they
    /// meet, and the divider grows from both sides. Then the marks change into corners, and the
    /// title bar and the panel appear.
    pub(super) fn intro_rows(&self, film_h: usize, p: f64) -> Vec<String> {
        let c = self.color;
        let reset = c.reset();
        let total = Self::FILM_TOP + film_h + Self::BELOW_FILM;
        let (w, last) = (self.inner + 2, total - 1);
        let divider = Self::FILM_TOP + film_h + 3;
        // The progress of the border lines and of the divider, each from 0 to 1.
        let lines = smooth(0.16, 0.7, p);
        let cross = smooth(0.55, 0.85, p);
        // The rows of the complete window. The intro copies the title bar and
        // the panel rows from them.
        let done = self.rows(&vec![" ".repeat(self.inner); film_h], false);
        // The row and the column of each corner, and the progress at which its
        // mark appears.
        let corner_at = [
            (0, 0, 0.0),
            (0, w - 1, 0.04),
            (last, w - 1, 0.08),
            (last, 0, 0.12),
        ];
        let half = |n: usize| n as f64 / 2.0;
        let mut rows = Vec::with_capacity(total);
        for (r, done_row) in done.iter().enumerate() {
            // The title bar appears at 0.78 and the panel rows appear at 0.92.
            if (r == 0 && p >= 0.78) || (r > divider && r < last && p >= 0.92) {
                rows.push(done_row.clone());
                continue;
            }
            let mut row = String::new();
            for col in 0..w {
                let edge = col == 0 || col == w - 1;
                let from_side = col.min(w - 1 - col) as f64;
                let from_end = r.min(last - r) as f64;
                let corner = corner_at.iter().find(|(y, x, _)| *y == r && *x == col);
                let cell: Option<(char, String)> = if let Some((_, _, at)) = corner {
                    if p < 0.72 {
                        (p >= *at).then(|| ('+', c.fg(CLAY)))
                    } else {
                        // The border lines met before 0.72. The marks change into corners.
                        let ch = match (r == 0, col == 0) {
                            (true, true) => '╭',
                            (true, false) => '╮',
                            (false, true) => '╰',
                            (false, false) => '╯',
                        };
                        Some((ch, c.fg(STEEL)))
                    }
                } else if (r == 0 || r == last) && lines * half(w) >= from_side {
                    Some(('─', c.fg(STEEL)))
                } else if edge && r != divider && lines * half(total) >= from_end {
                    Some(('│', c.fg(STEEL)))
                } else if r == divider && cross > 0.0 && (edge || cross * half(w) >= from_side) {
                    let ch = if col == 0 {
                        '├'
                    } else if col == w - 1 {
                        '┤'
                    } else {
                        '─'
                    };
                    Some((ch, c.fg(STEEL)))
                } else {
                    None
                };
                match cell {
                    Some((ch, style)) => row.push_str(&format!("{style}{ch}{reset}")),
                    None => row.push(' '),
                }
            }
            rows.push(row);
        }
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::splash::film::Film;
    use crate::splash::test_util::strip;

    #[test]
    fn window_rows_line_up_and_show_the_host() {
        for cols in [60, 80, 100] {
            let film = Film::new(cols, ColorMode::TrueColor);
            let window = Window {
                inner: cols,
                color: ColorMode::TrueColor,
                host: "https://try.feldera.com/".to_string(),
                muted: STEEL,
            };
            let rows = window.rows(&film.final_frame(), true);
            assert_eq!(rows.len(), Window::FILM_TOP + film.h + Window::BELOW_FILM);
            for row in &rows {
                let plain = strip(row);
                assert_eq!(plain.chars().count(), cols + 2, "ragged row: {plain}");
                let (first, last) = (plain.chars().next().unwrap(), plain.chars().last().unwrap());
                assert!(
                    "╭│├╰".contains(first) && "╮│┤╯".contains(last),
                    "open border: {plain}"
                );
            }
            let all: String = rows.iter().map(|r| strip(r)).collect();
            assert!(
                all.contains("Host  try.feldera.com"),
                "host missing at {cols} columns"
            );
            assert!(!all.contains("https://"));
        }
    }
}
