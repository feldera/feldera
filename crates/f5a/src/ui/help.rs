//! The `?` overlay: the keys of the active tab, then the keys that work on
//! every tab.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::app::state::View;

use super::theme;
use super::widgets::{centered_rect, panel};

/// A titled group of `(keys, what they do)` rows.
struct Section {
    title: &'static str,
    entries: &'static [(&'static str, &'static str)],
}

const PIPELINES: &[Section] = &[
    Section {
        title: "MOVE AND SELECT",
        entries: &[
            ("j k / ↑ ↓", "move; stops at the ends; the wheel scrolls"),
            ("pgup pgdn / g G", "a page / first and last"),
            (
                "click",
                "select; a double-click jumps by column: name to SQL, numbers to Metrics, status to Logs",
            ),
            (
                "o O / header click",
                "cycle or reverse the sort / sort by that column",
            ),
            ("space", "mark the row; esc clears every mark"),
            ("shift-↕", "mark a range from where shift was first pressed"),
            (
                "enter",
                "open Metrics (Logs for a broken, stopped pipeline)",
            ),
        ],
    },
    Section {
        title: "LIFECYCLE · the marked pipelines, else the row",
        entries: &[
            ("s", "start"),
            ("p / P", "pause / resume"),
            ("x / X", "stop with a checkpoint / force-stop"),
            ("C", "clear storage"),
            (":restart", "force-stop, wait for Stopped, start again"),
            (":delete", "remove permanently; always confirms"),
            ("e", "show the deployment error"),
            (
                "--ask",
                "start f5a with this flag to confirm destructive actions",
            ),
        ],
    },
    Section {
        title: "PROFILES AND BUNDLES",
        entries: &[
            (
                ":samply 30",
                "record a 30s Samply CPU profile, saved when done",
            ),
            ("b", "open the saved profile in the browser"),
            (
                ":bundle",
                "download the support bundle; B shows it in the file manager",
            ),
            (":bundle settings", "choose what the bundle gathers"),
            ("f / v", "open Hotspots / the SQL"),
        ],
    },
];

const METRICS: &[Section] = &[Section {
    title: "METRICS",
    entries: &[
        (
            "j k / pgup pgdn",
            "switch to the next pipeline / a page further",
        ),
        ("enter", "open Connectors"),
        ("f", "open Hotspots"),
    ],
}];

const CONNECTORS: &[Section] = &[
    Section {
        title: "MOVE AND FOLD",
        entries: &[
            ("j k / pgup pgdn / g G", "move through the tree"),
            ("← →", "fold / unfold; ← on a connector climbs to its table"),
            (
                "enter",
                "fold a table or view; on a connector, browse its errors",
            ),
            (
                "j k (errors)",
                "one error at a time; enter or esc returns to the tree",
            ),
            (
                "o O / header / :sort",
                "sort; tables and views sort by their totals",
            ),
        ],
    },
    Section {
        title: "ACT · the marked connectors, else the row",
        entries: &[
            (
                "space / shift-↕ / esc",
                "mark / mark a range / clear the marks",
            ),
            ("s / P", "start"),
            ("x / p", "pause; only input connectors pause"),
            (
                "v / double-click",
                "the connector's config in the SQL; esc returns",
            ),
        ],
    },
];

const HOTSPOTS: &[Section] = &[
    Section {
        title: "MOVE",
        entries: &[
            ("j k / pgup pgdn / g G", "move through the operators"),
            ("→ / ← or esc", "scroll the details / back to the table"),
            (
                "o O / header click",
                "sort by cost, skew, operator, relation, or SQL line",
            ),
        ],
    },
    Section {
        title: "COSTS",
        entries: &[
            (
                "m",
                "cycle the metric: time, memory, storage, state records",
            ),
            ("f", "profile the circuit again"),
            ("enter", "the operator's SQL; esc returns here"),
        ],
    },
    Section {
        title: "READING THE TABLE",
        entries: &[
            ("SKEW", "the busiest worker over the mean; 1.0× is balanced"),
            (
                "~",
                "a relation and line borrowed from a neighboring operator",
            ),
        ],
    },
];

const SQL: &[Section] = &[Section {
    title: "SQL",
    entries: &[
        ("j k / pgup pgdn / g G", "scroll"),
        ("f", "open Hotspots"),
        ("esc / q", "back to the tab that opened the SQL"),
    ],
}];

const LOGS: &[Section] = &[Section {
    title: "LOGS",
    entries: &[
        ("j k / pgup pgdn", "scroll"),
        ("g / G", "the oldest line / follow the tail"),
    ],
}];

const EVERYWHERE: Section = Section {
    title: "EVERY TAB",
    entries: &[
        ("1..6 / tab / click", "switch tabs"),
        ("esc / q", "back; q on Pipelines quits; ctrl-c always quits"),
        (": / /", "command bar (tab completes) / filter pipelines"),
        ("r / T / l", "refresh now / switch tenant / logs"),
        (":refresh-every 5", "poll cadence; :tenant name; :quit"),
    ],
};

fn sections(view: View) -> &'static [Section] {
    match view {
        View::Pipelines => PIPELINES,
        View::Metrics => METRICS,
        View::Connectors => CONNECTORS,
        View::Profiler => HOTSPOTS,
        View::Sql => SQL,
        View::Logs => LOGS,
    }
}

const KEY_WIDTH: usize = 24;

fn section_lines(section: &Section, lines: &mut Vec<Line<'static>>) {
    lines.push(Line::from(Span::styled(
        section.title,
        theme::accent(theme::MAGENTA),
    )));
    for (keys, what) in section.entries {
        lines.push(Line::from(vec![
            Span::styled(format!("  {keys:<KEY_WIDTH$}"), theme::accent(theme::CYAN)),
            Span::styled(*what, theme::text()),
        ]));
    }
}

pub fn render(frame: &mut Frame, area: Rect, view: View) {
    let mut lines = Vec::new();
    for section in sections(view) {
        section_lines(section, &mut lines);
        lines.push(Line::from(""));
    }
    section_lines(&EVERYWHERE, &mut lines);
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  press any key to close",
        theme::muted(),
    )));
    let widest = lines.iter().map(Line::width).max().unwrap_or(0);
    // Sized to its content so no entry falls off the edge.
    let popup = centered_rect(
        (widest as u16).saturating_add(4),
        (lines.len() as u16).saturating_add(2),
        area,
    );
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines).block(panel(&format!("HELP · {}", view.title()), theme::CYAN)),
        popup,
    );
}

#[cfg(test)]
mod tests {
    use super::{KEY_WIDTH, sections};
    use crate::app::state::View;

    #[test]
    fn every_tab_has_help_with_keys_that_fit_their_column() {
        for view in View::ALL {
            assert!(!sections(view).is_empty(), "{view:?}");
            for section in sections(view) {
                for (keys, _) in section.entries {
                    assert!(keys.chars().count() < KEY_WIDTH, "{keys} is too wide");
                }
            }
        }
    }
}
