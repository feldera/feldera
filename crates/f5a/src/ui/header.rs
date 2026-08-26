//! The k9s-style header: context block, key hints, and the wordmark.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::app::App;
use crate::app::state::{Connection, View};

use super::logo::{LOGO, LOGO_SMALL, logo_small_width, logo_width};
use super::theme;

pub const HEADER_HEIGHT: u16 = 6;

/// Label column plus one space of breathing room per context line.
const CONTEXT_LABEL_WIDTH: usize = 10;

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let logo_columns = if area.width >= logo_width() + 74 {
        logo_width() + 2
    } else if area.width >= logo_small_width() + 64 {
        logo_small_width() + 2
    } else {
        0
    };
    // Give the context block exactly what its longest line needs, so long
    // instance URLs and tenant names show in full on wide terminals; the key
    // hints absorb whatever remains.
    let lines = context_lines(app);
    let widest_line = lines
        .iter()
        .map(Line::width)
        .max()
        .unwrap_or(CONTEXT_LABEL_WIDTH) as u16;
    let hints_minimum = 20u16;
    let context_columns = (widest_line + 2).clamp(
        30,
        area.width
            .saturating_sub(logo_columns + hints_minimum)
            .max(30),
    );
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(context_columns),
            Constraint::Min(hints_minimum),
            Constraint::Length(logo_columns),
        ])
        .split(area);
    frame.render_widget(Paragraph::new(lines), chunks[0]);
    render_hints(frame, chunks[1], app);
    if logo_columns > 0 {
        render_logo(frame, chunks[2], logo_columns == logo_width() + 2);
    }
}

fn context_lines(app: &App) -> Vec<Line<'static>> {
    let (connection_label, connection_style) = match &app.connection {
        Connection::Connecting => ("connecting…".to_string(), theme::accent(theme::YELLOW)),
        Connection::Online { latency_millis } => (
            format!("ok ({latency_millis}ms)"),
            theme::accent(theme::GREEN),
        ),
        Connection::Lost { .. } => ("LOST".to_string(), theme::accent(theme::RED)),
    };
    let mut edition = app.instance.edition.clone();
    if let Some(update) = &app.instance.update_available {
        edition.push_str(&format!(" (update {update})"));
    }
    vec![
        context_line("Instance:", app.host.clone(), theme::accent(theme::CYAN)),
        context_line(
            "Version:",
            format!("{} {}", app.instance.version, app.instance.short_revision()),
            theme::text(),
        ),
        context_line("Edition:", edition, theme::text()),
        context_line(
            "Tenant:",
            app.session.tenant_label().to_string(),
            theme::accent(theme::MAGENTA),
        ),
        context_line(
            "Role:",
            app.session.role_label().to_string(),
            theme::accent(theme::BLUE),
        ),
        Line::from(vec![
            Span::styled(format!("{:<CONTEXT_LABEL_WIDTH$}", "API:"), theme::muted()),
            Span::styled(connection_label, connection_style),
        ]),
    ]
}

fn context_line(label: &str, value: String, style: ratatui::style::Style) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label:<CONTEXT_LABEL_WIDTH$}"), theme::muted()),
        Span::styled(value, style),
    ])
}

/// The most useful keys for the active view, k9s-style.
fn hints(view: View) -> Vec<(&'static str, &'static str)> {
    match view {
        View::Pipelines => vec![
            ("<enter>", "metrics"),
            ("<s>", "start"),
            ("<p/P>", "pause/resume"),
            ("<x/X>", "stop/force-stop"),
            ("<C>", "clear"),
            (":samply", "cpu profile"),
            (":bundle", "support bundle"),
            ("<b>", "open profile"),
            ("<B>", "show bundle"),
            ("<f>", "hotspots"),
            ("<v>", "sql"),
            ("<e>", "error detail"),
            ("<l>", "logs"),
            ("<space>", "mark multi"),
            ("<shift-↕>", "range mark"),
            ("<o/O>", "sort/reverse"),
            ("</>", "filter"),
            ("<:>", "command"),
        ],
        View::Metrics => vec![
            ("<enter>", "connectors"),
            ("<j/k>", "switch pipeline"),
            ("<f>", "hotspots"),
            ("<esc>", "back"),
            ("<:>", "command"),
        ],
        View::Connectors => vec![
            ("<enter>", "inspect"),
            ("<j/k>", "select"),
            ("<p/P>", "pause/resume"),
            ("<esc>", "back"),
            ("<:>", "command"),
        ],
        View::Profiler => vec![
            ("<enter>", "jump to sql"),
            ("<j/k>", "select"),
            ("<m>", "cycle metric"),
            ("<f>", "re-profile"),
            ("<esc>", "back"),
        ],
        View::Sql => vec![
            ("<j/k>", "scroll"),
            ("<g/G>", "top/bottom"),
            ("<f>", "hotspots"),
            ("<esc>", "back"),
        ],
        View::Logs => vec![
            ("<j/k>", "scroll"),
            ("<G>", "follow tail"),
            ("<g>", "oldest"),
            ("<esc>", "back"),
            ("<:>", "command"),
        ],
    }
}

fn render_hints(frame: &mut Frame, area: Rect, app: &App) {
    let hints = hints(app.view);
    let rows = HEADER_HEIGHT as usize;
    let mut columns: Vec<Vec<Line>> = Vec::new();
    for chunk in hints.chunks(rows) {
        columns.push(
            chunk
                .iter()
                .map(|(key, label)| {
                    Line::from(vec![
                        Span::styled(format!("{key:>8} "), theme::accent(theme::BLUE)),
                        Span::styled((*label).to_string(), theme::muted()),
                    ])
                })
                .collect(),
        );
    }
    let column_width = 24u16;
    let mut x = area.x + 1;
    for column in columns {
        if x + column_width > area.right() {
            break;
        }
        let column_area = Rect {
            x,
            y: area.y,
            width: column_width,
            height: area.height,
        };
        frame.render_widget(Paragraph::new(column), column_area);
        x += column_width;
    }
}

fn render_logo(frame: &mut Frame, area: Rect, large: bool) {
    let rows: Vec<Line> = if large {
        LOGO.iter()
            .zip(theme::LOGO_GRADIENT)
            .map(|(row, color)| Line::from(Span::styled((*row).to_string(), theme::accent(color))))
            .collect()
    } else {
        LOGO_SMALL
            .iter()
            .zip(theme::LOGO_GRADIENT.iter().step_by(2))
            .map(|(row, color)| Line::from(Span::styled((*row).to_string(), theme::accent(*color))))
            .collect()
    };
    frame.render_widget(
        Paragraph::new(rows).alignment(ratatui::layout::Alignment::Right),
        area,
    );
}
