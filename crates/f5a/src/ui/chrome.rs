//! Bottom chrome: the prompt/status line and the tab bar.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::app::App;
use crate::app::state::{Connection, Prompt, TabHit, ToastLevel, View};

use super::format::truncate;
use super::theme;

/// The prompt line: `:` command input, `/` filter input, or the toast.
pub fn render_status_line(frame: &mut Frame, area: Rect, app: &App) {
    let line = match &app.prompt {
        Prompt::Command { input, .. } => Line::from(vec![
            Span::styled(" :", theme::accent(theme::CYAN)),
            Span::styled(input.clone(), theme::text()),
            Span::styled("█", theme::accent(theme::CYAN)),
            Span::styled("  (tab completes, enter runs, esc closes)", theme::muted()),
        ]),
        Prompt::Filter { input } => Line::from(vec![
            Span::styled(" /", theme::accent(theme::YELLOW)),
            Span::styled(input.clone(), theme::text()),
            Span::styled("█", theme::accent(theme::YELLOW)),
            Span::styled("  (enter keeps filter, esc clears)", theme::muted()),
        ]),
        Prompt::Closed => match &app.toast {
            Some(toast) => {
                let style = match toast.level {
                    ToastLevel::Info => Style::default().fg(theme::BLUE),
                    ToastLevel::Success => Style::default().fg(theme::GREEN),
                    ToastLevel::Error => theme::accent(theme::RED),
                };
                Line::from(vec![
                    Span::styled(" ▌ ", style),
                    Span::styled(
                        truncate(&toast.message, area.width.saturating_sub(4) as usize),
                        style,
                    ),
                ])
            }
            None => idle_line(app, area.width as usize),
        },
    };
    app.viewport.status_line_row.set(Some(area.y));
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(theme::PANEL)),
        area,
    );
}

/// The status line when nothing is typed and no toast shows: the key hints,
/// the active filter, and the Samply capture in the spotlight. A capture's
/// account carries a file path, so when both cannot fit the hints yield.
fn idle_line(app: &App, width: usize) -> Line<'static> {
    const HINTS: &str = " : command   / filter   ? help   q quit";
    let mut spans = Vec::new();
    let samply = app.capture_spotlight().map(|capture| {
        (
            capture.headline(app.now_millis),
            theme::capture_steady_style(&capture.phase),
        )
    });
    let hints_fit = samply
        .as_ref()
        .is_none_or(|(headline, _)| HINTS.len() + 3 + headline.chars().count() <= width);
    if hints_fit {
        spans.push(Span::styled(HINTS, theme::muted()));
    }
    if !app.filter.is_empty() {
        spans.push(Span::styled(
            format!("   filter: {}", app.filter),
            theme::accent(theme::YELLOW),
        ));
    }
    if let Some((headline, style)) = samply {
        let prefix = if hints_fit { "   " } else { " ▌ " };
        let room = width.saturating_sub(HINTS.len() * usize::from(hints_fit) + prefix.len());
        spans.push(Span::styled(
            format!("{prefix}{}", truncate(&headline, room)),
            style,
        ));
    }
    Line::from(spans)
}

/// The tab bar with the busy spinner and refresh cadence on the right. Each
/// tab's column range goes back to the app so a click on it switches views.
pub fn render_tab_bar(frame: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(30), Constraint::Length(30)])
        .split(area);

    let mut spans = Vec::new();
    let mut tabs = Vec::with_capacity(View::ALL.len());
    let mut x = chunks[0].x;
    for (index, view) in View::ALL.iter().enumerate() {
        let label = format!(" {} {} ", index + 1, view.title());
        // Labels are ASCII, so bytes are columns; a tab cut off by a narrow
        // terminal is clickable only where it is visible.
        let x_end = x.saturating_add(label.len() as u16).min(chunks[0].right());
        if x < x_end {
            tabs.push(TabHit {
                x_start: x,
                x_end,
                view: *view,
            });
        }
        x = x.saturating_add(label.len() as u16 + 1);
        if *view == app.view {
            spans.push(Span::styled(
                label,
                Style::default()
                    .fg(theme::BG)
                    .bg(theme::CYAN)
                    .add_modifier(Modifier::BOLD),
            ));
        } else {
            spans.push(Span::styled(label, theme::muted()));
        }
        spans.push(Span::raw(" "));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), chunks[0]);
    app.viewport.tab_bar_row.set(Some(area.y));
    *app.viewport.tabs.borrow_mut() = tabs;

    let spinner = if app.is_busy() {
        theme::SPINNER[app.spinner_frame % theme::SPINNER.len()]
    } else {
        " "
    };
    let connection = match &app.connection {
        Connection::Online { latency_millis } => format!("{latency_millis}ms"),
        Connection::Connecting => "…".to_string(),
        Connection::Lost { .. } => "offline".to_string(),
    };
    let right = Line::from(vec![
        Span::styled(spinner, theme::accent(theme::MAGENTA)),
        Span::styled(
            format!(" {connection} · every {}s ", app.refresh_secs),
            theme::muted(),
        ),
    ]);
    frame.render_widget(Paragraph::new(right).alignment(Alignment::Right), chunks[1]);
}
