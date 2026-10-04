//! Modal overlays: help, confirmations, tenant picker, and text viewers.

use ratatui::Frame;
use ratatui::layout::{Alignment, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::app::App;
use crate::app::bundle_settings::{BundleSetting, limit_label};
use crate::app::state::Overlay;
use crate::model::text::wrap_words;

use super::theme;
use super::widgets::{centered_rect, panel, scrollbar};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    match &app.overlay {
        Overlay::None => {}
        Overlay::Help => super::help::render(frame, area, app.view),
        Overlay::Confirm { actions } => {
            let height = if actions.len() > 1 { 9 } else { 7 };
            let popup = centered_rect(70, height, area);
            frame.render_widget(Clear, popup);
            let question = match actions.as_slice() {
                [action] => format!("{}?", action.describe()),
                [first, ..] => format!("{} {} pipelines?", first.verb(), actions.len()),
                [] => "nothing to do?".to_string(),
            };
            let mut lines = vec![
                Line::from(""),
                Line::from(Span::styled(question, theme::accent(theme::YELLOW)))
                    .alignment(Alignment::Center),
            ];
            if actions.len() > 1 {
                let names = actions
                    .iter()
                    .map(|action| action.pipeline())
                    .collect::<Vec<_>>()
                    .join(", ");
                lines.push(
                    Line::from(Span::styled(
                        super::format::truncate(&names, 64),
                        theme::muted(),
                    ))
                    .alignment(Alignment::Center),
                );
            }
            lines.extend(vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("y / enter", theme::accent(theme::GREEN)),
                    Span::styled("  confirm      ", theme::text()),
                    Span::styled("n / esc", theme::accent(theme::RED)),
                    Span::styled("  cancel", theme::text()),
                ])
                .alignment(Alignment::Center),
            ]);
            frame.render_widget(
                Paragraph::new(lines).block(panel("CONFIRM", theme::RED)),
                popup,
            );
        }
        Overlay::Tenants { selected } => {
            let height = (app.session.memberships.len() as u16 + 4).min(area.height);
            let popup = centered_rect(52, height, area);
            frame.render_widget(Clear, popup);
            let lines: Vec<Line> = app
                .session
                .memberships
                .iter()
                .enumerate()
                .map(|(index, membership)| {
                    let marker = if membership.name == app.session.tenant_label() {
                        "●"
                    } else {
                        " "
                    };
                    let label = format!(
                        " {marker} {:<28} {:<8}",
                        super::format::truncate(&membership.name, 28),
                        membership.role
                    );
                    if index == *selected {
                        Line::from(Span::styled(
                            label,
                            Style::default()
                                .fg(theme::BG)
                                .bg(theme::CYAN)
                                .add_modifier(Modifier::BOLD),
                        ))
                    } else {
                        Line::from(Span::styled(label, theme::text()))
                    }
                })
                .collect();
            frame.render_widget(
                Paragraph::new(lines).block(panel("SWITCH TENANT · enter selects", theme::GREEN)),
                popup,
            );
        }
        Overlay::BundleSettings { cursor } => {
            let options = &app.bundle_options;
            let popup = centered_rect(58, BundleSetting::ALL.len() as u16 + 4, area);
            frame.render_widget(Clear, popup);
            let mut lines: Vec<Line> = BundleSetting::ALL
                .iter()
                .enumerate()
                .map(|(index, setting)| {
                    let text = match setting {
                        BundleSetting::Limit => {
                            format!(" ◂ {:<36}{:>14} ▸", setting.label(), limit_label(options))
                        }
                        BundleSetting::Download => format!(" ▶ {}", setting.label()),
                        _ => format!(
                            " [{}] {}",
                            if setting.is_on(options) == Some(true) {
                                "x"
                            } else {
                                " "
                            },
                            setting.label()
                        ),
                    };
                    if index == *cursor {
                        Line::from(Span::styled(
                            format!("{text:<56}"),
                            Style::default()
                                .fg(theme::BG)
                                .bg(theme::CYAN)
                                .add_modifier(Modifier::BOLD),
                        ))
                    } else {
                        Line::from(Span::styled(text, theme::text()))
                    }
                })
                .collect();
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "  space toggles · ←/→ steps · enter on Download starts",
                theme::muted(),
            )));
            frame.render_widget(
                Paragraph::new(lines).block(panel("SUPPORT BUNDLE", theme::GREEN)),
                popup,
            );
        }
        Overlay::TextViewer {
            title,
            body,
            scroll,
        } => {
            let (popup, lines) = fitted_text_popup(area, title, body);
            render_scrolling_text(frame, popup, app, title, theme::RED, lines, *scroll);
        }
    }
}

/// Size a text popup to its content, centered: as wide as its longest line
/// and as tall as the wrapped text, within a margin of the screen.
fn fitted_text_popup(area: Rect, title: &str, body: &str) -> (Rect, Vec<String>) {
    let max_width = area.width.saturating_sub(8).min(100);
    let max_height = area.height.saturating_sub(4);
    // Borders take two columns; the title needs room to read.
    let min_width = (title.chars().count() as u16)
        .saturating_add(8)
        .min(max_width);
    let widest_line = body
        .lines()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0)
        .min(u16::MAX as usize) as u16;
    let width = widest_line.saturating_add(2).clamp(min_width, max_width);
    let lines = wrap_words(body, width.saturating_sub(2).max(1) as usize);
    let line_count = lines.len().min(u16::MAX as usize) as u16;
    let height = line_count.saturating_add(2).min(max_height);
    (centered_rect(width, height, area), lines)
}

/// Draw `lines` in a titled popup, scrolled by `scroll` but never past the
/// end, with a scrollbar when they overflow. The largest useful offset goes
/// back to the reducer so the keys stop where the text does.
fn render_scrolling_text(
    frame: &mut Frame,
    popup: Rect,
    app: &App,
    title: &str,
    accent: Color,
    lines: Vec<String>,
    scroll: u16,
) {
    frame.render_widget(Clear, popup);
    let inner_height = popup.height.saturating_sub(2) as usize;
    let line_count = lines.len();
    let max_scroll = line_count.saturating_sub(inner_height);
    app.viewport
        .overlay_scroll_max
        .set(max_scroll.min(u16::MAX as usize) as u16);
    let scroll = (scroll as usize).min(max_scroll);
    let text: Vec<Line> = lines.into_iter().map(Line::from).collect();
    frame.render_widget(
        Paragraph::new(text)
            .block(panel(title, accent))
            .style(theme::text())
            .scroll((scroll as u16, 0)),
        popup,
    );
    // The bar runs between the rounded corners, not over them.
    let bar_area = Rect {
        x: popup.x,
        y: popup.y.saturating_add(1),
        width: popup.width,
        height: popup.height.saturating_sub(2),
    };
    scrollbar(frame, bar_area, scroll, line_count, inner_height);
}
