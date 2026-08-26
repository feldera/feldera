//! The Logs view: compile and deployment diagnostics above the log stream.

use ratatui::Frame;
use ratatui::layout::{Alignment, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::app::App;
use crate::app::state::LogRow;
use crate::model::diagnostics::Severity;

use super::theme;
use super::widgets::{centered_rect, panel};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let Some(pipeline) = &app.logs.pipeline else {
        let block = panel("LOGS", theme::ORANGE);
        frame.render_widget(block, area);
        let inner = centered_rect(area.width.saturating_sub(6).min(60), 1, area);
        frame.render_widget(
            Paragraph::new(Span::styled(
                "Select a pipeline and open this tab to follow its logs.",
                theme::accent(theme::YELLOW),
            ))
            .alignment(Alignment::Center),
            inner,
        );
        return;
    };

    let stream_state = if app.logs.streaming {
        "● streaming".to_string()
    } else if let Some(error) = &app.logs.last_error {
        format!("○ {error}")
    } else {
        "○ end of stream".to_string()
    };
    let follow = if app.logs.scroll.is_none() {
        " · FOLLOW"
    } else {
        ""
    };
    let title = format!("LOGS · {pipeline} · {stream_state}{follow}");

    let rows = app.logs.rows();
    let visible_height = area.height.saturating_sub(2) as usize;
    let last_row = rows.len().saturating_sub(1);
    let anchor = app.logs.scroll.unwrap_or(last_row).min(last_row);
    // The anchor line sits at the bottom of the pane.
    let first = (anchor + 1).saturating_sub(visible_height);
    let lines: Vec<Line> = rows
        .iter()
        .skip(first)
        .take(visible_height)
        .map(render_row)
        .collect();

    let footer = format!(
        " {} lines · j/k scroll · G follows · l re-enter ",
        rows.len()
    );
    frame.render_widget(
        Paragraph::new(lines).block(
            panel(&title, theme::ORANGE)
                .title_bottom(Line::from(Span::styled(footer, theme::muted()))),
        ),
        area,
    );
}

fn render_row(row: &LogRow) -> Line<'static> {
    match row {
        LogRow::SectionTitle(title, severity) => {
            let color = match severity {
                Severity::Error => theme::RED,
                Severity::Warning => theme::YELLOW,
            };
            Line::from(Span::styled(format!("▌ {title}"), theme::accent(color)))
        }
        LogRow::SectionText(text) => Line::from(Span::styled(format!("  {text}"), theme::text())),
        LogRow::Separator => Line::from(Span::styled("─".repeat(60), theme::muted())),
        LogRow::Log(line) => Line::from(Span::styled(line.clone(), log_style(line))),
    }
}

/// Color log lines by their apparent level.
fn log_style(line: &str) -> Style {
    let lowered = line.to_lowercase();
    if lowered.contains("error") || lowered.contains("panic") || lowered.contains("fatal") {
        Style::default().fg(theme::RED)
    } else if lowered.contains("warn") {
        Style::default().fg(theme::YELLOW)
    } else if lowered.contains("info") {
        Style::default().fg(theme::TEXT)
    } else {
        Style::default()
            .fg(theme::MUTED)
            .add_modifier(Modifier::DIM)
    }
}

#[cfg(test)]
mod tests {
    use super::log_style;
    use crate::ui::theme;

    #[test]
    fn log_lines_color_by_level() {
        assert_eq!(log_style("2026-01-01 ERROR boom").fg, Some(theme::RED));
        assert_eq!(log_style("thread panicked at x").fg, Some(theme::RED));
        assert_eq!(log_style("WARN slow consumer").fg, Some(theme::YELLOW));
        assert_eq!(log_style("INFO started").fg, Some(theme::TEXT));
        assert_eq!(log_style("plain continuation").fg, Some(theme::MUTED));
    }
}
