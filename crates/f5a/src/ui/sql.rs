//! The SQL program with syntax highlighting and profiler heat.

use ratatui::Frame;
use ratatui::layout::{Alignment, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::app::App;

use super::theme;
use super::widgets::{centered_rect, panel};

const KEYWORDS: [&str; 33] = [
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "HAVING",
    "ORDER",
    "LIMIT",
    "JOIN",
    "LEFT",
    "RIGHT",
    "FULL",
    "OUTER",
    "INNER",
    "CROSS",
    "ON",
    "AS",
    "AND",
    "OR",
    "NOT",
    "CREATE",
    "TABLE",
    "VIEW",
    "MATERIALIZED",
    "WITH",
    "UNION",
    "ALL",
    "DISTINCT",
    "INSERT",
    "INTO",
    "VALUES",
    "CASE",
    "LATENESS",
];

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let Some(document) = &app.sql else {
        let block = panel("SQL", theme::GREEN);
        frame.render_widget(block, area);
        let message = if app.loading_sql {
            "Loading the program…"
        } else {
            "Select a pipeline and open this tab to read its SQL."
        };
        let inner = centered_rect(area.width.saturating_sub(6).min(70), 1, area);
        frame.render_widget(
            Paragraph::new(Span::styled(message, theme::accent(theme::YELLOW)))
                .alignment(Alignment::Center),
            inner,
        );
        return;
    };

    let line_count = document.line_count();
    let heat = app
        .report
        .as_ref()
        .filter(|_| app.report_pipeline.as_deref() == Some(document.pipeline.as_str()))
        .map(|report| report.line_heat(app.metric, line_count));
    let title = match &heat {
        Some(_) => format!(
            "SQL · {} · heat: {} (m cycles on Hotspots)",
            document.pipeline,
            app.metric.label()
        ),
        None => format!("SQL · {}", document.pipeline),
    };

    let visible_height = area.height.saturating_sub(2) as usize;
    let scroll = app.sql_scroll.min(line_count.saturating_sub(1));
    let lines: Vec<Line> = document
        .text
        .lines()
        .enumerate()
        .skip(scroll)
        .take(visible_height)
        .map(|(index, line)| render_line(index + 1, line, heat.as_deref(), app.sql_focus))
        .collect();

    let footer = format!(
        " line {}-{} of {line_count} ",
        scroll + 1,
        (scroll + visible_height).min(line_count.max(1))
    );
    frame.render_widget(
        Paragraph::new(lines).block(
            panel(&title, theme::GREEN)
                .title_bottom(Line::from(Span::styled(footer, theme::muted()))),
        ),
        area,
    );
}

fn render_line(
    line_number: usize,
    line: &str,
    heat: Option<&[f64]>,
    focus: Option<(usize, usize)>,
) -> Line<'static> {
    let line_heat = heat
        .and_then(|heat| heat.get(line_number - 1))
        .copied()
        .unwrap_or(0.0);
    let focused = focus.is_some_and(|(first, last)| (first..=last).contains(&line_number));

    let mut spans = vec![Span::styled(format!("{line_number:>4} "), theme::muted())];
    // The heat gutter marks costly lines even when scrolled quickly.
    if heat.is_some() {
        let gutter = if line_heat > 0.0 { "▌" } else { " " };
        spans.push(Span::styled(
            format!("{gutter} "),
            Style::default().fg(theme::heat_color(line_heat)),
        ));
    }
    spans.extend(highlight(line));

    let mut rendered = Line::from(spans);
    if focused {
        rendered = rendered.style(
            Style::default()
                .bg(theme::SELECTION)
                .add_modifier(Modifier::BOLD),
        );
    } else if line_heat > 0.5 {
        rendered = rendered.style(Style::default().bg(ratatui::style::Color::Rgb(
            40 + (40.0 * line_heat) as u8,
            18,
            18,
        )));
    }
    rendered
}

/// Word-level SQL highlighting; comments win over everything.
pub fn highlight(line: &str) -> Vec<Span<'static>> {
    if let Some(comment_start) = line.find("--") {
        let (code, comment) = line.split_at(comment_start);
        let mut spans = highlight_code(code);
        spans.push(Span::styled(
            comment.to_string(),
            Style::default()
                .fg(theme::MUTED)
                .add_modifier(Modifier::ITALIC),
        ));
        return spans;
    }
    highlight_code(line)
}

fn highlight_code(code: &str) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut word = String::new();
    let mut in_string = false;
    let mut string_literal = String::new();
    for character in code.chars() {
        if in_string {
            string_literal.push(character);
            if character == '\'' {
                spans.push(Span::styled(
                    std::mem::take(&mut string_literal),
                    Style::default().fg(theme::YELLOW),
                ));
                in_string = false;
            }
            continue;
        }
        if character == '\'' {
            flush_word(&mut spans, &mut word);
            in_string = true;
            string_literal.push(character);
            continue;
        }
        if character.is_ascii_alphanumeric() || character == '_' {
            word.push(character);
            continue;
        }
        flush_word(&mut spans, &mut word);
        spans.push(Span::styled(
            character.to_string(),
            Style::default().fg(theme::TEXT),
        ));
    }
    flush_word(&mut spans, &mut word);
    if in_string {
        spans.push(Span::styled(
            string_literal,
            Style::default().fg(theme::YELLOW),
        ));
    }
    spans
}

fn flush_word(spans: &mut Vec<Span<'static>>, word: &mut String) {
    if word.is_empty() {
        return;
    }
    let uppercase = word.to_ascii_uppercase();
    let style = if KEYWORDS.contains(&uppercase.as_str()) {
        Style::default()
            .fg(theme::MAGENTA)
            .add_modifier(Modifier::BOLD)
    } else if word.chars().all(|character| character.is_ascii_digit()) {
        Style::default().fg(theme::ORANGE)
    } else {
        Style::default().fg(theme::CYAN)
    };
    spans.push(Span::styled(std::mem::take(word), style));
}

#[cfg(test)]
mod tests {
    use super::highlight;

    fn texts(line: &str) -> Vec<String> {
        highlight(line)
            .into_iter()
            .map(|span| span.content.to_string())
            .collect()
    }

    #[test]
    fn keywords_identifiers_numbers_split_into_spans() {
        let spans = texts("SELECT id FROM orders WHERE amount > 42");
        assert!(spans.contains(&"SELECT".to_string()));
        assert!(spans.contains(&"orders".to_string()));
        assert!(spans.contains(&"42".to_string()));
    }

    #[test]
    fn string_literals_stay_one_span() {
        let spans = texts("WHERE region = 'em ea'");
        assert!(spans.contains(&"'em ea'".to_string()));
        // Unterminated strings must not be dropped.
        let spans = texts("VALUES ('oops");
        assert!(spans.contains(&"'oops".to_string()));
    }

    #[test]
    fn comments_swallow_the_rest_of_the_line() {
        let spans = texts("SELECT 1 -- the answer");
        assert_eq!(spans.last().unwrap(), "-- the answer");
    }
}
