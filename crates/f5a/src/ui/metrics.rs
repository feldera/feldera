//! The per-pipeline metrics view: live gauges, charts, and the full table.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::symbols;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Cell, Chart, Dataset, GraphType, Paragraph, Row, Table};

use crate::app::App;
use crate::model::stats::PipelineStats;

use super::format::{compact_number, human_bytes, rate, uptime};
use super::theme;
use super::widgets::{centered_rect, kv_line, panel};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let Some(selected) = app.selected_pipeline() else {
        frame.render_widget(panel("METRICS", theme::GREEN), area);
        render_message(frame, area, "Select a pipeline on the Pipelines tab first.");
        return;
    };
    let title = format!("METRICS · {}", selected.name);
    let Some(stats) = &app.detail else {
        frame.render_widget(panel(&title, theme::GREEN), area);
        let message = app
            .detail_error
            .clone()
            .unwrap_or_else(|| "Waiting for the first stats sample…".to_string());
        render_message(frame, area, &message);
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),
            Constraint::Min(9),
            Constraint::Length(9),
        ])
        .split(area);
    render_summary(frame, chunks[0], app, stats, &title);
    render_charts(frame, chunks[1], app);
    render_metric_table(frame, chunks[2], stats);
}

fn render_message(frame: &mut Frame, area: Rect, message: &str) {
    let inner = centered_rect(area.width.saturating_sub(6).min(70), 3, area);
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            message.to_string(),
            theme::accent(theme::YELLOW),
        )))
        .alignment(Alignment::Center),
        inner,
    );
}

fn render_summary(frame: &mut Frame, area: Rect, app: &App, stats: &PipelineStats, title: &str) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Ratio(1, 3),
            Constraint::Ratio(1, 3),
            Constraint::Ratio(1, 3),
        ])
        .split(area);

    let state_style = match stats.state.as_str() {
        "Running" => theme::accent(theme::GREEN),
        "Paused" => Style::default().fg(theme::YELLOW),
        _ => theme::muted(),
    };
    let progress = vec![
        kv_line("STATE", stats.state.clone(), state_style),
        kv_line("UPTIME", uptime(stats.uptime_msecs), theme::text()),
        kv_line(
            "INGESTED",
            compact_number(stats.total_input_records),
            theme::text(),
        ),
        kv_line(
            "PROCESSED",
            compact_number(stats.total_processed_records),
            theme::accent(theme::CYAN),
        ),
        kv_line(
            "BUFFERED",
            compact_number(stats.buffered_input_records),
            Style::default().fg(theme::YELLOW),
        ),
        kv_line(
            "COMPLETE",
            stats.pipeline_complete.to_string(),
            theme::muted(),
        ),
    ];
    frame.render_widget(
        Paragraph::new(progress).block(panel(title, theme::GREEN)),
        columns[0],
    );

    let current = app.history.current_throughput().unwrap_or(0.0);
    let average = app.history.average_throughput().unwrap_or(0.0);
    let rates = vec![
        kv_line("LIVE", rate(current), theme::accent(theme::GREEN)),
        kv_line("AVG", rate(average), theme::text()),
        kv_line(
            "IN BYTES",
            human_bytes(stats.total_input_bytes()),
            theme::text(),
        ),
        kv_line(
            "CONNECTORS",
            format!("{} in · {} out", stats.inputs.len(), stats.outputs.len()),
            theme::text(),
        ),
        kv_line(
            "CONN ERRS",
            stats.connector_error_count().to_string(),
            if stats.connector_error_count() > 0 {
                theme::accent(theme::RED)
            } else {
                theme::muted()
            },
        ),
    ];
    frame.render_widget(
        Paragraph::new(rates).block(panel("THROUGHPUT", theme::CYAN)),
        columns[1],
    );

    let (pressure_glyph, pressure_color) =
        super::pipelines::pressure_decoration(&stats.memory_pressure);
    let resources = vec![
        kv_line(
            "MEMORY",
            format!("{}{pressure_glyph}", human_bytes(stats.rss_bytes)),
            theme::accent(pressure_color),
        ),
        kv_line(
            "PRESSURE",
            format!("{}{pressure_glyph}", stats.memory_pressure),
            if stats.memory_pressure == "low" {
                theme::muted()
            } else {
                theme::accent(pressure_color)
            },
        ),
        {
            let quota = app
                .selected_name
                .as_deref()
                .and_then(|name| app.storage_quotas.get(name))
                .copied();
            let pressure = quota
                .map(|quota| crate::model::stats::pressure_level(stats.storage_bytes, quota))
                .unwrap_or("low");
            let (glyph, color) = super::pipelines::pressure_decoration(pressure);
            let color = if pressure == "low" {
                theme::ORANGE
            } else {
                color
            };
            let value = match quota {
                Some(quota) => format!(
                    "{}{glyph} / {}",
                    human_bytes(stats.storage_bytes),
                    human_bytes(quota)
                ),
                None => human_bytes(stats.storage_bytes),
            };
            kv_line("STORAGE", value, Style::default().fg(color))
        },
        kv_line(
            "CPU TIME",
            format!("{:.1}s", stats.cpu_msecs as f64 / 1_000.0),
            theme::text(),
        ),
        kv_line(
            "CPU CORES",
            if stats.uptime_msecs > 0 {
                format!(
                    "{:.2} avg",
                    stats.cpu_msecs as f64 / stats.uptime_msecs as f64
                )
            } else {
                "-".to_string()
            },
            theme::text(),
        ),
    ];
    frame.render_widget(
        Paragraph::new(resources).block(panel("RESOURCES", theme::MAGENTA)),
        columns[2],
    );
}

fn render_charts(frame: &mut Frame, area: Rect, app: &App) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)])
        .split(area);
    let throughput = app.history.throughput_series();
    render_chart(
        frame,
        columns[0],
        "THROUGHPUT rec/s",
        theme::GREEN,
        &throughput,
        |value| compact_number(value as i64),
    );
    let memory = app.history.memory_series();
    render_chart(
        frame,
        columns[1],
        "MEMORY",
        theme::MAGENTA,
        &memory,
        |value| human_bytes(value as i64),
    );
}

/// Draw one braille line chart with relative-seconds x labels.
fn render_chart(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    color: ratatui::style::Color,
    series: &[(f64, f64)],
    format_y: impl Fn(f64) -> String,
) {
    if series.len() < 2 {
        frame.render_widget(panel(title, color), area);
        render_message(frame, area, "collecting samples…");
        return;
    }
    let newest = series.last().map(|(x, _)| *x).unwrap_or(0.0);
    let relative: Vec<(f64, f64)> = series.iter().map(|(x, y)| (x - newest, *y)).collect();
    let oldest = relative.first().map(|(x, _)| *x).unwrap_or(-60.0);
    let peak = relative.iter().map(|(_, y)| *y).fold(0.0, f64::max);
    let ceiling = if peak <= 0.0 { 1.0 } else { peak * 1.15 };
    let dataset = Dataset::default()
        .marker(symbols::Marker::Braille)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(color))
        .data(&relative);
    let chart = Chart::new(vec![dataset])
        .block(panel(title, color))
        .x_axis(
            Axis::default()
                .bounds([oldest, 0.0])
                .labels(vec![
                    Span::styled(format!("{:.0}s", oldest), theme::muted()),
                    Span::styled("now", theme::muted()),
                ])
                .style(theme::muted()),
        )
        .y_axis(
            Axis::default()
                .bounds([0.0, ceiling])
                .labels(vec![
                    Span::styled("0", theme::muted()),
                    Span::styled(format_y(ceiling / 2.0), theme::muted()),
                    Span::styled(format_y(ceiling), theme::muted()),
                ])
                .style(theme::muted()),
        );
    frame.render_widget(chart, area);
}

/// Every scalar the server sent, in a multi-column table.
fn render_metric_table(frame: &mut Frame, area: Rect, stats: &PipelineStats) {
    let entries: Vec<(String, String)> = stats
        .all_metrics
        .iter()
        .map(|(key, value)| (key.clone(), summarize(value)))
        .collect();
    let visible_rows = area.height.saturating_sub(2).max(1) as usize;
    let column_count = entries.len().div_ceil(visible_rows).clamp(1, 3);
    let per_column = entries.len().div_ceil(column_count);

    let rows = (0..per_column).map(|row_index| {
        let mut cells = Vec::with_capacity(column_count * 2);
        for column in 0..column_count {
            match entries.get(column * per_column + row_index) {
                Some((key, value)) => {
                    cells.push(Cell::from(key.clone()).style(theme::muted()));
                    cells.push(Cell::from(value.clone()).style(theme::text()));
                }
                None => {
                    cells.push(Cell::from(""));
                    cells.push(Cell::from(""));
                }
            }
        }
        Row::new(cells)
    });
    let mut constraints = Vec::new();
    for _ in 0..column_count {
        constraints.push(Constraint::Min(24));
        constraints.push(Constraint::Length(14));
    }
    frame.render_widget(
        Table::new(rows, constraints)
            .block(panel("ALL GLOBAL METRICS", theme::BLUE))
            .column_spacing(1),
        area,
    );
}

/// Compact display of an arbitrary metric value.
fn summarize(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Number(number) => match number.as_i64() {
            Some(integer) if integer.abs() >= 10_000 => compact_number(integer),
            _ => number.to_string(),
        },
        serde_json::Value::String(text) => super::format::truncate(text, 14),
        serde_json::Value::Bool(flag) => flag.to_string(),
        serde_json::Value::Null => "-".to_string(),
        other => super::format::truncate(&other.to_string(), 14),
    }
}

#[cfg(test)]
mod tests {
    use super::summarize;
    use serde_json::json;

    #[test]
    fn metric_values_summarize_compactly() {
        assert_eq!(summarize(&json!(150_000)), "150.0K");
        assert_eq!(summarize(&json!(42)), "42");
        assert_eq!(summarize(&json!(1.5)), "1.5");
        assert_eq!(summarize(&json!("Running")), "Running");
        assert_eq!(summarize(&json!(true)), "true");
        assert_eq!(summarize(&json!(null)), "-");
        assert_eq!(summarize(&json!({"a": 1})), "{\"a\":1}");
    }
}
