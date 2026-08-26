//! The hotspot ranking joined with SQL positions.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table};

use crate::app::App;
use crate::model::dataflow::CostMetric;

use super::format::{compact_number, human_bytes, truncate};
use super::theme;
use super::widgets::{bar, centered_rect, panel, scrollbar, table_offset};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let title = match &app.report_pipeline {
        Some(pipeline) => format!(
            "HOTSPOTS · {pipeline} · by {} (m cycles)",
            app.metric.label()
        ),
        None => "HOTSPOTS".to_string(),
    };
    let block = panel(&title, theme::MAGENTA);
    let Some(report) = &app.report else {
        frame.render_widget(block, area);
        let message = if app.loading_report {
            "Profiling the circuit… this takes a few seconds."
        } else {
            "Press f (or :profile) on a running pipeline to rank its operators by cost."
        };
        let inner = centered_rect(area.width.saturating_sub(6).min(76), 1, area);
        frame.render_widget(
            Paragraph::new(Span::styled(message, theme::accent(theme::YELLOW)))
                .alignment(Alignment::Center),
            inner,
        );
        return;
    };

    if report.is_empty() {
        frame.render_widget(block, area);
        let inner = centered_rect(area.width.saturating_sub(6).min(76), 1, area);
        frame.render_widget(
            Paragraph::new(Span::styled(
                "The profile has no cost data; let the pipeline process data and press f again.",
                theme::accent(theme::YELLOW),
            ))
            .alignment(Alignment::Center),
            inner,
        );
        return;
    }

    let order = report.ranking(app.metric);
    let total_cost = report.total_cost(app.metric).max(f64::EPSILON);
    let viewport_height = area.height.saturating_sub(3) as usize;
    let offset = table_offset(Some(app.selected_hotspot), order.len(), viewport_height);
    app.viewport.hotspots.set((offset, viewport_height));
    let hotspot_count = order.len();
    let rows = order
        .iter()
        .enumerate()
        .skip(offset)
        .take(viewport_height)
        .map(|(rank, index)| {
            let hotspot = &report.hotspots[*index];
            let cost = hotspot.cost(app.metric);
            let share = cost / total_cost;
            let cost_label = match app.metric {
                CostMetric::Time => format!("{:.2}s", hotspot.time_seconds),
                CostMetric::Memory => human_bytes(hotspot.memory_bytes),
                CostMetric::Records => compact_number(hotspot.state_records),
            };
            let sql_ref = hotspot
                .first_line()
                .map(|line| format!("L{line}"))
                .unwrap_or_else(|| "-".to_string());
            let style = if rank == app.selected_hotspot {
                Style::default()
                    .bg(theme::SELECTION)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(format!("{:>3}", rank + 1)).style(theme::muted()),
                Cell::from(bar(share, 12)).style(Style::default().fg(theme::heat_color(share))),
                Cell::from(format!("{:>5.1}%", share * 100.0))
                    .style(Style::default().fg(theme::heat_color(share))),
                Cell::from(cost_label).style(theme::text()),
                Cell::from(truncate(&hotspot.operator, 34)).style(theme::accent(theme::CYAN)),
                Cell::from(truncate(hotspot.relation.as_deref().unwrap_or("-"), 20))
                    .style(theme::text()),
                Cell::from(sql_ref).style(theme::accent(theme::GREEN)),
            ])
            .style(style)
        });

    let footer = format!(
        "{} operators · {} workers · {:.1}s total runtime · {} mapped to SQL",
        report.hotspots.len(),
        report.worker_count,
        report.total_time_seconds,
        report.mapped_count
    );
    let table = Table::new(
        rows,
        [
            Constraint::Length(3),
            Constraint::Length(12),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Min(24),
            Constraint::Length(20),
            Constraint::Length(6),
        ],
    )
    .header(
        Row::new([
            "#",
            "COST SHARE",
            "%",
            "COST",
            "OPERATOR",
            "RELATION",
            "SQL",
        ])
        .style(theme::accent(theme::MAGENTA)),
    )
    .block(block.title_bottom(Line::from(Span::styled(
        format!(" {footer} "),
        theme::muted(),
    ))))
    .column_spacing(1);
    frame.render_widget(table, area);
    scrollbar(frame, area, offset, hotspot_count, viewport_height);
}
