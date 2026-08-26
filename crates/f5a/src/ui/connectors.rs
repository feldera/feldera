//! Connector table for the watched pipeline.

use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Cell, Row, Table};

use crate::app::App;
use crate::model::stats::ConnectorRow;

use super::format::{compact_number, human_bytes, truncate};
use super::theme;
use super::widgets::{centered_rect, panel, scrollbar, table_offset};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let title = match app.selected_pipeline() {
        Some(row) => format!("CONNECTORS · {}", row.name),
        None => "CONNECTORS".to_string(),
    };
    let connectors = app.connectors();
    let block = panel(&title, theme::CYAN);
    if connectors.is_empty() {
        frame.render_widget(block, area);
        let message = if app.selected_pipeline().is_none() {
            "Select a pipeline first."
        } else if app.detail.is_none() {
            "Waiting for stats; connectors appear once the pipeline is deployed."
        } else {
            "This pipeline has no connectors."
        };
        let inner = centered_rect(area.width.saturating_sub(6).min(74), 1, area);
        frame.render_widget(
            ratatui::widgets::Paragraph::new(message)
                .style(theme::accent(theme::YELLOW))
                .alignment(ratatui::layout::Alignment::Center),
            inner,
        );
        return;
    }

    let viewport_height = area.height.saturating_sub(3) as usize;
    let offset = table_offset(
        Some(app.selected_connector),
        connectors.len(),
        viewport_height,
    );
    app.viewport.connectors.set((offset, viewport_height));
    let count = connectors.len();
    let rows = connectors
        .iter()
        .enumerate()
        .skip(offset)
        .take(viewport_height)
        .map(|(index, connector)| {
            let style = if index == app.selected_connector {
                Style::default()
                    .bg(theme::SELECTION)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            connector_row(connector).style(style)
        });
    let table = Table::new(
        rows,
        [
            Constraint::Length(4),
            Constraint::Min(18),
            Constraint::Min(14),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Length(10),
            Constraint::Length(9),
            Constraint::Length(7),
        ],
    )
    .header(
        Row::new([
            "DIR", "ENDPOINT", "RELATION", "STATUS", "RECORDS", "BYTES", "BUFFER", "ERRORS",
        ])
        .style(theme::accent(theme::CYAN)),
    )
    .block(block)
    .column_spacing(1);
    frame.render_widget(table, area);
    scrollbar(frame, area, offset, count, viewport_height);
}

fn connector_row(connector: &ConnectorRow) -> Row<'static> {
    let status = connector.status_label();
    let status_style = match status {
        "Fatal" => theme::accent(theme::RED),
        "Paused" => Style::default().fg(theme::YELLOW),
        "EOI" => Style::default().fg(theme::BLUE),
        _ => theme::accent(theme::GREEN),
    };
    Row::new(vec![
        Cell::from(connector.kind.label()).style(theme::muted()),
        Cell::from(truncate(&connector.endpoint_name, 26)).style(theme::text()),
        Cell::from(truncate(&connector.relation, 20)).style(theme::text()),
        Cell::from(status).style(status_style),
        Cell::from(compact_number(connector.records)).style(theme::text()),
        Cell::from(human_bytes(connector.bytes)).style(theme::text()),
        Cell::from(compact_number(connector.buffered_records))
            .style(Style::default().fg(theme::YELLOW)),
        Cell::from(connector.error_count.to_string()).style(if connector.error_count > 0 {
            theme::accent(theme::RED)
        } else {
            theme::muted()
        }),
    ])
}
