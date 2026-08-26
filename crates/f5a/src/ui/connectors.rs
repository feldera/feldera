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
    let (endpoint_width, relation_width) = text_column_widths(area.width, &connectors);
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
            connector_row(connector, endpoint_width, relation_width).style(style)
        });
    let table = Table::new(
        rows,
        [
            Constraint::Length(FIXED_WIDTHS[0]),
            Constraint::Length(endpoint_width),
            Constraint::Length(relation_width),
            Constraint::Length(FIXED_WIDTHS[1]),
            Constraint::Length(FIXED_WIDTHS[2]),
            Constraint::Length(FIXED_WIDTHS[3]),
            Constraint::Length(FIXED_WIDTHS[4]),
            Constraint::Length(FIXED_WIDTHS[5]),
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

/// DIR, STATUS, RECORDS, BYTES, BUFFER, ERRORS.
const FIXED_WIDTHS: [u16; 6] = [4, 8, 9, 10, 9, 7];
const COLUMN_COUNT: u16 = 8;
const MIN_TEXT_WIDTH: u16 = 8;

/// Widths for ENDPOINT and RELATION: their widest value when the terminal has
/// room, otherwise the leftover space split so neither drops below
/// `MIN_TEXT_WIDTH` and the shorter column keeps what it needs.
fn text_column_widths(area_width: u16, connectors: &[&ConnectorRow]) -> (u16, u16) {
    let widest = |label: &str, value: fn(&ConnectorRow) -> &str| {
        connectors
            .iter()
            .map(|connector| value(connector).chars().count())
            .chain([label.len()])
            .max()
            .unwrap_or(0)
            .min(u16::MAX as usize) as u16
    };
    let endpoint_wanted = widest("ENDPOINT", ConnectorRow::connector_name);
    let relation_wanted = widest("RELATION", |connector| &connector.relation);
    // Two border cells plus one spacing cell between each pair of columns.
    let fixed: u16 = FIXED_WIDTHS.iter().sum::<u16>() + 2 + (COLUMN_COUNT - 1);
    let available = area_width.saturating_sub(fixed).max(2 * MIN_TEXT_WIDTH);
    if endpoint_wanted + relation_wanted <= available {
        return (endpoint_wanted, relation_wanted);
    }
    let half = available / 2;
    if relation_wanted <= half {
        (available - relation_wanted, relation_wanted)
    } else if endpoint_wanted <= half {
        (endpoint_wanted, available - endpoint_wanted)
    } else {
        (available - half, half)
    }
}

fn connector_row(
    connector: &ConnectorRow,
    endpoint_width: u16,
    relation_width: u16,
) -> Row<'static> {
    let status = connector.status_label();
    let status_style = match status {
        "Fatal" => theme::accent(theme::RED),
        "Paused" => Style::default().fg(theme::YELLOW),
        "EOI" => Style::default().fg(theme::BLUE),
        _ => theme::accent(theme::GREEN),
    };
    Row::new(vec![
        Cell::from(connector.kind.label()).style(theme::muted()),
        Cell::from(truncate(
            connector.connector_name(),
            endpoint_width as usize,
        ))
        .style(theme::text()),
        Cell::from(truncate(&connector.relation, relation_width as usize)).style(theme::text()),
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
