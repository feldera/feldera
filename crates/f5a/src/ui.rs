//! Rendering. Every function here is pure: `&App` in, widgets out.

pub mod chrome;
pub mod connectors;
pub mod format;
pub mod header;
pub mod logo;
pub mod logs;
pub mod metrics;
pub mod overlays;
pub mod pipelines;
pub mod profiler;
#[cfg(test)]
mod render_tests;
pub mod sql;
pub mod theme;
pub mod widgets;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::Style;
use ratatui::widgets::{Block, Paragraph};

use crate::app::App;
use crate::app::state::View;

/// Minimum terminal size the layout needs.
const MIN_WIDTH: u16 = 70;
const MIN_HEIGHT: u16 = 16;

/// Render one frame.
pub fn render(frame: &mut Frame, app: &App) {
    let area = frame.area();
    frame.render_widget(Block::default().style(Style::default().bg(theme::BG)), area);
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        frame.render_widget(
            Paragraph::new(format!("f5a needs at least {MIN_WIDTH}x{MIN_HEIGHT} cells"))
                .alignment(ratatui::layout::Alignment::Center)
                .style(theme::accent(theme::YELLOW)),
            area,
        );
        return;
    }

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(header::HEADER_HEIGHT),
            Constraint::Min(8),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(area);
    header::render(frame, rows[0], app);
    match app.view {
        View::Pipelines => pipelines::render(frame, rows[1], app),
        View::Metrics => metrics::render(frame, rows[1], app),
        View::Connectors => connectors::render(frame, rows[1], app),
        View::Profiler => profiler::render(frame, rows[1], app),
        View::Sql => sql::render(frame, rows[1], app),
        View::Logs => logs::render(frame, rows[1], app),
    }
    chrome::render_status_line(frame, rows[2], app);
    chrome::render_tab_bar(frame, rows[3], app);
    overlays::render(frame, area, app);
}
