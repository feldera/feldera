//! The dashboard: one live row per pipeline.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table};

use crate::app::App;
use crate::app::state::{ClickTarget, ColumnHit, Connection, SortColumn};
use crate::model::pipeline::{Health, PipelineRow, deployment_health, program_health};

use super::format::{compact_number, human_bytes, rate, sparkline};
use super::theme;
use super::widgets::{centered_rect, panel, scrollbar, table_offset};

/// The table's columns, in display order. Narrow terminals drop the
/// lower-priority ones so the important numbers stay readable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Column {
    Name,
    Tags,
    Status,
    Program,
    Runtime,
    Rps,
    Activity,
    Records,
    Buffer,
    Memory,
    Storage,
    Errors,
    Age,
    /// Samply capture state; present only while a capture exists.
    Samply,
    /// Support bundle download state; present only while one exists.
    Bundle,
}

impl Column {
    const fn title(self) -> &'static str {
        match self {
            Self::Name => "NAME",
            Self::Tags => "TAGS",
            Self::Status => "STATUS",
            Self::Samply => "SAMPLY",
            Self::Bundle => "BUNDLE",
            Self::Program => "PROGRAM",
            Self::Runtime => "RUNTIME",
            Self::Rps => "RPS",
            Self::Activity => "ACTIVITY",
            Self::Records => "RECORDS",
            Self::Buffer => "BUFFER",
            Self::Memory => "MEMORY",
            Self::Storage => "STORAGE",
            Self::Errors => "ERR",
            Self::Age => "AGE",
        }
    }

    const ALL: [Self; 15] = [
        Self::Name,
        Self::Tags,
        Self::Status,
        Self::Program,
        Self::Runtime,
        Self::Rps,
        Self::Activity,
        Self::Records,
        Self::Buffer,
        Self::Memory,
        Self::Storage,
        Self::Errors,
        Self::Age,
        Self::Samply,
        Self::Bundle,
    ];

    /// Least important first: dropped one by one until the table fits.
    const DROP_ORDER: [Self; 8] = [
        Self::Buffer,
        Self::Activity,
        Self::Runtime,
        Self::Storage,
        Self::Program,
        Self::Tags,
        Self::Age,
        Self::Errors,
    ];

    /// The view a double click on this column jumps to.
    const fn click_target(self) -> ClickTarget {
        match self {
            Self::Name | Self::Tags => ClickTarget::Sql,
            Self::Status | Self::Program | Self::Errors => ClickTarget::Logs,
            Self::Samply => ClickTarget::Samply,
            Self::Bundle => ClickTarget::Bundle,
            Self::Runtime
            | Self::Rps
            | Self::Activity
            | Self::Records
            | Self::Buffer
            | Self::Memory
            | Self::Storage
            | Self::Age => ClickTarget::Metrics,
        }
    }

    /// The sort key a click on this column's header applies.
    const fn sort_column(self) -> Option<SortColumn> {
        match self {
            Self::Name => Some(SortColumn::Name),
            Self::Status => Some(SortColumn::Status),
            Self::Rps => Some(SortColumn::Throughput),
            Self::Records => Some(SortColumn::Records),
            Self::Memory => Some(SortColumn::Memory),
            Self::Storage => Some(SortColumn::Storage),
            Self::Age => Some(SortColumn::Age),
            _ => None,
        }
    }

    /// Cell width; Name and Tags are sized from their content so nothing is
    /// cut when the terminal has room.
    fn width(self, name_width: u16, tags_width: u16) -> u16 {
        match self {
            Self::Name => name_width,
            Self::Tags => tags_width,
            Self::Status => 18,
            // Fits "▂▄▆█ 100s/120s": the waveform plus a three-digit window.
            Self::Samply => 14,
            // Fits "✖ failed".
            Self::Bundle => 8,
            Self::Program => 13,
            Self::Runtime => 12,
            Self::Rps => 9,
            Self::Activity => 12,
            Self::Records => 9,
            Self::Buffer => 9,
            Self::Memory => 12,
            Self::Storage => 12,
            Self::Errors => 5,
            Self::Age => 5,
        }
    }
}

/// Pick the columns that fit `area_width`, dropping the least important ones
/// first. Never over-constrains, so no column gets silently squeezed. The
/// Tags, Samply, and Bundle columns only exist while they have something to
/// show.
fn visible_columns(
    area_width: u16,
    name_width: u16,
    tags_width: u16,
    any_tags: bool,
    any_samply: bool,
    any_bundle: bool,
) -> Vec<Column> {
    let mut columns: Vec<Column> = Column::ALL.to_vec();
    if !any_tags {
        columns.retain(|column| *column != Column::Tags);
    }
    if !any_samply {
        columns.retain(|column| *column != Column::Samply);
    }
    if !any_bundle {
        columns.retain(|column| *column != Column::Bundle);
    }
    let total = |columns: &[Column]| -> u16 {
        let cells: u16 = columns
            .iter()
            .map(|column| column.width(name_width, tags_width))
            .sum();
        // One space between columns plus the panel borders.
        cells + columns.len().saturating_sub(1) as u16 + 2
    };
    for drop in Column::DROP_ORDER {
        if total(&columns) <= area_width || columns.len() <= 5 {
            break;
        }
        columns.retain(|column| *column != drop);
    }
    columns
}

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let rows = app.visible_rows();
    let marked_note = if app.marked.is_empty() {
        String::new()
    } else {
        format!("  {} marked", app.marked.len())
    };
    let block = panel(
        &format!(
            "PIPELINES [{}]{marked_note}  sort:{}{}",
            rows.len(),
            app.sort.title(),
            if app.sort_descending { "↓" } else { "↑" }
        ),
        theme::CYAN,
    );
    if rows.is_empty() {
        app.viewport.pipelines.set((0, 0));
        frame.render_widget(block, area);
        render_empty_state(frame, area, app);
        return;
    }

    let any_tags = rows.iter().any(|row| !row.tags.is_empty());
    let widest_name = rows
        .iter()
        .map(|row| {
            let queued = app
                .queued_label(&row.name)
                .map(|label| label.chars().count() + 2)
                .unwrap_or(0);
            let inflight = app
                .inflight
                .get(&row.name)
                .map(|action| action.verb().chars().count() + 2)
                .unwrap_or(0);
            (row.name.chars().count() + queued + inflight) as u16
        })
        .max()
        .unwrap_or(0);
    // Mark and marker glyphs take three cells; cap so one long name cannot
    // push the metrics off screen.
    let name_width = (widest_name + 3).clamp(18, area.width / 2);
    let tags_width = rows
        .iter()
        .map(|row| row.tags_label().chars().count() as u16)
        .max()
        .unwrap_or(0)
        .clamp(4, 40);
    let any_samply = rows.iter().any(|row| app.samply.contains_key(&row.name));
    let any_bundle = rows.iter().any(|row| app.bundles.contains_key(&row.name));
    let columns = visible_columns(
        area.width, name_width, tags_width, any_tags, any_samply, any_bundle,
    );
    let selected = app.selected_index();

    // Scroll just enough to keep the selection visible, and report the
    // offset and column ranges back for mouse handling.
    let viewport_height = area.height.saturating_sub(3) as usize;
    let offset = table_offset(selected, rows.len(), viewport_height);
    app.viewport.pipelines.set((offset, viewport_height));
    let mut column_hits = Vec::with_capacity(columns.len());
    let mut x = area.x + 1;
    for column in &columns {
        let width = column.width(name_width, tags_width);
        column_hits.push(ColumnHit {
            x_start: x,
            x_end: x + width,
            target: column.click_target(),
            sort: column.sort_column(),
        });
        x += width + 1;
    }
    *app.viewport.click_columns.borrow_mut() = column_hits;

    let table_rows = rows
        .iter()
        .enumerate()
        .skip(offset)
        .take(viewport_height)
        .map(|(index, row)| {
            let is_selected = Some(index) == selected;
            table_row(app, row, &columns, is_selected)
        });
    let constraints = columns
        .iter()
        .map(|column| Constraint::Length(column.width(name_width, tags_width)));
    let header_cells = columns.iter().map(|column| {
        // The active sort column wears its direction; headers are clickable.
        if column.sort_column() == Some(app.sort) {
            format!(
                "{}{}",
                column.title(),
                if app.sort_descending { "↓" } else { "↑" }
            )
        } else {
            column.title().to_string()
        }
    });
    let table = Table::new(table_rows, constraints)
        .header(
            Row::new(header_cells)
                .style(theme::accent(theme::CYAN))
                .bottom_margin(0),
        )
        .block(block)
        .column_spacing(1);
    frame.render_widget(table, area);
    scrollbar(frame, area, offset, rows.len(), viewport_height);
}

/// Glyph and color escalating with the engine's memory pressure.
pub fn pressure_decoration(pressure: &str) -> (&'static str, ratatui::style::Color) {
    match pressure {
        "moderate" => ("▲", theme::YELLOW),
        "high" => ("♨", theme::ORANGE),
        "critical" => ("🔥", theme::RED),
        _ => ("", theme::BLUE),
    }
}

/// While storage is being cleared, its cell burns the last-known size to
/// dust: characters crumble into `▒` and `·`, then vanish, then the label
/// reforms and burns again.
fn clearing_storage_cell<'a>(app: &App, row: &PipelineRow) -> Option<Cell<'a>> {
    // Only clears the server actually performs burn: its `Clearing` status,
    // or the flash window a successful clear opened. A rejected clear (e.g.
    // on a running pipeline) shows nothing.
    let clearing = row.storage_status == "Clearing"
        || app
            .clearing_flash
            .get(&row.name)
            .is_some_and(|until| *until > app.now_millis);
    if !clearing {
        return None;
    }
    let label = app
        .live_of(&row.name)
        .map(|cached| human_bytes(cached.storage_bytes))
        .unwrap_or_else(|| "· · ·".to_string());
    let phase = app.spinner_frame % DISSOLVE_PHASES;
    let flame = if app.spinner_frame.is_multiple_of(2) {
        theme::ORANGE
    } else {
        theme::RED
    };
    Some(Cell::from(dissolve(&label, phase)).style(Style::default().fg(flame)))
}

/// One burn cycle: intact at phase 0, fully gone near the end.
const DISSOLVE_PHASES: usize = 14;

/// Crumble `label` according to `phase`: each character has its own crumble
/// order, passing through dense dust, fine dust, then nothing.
fn dissolve(label: &str, phase: usize) -> String {
    label
        .chars()
        .enumerate()
        .map(|(index, character)| {
            if character == ' ' {
                return ' ';
            }
            // A fixed per-character order makes the crumble deterministic
            // but scattered rather than a left-to-right wipe.
            let crumble_at = (index * 7 + 3) % 8;
            match phase {
                phase if phase > crumble_at + 6 => ' ',
                phase if phase > crumble_at + 3 => '·',
                phase if phase > crumble_at => '▒',
                _ => character,
            }
        })
        .collect()
}

/// The marker before the name: a spinner while transitioning, otherwise a
/// steady health glyph.
fn marker(app: &App, row: &PipelineRow) -> Span<'static> {
    if row.deployment_error.is_some() {
        return Span::styled("✖ ", theme::accent(theme::RED));
    }
    if row.is_transitioning() {
        let frames = ["◐ ", "◓ ", "◑ ", "◒ "];
        return Span::styled(
            frames[app.spinner_frame % frames.len()],
            theme::accent(theme::YELLOW),
        );
    }
    match deployment_health(&row.deployment_status) {
        Health::Good => Span::styled("● ", theme::accent(theme::GREEN)),
        Health::Busy => Span::styled("◐ ", Style::default().fg(theme::YELLOW)),
        Health::Idle => Span::styled("○ ", theme::muted()),
        Health::Bad => Span::styled("✖ ", theme::accent(theme::RED)),
    }
}

/// The status label; when a steady state has a different destination the
/// label shows both ("Stopped⇢Running"). Transitional statuses like
/// `Provisioning` already say where they are going.
fn status_label(row: &PipelineRow) -> String {
    let steady = matches!(
        row.deployment_status.as_str(),
        "Running" | "Paused" | "Stopped" | "Suspended" | "Standby"
    );
    match &row.desired_status {
        Some(desired) if steady && desired != &row.deployment_status => {
            format!("{}⇢{desired}", row.deployment_status)
        }
        _ => row.deployment_status.clone(),
    }
}

fn table_row<'a>(
    app: &'a App,
    row: &'a PipelineRow,
    columns: &[Column],
    is_selected: bool,
) -> Row<'a> {
    // Runtime metrics are only truthful while `/stats` still answers; a
    // stopped pipeline would otherwise keep showing its last sample forever.
    let live = app.live_of(&row.name).filter(|_| row.is_queryable());
    let health = row.health();
    let spark_values: Vec<i64> = live
        .map(|live| live.spark.iter().copied().collect())
        .unwrap_or_default();

    let cells = columns.iter().map(|column| match column {
        Column::Name => {
            let mark = if app.marked.contains(&row.name) {
                Span::styled("✓", theme::accent(theme::YELLOW))
            } else {
                Span::raw(" ")
            };
            let mut spans = vec![
                mark,
                marker(app, row),
                Span::styled(row.name.clone(), theme::text()),
            ];
            // The request in flight shows at once; the status column only
            // moves when the next overview confirms it.
            if let Some(action) = app.inflight.get(&row.name) {
                spans.push(Span::styled(
                    format!(" ↻{}", action.verb()),
                    Style::default()
                        .fg(theme::CYAN)
                        .add_modifier(Modifier::ITALIC),
                ));
            }
            if let Some(queued) = app.queued_label(&row.name) {
                spans.push(Span::styled(
                    format!(" ⧗{queued}"),
                    Style::default()
                        .fg(theme::YELLOW)
                        .add_modifier(Modifier::ITALIC),
                ));
            }
            Cell::from(Line::from(spans))
        }
        Column::Tags => Cell::from(row.tags_label()).style(Style::default().fg(theme::MAGENTA)),
        Column::Status => Cell::from(status_label(row)).style(theme::health_style(health)),
        Column::Samply => match app.samply.get(&row.name) {
            Some(capture) => Cell::from(capture.label(app.now_millis))
                .style(theme::capture_style(&capture.phase, app.now_millis)),
            None => Cell::from(""),
        },
        Column::Bundle => match app.bundles.get(&row.name) {
            Some(capture) => Cell::from(capture.label(app.now_millis))
                .style(theme::capture_style(&capture.phase, app.now_millis)),
            None => Cell::from(""),
        },
        Column::Program => Cell::from(row.program_status.clone())
            .style(theme::health_style(program_health(&row.program_status))),
        Column::Runtime => Cell::from(row.runtime_label()).style(theme::muted()),
        Column::Rps => Cell::from(live.map_or("-".to_string(), |live| rate(live.throughput)))
            .style(theme::accent(theme::GREEN)),
        Column::Activity => {
            Cell::from(sparkline(&spark_values, 12)).style(Style::default().fg(theme::MAGENTA))
        }
        Column::Records => {
            Cell::from(live.map_or("-".to_string(), |live| compact_number(live.records)))
                .style(theme::text())
        }
        Column::Buffer => Cell::from(live.map_or("-".to_string(), |live| {
            compact_number(live.buffered_records)
        }))
        .style(Style::default().fg(theme::YELLOW)),
        Column::Memory => match live {
            Some(live) => {
                let (glyph, color) = pressure_decoration(&live.memory_pressure);
                Cell::from(format!("{}{glyph}", human_bytes(live.rss_bytes)))
                    .style(Style::default().fg(color))
            }
            None => Cell::from("-").style(Style::default().fg(theme::BLUE)),
        },
        Column::Storage => {
            if let Some(cell) = clearing_storage_cell(app, row) {
                cell
            } else if app.is_clear_queued(&row.name) {
                Cell::from("⧗ clear").style(
                    Style::default()
                        .fg(theme::YELLOW)
                        .add_modifier(Modifier::ITALIC),
                )
            } else {
                match live {
                    Some(live) => {
                        let pressure = app
                            .storage_quotas
                            .get(&row.name)
                            .map(|quota| {
                                crate::model::stats::pressure_level(live.storage_bytes, *quota)
                            })
                            .unwrap_or("low");
                        let (glyph, color) = pressure_decoration(pressure);
                        let color = if pressure == "low" {
                            theme::ORANGE
                        } else {
                            color
                        };
                        Cell::from(format!("{}{glyph}", human_bytes(live.storage_bytes)))
                            .style(Style::default().fg(color))
                    }
                    // Storage outlives the deployment: while the status says
                    // it is still in use, show the last-known footprint
                    // dimmed; once cleared, show nothing.
                    None if row.storage_status != "Cleared" => Cell::from(
                        app.live_of(&row.name)
                            .map_or("-".to_string(), |cached| human_bytes(cached.storage_bytes)),
                    )
                    .style(theme::muted()),
                    None => Cell::from("-").style(theme::muted()),
                }
            }
        }
        Column::Errors => Cell::from(row.connector_error_count.to_string()).style(
            if row.connector_error_count > 0 {
                theme::accent(theme::RED)
            } else {
                theme::muted()
            },
        ),
        Column::Age => Cell::from(row.status_age(app.now_millis)).style(theme::muted()),
    });

    let marked = app.marked.contains(&row.name);
    let style = if is_selected {
        let mut style = Style::default()
            .bg(theme::SELECTION)
            .add_modifier(Modifier::BOLD);
        if marked {
            style = style.fg(theme::YELLOW);
        }
        style
    } else if marked {
        Style::default()
            .bg(ratatui::style::Color::Rgb(52, 46, 14))
            .fg(theme::YELLOW)
    } else {
        Style::default()
    };
    Row::new(cells.collect::<Vec<_>>()).style(style)
}

fn render_empty_state(frame: &mut Frame, area: Rect, app: &App) {
    let inner = centered_rect(60, 6, area);
    let lines = match &app.connection {
        Connection::Connecting => vec![
            Line::from(Span::styled(
                "Connecting to the instance…",
                theme::accent(theme::YELLOW),
            )),
            Line::from(Span::styled(&*app.host, theme::muted())),
        ],
        Connection::Lost { error, .. } => vec![
            Line::from(Span::styled(
                "Cannot reach the instance",
                theme::accent(theme::RED),
            )),
            Line::from(Span::styled(error.clone(), theme::text())),
            Line::from(Span::styled("retrying in the background…", theme::muted())),
        ],
        Connection::Online { .. } if !app.filter.is_empty() => vec![
            Line::from(Span::styled(
                format!("No pipeline matches `{}`", app.filter),
                theme::accent(theme::YELLOW),
            )),
            Line::from(Span::styled("esc clears the filter", theme::muted())),
        ],
        Connection::Online { .. } => vec![
            Line::from(Span::styled("No pipelines yet", theme::accent(theme::CYAN))),
            Line::from(Span::styled(
                "Create one in the web console or with fda; it appears here live.",
                theme::muted(),
            )),
        ],
    };
    frame.render_widget(Paragraph::new(lines).alignment(Alignment::Center), inner);
}

#[cfg(test)]
mod tests {
    use super::{Column, status_label, visible_columns};
    use crate::model::pipeline::PipelineRow;

    #[test]
    fn wide_terminals_show_every_column() {
        let columns = visible_columns(200, 20, 12, true, true, true);
        assert_eq!(columns.len(), 15);
        assert!(columns.contains(&Column::Tags));
        assert!(columns.contains(&Column::Samply));
        assert!(columns.contains(&Column::Runtime));
        assert!(columns.contains(&Column::Activity));
    }

    #[test]
    fn columns_drop_by_priority_until_the_table_fits() {
        let columns = visible_columns(100, 20, 12, true, false, false);
        assert!(columns.contains(&Column::Name));
        assert!(columns.contains(&Column::Status));
        assert!(columns.contains(&Column::Rps));
        assert!(!columns.contains(&Column::Buffer), "buffer drops first");
        assert!(!columns.contains(&Column::Activity));
        // A wide name column costs other columns, never squeezes them.
        let with_long_names = visible_columns(160, 70, 12, true, false, false);
        let with_short_names = visible_columns(160, 20, 12, true, false, false);
        assert!(with_long_names.len() < with_short_names.len());
    }

    #[test]
    fn even_tiny_terminals_keep_the_core_columns() {
        let columns = visible_columns(30, 18, 4, true, false, false);
        assert_eq!(columns.len(), 5);
        assert!(columns.contains(&Column::Name));
        assert!(columns.contains(&Column::Records));
    }

    #[test]
    fn the_tags_column_needs_tagged_pipelines() {
        assert!(!visible_columns(300, 20, 12, false, false, false).contains(&Column::Tags));
        assert!(visible_columns(300, 20, 12, true, false, false).contains(&Column::Tags));
    }

    #[test]
    fn the_bundle_column_trails_everything_and_hides_when_idle() {
        assert!(!visible_columns(300, 20, 12, true, true, false).contains(&Column::Bundle));
        let columns = visible_columns(300, 20, 12, true, true, true);
        assert_eq!(columns.last(), Some(&Column::Bundle));
        let without_samply = visible_columns(300, 20, 12, true, false, true);
        assert_eq!(without_samply.last(), Some(&Column::Bundle));
        assert!(!without_samply.contains(&Column::Samply));
        assert!(visible_columns(30, 18, 4, true, false, true).contains(&Column::Bundle));
    }

    #[test]
    fn the_samply_column_exists_only_while_a_capture_does() {
        assert!(!visible_columns(300, 20, 12, true, false, false).contains(&Column::Samply));
        let columns = visible_columns(300, 20, 12, true, true, false);
        assert!(columns.contains(&Column::Samply));
        // It is the last column and never gets dropped for width.
        assert_eq!(columns.last(), Some(&Column::Samply));
        assert!(visible_columns(30, 18, 4, true, true, false).contains(&Column::Samply));
    }

    #[test]
    fn the_dissolve_burns_scatters_and_finishes_clean() {
        let label = "448.3 MiB";
        assert_eq!(super::dissolve(label, 0), label, "phase 0 is intact");
        let mid = super::dissolve(label, 6);
        assert_ne!(mid, label);
        assert!(mid.contains('▒') || mid.contains('·'));
        let done = super::dissolve(label, super::DISSOLVE_PHASES - 1);
        assert!(
            done.chars().all(|c| matches!(c, ' ' | '·' | '▒')),
            "late phases hold only dust: {done}"
        );
        assert_eq!(
            done.chars().count(),
            label.chars().count(),
            "cell width is stable"
        );
    }

    #[test]
    fn pressure_decorations_escalate_to_fire() {
        use super::pressure_decoration;
        assert_eq!(pressure_decoration("low").0, "");
        assert_eq!(pressure_decoration("moderate").0, "▲");
        assert_eq!(pressure_decoration("high").0, "♨");
        assert_eq!(pressure_decoration("critical").0, "🔥");
        assert_eq!(pressure_decoration("something-new").0, "");
    }

    #[test]
    fn status_labels_show_the_destination_from_steady_states_only() {
        let row = |status: &str, desired: Option<&str>| PipelineRow {
            deployment_status: status.to_string(),
            desired_status: desired.map(str::to_string),
            ..Default::default()
        };
        assert_eq!(
            status_label(&row("Stopped", Some("Running"))),
            "Stopped⇢Running"
        );
        assert_eq!(status_label(&row("Running", Some("Running"))), "Running");
        assert_eq!(
            status_label(&row("Provisioning", Some("Running"))),
            "Provisioning",
            "transitional statuses already name their destination"
        );
    }
}
