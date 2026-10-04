//! The Connectors view: connectors grouped under their table or view in a
//! sortable tree, beside a detail pane for the row under the cursor.
//!
//! ```text
//! ╭ CONNECTORS · p [3]  sort:name↑ ───────────╮╭ kafka ─────────────────────╮
//! │ NAME            STATUS   RPS  RECORDS ERR ││ relation   orders · table  │
//! │ ▾ orders table  Paused  5/s      600    2 ││ status     Active          │
//! │   ├─ http       Paused  0/s      100    0 ││ throughput 5/s · 40 B/s    │
//! │   └─ kafka      Active  5/s      500    2 ││ RECENT ERRORS              │
//! ```

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table};

use crate::app::App;
use crate::app::connector_tree::{
    ConnectorNode, ConnectorSort, ConnectorTree, RelationGroup, TreeRow,
};
use crate::app::state::SortHit;
use crate::model::stats::{ConnectorKind, ConnectorRow, ConnectorStatus, Frontier};
use crate::model::text::wrap_words;

use super::format::{
    compact_number, human_bytes, latency_micros, rate, sparkline, truncate, uptime,
};
use super::theme;
use super::widgets::{
    centered_rect, kv_line, panel, panel_title, scrollbar, sort_label, split_beside, table_offset,
};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let tree = app.connector_tree();
    if tree.rows.is_empty() {
        app.viewport.connectors.set((0, 0));
        app.viewport.connector_tree_right.set(None);
        app.viewport.connector_columns.borrow_mut().clear();
        render_empty(frame, area, app);
        return;
    }
    let natural_width = table_width(&Column::ALL, natural_name_width(&tree));
    let (tree_area, detail_area) = split_beside(area, natural_width);
    let is_beside = detail_area.is_some_and(|detail| detail.x > tree_area.x);
    app.viewport
        .connector_tree_right
        .set(is_beside.then(|| tree_area.right()));
    render_tree(frame, tree_area, app, &tree);
    if let Some(detail_area) = detail_area {
        render_detail(frame, detail_area, app, &tree);
    }
}

fn render_empty(frame: &mut Frame, area: Rect, app: &App) {
    let pipeline = app.selected_pipeline().map(|row| row.name.as_str());
    frame.render_widget(
        panel(&panel_title("CONNECTORS", pipeline, None, &[]), theme::CYAN),
        area,
    );
    let message = if app.selected_pipeline().is_none() {
        "Select a pipeline first."
    } else if app.detail.is_none() {
        "Waiting for stats; connectors appear once the pipeline is deployed."
    } else {
        "This pipeline has no connectors."
    };
    let inner = centered_rect(area.width.saturating_sub(6).min(74), 1, area);
    frame.render_widget(
        Paragraph::new(message)
            .style(theme::accent(theme::YELLOW))
            .alignment(Alignment::Center),
        inner,
    );
}

/// Tree columns, in display order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Column {
    Name,
    Status,
    Rate,
    Records,
    Backlog,
    Errors,
}

impl Column {
    const ALL: [Self; 6] = [
        Self::Name,
        Self::Status,
        Self::Rate,
        Self::Records,
        Self::Backlog,
        Self::Errors,
    ];

    /// Least important first: dropped one by one until the tree fits.
    const DROP_ORDER: [Self; 3] = [Self::Backlog, Self::Records, Self::Rate];

    const fn title(self) -> &'static str {
        match self {
            Self::Name => "NAME",
            Self::Status => "STATUS",
            Self::Rate => "RPS",
            Self::Records => "RECORDS",
            Self::Backlog => "BUFFER",
            Self::Errors => "ERR",
        }
    }

    const fn sort(self) -> ConnectorSort {
        match self {
            Self::Name => ConnectorSort::Name,
            Self::Status => ConnectorSort::Status,
            Self::Rate => ConnectorSort::Rate,
            Self::Records => ConnectorSort::Records,
            Self::Backlog => ConnectorSort::Backlog,
            Self::Errors => ConnectorSort::Errors,
        }
    }

    const fn width(self, name_width: u16) -> u16 {
        match self {
            Self::Name => name_width,
            // Fits "Unhealthy".
            Self::Status => 9,
            Self::Rate | Self::Records | Self::Backlog => 8,
            Self::Errors => 5,
        }
    }
}

/// Narrowest name column, and the share of it guaranteed before optional
/// columns are dropped.
const MIN_NAME_WIDTH: u16 = 12;
const NAME_WIDTH_BEFORE_DROPPING: u16 = 24;

/// Borders, cells, and one spacing cell between each pair of columns.
fn table_width(columns: &[Column], name_width: u16) -> u16 {
    let cells: u16 = columns.iter().map(|column| column.width(name_width)).sum();
    cells + columns.len().saturating_sub(1) as u16 + 2
}

const RELATION_PREFIX: &str = "▾ ";
const CONNECTOR_PREFIX: &str = "  ├─ ";

/// Width that shows every name in full: mark, fold glyph or branch, name,
/// and a relation's kind.
fn natural_name_width(tree: &ConnectorTree) -> u16 {
    let relations = tree.groups.iter().map(|group| {
        1 + RELATION_PREFIX.chars().count()
            + group.name.chars().count()
            + 1
            + group.kind.relation_noun().len()
    });
    let connectors = tree.all_connectors().map(|connector| {
        1 + CONNECTOR_PREFIX.chars().count() + connector.connector_name().chars().count()
    });
    relations
        .chain(connectors)
        .max()
        .unwrap_or(0)
        .clamp(MIN_NAME_WIDTH as usize, u16::MAX as usize) as u16
}

/// Columns that fit `area_width` and the name width they leave.
fn fit_columns(area_width: u16, natural_name: u16) -> (Vec<Column>, u16) {
    let mut columns = Column::ALL.to_vec();
    let guaranteed_name = natural_name.min(NAME_WIDTH_BEFORE_DROPPING);
    for drop in Column::DROP_ORDER {
        if table_width(&columns, guaranteed_name) <= area_width {
            break;
        }
        columns.retain(|column| *column != drop);
    }
    let available = area_width.saturating_sub(table_width(&columns, 0));
    (columns, natural_name.min(available).max(MIN_NAME_WIDTH))
}

fn render_tree(frame: &mut Frame, area: Rect, app: &App, tree: &ConnectorTree) {
    let pane = &app.connector_pane;
    let arrow = if pane.sort_descending { "↓" } else { "↑" };
    let marked_note = if pane.marked.is_empty() {
        String::new()
    } else {
        format!("{} marked", pane.marked.len())
    };
    let pipeline = app.selected_pipeline().map(|row| row.name.as_str());
    let connector_count: usize = tree.groups.iter().map(|group| group.connectors.len()).sum();
    let title = panel_title(
        "CONNECTORS",
        pipeline,
        Some(connector_count),
        &[
            marked_note,
            sort_label(pane.sort.title(), pane.sort_descending),
        ],
    );

    let (columns, name_width) = fit_columns(area.width, natural_name_width(tree));
    let mut hits = Vec::with_capacity(columns.len());
    let mut x = area.x + 1;
    for column in &columns {
        let width = column.width(name_width);
        hits.push(SortHit {
            x_start: x,
            x_end: x + width,
            sort: Some(column.sort()),
        });
        x += width + 1;
    }
    *app.viewport.connector_columns.borrow_mut() = hits;

    let selected = pane.cursor.as_ref().and_then(|node| tree.position(node));
    let viewport_height = area.height.saturating_sub(3) as usize;
    let offset = table_offset(
        selected,
        tree.rows.len(),
        viewport_height,
        app.viewport.connectors.get().0,
    );
    app.viewport.connectors.set((offset, viewport_height));
    let rows = tree
        .rows
        .iter()
        .enumerate()
        .skip(offset)
        .take(viewport_height)
        .map(|(index, row)| {
            tree_row(
                app,
                tree,
                row,
                &columns,
                name_width,
                Some(index) == selected,
            )
        });
    let header = columns.iter().map(|column| {
        if column.sort() == pane.sort {
            format!("{}{arrow}", column.title())
        } else {
            column.title().to_string()
        }
    });
    let table = Table::new(
        rows,
        columns
            .iter()
            .map(|column| Constraint::Length(column.width(name_width))),
    )
    .header(Row::new(header).style(theme::accent(theme::CYAN)))
    .block(panel(
        &title,
        // The tree dims while the keys scroll the detail pane.
        if pane.is_detail_focused {
            theme::MUTED
        } else {
            theme::CYAN
        },
    ))
    .column_spacing(1);
    frame.render_widget(table, area);
    scrollbar(frame, area, offset, tree.rows.len(), viewport_height);
}

/// The numbers one tree row shows.
struct RowFigures {
    status: ConnectorStatus,
    records_per_sec: f64,
    records: i64,
    backlog_records: i64,
    error_count: i64,
}

fn tree_row<'a>(
    app: &App,
    tree: &ConnectorTree,
    row: &TreeRow,
    columns: &[Column],
    name_width: u16,
    is_selected: bool,
) -> Row<'a> {
    let pane = &app.connector_pane;
    let group = &tree.groups[row.group];
    let connector = tree.connector_of(row);
    let marked_count = match connector {
        Some(connector) => usize::from(pane.marked.contains(&connector.endpoint_name)),
        None => group
            .connectors
            .iter()
            .filter(|connector| pane.marked.contains(&connector.endpoint_name))
            .count(),
    };
    let covered = if connector.is_some() {
        1
    } else {
        group.connectors.len()
    };
    let (mark, is_marked) = match marked_count {
        0 => (" ", false),
        count if count == covered => ("✓", true),
        _ => ("◦", false),
    };
    let figures = match connector {
        Some(connector) => RowFigures {
            status: connector.status(),
            records_per_sec: pane.records_per_sec(&connector.endpoint_name),
            records: connector.records,
            backlog_records: connector.backlog_records(),
            error_count: connector.error_count,
        },
        None => RowFigures {
            status: group.status(),
            records_per_sec: group.records_per_sec(pane),
            records: group.records(),
            backlog_records: group.backlog_records(),
            error_count: group.error_count(),
        },
    };
    let name = name_cell(app, group, connector, row, mark, name_width);
    let cells = columns.iter().map(|column| match column {
        Column::Name => name.clone(),
        Column::Status => Cell::from(figures.status.label()).style(status_style(figures.status)),
        Column::Rate => {
            Cell::from(rate(figures.records_per_sec)).style(Style::default().fg(theme::GREEN))
        }
        Column::Records => Cell::from(compact_number(figures.records)).style(theme::text()),
        Column::Backlog => Cell::from(compact_number(figures.backlog_records)).style(
            if figures.backlog_records > 0 {
                Style::default().fg(theme::YELLOW)
            } else {
                theme::muted()
            },
        ),
        Column::Errors => {
            Cell::from(figures.error_count.to_string()).style(error_style(figures.error_count))
        }
    });
    let style = if is_selected {
        let style = Style::default()
            .bg(theme::SELECTION)
            .add_modifier(Modifier::BOLD);
        if is_marked {
            style.fg(theme::YELLOW)
        } else {
            style
        }
    } else if is_marked {
        Style::default()
            .bg(ratatui::style::Color::Rgb(52, 46, 14))
            .fg(theme::YELLOW)
    } else {
        Style::default()
    };
    Row::new(cells.collect::<Vec<_>>()).style(style)
}

fn name_cell<'a>(
    app: &App,
    group: &RelationGroup,
    connector: Option<&ConnectorRow>,
    row: &TreeRow,
    mark: &'static str,
    name_width: u16,
) -> Cell<'a> {
    let mark = Span::styled(mark, theme::accent(theme::YELLOW));
    let room = |prefix: &str| (name_width as usize).saturating_sub(1 + prefix.chars().count());
    let spans = match connector {
        Some(connector) => {
            let branch = if row.is_last {
                "  └─ "
            } else {
                CONNECTOR_PREFIX
            };
            vec![
                mark,
                Span::styled(branch, theme::muted()),
                Span::styled(
                    truncate(connector.connector_name(), room(branch)),
                    theme::text(),
                ),
            ]
        }
        None => {
            let is_collapsed = app.connector_pane.collapsed.contains(&group.node());
            let fold = if is_collapsed {
                "▸ "
            } else {
                RELATION_PREFIX
            };
            let noun = group.kind.relation_noun();
            let name_room = room(fold).saturating_sub(1 + noun.len()).max(1);
            let name = truncate(group.name, name_room);
            vec![
                mark,
                Span::styled(fold, theme::accent(theme::CYAN)),
                Span::styled(name, theme::text().add_modifier(Modifier::BOLD)),
                Span::styled(format!(" {noun}"), theme::muted()),
            ]
        }
    };
    Cell::from(Line::from(spans))
}

fn status_style(status: ConnectorStatus) -> Style {
    match status {
        ConnectorStatus::Fatal => theme::accent(theme::RED),
        ConnectorStatus::Unhealthy => theme::accent(theme::ORANGE),
        ConnectorStatus::Paused => Style::default().fg(theme::YELLOW),
        ConnectorStatus::Eoi => Style::default().fg(theme::BLUE),
        ConnectorStatus::Active => theme::accent(theme::GREEN),
    }
}

fn error_style(error_count: i64) -> Style {
    if error_count > 0 {
        theme::accent(theme::RED)
    } else {
        theme::muted()
    }
}

fn render_detail(frame: &mut Frame, area: Rect, app: &App, tree: &ConnectorTree) {
    let width = area.width.saturating_sub(2) as usize;
    let pane = &app.connector_pane;
    let mut selected_block = None;
    let (title, lines) = match pane.cursor.as_ref() {
        Some(ConnectorNode::Endpoint(endpoint)) => match tree.find_connector(endpoint) {
            Some(connector) => {
                let (lines, block) = connector_lines(app, connector, width);
                selected_block = block;
                (connector.connector_name().to_string(), lines)
            }
            None => ("DETAILS".to_string(), Vec::new()),
        },
        Some(node) => match tree.group_of(node) {
            Some(group) => (
                format!("{} · {}", group.name, group.kind.relation_noun()),
                relation_lines(app, group, width),
            ),
            None => ("DETAILS".to_string(), Vec::new()),
        },
        None => ("DETAILS".to_string(), Vec::new()),
    };
    let visible_lines = area.height.saturating_sub(2) as usize;
    let previous_offset = app.viewport.connector_detail.get().0;
    let offset = if pane.is_detail_focused {
        detail_offset(previous_offset, selected_block, lines.len(), visible_lines)
    } else {
        0
    };
    app.viewport.connector_detail.set((offset, visible_lines));
    let accent = if pane.is_detail_focused {
        theme::CYAN
    } else {
        theme::BLUE
    };
    let line_count = lines.len();
    let block = panel(&title, accent);
    frame.render_widget(
        Paragraph::new(lines)
            .block(block)
            .scroll((offset.min(u16::MAX as usize) as u16, 0)),
        area,
    );
    // The bar runs between the rounded corners, not over them.
    let bar_area = Rect {
        y: area.y.saturating_add(1),
        height: visible_lines as u16,
        ..area
    };
    scrollbar(frame, bar_area, offset, line_count, visible_lines);
}

/// First visible line of the detail pane: keeps `previous` while the
/// selected error's lines `[start, end]` stay on screen, else scrolls just
/// enough to show them, its first line winning when they do not fit. A
/// selection within the first screenful shows from the top, so walking back
/// up to the newest errors brings the stats back.
pub(super) fn detail_offset(
    previous: usize,
    selected_block: Option<(usize, usize)>,
    line_count: usize,
    visible_lines: usize,
) -> usize {
    let max_offset = line_count.saturating_sub(visible_lines);
    let Some((start, end)) = selected_block else {
        return previous.min(max_offset);
    };
    if end < visible_lines {
        return 0;
    }
    let mut offset = previous;
    if end >= offset + visible_lines {
        offset = end + 1 - visible_lines.max(1);
    }
    offset.min(start).min(max_offset)
}

fn section(title: &str) -> Line<'static> {
    Line::from(Span::styled(
        format!(" {title}"),
        theme::accent(theme::MAGENTA),
    ))
}

/// `text` wrapped to `width`, each line indented and styled.
fn wrapped(text: &str, width: usize, indent: usize, style: Style) -> Vec<Line<'static>> {
    wrap_words(text, width.saturating_sub(indent).max(1))
        .into_iter()
        .map(|line| Line::from(Span::styled(format!("{}{line}", " ".repeat(indent)), style)))
        .collect()
}

fn ago(app: &App, at_millis: Option<i64>) -> String {
    match at_millis {
        Some(at_millis) if app.now_millis > at_millis => {
            format!("{} ago", uptime(app.now_millis - at_millis))
        }
        Some(_) => "just now".to_string(),
        None => "-".to_string(),
    }
}

/// The connector's lines, and the lines `[start, end]` of the selected
/// error while the error list has focus.
fn connector_lines(
    app: &App,
    connector: &ConnectorRow,
    width: usize,
) -> (Vec<Line<'static>>, Option<(usize, usize)>) {
    // `kv_line` pads labels to 15 cells.
    let value_width = width.saturating_sub(15);
    let fit = |text: String| truncate(&text, value_width);
    let live = app.connector_pane.live.get(&connector.endpoint_name);
    let records_per_sec = live.map_or(0.0, |live| live.records_per_sec);
    let bytes_per_sec = live.map_or(0.0, |live| live.bytes_per_sec);
    let spark: Vec<i64> = live
        .map(|live| live.spark.iter().copied().collect())
        .unwrap_or_default();
    let direction = match connector.kind {
        ConnectorKind::Input => "input",
        ConnectorKind::Output => "output",
    };
    let status = connector.status();
    let health = match &connector.health {
        Some(health) if health.is_healthy => " · healthy".to_string(),
        Some(health) => format!(
            " · {}",
            health.description.as_deref().unwrap_or("unhealthy")
        ),
        None => String::new(),
    };
    let mut lines = vec![
        kv_line(
            "relation",
            fit(format!(
                "{} · {} · {direction}",
                connector.relation,
                connector.kind.relation_noun()
            )),
            theme::text(),
        ),
        kv_line(
            "status",
            fit(format!("{}{health}", status.label())),
            status_style(status),
        ),
        kv_line(
            "throughput",
            fit(format!(
                "{} · {}/s",
                rate(records_per_sec),
                human_bytes(bytes_per_sec as i64)
            )),
            Style::default().fg(theme::GREEN),
        ),
        kv_line(
            "activity",
            sparkline(&spark, value_width),
            Style::default().fg(theme::MAGENTA),
        ),
    ];
    match connector.kind {
        ConnectorKind::Input => input_lines(app, connector, value_width, &mut lines),
        ConnectorKind::Output => output_lines(app, connector, value_width, &mut lines),
    }
    if let Some(fatal_error) = &connector.fatal_error {
        lines.push(Line::from(""));
        lines.push(section("FATAL ERROR"));
        lines.extend(wrapped(fatal_error, width, 2, theme::accent(theme::RED)));
    }
    let mut selected_block = None;
    if connector.error_count > 0 {
        let listed = app
            .connector_pane
            .listed_errors(&connector.endpoint_name)
            .len();
        lines.push(Line::from(""));
        lines.push(section(&format!(
            "RECENT ERRORS · {listed} kept of {}",
            connector.error_count
        )));
        let (error_lines, block) = recent_error_lines(app, connector, width);
        selected_block = block.map(|(start, end)| (lines.len() + start, lines.len() + end));
        lines.extend(error_lines);
    }
    (lines, selected_block)
}

fn input_lines(
    app: &App,
    connector: &ConnectorRow,
    value_width: usize,
    lines: &mut Vec<Line<'static>>,
) {
    let fit = |text: String| truncate(&text, value_width);
    lines.push(kv_line(
        "records",
        fit(format!(
            "{} · {}",
            compact_number(connector.records),
            human_bytes(connector.bytes)
        )),
        theme::text(),
    ));
    lines.push(kv_line(
        "buffered",
        fit(format!(
            "{} records · {}",
            compact_number(connector.buffered_records),
            human_bytes(connector.buffered_bytes)
        )),
        Style::default().fg(theme::YELLOW),
    ));
    lines.push(kv_line(
        "latency p99",
        connector
            .latency_p99_micros
            .map_or("-".to_string(), latency_micros),
        theme::text(),
    ));
    lines.push(kv_line(
        "errors",
        fit(format!(
            "parse {} · transport {}",
            connector.parse_errors, connector.transport_errors
        )),
        error_style(connector.error_count),
    ));
    lines.push(kv_line(
        "barrier",
        if connector.barrier {
            "yes, holds back checkpoints".to_string()
        } else {
            "no".to_string()
        },
        if connector.barrier {
            Style::default().fg(theme::YELLOW)
        } else {
            theme::muted()
        },
    ));
    if let Some(frontier) = &connector.frontier {
        frontier_lines(app, frontier, value_width, lines);
    }
}

/// When the newest fully processed input completed, how long it took from
/// ingest to completion, and where in the source it sits.
fn frontier_lines(
    app: &App,
    frontier: &Frontier,
    value_width: usize,
    lines: &mut Vec<Line<'static>>,
) {
    let end_to_end = match (frontier.ingested_at_millis, frontier.completed_at_millis) {
        (Some(ingested), Some(completed)) => {
            format!(
                " · {} end to end",
                latency_micros((completed - ingested) * 1_000)
            )
        }
        _ => String::new(),
    };
    lines.push(kv_line(
        "frontier",
        truncate(
            &format!(
                "completed {}{end_to_end}",
                ago(app, frontier.completed_at_millis)
            ),
            value_width,
        ),
        theme::text(),
    ));
    if !frontier.metadata.is_null() {
        let metadata = serde_json::to_string(&frontier.metadata).unwrap_or_default();
        lines.push(kv_line(
            "position",
            truncate(&crate::model::text::terminal_safe(&metadata), value_width),
            theme::muted(),
        ));
    }
}

fn output_lines(
    app: &App,
    connector: &ConnectorRow,
    value_width: usize,
    lines: &mut Vec<Line<'static>>,
) {
    let fit = |text: String| truncate(&text, value_width);
    let plural = |count: i64, noun: &str| {
        format!(
            "{} {noun}{}",
            compact_number(count),
            if count == 1 { "" } else { "es" }
        )
    };
    lines.push(kv_line(
        "transmitted",
        fit(format!(
            "{} records · {}",
            compact_number(connector.records),
            human_bytes(connector.bytes)
        )),
        theme::text(),
    ));
    lines.push(kv_line(
        "queued",
        fit(format!(
            "{} records · {}",
            compact_number(connector.queued_records),
            plural(connector.queued_batches, "batch")
        )),
        Style::default().fg(theme::YELLOW),
    ));
    lines.push(kv_line(
        "buffered",
        fit(format!(
            "{} records · {}",
            compact_number(connector.buffered_records),
            plural(connector.buffered_batches, "batch")
        )),
        Style::default().fg(theme::YELLOW),
    ));
    lines.push(kv_line(
        "memory",
        human_bytes(connector.memory_bytes),
        Style::default().fg(theme::BLUE),
    ));
    // The endpoint has handled the output of this many pipeline input
    // records; the gap to the circuit's count is how far it trails.
    let processed = app
        .detail
        .as_ref()
        .map_or(0, |stats| stats.total_processed_records);
    let lag = processed
        .saturating_sub(connector.processed_input_records)
        .max(0);
    lines.push(kv_line(
        "progress",
        fit(format!(
            "{} of {} inputs · lag {}",
            compact_number(connector.processed_input_records),
            compact_number(processed),
            compact_number(lag)
        )),
        if lag > 0 {
            Style::default().fg(theme::YELLOW)
        } else {
            theme::text()
        },
    ));
    if let Some(written) = connector.batch_records_written {
        lines.push(kv_line(
            "open batch",
            fit(format!("{} records written", compact_number(written))),
            theme::text(),
        ));
    }
    lines.push(kv_line(
        "errors",
        fit(format!(
            "encode {} · transport {}",
            connector.encode_errors, connector.transport_errors
        )),
        error_style(connector.error_count),
    ));
}

/// One line per error, newest first: age, kind, and the start of the
/// message. While the list has focus, the selected error expands to its
/// full message; its lines come back as `[start, end]`.
fn recent_error_lines(
    app: &App,
    connector: &ConnectorRow,
    width: usize,
) -> (Vec<Line<'static>>, Option<(usize, usize)>) {
    let pane = &app.connector_pane;
    let listed = pane
        .recent_errors
        .as_ref()
        .filter(|listed| listed.endpoint_name == connector.endpoint_name);
    let note = |text: &str| {
        vec![Line::from(Span::styled(
            format!("  {text}"),
            theme::muted(),
        ))]
    };
    match listed.map(|listed| &listed.result) {
        Some(Ok(errors)) if errors.is_empty() => {
            return (note("the server kept no messages"), None);
        }
        Some(Ok(_)) => {}
        Some(Err(error)) => {
            let lines = wrapped(&format!("unavailable: {error}"), width, 2, theme::muted());
            return (lines, None);
        }
        None => return (note("loading…"), None),
    }
    let errors = pane.listed_errors(&connector.endpoint_name);
    let selected = pane
        .is_detail_focused
        .then(|| pane.selected_error_position(errors))
        .flatten();
    let mut lines = Vec::with_capacity(errors.len());
    let mut selected_block = None;
    for (position, error) in errors.iter().enumerate() {
        let prefix = format!(
            "  {:<11} {:<9} ",
            ago(app, error.at_millis),
            error.kind.label()
        );
        if Some(position) != selected {
            let first_line = error.message.lines().next().unwrap_or_default();
            let room = width.saturating_sub(prefix.chars().count());
            lines.push(Line::from(vec![
                Span::styled(prefix, Style::default().fg(theme::RED)),
                Span::styled(truncate(first_line, room), theme::text()),
            ]));
            continue;
        }
        // The whole message wraps in the message column, highlighted.
        let start = lines.len();
        let indent = prefix.chars().count();
        let message_width = width.saturating_sub(indent).max(1);
        for (line_index, text) in wrap_words(&error.message, message_width)
            .into_iter()
            .enumerate()
        {
            let lead = if line_index == 0 {
                Span::styled(prefix.clone(), theme::accent(theme::RED))
            } else {
                Span::raw(" ".repeat(indent))
            };
            lines.push(
                Line::from(vec![lead, Span::styled(text, theme::text())])
                    .style(Style::default().bg(theme::SELECTION)),
            );
        }
        selected_block = Some((start, lines.len() - 1));
    }
    (lines, selected_block)
}

fn relation_lines(app: &App, group: &RelationGroup, width: usize) -> Vec<Line<'static>> {
    let value_width = width.saturating_sub(15);
    let fit = |text: String| truncate(&text, value_width);
    let direction = match group.kind {
        ConnectorKind::Input => "input",
        ConnectorKind::Output => "output",
    };
    let count = group.connectors.len();
    let counts = group
        .status_counts()
        .iter()
        .map(|(status, count)| format!("{count} {}", status.label()))
        .collect::<Vec<_>>()
        .join(" · ");
    vec![
        kv_line(
            "connectors",
            fit(format!(
                "{count} {direction} connector{}",
                if count == 1 { "" } else { "s" }
            )),
            theme::text(),
        ),
        kv_line("status", fit(counts), status_style(group.status())),
        kv_line(
            "throughput",
            rate(group.records_per_sec(&app.connector_pane)),
            Style::default().fg(theme::GREEN),
        ),
        kv_line(
            "records",
            fit(format!(
                "{} · {}",
                compact_number(group.records()),
                human_bytes(group.bytes())
            )),
            theme::text(),
        ),
        kv_line(
            "buffered",
            compact_number(group.backlog_records()),
            Style::default().fg(theme::YELLOW),
        ),
        kv_line(
            "errors",
            group.error_count().to_string(),
            error_style(group.error_count()),
        ),
        Line::from(""),
        Line::from(Span::styled(
            truncate(
                "  space marks all · s/x start or pause all · ←/→ fold",
                width,
            ),
            theme::muted(),
        )),
    ]
}

#[cfg(test)]
mod tests {
    use super::{Column, fit_columns, table_width};

    #[test]
    fn wide_areas_keep_every_column_and_full_names() {
        let (columns, name_width) = fit_columns(120, 30);
        assert_eq!(columns, Column::ALL.to_vec());
        assert_eq!(name_width, 30);
    }

    #[test]
    fn narrow_areas_drop_optional_columns_before_squeezing_names() {
        let (columns, name_width) = fit_columns(60, 40);
        assert!(!columns.contains(&Column::Backlog));
        assert!(columns.contains(&Column::Status) && columns.contains(&Column::Errors));
        assert!(name_width >= 24);
        assert!(table_width(&columns, name_width) <= 60);
        let (columns, name_width) = fit_columns(20, 40);
        assert_eq!(
            columns,
            vec![Column::Name, Column::Status, Column::Errors],
            "the core columns stay"
        );
        assert_eq!(name_width, super::MIN_NAME_WIDTH);
    }
}
