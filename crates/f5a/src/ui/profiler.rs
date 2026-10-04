//! The hotspot ranking joined with SQL positions, beside a detail pane for
//! the selected operator.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table};

use crate::app::App;
use crate::app::state::SortHit;
use crate::model::dataflow::{CostMetric, Hotspot, HotspotReport, HotspotSort};
use crate::model::profile::{MetricInfo, MetricValue};

use super::format::{compact_number, human_bytes, sparkline, truncate};
use super::theme;
use super::widgets::{
    bar, centered_rect, kv_line, panel, panel_title, scrollbar, sort_label, split_beside,
    table_offset,
};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    // The pipeline the report describes, else the one a profile would.
    let pipeline = app
        .report_pipeline
        .as_deref()
        .or_else(|| app.selected_pipeline().map(|row| row.name.as_str()));
    let count = app.report.as_ref().map(|report| report.hotspots.len());
    let extras = if count.is_some() {
        vec![
            format!("metric:{}", app.metric.label().to_lowercase()),
            sort_label(
                app.hotspot_order.column.title(),
                app.hotspot_order.is_descending,
            ),
        ]
    } else {
        Vec::new()
    };
    let title = panel_title("HOTSPOTS", pipeline, count, &extras);
    let report = match &app.report {
        Some(report) if !report.is_empty() => report,
        report => {
            let message = if report.is_some() {
                "The profile has no cost data; let the pipeline process data and press f again."
            } else if app.loading_report {
                "Profiling the circuit… this takes a few seconds."
            } else {
                "Press f (or :profile) on a running pipeline to rank its operators by cost."
            };
            app.viewport.hotspot_table_right.set(None);
            render_message(frame, area, &title, message);
            return;
        }
    };
    // The circuit-wide summary spans both panes so it never gets cut.
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(1)])
        .split(area);
    frame.render_widget(
        Paragraph::new(Span::styled(
            truncate(&format!(" {}", footer(app, report)), area.width as usize),
            theme::muted(),
        )),
        parts[1],
    );
    let order = app.ranked_hotspots();
    let (table_area, detail_area) = split_beside(parts[0], natural_table_width(report));
    let is_beside = detail_area.is_some_and(|detail| detail.x > table_area.x);
    app.viewport
        .hotspot_table_right
        .set(is_beside.then(|| table_area.right()));
    render_table(frame, table_area, app, report, &order, &title);
    let selected = order
        .get(app.selected_hotspot)
        .map(|index| &report.hotspots[*index]);
    match (detail_area, selected) {
        (Some(detail_area), Some(hotspot)) => {
            render_detail(frame, detail_area, app, report, hotspot);
        }
        // No pane, nothing for `→` to move to.
        _ => app.viewport.hotspot_detail.set((0, 0)),
    }
}

fn render_message(frame: &mut Frame, area: Rect, title: &str, message: &str) {
    frame.render_widget(panel(title, theme::MAGENTA), area);
    let inner = centered_rect(area.width.saturating_sub(6).min(80), 1, area);
    frame.render_widget(
        Paragraph::new(Span::styled(message, theme::accent(theme::YELLOW)))
            .alignment(Alignment::Center),
        inner,
    );
}

/// Table columns, in display order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Column {
    Rank,
    Share,
    Percent,
    Cost,
    Skew,
    Operator,
    Relation,
    Line,
}

impl Column {
    const ALL: [Self; 8] = [
        Self::Rank,
        Self::Share,
        Self::Percent,
        Self::Cost,
        Self::Skew,
        Self::Operator,
        Self::Relation,
        Self::Line,
    ];

    const fn title(self) -> &'static str {
        match self {
            Self::Rank => "#",
            Self::Share => "COST SHARE",
            Self::Percent => "%",
            Self::Cost => "COST",
            Self::Skew => "SKEW",
            Self::Operator => "OPERATOR",
            Self::Relation => "RELATION",
            Self::Line => "SQL",
        }
    }

    const fn sort(self) -> Option<HotspotSort> {
        match self {
            Self::Rank => None,
            Self::Share | Self::Percent | Self::Cost => Some(HotspotSort::Cost),
            Self::Skew => Some(HotspotSort::Skew),
            Self::Operator => Some(HotspotSort::Operator),
            Self::Relation => Some(HotspotSort::Relation),
            Self::Line => Some(HotspotSort::Line),
        }
    }

    /// Fixed widths; the operator and relation columns size to their text.
    const fn fixed_width(self) -> u16 {
        match self {
            Self::Rank => 4,
            Self::Share => 10,
            Self::Percent => 6,
            Self::Cost => 10,
            Self::Skew => 6,
            Self::Line => 6,
            Self::Operator | Self::Relation => 0,
        }
    }
}

const MIN_TEXT_WIDTH: u16 = 12;
const MAX_TEXT_WIDTH: u16 = 48;

/// Borders, fixed cells, and one spacing cell between each pair of columns.
fn chrome_width() -> u16 {
    let fixed: u16 = Column::ALL.iter().map(|column| column.fixed_width()).sum();
    fixed + Column::ALL.len() as u16 - 1 + 2
}

/// Widest value of a text column, header included, within the bounds.
fn natural_width<'a>(title: &str, values: impl Iterator<Item = &'a str>) -> u16 {
    values
        .map(|value| value.chars().count() + 1)
        .chain([title.len() + 1])
        .max()
        .unwrap_or(0)
        .clamp(MIN_TEXT_WIDTH as usize, MAX_TEXT_WIDTH as usize) as u16
}

/// Natural widths of the operator and relation columns; a borrowed
/// relation carries its `~` mark.
fn natural_text_widths(report: &HotspotReport) -> (u16, u16) {
    let relations: Vec<String> = report.hotspots.iter().map(relation_label).collect();
    (
        natural_width(
            Column::Operator.title(),
            report
                .hotspots
                .iter()
                .map(|hotspot| hotspot.operator.as_str()),
        ),
        natural_width(
            Column::Relation.title(),
            relations.iter().map(String::as_str),
        ),
    )
}

/// Width that shows every operator and relation in full.
fn natural_table_width(report: &HotspotReport) -> u16 {
    let (operator, relation) = natural_text_widths(report);
    chrome_width() + operator + relation
}

/// Split `available` cells between the operator and relation columns: both
/// in full when they fit, else the shorter keeps what it needs up to half.
///
/// ```text
/// fit_text(60, 20, 30) == (20, 30)    fit_text(40, 10, 50) == (10, 30)
/// ```
fn fit_text(available: u16, operator: u16, relation: u16) -> (u16, u16) {
    let available = available.max(2 * MIN_TEXT_WIDTH);
    if operator + relation <= available {
        return (operator, relation);
    }
    let half = available / 2;
    if relation <= half {
        (available - relation, relation)
    } else if operator <= half {
        (operator, available - operator)
    } else {
        (available - half, half)
    }
}

fn render_table(
    frame: &mut Frame,
    area: Rect,
    app: &App,
    report: &HotspotReport,
    order: &[usize],
    title: &str,
) {
    let (operator_natural, relation_natural) = natural_text_widths(report);
    let (operator_width, relation_width) = fit_text(
        area.width.saturating_sub(chrome_width()),
        operator_natural,
        relation_natural,
    );
    let width_of = |column: Column| match column {
        Column::Operator => operator_width,
        Column::Relation => relation_width,
        fixed => fixed.fixed_width(),
    };

    let mut hits = Vec::with_capacity(Column::ALL.len());
    let mut x = area.x + 1;
    for column in Column::ALL {
        let width = width_of(column);
        hits.push(SortHit {
            x_start: x,
            x_end: x + width,
            sort: column.sort(),
        });
        x += width + 1;
    }
    *app.viewport.hotspot_columns.borrow_mut() = hits;

    let viewport_height = area.height.saturating_sub(3) as usize;
    let offset = table_offset(
        Some(app.selected_hotspot),
        order.len(),
        viewport_height,
        app.viewport.hotspots.get().0,
    );
    app.viewport.hotspots.set((offset, viewport_height));
    let total_cost = report.total_cost(app.metric).max(f64::EPSILON);
    let rows = order
        .iter()
        .enumerate()
        .skip(offset)
        .take(viewport_height)
        .map(|(rank, index)| {
            let hotspot = &report.hotspots[*index];
            let share = hotspot.cost(app.metric) / total_cost;
            let style = if rank == app.selected_hotspot {
                Style::default()
                    .bg(theme::SELECTION)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(format!("{:>3}", rank + 1)).style(theme::muted()),
                Cell::from(bar(share, Column::Share.fixed_width() as usize))
                    .style(Style::default().fg(theme::heat_color(share))),
                Cell::from(format!("{:>5.1}%", share * 100.0))
                    .style(Style::default().fg(theme::heat_color(share))),
                Cell::from(cost_label(app.metric, hotspot.cost(app.metric))).style(theme::text()),
                skew_cell(hotspot.skew(app.metric)),
                Cell::from(truncate(&hotspot.operator, operator_width as usize))
                    .style(theme::accent(theme::CYAN)),
                Cell::from(truncate(&relation_label(hotspot), relation_width as usize)).style(
                    if hotspot.inferred_from.is_some() {
                        theme::muted()
                    } else {
                        theme::text()
                    },
                ),
                Cell::from(sql_reference(hotspot)).style(if hotspot.inferred_from.is_some() {
                    Style::default().fg(theme::GREEN)
                } else {
                    theme::accent(theme::GREEN)
                }),
            ])
            .style(style)
        });

    let arrow = if app.hotspot_order.is_descending {
        "↓"
    } else {
        "↑"
    };
    let header = Column::ALL.map(|column| {
        // One arrow, on the column that names the sort best.
        let is_sorted = column.sort() == Some(app.hotspot_order.column)
            && !matches!(column, Column::Share | Column::Percent);
        if is_sorted {
            format!("{}{arrow}", column.title())
        } else {
            column.title().to_string()
        }
    });
    let table = Table::new(
        rows,
        Column::ALL.map(|column| Constraint::Length(width_of(column))),
    )
    .header(Row::new(header).style(theme::accent(theme::MAGENTA)))
    .block(panel(
        title,
        // The table dims while the keys scroll the detail pane.
        if app.is_hotspot_detail_focused {
            theme::MUTED
        } else {
            theme::MAGENTA
        },
    ))
    .column_spacing(1);
    frame.render_widget(table, area);
    scrollbar(frame, area, offset, order.len(), viewport_height);
}

fn cost_label(metric: CostMetric, cost: f64) -> String {
    match metric {
        CostMetric::Time => format!("{cost:.2}s"),
        CostMetric::Memory | CostMetric::Storage => human_bytes(cost as i64),
        CostMetric::Records => compact_number(cost as i64),
    }
}

/// `~` marks a relation or line borrowed from a neighboring operator.
fn inferred_mark(hotspot: &Hotspot) -> &'static str {
    if hotspot.inferred_from.is_some() {
        "~"
    } else {
        ""
    }
}

fn relation_label(hotspot: &Hotspot) -> String {
    match &hotspot.relation {
        Some(relation) => format!("{}{relation}", inferred_mark(hotspot)),
        None => "-".to_string(),
    }
}

fn sql_reference(hotspot: &Hotspot) -> String {
    hotspot.first_line().map_or_else(
        || "-".to_string(),
        |line| format!("{}L{line}", inferred_mark(hotspot)),
    )
}

/// The busiest worker over the mean, colored as it departs from balance.
fn skew_cell(skew: Option<f64>) -> Cell<'static> {
    let Some(skew) = skew else {
        return Cell::from("-").style(theme::muted());
    };
    let style = if skew >= 4.0 {
        theme::accent(theme::RED)
    } else if skew >= 2.0 {
        Style::default().fg(theme::ORANGE)
    } else if skew >= 1.3 {
        Style::default().fg(theme::YELLOW)
    } else {
        theme::muted()
    };
    Cell::from(format!("{skew:>4.1}×")).style(style)
}

/// What the whole circuit spent under the current metric. For memory it
/// sets the operators' state beside what the profile reports outside them
/// and the process RSS, which also holds allocator and input buffers.
fn footer(app: &App, report: &HotspotReport) -> String {
    let operators = report.hotspots.len();
    match app.metric {
        CostMetric::Time => format!(
            "{operators} operators · {} workers · {:.1}s operator time · {} mapped to SQL, {} ~inferred",
            report.worker_count,
            report.total_time_seconds,
            report.mapped_count,
            report.inferred_count
        ),
        CostMetric::Memory => {
            let memory = report.memory;
            let mut parts = vec![
                format!(
                    "operator state {}",
                    human_bytes(report.total_cost(CostMetric::Memory) as i64)
                ),
                format!("bloom filters {}", human_bytes(memory.bloom_filter_bytes)),
                format!(
                    "buffer cache {} of {}",
                    human_bytes(memory.buffer_cache_bytes),
                    human_bytes(memory.buffer_cache_capacity_bytes)
                ),
            ];
            let rss = app
                .report_pipeline
                .as_deref()
                .and_then(|pipeline| app.live_of(pipeline))
                .map(|live| live.rss_bytes)
                .filter(|rss| *rss > 0);
            if let Some(rss) = rss {
                parts.push(format!("RSS {}", human_bytes(rss)));
            }
            parts.join(" · ")
        }
        CostMetric::Storage => {
            let mut summary = format!(
                "{operators} operators · spines hold {}",
                human_bytes(report.total_cost(CostMetric::Storage) as i64)
            );
            let pipeline_storage = app
                .report_pipeline
                .as_deref()
                .and_then(|pipeline| app.live_of(pipeline))
                .map(|live| live.storage_bytes)
                .filter(|bytes| *bytes > 0);
            if let Some(bytes) = pipeline_storage {
                summary.push_str(&format!(" · pipeline storage {}", human_bytes(bytes)));
            }
            summary
        }
        CostMetric::Records => format!(
            "{operators} operators · {} state records",
            compact_number(report.total_cost(CostMetric::Records) as i64)
        ),
    }
}

/// Readings the summary lines at the top of the pane already show.
const SUMMARIZED_METRICS: [&str; 6] = [
    "runtime_seconds",
    "used_memory_bytes",
    "spine_storage_size_bytes",
    "state_records_count",
    "invocations_count",
    "persistent_id",
];

fn render_detail(
    frame: &mut Frame,
    area: Rect,
    app: &App,
    report: &HotspotReport,
    hotspot: &Hotspot,
) {
    let width = area.width.saturating_sub(2) as usize;
    let value_width = width.saturating_sub(15);
    let fit = |text: String| truncate(&text, value_width);
    let share = |metric: CostMetric| {
        let total = report.total_cost(metric);
        if total > 0.0 {
            format!(" · {:.1}%", hotspot.cost(metric) * 100.0 / total)
        } else {
            String::new()
        }
    };
    let relation = match (&hotspot.relation, hotspot.first_line()) {
        (Some(relation), Some(line)) => format!("{relation} · L{line}"),
        (Some(relation), None) => relation.clone(),
        (None, Some(line)) => format!("L{line}"),
        (None, None) => "- (not mapped to SQL)".to_string(),
    };
    let relation = format!("{}{relation}", inferred_mark(hotspot));
    let mut lines = vec![kv_line("relation", fit(relation), theme::text())];
    if let Some(owner) = &hotspot.inferred_from {
        lines.push(kv_line(
            "inferred",
            fit(format!("from neighbor {owner}")),
            theme::muted(),
        ));
    }
    lines.extend([
        kv_line(
            "node",
            fit(match &hotspot.persistent_id {
                Some(id) => format!("{} · {id}", hotspot.node_id),
                None => hotspot.node_id.clone(),
            }),
            theme::muted(),
        ),
        kv_line(
            "time",
            fit(format!(
                "{:.2}s{}",
                hotspot.time_seconds,
                share(CostMetric::Time)
            )),
            theme::text(),
        ),
        kv_line(
            "memory",
            fit(format!(
                "{}{}",
                human_bytes(hotspot.memory_bytes),
                share(CostMetric::Memory)
            )),
            theme::text(),
        ),
        kv_line(
            "storage",
            fit(format!(
                "{}{}",
                human_bytes(hotspot.storage_bytes),
                share(CostMetric::Storage)
            )),
            theme::text(),
        ),
        kv_line(
            "state",
            fit(format!(
                "{} records{}",
                compact_number(hotspot.state_records),
                share(CostMetric::Records)
            )),
            theme::text(),
        ),
        kv_line(
            "invocations",
            compact_number(hotspot.invocations),
            theme::text(),
        ),
        Line::from(""),
    ]);
    lines.extend(worker_lines(app.metric, hotspot, width));
    lines.extend(metric_lines(&report.catalog, hotspot, width));
    // The renderer knows the height; the keys clamp against it.
    let visible_lines = area.height.saturating_sub(2) as usize;
    let max_scroll = lines.len().saturating_sub(visible_lines);
    app.viewport.hotspot_detail.set((max_scroll, visible_lines));
    let scroll = app.hotspot_detail_scroll.min(max_scroll);
    let accent = if app.is_hotspot_detail_focused {
        theme::CYAN
    } else {
        theme::BLUE
    };
    let line_count = lines.len();
    frame.render_widget(
        Paragraph::new(lines)
            .block(panel(&hotspot.operator, accent))
            .scroll((scroll.min(u16::MAX as usize) as u16, 0)),
        area,
    );
    // The bar runs between the rounded corners, not over them.
    let bar_area = Rect {
        y: area.y.saturating_add(1),
        height: visible_lines as u16,
        ..area
    };
    scrollbar(frame, bar_area, scroll, line_count, visible_lines);
}

/// One bar per worker under the current metric, and how far the busiest
/// worker departs from the mean.
fn worker_lines(metric: CostMetric, hotspot: &Hotspot, width: usize) -> Vec<Line<'static>> {
    let costs = hotspot.worker_costs(metric);
    let mut lines = vec![section(&format!(
        "WORKERS · {} per worker",
        metric.label().to_lowercase()
    ))];
    let Some(skew) = hotspot.skew(metric) else {
        lines.push(Line::from(Span::styled(
            "  nothing spent on any worker",
            theme::muted(),
        )));
        return lines;
    };
    // Integer bars; milliseconds keep sub-second times apart.
    let scale = if metric == CostMetric::Time { 1e3 } else { 1.0 };
    let bars: Vec<i64> = costs.iter().map(|cost| (cost * scale) as i64).collect();
    // A zero floor keeps idle workers at the bottom of the scale.
    let mut with_floor = bars.clone();
    with_floor.push(0);
    let chart = sparkline(&with_floor, with_floor.len());
    let chart: String = chart.chars().take(bars.len()).collect();
    for row in wrap_chars(&chart, width.saturating_sub(4)) {
        lines.push(Line::from(Span::styled(
            format!("  {row}"),
            Style::default().fg(theme::MAGENTA),
        )));
    }
    let (busiest, _) = costs
        .iter()
        .enumerate()
        .fold((0, f64::MIN), |best, (index, cost)| {
            if *cost > best.1 { (index, *cost) } else { best }
        });
    let idle = costs.iter().filter(|cost| **cost <= 0.0).count();
    let mut summary = format!("  busiest worker {busiest} · {skew:.1}× the mean");
    if idle > 0 {
        summary.push_str(&format!(" · {idle} idle"));
    }
    lines.push(Line::from(Span::styled(
        truncate(&summary, width),
        theme::muted(),
    )));
    lines
}

/// `text` cut into rows of at most `width` characters.
fn wrap_chars(text: &str, width: usize) -> Vec<String> {
    let characters: Vec<char> = text.chars().collect();
    characters
        .chunks(width.max(1))
        .map(|chunk| chunk.iter().collect())
        .collect()
}

fn section(title: &str) -> Line<'static> {
    Line::from(Span::styled(
        format!(" {title}"),
        theme::accent(theme::MAGENTA),
    ))
}

/// Every other reading of the operator, grouped by the profile's own
/// categories in its own order; readings it does not describe come last.
fn metric_lines(catalog: &[MetricInfo], hotspot: &Hotspot, width: usize) -> Vec<Line<'static>> {
    let shown = |name: &str| {
        !SUMMARIZED_METRICS.contains(&name)
            && hotspot
                .metrics
                .get(name)
                .is_some_and(|value| !matches!(value, MetricValue::Other(_)))
    };
    let mut grouped: Vec<(String, Vec<(String, bool)>)> = Vec::new();
    let mut push = |category: &str, name: &str, is_advanced: bool| match grouped
        .iter_mut()
        .find(|(seen, _)| seen == category)
    {
        Some((_, names)) => names.push((name.to_string(), is_advanced)),
        None => grouped.push((category.to_string(), vec![(name.to_string(), is_advanced)])),
    };
    for info in catalog.iter().filter(|info| shown(&info.name)) {
        push(&info.category, &info.name, info.is_advanced);
    }
    for name in hotspot.metrics.keys().filter(|name| shown(name)) {
        if !catalog.iter().any(|info| &info.name == name) {
            push("Other", name, false);
        }
    }
    let mut lines = Vec::new();
    for (category, names) in grouped {
        lines.push(Line::from(""));
        lines.push(section(&category.to_uppercase()));
        for (name, is_advanced) in names {
            let value = hotspot.metrics[&name].display();
            let label_style = if is_advanced {
                theme::muted()
            } else {
                theme::text()
            };
            let label = truncate(&metric_label(&name), 24);
            lines.push(Line::from(vec![
                Span::styled(format!("  {label:<25}"), label_style),
                Span::styled(
                    truncate(&value, width.saturating_sub(27)),
                    theme::accent(theme::CYAN),
                ),
            ]));
        }
    }
    lines
}

/// `allocated_memory_bytes` reads as `allocated memory`: the value carries
/// the unit.
fn metric_label(name: &str) -> String {
    let base = ["_bytes", "_count", "_seconds", "_percent"]
        .iter()
        .find_map(|suffix| name.strip_suffix(suffix))
        .unwrap_or(name);
    base.replace('_', " ")
}

#[cfg(test)]
mod tests {
    use super::{metric_label, wrap_chars};

    #[test]
    fn metric_labels_drop_the_unit_the_value_carries() {
        assert_eq!(metric_label("allocated_memory_bytes"), "allocated memory");
        assert_eq!(
            metric_label("bloom_filter_hit_rate_percent"),
            "bloom filter hit rate"
        );
        assert_eq!(metric_label("steps"), "steps");
    }

    #[test]
    fn text_columns_share_the_width_the_shorter_one_first() {
        use super::fit_text;
        assert_eq!(fit_text(60, 20, 30), (20, 30), "both fit");
        assert_eq!(
            fit_text(40, 10, 50),
            (10, 30),
            "the short operator stays whole"
        );
        assert_eq!(fit_text(40, 50, 10), (30, 10));
        assert_eq!(fit_text(40, 50, 50), (20, 20));
        assert_eq!(fit_text(5, 50, 50), (12, 12), "never below the minimum");
    }

    #[test]
    fn character_rows_split_at_the_width() {
        assert_eq!(wrap_chars("abcde", 2), vec!["ab", "cd", "e"]);
        assert!(wrap_chars("", 3).is_empty());
    }
}
