//! Application state. Nothing here touches the network or the terminal.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};

use crate::gateway::{Action, BundleOptions, DownloadKind};
use crate::http::ApiError;
use crate::model::dataflow::{CostMetric, HotspotReport};
use crate::model::diagnostics::{DiagnosticSection, Severity};
use crate::model::header::{InstanceInfo, Session};
use crate::model::pipeline::PipelineRow;
use crate::model::stats::PipelineStats;
use crate::model::timeseries::MetricHistory;

use super::capture::Capture;

/// The main content panes, in tab order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum View {
    #[default]
    Pipelines,
    Metrics,
    Connectors,
    Profiler,
    Sql,
    Logs,
}

impl View {
    pub const ALL: [Self; 6] = [
        Self::Pipelines,
        Self::Metrics,
        Self::Connectors,
        Self::Profiler,
        Self::Sql,
        Self::Logs,
    ];

    pub const fn title(self) -> &'static str {
        match self {
            Self::Pipelines => "Pipelines",
            Self::Metrics => "Metrics",
            Self::Connectors => "Connectors",
            Self::Profiler => "Hotspots",
            Self::Sql => "SQL",
            Self::Logs => "Logs",
        }
    }

    pub fn next(self, backwards: bool) -> Self {
        let position = Self::ALL
            .iter()
            .position(|view| *view == self)
            .expect("view is in the tab list");
        let count = Self::ALL.len();
        let next = if backwards {
            (position + count - 1) % count
        } else {
            (position + 1) % count
        };
        Self::ALL[next]
    }

    pub fn from_digit(digit: char) -> Option<Self> {
        let index = digit.to_digit(10)? as usize;
        (1..=Self::ALL.len())
            .contains(&index)
            .then(|| Self::ALL[index - 1])
    }
}

/// Modal surfaces stacked over the active view.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum Overlay {
    #[default]
    None,
    Help,
    /// Confirm one action, or a whole batch from a multi-selection.
    Confirm {
        actions: Vec<Action>,
    },
    Tenants {
        selected: usize,
    },
    /// The support bundle dialog; `cursor` indexes
    /// [`BundleSetting::ALL`](super::bundle_settings::BundleSetting::ALL).
    BundleSettings {
        cursor: usize,
    },
    /// Raw JSON of the selected connector.
    ConnectorInspect {
        scroll: u16,
    },
    /// Long-form text, e.g. a deployment error.
    TextViewer {
        title: String,
        body: String,
        scroll: u16,
    },
}

/// The `:` command bar and the `/` filter prompt.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Prompt {
    #[default]
    Closed,
    Command {
        input: String,
        history_index: Option<usize>,
    },
    Filter {
        input: String,
    },
}

/// Severity of the transient status message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToastLevel {
    Info,
    Success,
    Error,
}

/// A transient status message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Toast {
    pub message: String,
    pub level: ToastLevel,
    pub expires_at_millis: i64,
}

/// Reachability of the instance, shown in the header.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Connection {
    #[default]
    Connecting,
    Online {
        latency_millis: u64,
    },
    Lost {
        error: String,
        since_millis: i64,
    },
}

/// Live per-row enrichment sampled from each running pipeline's `/stats`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowLive {
    pub records: i64,
    pub buffered_records: i64,
    pub rss_bytes: i64,
    pub storage_bytes: i64,
    pub uptime_millis: i64,
    pub throughput: f64,
    /// Current memory pressure level reported by the engine.
    pub memory_pressure: String,
    /// Recent throughput samples for the row sparkline.
    pub spark: VecDeque<i64>,
    last_sample: Option<(i64, i64)>,
}

impl RowLive {
    const SPARK_CAPACITY: usize = 30;

    /// Fold in a stats sample, deriving throughput from the previous one.
    pub fn observe(&mut self, stats: &PipelineStats, now_millis: i64) {
        self.records = stats.total_processed_records;
        self.buffered_records = stats.buffered_input_records;
        self.rss_bytes = stats.rss_bytes;
        self.storage_bytes = stats.storage_bytes;
        self.uptime_millis = stats.uptime_msecs;
        self.memory_pressure = stats.memory_pressure.clone();
        if let Some((last_millis, last_records)) = self.last_sample {
            let gap_millis = now_millis.saturating_sub(last_millis);
            // The detail poller and the row poller can both sample the same
            // pipeline around a watch switch; a near-zero gap would fabricate
            // a bogus rate, so such samples only refresh the counters above.
            if gap_millis < 300 {
                return;
            }
            let records = stats.total_processed_records.saturating_sub(last_records);
            // A restart resets counters; show zero rather than a negative rate.
            self.throughput = (records.max(0) as f64 * 1_000.0 / gap_millis as f64).max(0.0);
            if self.spark.len() == Self::SPARK_CAPACITY {
                self.spark.pop_front();
            }
            self.spark.push_back(self.throughput as i64);
        }
        self.last_sample = Some((now_millis, stats.total_processed_records));
    }
}

/// Columns the pipelines table can sort by.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SortColumn {
    #[default]
    Name,
    Status,
    Throughput,
    Records,
    Memory,
    Storage,
    Age,
}

impl SortColumn {
    pub const fn title(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Status => "status",
            Self::Throughput => "rps",
            Self::Records => "records",
            Self::Memory => "memory",
            Self::Storage => "storage",
            Self::Age => "age",
        }
    }

    pub fn cycle(self) -> Self {
        match self {
            Self::Name => Self::Status,
            Self::Status => Self::Throughput,
            Self::Throughput => Self::Records,
            Self::Records => Self::Memory,
            Self::Memory => Self::Storage,
            Self::Storage => Self::Age,
            Self::Age => Self::Name,
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "name" => Some(Self::Name),
            "status" => Some(Self::Status),
            "rps" | "throughput" => Some(Self::Throughput),
            "records" => Some(Self::Records),
            "memory" | "mem" => Some(Self::Memory),
            "storage" => Some(Self::Storage),
            "age" => Some(Self::Age),
            _ => None,
        }
    }
}

/// A fetched SQL program with its owning pipeline.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SqlDocument {
    pub pipeline: String,
    pub text: String,
}

impl SqlDocument {
    pub fn line_count(&self) -> usize {
        self.text.lines().count()
    }
}

/// The first terminal row holding table data: header (6 rows), panel
/// border, table header. Mouse clicks map through this to row indices.
pub const FIRST_TABLE_ROW: u16 = 8;

/// Which view a double-clicked table column jumps to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClickTarget {
    Sql,
    Metrics,
    Logs,
    /// Opens the row's saved Samply profile instead of switching views.
    Samply,
    /// Reveals the row's saved support bundle instead of switching views.
    Bundle,
}

/// One column of the rendered pipelines table, for mouse hit-testing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnHit {
    pub x_start: u16,
    pub x_end: u16,
    /// The view a double click on a data cell jumps to.
    pub target: ClickTarget,
    /// What a click on the column header sorts by, when sortable.
    pub sort: Option<SortColumn>,
}

/// One tab of the bottom bar: the columns `[x_start, x_end)` it occupies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TabHit {
    pub x_start: u16,
    pub x_end: u16,
    pub view: View,
}

/// State the renderer writes back so mouse handling can interpret clicks:
/// the scroll offset each table rendered with, the horizontal column ranges
/// of the pipelines table, and where the bottom tabs sit. This is a
/// view-layer cache, not app state.
#[derive(Clone, Debug, Default)]
pub struct ViewportShare {
    /// `(first_visible_row, visible_row_count)` per table.
    pub pipelines: Cell<(usize, usize)>,
    pub connectors: Cell<(usize, usize)>,
    pub hotspots: Cell<(usize, usize)>,
    /// Largest useful scroll offset of the open text overlay, so the keys
    /// stop where the text ends.
    pub overlay_scroll_max: Cell<u16>,
    pub click_columns: RefCell<Vec<ColumnHit>>,
    /// Terminal row of the tab bar; `None` before the first frame.
    pub tab_bar_row: Cell<Option<u16>>,
    pub tabs: RefCell<Vec<TabHit>>,
    /// Terminal row of the status line, where a saved profile's path shows;
    /// `None` before the first frame.
    pub status_line_row: Cell<Option<u16>>,
}

impl ViewportShare {
    /// The view whose bottom tab sits under the terminal cell, if any.
    ///
    /// ```
    /// use f5a::app::state::{TabHit, View, ViewportShare};
    ///
    /// let viewport = ViewportShare::default();
    /// viewport.tab_bar_row.set(Some(9));
    /// *viewport.tabs.borrow_mut() = vec![TabHit { x_start: 0, x_end: 12, view: View::Metrics }];
    /// assert_eq!(viewport.view_at(9, 4), Some(View::Metrics));
    /// assert_eq!(viewport.view_at(8, 4), None);
    /// ```
    pub fn view_at(&self, row: u16, column: u16) -> Option<View> {
        if self.tab_bar_row.get() != Some(row) {
            return None;
        }
        self.tabs
            .borrow()
            .iter()
            .find(|tab| (tab.x_start..tab.x_end).contains(&column))
            .map(|tab| tab.view)
    }

    fn hit_at(&self, x: u16) -> Option<ColumnHit> {
        self.click_columns
            .borrow()
            .iter()
            .find(|hit| (hit.x_start..hit.x_end).contains(&x))
            .copied()
    }

    /// The jump target under column `x`, if any.
    pub fn target_at(&self, x: u16) -> Option<ClickTarget> {
        self.hit_at(x).map(|hit| hit.target)
    }

    /// The sort key under column `x`, if that column is sortable.
    pub fn sort_at(&self, x: u16) -> Option<SortColumn> {
        self.hit_at(x).and_then(|hit| hit.sort)
    }
}

/// A lifecycle action waiting in a pipeline's queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueuedAction {
    pub action: Action,
    /// When it entered the queue; queued actions expire so a forgotten
    /// destructive step cannot fire long after it was requested.
    pub queued_at_millis: i64,
}

/// One semantic row of the Logs view's content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogRow {
    SectionTitle(String, Severity),
    SectionText(String),
    Separator,
    Log(String),
}

/// The Logs view: diagnostics plus a bounded tail of the log stream.
#[derive(Clone, Debug, Default)]
pub struct LogState {
    pub pipeline: Option<String>,
    pub lines: VecDeque<String>,
    /// `None` follows the newest line; `Some` is a manual scroll position.
    pub scroll: Option<usize>,
    /// Whether a stream task is (believed to be) running.
    pub streaming: bool,
    pub last_error: Option<String>,
    /// Earliest instant a closed stream may reconnect; `i64::MAX` while one
    /// is active or none is wanted.
    pub retry_at_millis: i64,
    pub diagnostics: Vec<DiagnosticSection>,
    pub diagnostics_pipeline: Option<String>,
    pub loading_diagnostics: bool,
}

impl LogState {
    const MAX_LINES: usize = 2_000;

    /// Point the view at a pipeline, dropping the previous stream's content.
    pub fn reset(&mut self, pipeline: String) {
        self.pipeline = Some(pipeline);
        self.lines.clear();
        self.scroll = None;
        self.streaming = false;
        self.last_error = None;
        self.retry_at_millis = i64::MAX;
    }

    pub fn push_line(&mut self, line: &str) {
        if self.lines.len() == Self::MAX_LINES {
            self.lines.pop_front();
        }
        self.lines.push_back(crate::model::text::terminal_safe(
            &crate::model::text::strip_ansi(line),
        ));
    }

    /// Diagnostic bodies wrap at this width so error messages read in full;
    /// stream lines stay unwrapped like any log viewer.
    const WRAP_COLUMNS: usize = 150;

    fn wrapped(body: &str) -> impl Iterator<Item = String> + '_ {
        body.lines().flat_map(|line| {
            let characters: Vec<char> = line.chars().collect();
            if characters.is_empty() {
                return vec![String::new()];
            }
            characters
                .chunks(Self::WRAP_COLUMNS)
                .map(|chunk| chunk.iter().collect::<String>())
                .collect::<Vec<_>>()
        })
    }

    /// The full content, diagnostics first, then the stream.
    pub fn rows(&self) -> Vec<LogRow> {
        let mut rows = Vec::with_capacity(self.lines.len() + 16);
        for section in &self.diagnostics {
            rows.push(LogRow::SectionTitle(
                section.title.clone(),
                section.severity,
            ));
            rows.extend(Self::wrapped(&section.body).map(LogRow::SectionText));
        }
        if !self.diagnostics.is_empty() {
            rows.push(LogRow::Separator);
        }
        rows.extend(self.lines.iter().map(|line| LogRow::Log(line.clone())));
        rows
    }

    pub fn row_count(&self) -> usize {
        let diagnostic_rows: usize = self
            .diagnostics
            .iter()
            .map(|section| 1 + Self::wrapped(&section.body).count())
            .sum();
        let separator = usize::from(!self.diagnostics.is_empty());
        diagnostic_rows + separator + self.lines.len()
    }
}

/// The whole application state.
#[derive(Clone, Debug, Default)]
pub struct App {
    // Data fetched from the instance.
    pub host: String,
    pub instance: InstanceInfo,
    pub session: Session,
    pub pipelines: Vec<PipelineRow>,
    pub live: HashMap<String, RowLive>,
    pub detail: Option<PipelineStats>,
    /// Why the detail pane has no data (e.g. the pipeline is stopped).
    pub detail_error: Option<String>,
    pub history: MetricHistory,
    pub sql: Option<SqlDocument>,
    pub report: Option<HotspotReport>,
    pub report_pipeline: Option<String>,
    /// Samply CPU profile captures, one per pipeline; finished ones linger
    /// so the saved path stays visible, then fade.
    pub samply: HashMap<String, Capture>,
    /// Support bundle downloads, one per pipeline, with the same lifecycle.
    pub bundles: HashMap<String, Capture>,
    /// What `:bundle` asks the server to gather; edited in the dialog.
    pub bundle_options: BundleOptions,
    pub logs: LogState,

    /// Lifecycle actions waiting for their pipeline to settle, per pipeline.
    pub queued: HashMap<String, VecDeque<QueuedAction>>,
    /// The lifecycle action currently running against each pipeline.
    pub inflight: HashMap<String, Action>,
    /// Pipelines whose storage-clear burn animation stays visible until the
    /// given instant, so even a millisecond clear is seen.
    pub clearing_flash: HashMap<String, i64>,
    /// Configured storage quota per pipeline, in bytes, for pressure
    /// coloring; absent when no quota is set.
    pub storage_quotas: HashMap<String, i64>,
    /// Multi-selected pipelines; lifecycle actions apply to all of them.
    pub marked: HashSet<String>,
    /// Row where the current shift run began; the run's span is every row
    /// between it and the cursor. `None` outside a run.
    pub mark_anchor: Option<String>,
    /// Last table click, for double-click detection: `(row, at_millis)`.
    pub last_click: Option<(u16, i64)>,
    /// Scroll offsets and column ranges the renderer reported back.
    pub viewport: ViewportShare,

    // Navigation and interaction state.
    pub view: View,
    pub overlay: Overlay,
    pub prompt: Prompt,
    pub filter: String,
    pub selected_name: Option<String>,
    pub selected_connector: usize,
    pub selected_hotspot: usize,
    pub sql_scroll: usize,
    /// Lines highlighted after jumping from a hotspot, 1-based inclusive.
    pub sql_focus: Option<(usize, usize)>,
    pub metric: CostMetric,
    pub sort: SortColumn,
    pub sort_descending: bool,
    pub command_history: Vec<String>,

    // Status surfaces.
    pub connection: Connection,
    pub toast: Option<Toast>,
    pub loading_sql: bool,
    pub loading_report: bool,
    pub pending_action_count: usize,
    pub refresh_secs: u64,
    /// Whether destructive actions (stop, restart, clear) confirm first;
    /// set by the `--ask` flag, off by default.
    pub ask_before_destructive: bool,
    pub spinner_frame: usize,
    pub now_millis: i64,
    pub first_overview_seen: bool,
}

impl App {
    const TOAST_LIFETIME_MILLIS: i64 = 5_000;

    pub fn new(host: String, refresh_secs: u64) -> Self {
        Self {
            host,
            refresh_secs,
            ..Default::default()
        }
    }

    /// Rows surviving the current filter, in the current sort order.
    pub fn visible_rows(&self) -> Vec<&PipelineRow> {
        let needle = self.filter.to_lowercase();
        let mut rows: Vec<&PipelineRow> = self
            .pipelines
            .iter()
            .filter(|row| needle.is_empty() || row.name.to_lowercase().contains(&needle))
            .collect();
        rows.sort_by(|left, right| {
            let ordering = match self.sort {
                SortColumn::Name => left.name.cmp(&right.name),
                SortColumn::Status => left
                    .deployment_status
                    .cmp(&right.deployment_status)
                    .then_with(|| left.name.cmp(&right.name)),
                SortColumn::Throughput => order_f64(
                    self.live_of(&left.name).map_or(0.0, |live| live.throughput),
                    self.live_of(&right.name)
                        .map_or(0.0, |live| live.throughput),
                )
                .then_with(|| left.name.cmp(&right.name)),
                SortColumn::Records => self
                    .live_of(&left.name)
                    .map_or(0, |live| live.records)
                    .cmp(&self.live_of(&right.name).map_or(0, |live| live.records))
                    .then_with(|| left.name.cmp(&right.name)),
                SortColumn::Memory => self
                    .live_of(&left.name)
                    .map_or(0, |live| live.rss_bytes)
                    .cmp(&self.live_of(&right.name).map_or(0, |live| live.rss_bytes))
                    .then_with(|| left.name.cmp(&right.name)),
                SortColumn::Storage => self
                    .live_of(&left.name)
                    .map_or(0, |live| live.storage_bytes)
                    .cmp(
                        &self
                            .live_of(&right.name)
                            .map_or(0, |live| live.storage_bytes),
                    )
                    .then_with(|| left.name.cmp(&right.name)),
                SortColumn::Age => left
                    .status_since_millis
                    .unwrap_or(0)
                    .cmp(&right.status_since_millis.unwrap_or(0))
                    .then_with(|| left.name.cmp(&right.name)),
            };
            if self.sort_descending {
                ordering.reverse()
            } else {
                ordering
            }
        });
        rows
    }

    pub fn live_of(&self, pipeline: &str) -> Option<&RowLive> {
        self.live.get(pipeline)
    }

    /// Comma-joined verbs of the actions queued behind `pipeline`, without
    /// clears: a queued clear shows in the storage column instead.
    pub fn queued_label(&self, pipeline: &str) -> Option<String> {
        let queue = self.queued.get(pipeline)?;
        let verbs: Vec<&str> = queue
            .iter()
            .filter(|queued| !matches!(queued.action, Action::Clear { .. }))
            .map(|queued| queued.action.verb())
            .collect();
        if verbs.is_empty() {
            return None;
        }
        Some(verbs.join(","))
    }

    /// Whether a clear waits in this pipeline's queue.
    pub fn is_clear_queued(&self, pipeline: &str) -> bool {
        self.queued.get(pipeline).is_some_and(|queue| {
            queue
                .iter()
                .any(|queued| matches!(queued.action, Action::Clear { .. }))
        })
    }

    /// The captures of one kind, keyed by pipeline.
    ///
    /// ```
    /// use f5a::app::App;
    /// use f5a::gateway::DownloadKind;
    ///
    /// let app = App::new("http://127.0.0.1:8080".to_string(), 2);
    /// assert!(app.captures(DownloadKind::SupportBundle).is_empty());
    /// ```
    pub fn captures(&self, kind: DownloadKind) -> &HashMap<String, Capture> {
        match kind {
            DownloadKind::SamplyProfile => &self.samply,
            DownloadKind::SupportBundle => &self.bundles,
        }
    }

    /// Mutable access to the captures of one kind.
    ///
    /// ```
    /// use f5a::app::App;
    /// use f5a::app::capture::Capture;
    /// use f5a::gateway::{BundleOptions, DownloadKind};
    ///
    /// let mut app = App::new("http://127.0.0.1:8080".to_string(), 2);
    /// let bundle = Capture::download(
    ///     DownloadKind::SupportBundle,
    ///     "orders".to_string(),
    ///     BundleOptions::default(),
    ///     0,
    /// );
    /// app.captures_mut(DownloadKind::SupportBundle).insert("orders".to_string(), bundle);
    /// assert!(app.bundles.contains_key("orders"));
    /// ```
    pub fn captures_mut(&mut self, kind: DownloadKind) -> &mut HashMap<String, Capture> {
        match kind {
            DownloadKind::SamplyProfile => &mut self.samply,
            DownloadKind::SupportBundle => &mut self.bundles,
        }
    }

    /// The capture the status line reports, across both kinds: the newest
    /// active one, else the most recently finished one.
    ///
    /// ```
    /// use f5a::app::App;
    /// use f5a::app::capture::Capture;
    ///
    /// let mut app = App::new("http://127.0.0.1:8080".to_string(), 2);
    /// assert!(app.capture_spotlight().is_none());
    /// let mut done = Capture::record("old".to_string(), 1, 0);
    /// done.saved("./old-samply-0.json.gz".to_string(), 50);
    /// app.samply.insert("old".to_string(), done);
    /// app.samply.insert("new".to_string(), Capture::record("new".to_string(), 30, 10));
    /// assert_eq!(app.capture_spotlight().map(|capture| capture.pipeline.as_str()), Some("new"));
    /// ```
    pub fn capture_spotlight(&self) -> Option<&Capture> {
        self.samply
            .values()
            .chain(self.bundles.values())
            .max_by_key(|capture| {
                (
                    capture.is_active(),
                    capture
                        .finished_millis()
                        .unwrap_or(capture.requested_millis),
                    // Stable pick between captures finished in the same instant.
                    std::cmp::Reverse(capture.pipeline.as_str()),
                )
            })
    }

    /// Index of the selected pipeline within the visible rows.
    pub fn selected_index(&self) -> Option<usize> {
        let selected = self.selected_name.as_deref()?;
        self.visible_rows()
            .iter()
            .position(|row| row.name == selected)
    }

    pub fn selected_pipeline(&self) -> Option<&PipelineRow> {
        let selected = self.selected_name.as_deref()?;
        self.pipelines.iter().find(|row| row.name == selected)
    }

    /// Move the selection by `delta` within the visible rows, parking at the
    /// ends: a held key or a fast wheel stops at the edge instead of cycling.
    pub fn move_selection(&mut self, delta: isize) {
        let names: Vec<String> = self
            .visible_rows()
            .iter()
            .map(|row| row.name.clone())
            .collect();
        if names.is_empty() {
            self.selected_name = None;
            return;
        }
        let current = self
            .selected_index()
            .map(|index| index as isize)
            .unwrap_or(0);
        let next = (current + delta).clamp(0, names.len() as isize - 1) as usize;
        self.select_pipeline(names[next].clone());
    }

    /// Visible rows from `from` through `to`, both inclusive and in either
    /// order; empty when either end is not visible.
    ///
    /// ```
    /// use f5a::app::App;
    /// use f5a::model::pipeline::PipelineRow;
    ///
    /// let mut app = App::new("http://127.0.0.1:8080".to_string(), 2);
    /// app.pipelines = ["a", "b", "c"]
    ///     .iter()
    ///     .map(|name| PipelineRow { name: name.to_string(), ..Default::default() })
    ///     .collect();
    /// assert_eq!(app.range_between("c", "a").len(), 3);
    /// assert!(app.range_between("a", "missing").is_empty());
    /// ```
    pub fn range_between(&self, from: &str, to: &str) -> HashSet<String> {
        let names: Vec<&str> = self
            .visible_rows()
            .iter()
            .map(|row| row.name.as_str())
            .collect();
        let position = |wanted: &str| names.iter().position(|name| *name == wanted);
        let (Some(from), Some(to)) = (position(from), position(to)) else {
            return HashSet::new();
        };
        let (low, high) = if from <= to { (from, to) } else { (to, from) };
        names[low..=high]
            .iter()
            .map(|name| name.to_string())
            .collect()
    }

    /// Select a pipeline by name, resetting per-pipeline panes when it changes.
    pub fn select_pipeline(&mut self, name: String) {
        if self.selected_name.as_deref() == Some(name.as_str()) {
            return;
        }
        self.selected_name = Some(name);
        self.detail = None;
        self.detail_error = None;
        self.history.clear();
        self.selected_connector = 0;
        self.selected_hotspot = 0;
        self.sql_scroll = 0;
        self.sql_focus = None;
    }

    /// Keep the selection valid after a refresh; default to the first row.
    pub fn reconcile_selection(&mut self) {
        let visible = self.visible_rows();
        let still_there = self
            .selected_name
            .as_deref()
            .is_some_and(|selected| visible.iter().any(|row| row.name == selected));
        if !still_there {
            let first = visible.first().map(|row| row.name.clone());
            match first {
                Some(name) => self.select_pipeline(name),
                None => self.selected_name = None,
            }
        }
    }

    /// Connectors of the watched pipeline, inputs first.
    pub fn connectors(&self) -> Vec<&crate::model::stats::ConnectorRow> {
        self.detail
            .iter()
            .flat_map(|stats| stats.connectors())
            .collect()
    }

    pub fn selected_connector(&self) -> Option<&crate::model::stats::ConnectorRow> {
        self.connectors().get(self.selected_connector).copied()
    }

    pub fn toast_info(&mut self, message: impl Into<String>) {
        self.set_toast(message, ToastLevel::Info);
    }

    pub fn toast_success(&mut self, message: impl Into<String>) {
        self.set_toast(message, ToastLevel::Success);
    }

    pub fn toast_error(&mut self, message: impl Into<String>) {
        self.set_toast(message, ToastLevel::Error);
    }

    fn set_toast(&mut self, message: impl Into<String>, level: ToastLevel) {
        self.toast = Some(Toast {
            message: message.into(),
            level,
            expires_at_millis: self.now_millis + Self::TOAST_LIFETIME_MILLIS,
        });
    }

    /// Record a failed API call in the most helpful place.
    pub fn note_api_error(&mut self, error: &ApiError) {
        if error.is_connection_loss() {
            if !matches!(self.connection, Connection::Lost { .. }) {
                self.connection = Connection::Lost {
                    error: error.to_string(),
                    since_millis: self.now_millis,
                };
            }
        } else {
            self.toast_error(error.to_string());
        }
    }

    /// Drop the toast once its lifetime passed; advance the spinner.
    pub fn tick(&mut self, now_millis: i64) {
        self.now_millis = now_millis;
        self.spinner_frame = self.spinner_frame.wrapping_add(1);
        if self
            .toast
            .as_ref()
            .is_some_and(|toast| toast.expires_at_millis <= now_millis)
        {
            self.toast = None;
        }
    }

    /// Whether any background work deserves a spinner.
    pub fn is_busy(&self) -> bool {
        self.loading_sql
            || self.loading_report
            || self.pending_action_count > 0
            || self
                .samply
                .values()
                .chain(self.bundles.values())
                .any(Capture::is_active)
            || matches!(self.connection, Connection::Connecting)
    }
}

fn order_f64(left: f64, right: f64) -> std::cmp::Ordering {
    left.total_cmp(&right)
}

#[cfg(test)]
mod tests {
    use super::{App, Connection, RowLive, SortColumn, View};
    use crate::http::ApiError;
    use crate::model::pipeline::PipelineRow;
    use crate::model::stats::PipelineStats;
    use serde_json::json;
    use std::collections::HashSet;

    fn app_with(names: &[&str]) -> App {
        let mut app = App::new("http://x".to_string(), 2);
        app.pipelines = names
            .iter()
            .map(|name| PipelineRow {
                name: name.to_string(),
                ..Default::default()
            })
            .collect();
        app.reconcile_selection();
        app
    }

    #[test]
    fn tabs_cycle_and_jump_by_digit() {
        assert_eq!(View::Pipelines.next(false), View::Metrics);
        assert_eq!(View::Pipelines.next(true), View::Logs);
        assert_eq!(View::Logs.next(false), View::Pipelines);
        assert_eq!(View::from_digit('1'), Some(View::Pipelines));
        assert_eq!(View::from_digit('6'), Some(View::Logs));
        assert_eq!(View::from_digit('7'), None);
        assert_eq!(View::from_digit('x'), None);
    }

    #[test]
    fn selection_moves_and_stops_at_the_ends() {
        let mut app = app_with(&["a", "b", "c"]);
        assert_eq!(app.selected_name.as_deref(), Some("a"));
        app.move_selection(1);
        assert_eq!(app.selected_name.as_deref(), Some("b"));
        app.move_selection(-2);
        assert_eq!(app.selected_name.as_deref(), Some("a"));
        app.move_selection(5);
        assert_eq!(app.selected_name.as_deref(), Some("c"));
        assert_eq!(app.selected_index(), Some(2));
    }

    #[test]
    fn tab_hits_resolve_only_on_the_tab_bar_row() {
        let viewport = super::ViewportShare::default();
        assert_eq!(viewport.view_at(0, 0), None);
        viewport.tab_bar_row.set(Some(9));
        *viewport.tabs.borrow_mut() = vec![
            super::TabHit {
                x_start: 2,
                x_end: 6,
                view: View::Sql,
            },
            super::TabHit {
                x_start: 7,
                x_end: 9,
                view: View::Logs,
            },
        ];
        assert_eq!(viewport.view_at(9, 2), Some(View::Sql));
        assert_eq!(viewport.view_at(9, 5), Some(View::Sql));
        assert_eq!(viewport.view_at(9, 6), None);
        assert_eq!(viewport.view_at(9, 8), Some(View::Logs));
        assert_eq!(viewport.view_at(8, 8), None);
    }

    #[test]
    fn range_between_covers_both_ends_in_either_direction() {
        let app = app_with(&["a", "b", "c", "d"]);
        let downward = app.range_between("a", "c");
        let expected: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        assert_eq!(downward, expected);
        assert_eq!(app.range_between("c", "a"), downward);
        assert!(app.range_between("missing", "a").is_empty());
        assert!(app.range_between("a", "missing").is_empty());
    }

    #[test]
    fn filters_narrow_the_visible_rows_and_selection_follows() {
        let mut app = app_with(&["orders", "users", "order_totals"]);
        app.filter = "ord".to_string();
        let visible: Vec<&str> = app
            .visible_rows()
            .iter()
            .map(|row| row.name.as_str())
            .collect();
        assert_eq!(visible, vec!["order_totals", "orders"]);
        app.selected_name = Some("users".to_string());
        app.reconcile_selection();
        assert_eq!(app.selected_name.as_deref(), Some("order_totals"));
    }

    #[test]
    fn empty_lists_clear_the_selection() {
        let mut app = app_with(&[]);
        app.move_selection(1);
        assert_eq!(app.selected_name, None);
        app.reconcile_selection();
        assert_eq!(app.selected_name, None);
        assert!(app.selected_pipeline().is_none());
    }

    #[test]
    fn switching_pipelines_resets_the_per_pipeline_panes() {
        let mut app = app_with(&["a", "b"]);
        app.sql_scroll = 40;
        app.selected_hotspot = 3;
        app.select_pipeline("b".to_string());
        assert_eq!(app.sql_scroll, 0);
        assert_eq!(app.selected_hotspot, 0);
        // Re-selecting the same pipeline keeps state.
        app.sql_scroll = 7;
        app.select_pipeline("b".to_string());
        assert_eq!(app.sql_scroll, 7);
    }

    #[test]
    fn live_rows_derive_throughput_from_consecutive_samples() {
        let stats = |records: i64| {
            PipelineStats::from_json(&json!({
                "global_metrics": {"total_processed_records": records}
            }))
        };
        let mut live = RowLive::default();
        live.observe(&stats(100), 1_000);
        assert_eq!(live.throughput, 0.0);
        live.observe(&stats(600), 2_000);
        assert_eq!(live.throughput, 500.0);
        assert_eq!(live.spark.len(), 1);
        // Counter reset after restart clamps to zero.
        live.observe(&stats(0), 3_000);
        assert_eq!(live.throughput, 0.0);
    }

    #[test]
    fn near_duplicate_samples_refresh_counters_but_not_the_rate() {
        let stats = |records: i64| {
            PipelineStats::from_json(&json!({
                "global_metrics": {"total_processed_records": records}
            }))
        };
        let mut live = RowLive::default();
        live.observe(&stats(0), 1_000);
        live.observe(&stats(1_000), 2_000);
        assert_eq!(live.throughput, 1_000.0);
        // A second poller samples 50ms later: counters move, rate does not.
        live.observe(&stats(1_010), 2_050);
        assert_eq!(live.records, 1_010);
        assert_eq!(live.throughput, 1_000.0);
        assert_eq!(live.spark.len(), 1);
        // The next regular sample measures from the accepted sample.
        live.observe(&stats(2_000), 3_000);
        assert_eq!(live.throughput, 1_000.0);
    }

    #[test]
    fn sorting_by_live_metrics_uses_the_enrichment() {
        let mut app = app_with(&["slow", "fast"]);
        app.live.insert(
            "fast".to_string(),
            RowLive {
                throughput: 100.0,
                ..Default::default()
            },
        );
        app.sort = SortColumn::Throughput;
        app.sort_descending = true;
        let visible: Vec<&str> = app
            .visible_rows()
            .iter()
            .map(|row| row.name.as_str())
            .collect();
        assert_eq!(visible, vec!["fast", "slow"]);
        assert_eq!(SortColumn::parse("mem"), Some(SortColumn::Memory));
        assert_eq!(SortColumn::parse("bogus"), None);
        assert_eq!(SortColumn::Name.cycle(), SortColumn::Status);
        assert_eq!(SortColumn::Age.cycle(), SortColumn::Name);
    }

    #[test]
    fn every_sort_column_orders_and_reverses() {
        let mut app = app_with(&["young", "old"]);
        app.pipelines[0].status_since_millis = Some(2_000);
        app.pipelines[0].deployment_status = "Running".to_string();
        app.pipelines[1].status_since_millis = Some(1_000);
        app.pipelines[1].deployment_status = "Paused".to_string();
        app.live.insert(
            "young".to_string(),
            RowLive {
                records: 10,
                rss_bytes: 10,
                storage_bytes: 10,
                throughput: 10.0,
                ..Default::default()
            },
        );
        app.live.insert(
            "old".to_string(),
            RowLive {
                records: 99,
                rss_bytes: 99,
                storage_bytes: 99,
                throughput: 99.0,
                ..Default::default()
            },
        );
        let first_of = |app: &App, sort: SortColumn, descending: bool| {
            let mut app = app.clone();
            app.sort = sort;
            app.sort_descending = descending;
            app.visible_rows()[0].name.clone()
        };
        assert_eq!(first_of(&app, SortColumn::Name, false), "old");
        assert_eq!(first_of(&app, SortColumn::Name, true), "young");
        assert_eq!(
            first_of(&app, SortColumn::Status, false),
            "old",
            "Paused < Running"
        );
        assert_eq!(first_of(&app, SortColumn::Records, true), "old");
        assert_eq!(first_of(&app, SortColumn::Memory, true), "old");
        assert_eq!(first_of(&app, SortColumn::Storage, true), "old");
        assert_eq!(first_of(&app, SortColumn::Age, false), "old");
    }

    #[test]
    fn sort_columns_have_titles_and_a_full_cycle() {
        let mut column = SortColumn::Name;
        let mut seen = Vec::new();
        for _ in 0..7 {
            seen.push(column.title());
            column = column.cycle();
        }
        assert_eq!(
            seen,
            vec![
                "name", "status", "rps", "records", "memory", "storage", "age"
            ]
        );
        assert_eq!(column, SortColumn::Name);
        for title in [
            "name", "status", "rps", "records", "memory", "storage", "age",
        ] {
            assert!(SortColumn::parse(title).is_some());
        }
    }

    #[test]
    fn spark_history_is_bounded() {
        let stats = |records: i64| {
            PipelineStats::from_json(&serde_json::json!({
                "global_metrics": {"total_processed_records": records}
            }))
        };
        let mut live = RowLive::default();
        for index in 0..40 {
            live.observe(&stats(index * 100), 1_000 * index);
        }
        assert_eq!(live.spark.len(), 30);
    }

    #[test]
    fn log_content_wraps_diagnostics_and_counts_rows_consistently() {
        use crate::model::diagnostics::{DiagnosticSection, Severity};
        let mut logs = super::LogState::default();
        logs.reset("p".to_string());
        logs.diagnostics = vec![DiagnosticSection {
            title: "DEPLOYMENT ERROR".to_string(),
            severity: Severity::Error,
            body: format!("{}\nshort", "x".repeat(400)),
        }];
        logs.push_line("a log line");
        let rows = logs.rows();
        assert_eq!(rows.len(), logs.row_count());
        // 400 chars wrap into three rows of at most 150.
        let text_rows = rows
            .iter()
            .filter(|row| matches!(row, super::LogRow::SectionText(_)))
            .count();
        assert_eq!(text_rows, 4);
        assert!(matches!(rows.last(), Some(super::LogRow::Log(_))));
    }

    #[test]
    fn log_lines_ring_and_sanitize() {
        let mut logs = super::LogState::default();
        logs.reset("p".to_string());
        for index in 0..2_100 {
            logs.push_line(&format!("\u{1b}[31mline {index}\u{1b}[0m"));
        }
        assert_eq!(logs.lines.len(), 2_000);
        assert_eq!(logs.lines.back().unwrap(), "line 2099");
        assert_eq!(logs.lines.front().unwrap(), "line 100");
    }

    #[test]
    fn toasts_expire_on_tick() {
        let mut app = app_with(&[]);
        app.now_millis = 1_000;
        app.toast_success("done");
        app.tick(2_000);
        assert!(app.toast.is_some());
        app.tick(7_000);
        assert!(app.toast.is_none());
    }

    #[test]
    fn connection_loss_is_sticky_but_other_errors_toast() {
        let mut app = app_with(&[]);
        app.note_api_error(&ApiError::Transport("refused".to_string()));
        assert!(matches!(app.connection, Connection::Lost { .. }));
        app.note_api_error(&ApiError::Status {
            status: 404,
            message: "nope".to_string(),
        });
        assert!(app.toast.is_some());
        assert!(matches!(app.connection, Connection::Lost { .. }));
    }

    #[test]
    fn busy_reflects_outstanding_work() {
        let mut app = app_with(&[]);
        assert!(app.is_busy(), "initial connect counts as busy");
        app.connection = Connection::Online { latency_millis: 3 };
        assert!(!app.is_busy());
        app.pending_action_count = 1;
        assert!(app.is_busy());
    }
}
