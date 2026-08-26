//! Messages into and effects out of the pure state machine.

use crate::gateway::{Action, BundleOptions, DownloadKind, Overview, PipelineDetail};
use crate::http::{ApiError, Download};
use crate::model::dataflow::HotspotReport;
use crate::model::diagnostics::DiagnosticSection;
use crate::model::stats::PipelineStats;

/// A terminal-independent key press.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyPress {
    Char(char),
    Up,
    Down,
    PageUp,
    PageDown,
    Home,
    End,
    Enter,
    Escape,
    Backspace,
    Tab,
    BackTab,
    /// Arrow keys that step a setting in a dialog.
    Left,
    Right,
    /// Mouse wheel, one step per event; moves the cursor without wrapping.
    ScrollUp,
    ScrollDown,
    /// Shift plus arrow: grow or shrink the marked range from its anchor.
    ShiftUp,
    ShiftDown,
    /// Left mouse click at this terminal cell; selects table rows, and a
    /// double click jumps to the view the clicked column belongs to.
    Click {
        row: u16,
        column: u16,
    },
}

/// Everything that can happen to the application.
#[derive(Clone, Debug)]
pub enum Msg {
    Key(KeyPress),
    /// A dashboard refresh completed.
    Overview {
        result: Result<Overview, ApiError>,
        latency_millis: u64,
        now_millis: i64,
    },
    /// Per-row stats enrichment for one pipeline on the dashboard.
    RowStats {
        pipeline: String,
        stats: PipelineStats,
        now_millis: i64,
    },
    /// A detail refresh for the watched pipeline completed.
    Detail {
        pipeline: String,
        result: Result<PipelineDetail, ApiError>,
        now_millis: i64,
    },
    SqlLoaded {
        pipeline: String,
        result: Result<String, ApiError>,
    },
    HotspotsLoaded {
        pipeline: String,
        result: Result<HotspotReport, ApiError>,
    },
    ActionFinished {
        action: Action,
        result: Result<(), ApiError>,
    },
    /// The server answered a request for one of the pipeline's downloads.
    DownloadFetched {
        kind: DownloadKind,
        pipeline: String,
        result: Result<Download, ApiError>,
    },
    /// The runner wrote a download to disk (or failed to).
    FileSaved {
        kind: DownloadKind,
        pipeline: String,
        path: String,
        result: Result<(), String>,
    },
    /// The runner launched `samply load` on a saved profile (or failed to).
    SamplyOpened {
        path: String,
        /// `Ok` names the browser that received the profile URL.
        result: Result<String, String>,
    },
    /// The runner showed a saved file in the file manager (or failed to).
    FileRevealed {
        path: String,
        result: Result<(), String>,
    },
    /// One line arrived on the followed log stream.
    LogLine {
        pipeline: String,
        line: String,
    },
    /// The log stream ended or failed; the app decides whether to retry.
    LogsClosed {
        pipeline: String,
        result: Result<(), ApiError>,
    },
    DiagnosticsLoaded {
        pipeline: String,
        result: Result<Vec<DiagnosticSection>, ApiError>,
    },
    /// Animation and timeout heartbeat.
    Tick {
        now_millis: i64,
    },
}

/// Work the runner must perform on behalf of the state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cmd {
    /// Ask the poller for an immediate refresh.
    Refresh,
    /// Tell the poller which pipeline deserves detail polling.
    Watch(Option<String>),
    /// Change the poller's refresh cadence.
    SetRefreshSecs(u64),
    Run(Action),
    FetchSql(String),
    FetchHotspots(String),
    /// Ask the server for one of this pipeline's downloads; `bundle` shapes a
    /// support bundle and is ignored for profiles.
    FetchDownload {
        kind: DownloadKind,
        pipeline: String,
        bundle: BundleOptions,
    },
    /// Persist a download under a generated file name.
    SaveDownload {
        kind: DownloadKind,
        pipeline: String,
        bytes: Vec<u8>,
    },
    /// Fetch compile and deployment errors for the Logs view.
    FetchDiagnostics(String),
    /// Follow this pipeline's log stream (replaces any active stream).
    StartLogs(String),
    /// Stop following logs.
    StopLogs,
    /// Open a saved Samply profile in the browser through `samply load`.
    OpenSamply {
        path: String,
    },
    /// Show a saved file in the desktop's file manager.
    RevealFile {
        path: String,
    },
    SwitchTenant(Option<String>),
    Quit,
}
