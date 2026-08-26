//! The reducer: `Msg` in, state change, `Cmd`s out.

use crate::gateway::{Action, DownloadKind};
use crate::model::stats::ConnectorKind;
use crate::model::timeseries::{TimePoint, TimeSeries};

use super::bundle_settings::{BundleSetting, step_limit};
use super::capture::{
    Capture, CapturePhase, DownloadOutcome, FINISHED_TTL_MILLIS, MAX_TRANSPORT_RETRIES, TickOutcome,
};
use super::commands::{Command, complete, parse};
use super::msg::{Cmd, KeyPress, Msg};
use super::state::{
    App, Connection, FIRST_TABLE_ROW, Overlay, Prompt, SortColumn, SqlDocument, View,
};

/// Apply one message. Returns the work the runner must perform.
pub fn update(app: &mut App, msg: Msg) -> Vec<Cmd> {
    match msg {
        Msg::Key(key) => on_key(app, key),
        Msg::Overview {
            result,
            latency_millis,
            now_millis,
        } => on_overview(app, result, latency_millis, now_millis),
        Msg::RowStats {
            pipeline,
            stats,
            now_millis,
        } => {
            app.now_millis = now_millis;
            app.live
                .entry(pipeline)
                .or_default()
                .observe(&stats, now_millis);
            Vec::new()
        }
        Msg::Detail {
            pipeline,
            result,
            now_millis,
        } => on_detail(app, pipeline, result, now_millis),
        Msg::SqlLoaded { pipeline, result } => {
            app.loading_sql = false;
            match result {
                Ok(text) => app.sql = Some(SqlDocument { pipeline, text }),
                Err(error) => app.toast_error(format!("SQL: {error}")),
            }
            Vec::new()
        }
        Msg::HotspotsLoaded { pipeline, result } => {
            app.loading_report = false;
            match result {
                Ok(report) => {
                    if report.is_empty() {
                        app.toast_info(
                            "Profile has no cost data yet; let the pipeline run and retry.",
                        );
                    } else {
                        app.toast_success(format!(
                            "Profiled {} operators across {} workers.",
                            report.hotspots.len(),
                            report.worker_count
                        ));
                    }
                    app.report = Some(report);
                    app.report_pipeline = Some(pipeline);
                    app.selected_hotspot = 0;
                }
                Err(error) => app.toast_error(format!("profile: {error}")),
            }
            Vec::new()
        }
        Msg::ActionFinished { action, result } => on_action_finished(app, action, result),
        Msg::DownloadFetched {
            kind,
            pipeline,
            result,
        } => on_download_fetched(app, kind, pipeline, result),
        Msg::FileSaved {
            kind,
            pipeline,
            path,
            result,
        } => {
            let now_millis = app.now_millis;
            let Some(capture) = app.captures_mut(kind).get_mut(&pipeline) else {
                return Vec::new();
            };
            match result {
                Ok(()) => {
                    capture.saved(path.clone(), now_millis);
                    app.toast_success(format!(
                        "{} `{pipeline}`: {} saved to {path}",
                        kind.tag(),
                        kind.noun()
                    ));
                }
                Err(error) => {
                    capture.fail(format!("could not save {path}: {error}"), now_millis);
                    app.toast_error(format!("could not save {path}: {error}"));
                }
            }
            Vec::new()
        }
        Msg::FileRevealed { path, result } => {
            match result {
                Ok(()) => app.toast_success(format!("Showing {path} in the file manager.")),
                Err(error) => app.toast_error(format!("could not show {path}: {error}")),
            }
            Vec::new()
        }
        Msg::SamplyOpened { path, result } => {
            match result {
                Ok(browser) => app.toast_success(format!(
                    "samply load is serving {path}; opened in {browser}."
                )),
                Err(error) => app.toast_error(format!("could not open {path}: {error}")),
            }
            Vec::new()
        }
        Msg::LogLine { pipeline, line } => {
            if app.logs.pipeline.as_deref() == Some(pipeline.as_str()) {
                app.logs.streaming = true;
                app.logs.last_error = None;
                app.logs.push_line(&line);
            }
            Vec::new()
        }
        Msg::LogsClosed { pipeline, result } => {
            if app.logs.pipeline.as_deref() == Some(pipeline.as_str()) {
                app.logs.streaming = false;
                let failed = result.is_err();
                if let Err(error) = result {
                    app.logs.last_error = Some(error.to_string());
                }
                // A gracefully ended stream of a stopped pipeline is the
                // complete history; reconnecting would only duplicate it.
                let live = app
                    .pipelines
                    .iter()
                    .find(|row| row.name == pipeline)
                    .is_some_and(|row| row.is_queryable());
                app.logs.retry_at_millis = if app.view == View::Logs && (failed || live) {
                    app.now_millis + 3_000
                } else {
                    i64::MAX
                };
            }
            Vec::new()
        }
        Msg::DiagnosticsLoaded { pipeline, result } => {
            app.logs.loading_diagnostics = false;
            match result {
                Ok(sections) => {
                    app.logs.diagnostics = sections;
                    app.logs.diagnostics_pipeline = Some(pipeline);
                }
                Err(error) => app.toast_error(format!("diagnostics: {error}")),
            }
            Vec::new()
        }
        Msg::Tick { now_millis } => {
            app.tick(now_millis);
            app.clearing_flash.retain(|_, until| *until > now_millis);
            let mut commands = tick_captures(app, now_millis);
            // Reconnect a dropped log stream while the Logs view watches it.
            if app.view == View::Logs
                && app.logs.pipeline.is_some()
                && !app.logs.streaming
                && now_millis >= app.logs.retry_at_millis
            {
                commands.push(start_log_stream(app));
            }
            commands
        }
    }
}

/// Advance every Samply capture: fetch profiles whose window closed, give up
/// on ones the server never delivers, and fade finished ones.
/// Both download kinds, for code that walks every capture.
const DOWNLOAD_KINDS: [DownloadKind; 2] =
    [DownloadKind::SamplyProfile, DownloadKind::SupportBundle];

fn tick_captures(app: &mut App, now_millis: i64) -> Vec<Cmd> {
    let mut commands = Vec::new();
    let mut gave_up = Vec::new();
    for kind in DOWNLOAD_KINDS {
        for capture in app.captures_mut(kind).values_mut() {
            match capture.tick(now_millis) {
                TickOutcome::Nothing => {}
                TickOutcome::Fetch => commands.push(Cmd::FetchDownload {
                    kind,
                    pipeline: capture.pipeline.clone(),
                    bundle: capture.bundle,
                }),
                TickOutcome::GaveUp => gave_up.push((kind, capture.pipeline.clone())),
            }
        }
        app.captures_mut(kind).retain(|_, capture| {
            capture
                .finished_millis()
                .is_none_or(|finished| now_millis - finished < FINISHED_TTL_MILLIS)
        });
    }
    for (kind, pipeline) in gave_up {
        app.toast_error(format!(
            "{} `{pipeline}`: the server never produced the {}",
            kind.tag(),
            kind.noun()
        ));
    }
    commands
}

fn on_download_fetched(
    app: &mut App,
    kind: DownloadKind,
    pipeline: String,
    result: Result<crate::http::Download, crate::http::ApiError>,
) -> Vec<Cmd> {
    let now_millis = app.now_millis;
    let Some(capture) = app.captures_mut(kind).get_mut(&pipeline) else {
        return Vec::new();
    };
    match capture.on_download(result, now_millis) {
        DownloadOutcome::Save(bytes) => vec![Cmd::SaveDownload {
            kind,
            pipeline,
            bytes,
        }],
        DownloadOutcome::Wait { retry: None } => Vec::new(),
        DownloadOutcome::Wait {
            retry: Some(attempt),
        } => {
            app.toast_info(format!(
                "{} `{pipeline}`: download interrupted; retrying ({attempt}/{MAX_TRANSPORT_RETRIES})…",
                kind.tag()
            ));
            Vec::new()
        }
        DownloadOutcome::Failed => {
            if let CapturePhase::Failed { error, .. } = &capture.phase {
                let error = error.clone();
                app.toast_error(format!("{} `{pipeline}`: {error}", kind.tag()));
            }
            Vec::new()
        }
    }
}

fn on_overview(
    app: &mut App,
    result: Result<crate::gateway::Overview, crate::http::ApiError>,
    latency_millis: u64,
    now_millis: i64,
) -> Vec<Cmd> {
    app.now_millis = now_millis;
    match result {
        Ok(overview) => {
            let was_lost = matches!(app.connection, Connection::Lost { .. });
            app.connection = Connection::Online { latency_millis };
            if was_lost {
                app.toast_success("Reconnected.");
            }
            app.instance = overview.instance;
            app.session = overview.session;
            app.live
                .retain(|name, _| overview.pipelines.iter().any(|row| &row.name == name));
            app.marked
                .retain(|name| overview.pipelines.iter().any(|row| &row.name == name));
            app.samply
                .retain(|name, _| overview.pipelines.iter().any(|row| &row.name == name));
            app.bundles
                .retain(|name, _| overview.pipelines.iter().any(|row| &row.name == name));
            app.pipelines = overview.pipelines;
            // The status payload carries each quota, so the table is rebuilt
            // wholesale: an edit that drops a quota drops its coloring too.
            app.storage_quotas = app
                .pipelines
                .iter()
                .filter_map(|row| {
                    row.storage_quota_bytes
                        .map(|quota| (row.name.clone(), quota))
                })
                .collect();
            app.first_overview_seen = true;
            let before = app.selected_name.clone();
            app.reconcile_selection();
            let mut commands = Vec::new();
            if app.selected_name != before {
                commands.push(Cmd::Watch(app.selected_name.clone()));
                if app.view == View::Logs {
                    commands.extend(ensure_logs(app));
                }
            }
            commands.extend(drain_queues(app));
            commands
        }
        Err(error) => {
            app.note_api_error(&error);
            if !error.is_connection_loss() {
                // A rejected list request (bad key, bad tenant) is fatal for
                // the dashboard; surface it prominently.
                app.connection = Connection::Lost {
                    error: error.to_string(),
                    since_millis: app.now_millis,
                };
            }
            Vec::new()
        }
    }
}

fn on_detail(
    app: &mut App,
    pipeline: String,
    result: Result<crate::gateway::PipelineDetail, crate::http::ApiError>,
    now_millis: i64,
) -> Vec<Cmd> {
    app.now_millis = now_millis;
    if app.selected_name.as_deref() != Some(pipeline.as_str()) {
        return Vec::new();
    }
    match result {
        Ok(detail) => {
            app.detail_error = None;
            app.live
                .entry(pipeline.clone())
                .or_default()
                .observe(&detail.stats, now_millis);
            if detail.series.points.is_empty() {
                // Servers without /time_series still feed the charts.
                app.history.merge(&TimeSeries {
                    points: vec![TimePoint {
                        at_millis: now_millis,
                        processed_records: detail.stats.total_processed_records,
                        memory_bytes: detail.stats.rss_bytes,
                        storage_bytes: detail.stats.storage_bytes,
                    }],
                });
            } else {
                app.history.merge(&detail.series);
            }
            app.detail = Some(detail.stats);
            let connector_count = app.connectors().len();
            app.selected_connector = app
                .selected_connector
                .min(connector_count.saturating_sub(1));
            Vec::new()
        }
        Err(error) => {
            if error.is_connection_loss() {
                app.note_api_error(&error);
            } else if app.selected_name.as_deref() == Some(pipeline.as_str()) {
                app.detail = None;
                app.detail_error = Some(error.to_string());
            }
            Vec::new()
        }
    }
}

fn on_action_finished(
    app: &mut App,
    action: Action,
    result: Result<(), crate::http::ApiError>,
) -> Vec<Cmd> {
    app.pending_action_count = app.pending_action_count.saturating_sub(1);
    if action.is_lifecycle() {
        app.inflight.remove(action.pipeline());
    }
    if let Action::Samply {
        pipeline,
        duration_secs,
    } = &action
    {
        return on_samply_started(app, pipeline, *duration_secs, result);
    }
    match result {
        Ok(()) => {
            app.toast_success(format!("Requested: {}.", action.describe()));
            // The 202 committed the desired status; show it now rather than
            // after the next overview, so the row reacts to the key press.
            let desired = match &action {
                Action::Start { .. } | Action::Resume { .. } | Action::Restart { .. } => {
                    Some("Running")
                }
                Action::Pause { .. } => Some("Paused"),
                Action::Stop { .. } => Some("Stopped"),
                _ => None,
            };
            if let Some(desired) = desired
                && let Some(row) = app
                    .pipelines
                    .iter_mut()
                    .find(|row| row.name == action.pipeline())
            {
                row.desired_status = Some(desired.to_string());
            }
            // The storage burn plays only for clears the server accepted.
            if matches!(action, Action::Clear { .. }) {
                app.clearing_flash.insert(
                    action.pipeline().to_string(),
                    app.now_millis + CLEAR_FLASH_MILLIS,
                );
            }
            // Queues drain on the next overview, never here: right after an
            // action is accepted the cached statuses are stale, and a queued
            // step would fire (or be dropped) against yesterday's state.
            vec![Cmd::Refresh]
        }
        Err(error) => {
            app.toast_error(format!("{}: {error}", action.describe()));
            // A failed step invalidates whatever was planned behind it.
            if let Some(queue) = app.queued.remove(action.pipeline())
                && !queue.is_empty()
            {
                app.toast_error(format!(
                    "{}: {error}; dropped {} queued action(s)",
                    action.describe(),
                    queue.len()
                ));
            }
            Vec::new()
        }
    }
}

fn on_key(app: &mut App, key: KeyPress) -> Vec<Cmd> {
    if app.overlay != Overlay::None {
        return on_overlay_key(app, key);
    }
    if app.prompt != Prompt::Closed {
        return on_prompt_key(app, key);
    }
    if let Some(commands) = on_global_key(app, key) {
        return commands;
    }
    match app.view {
        View::Pipelines => on_pipelines_key(app, key),
        View::Metrics => on_metrics_key(app, key),
        View::Connectors => on_connectors_key(app, key),
        View::Profiler => on_profiler_key(app, key),
        View::Sql => on_sql_key(app, key),
        View::Logs => on_logs_key(app, key),
    }
}

fn on_overlay_key(app: &mut App, key: KeyPress) -> Vec<Cmd> {
    match &mut app.overlay {
        Overlay::None => unreachable!("caller checked for an open overlay"),
        Overlay::Help => {
            app.overlay = Overlay::None;
            Vec::new()
        }
        Overlay::Confirm { actions } => {
            let actions = actions.clone();
            match key {
                KeyPress::Char('y') | KeyPress::Char('Y') | KeyPress::Enter => {
                    app.overlay = Overlay::None;
                    let mut commands = Vec::new();
                    for action in actions {
                        commands.extend(execute_or_queue(app, action));
                    }
                    commands
                }
                KeyPress::Char('n') | KeyPress::Escape | KeyPress::Char('q') => {
                    app.overlay = Overlay::None;
                    app.toast_info("Cancelled.");
                    Vec::new()
                }
                _ => Vec::new(),
            }
        }
        Overlay::BundleSettings { cursor } => {
            let rows = BundleSetting::ALL.len();
            let setting = BundleSetting::ALL[(*cursor).min(rows - 1)];
            match key {
                KeyPress::Up | KeyPress::Char('k') => *cursor = (*cursor + rows - 1) % rows,
                KeyPress::Down | KeyPress::Char('j') => *cursor = (*cursor + 1) % rows,
                KeyPress::Left | KeyPress::Char('h') | KeyPress::Char('-')
                    if setting == BundleSetting::Limit =>
                {
                    step_limit(&mut app.bundle_options, -1)
                }
                KeyPress::Right | KeyPress::Char('l') | KeyPress::Char('+')
                    if setting == BundleSetting::Limit =>
                {
                    step_limit(&mut app.bundle_options, 1)
                }
                KeyPress::Enter if setting == BundleSetting::Download => {
                    app.overlay = Overlay::None;
                    return start_bundle(app);
                }
                KeyPress::Char(' ') | KeyPress::Enter => setting.toggle(&mut app.bundle_options),
                KeyPress::Escape | KeyPress::Char('q') => app.overlay = Overlay::None,
                _ => {}
            }
            Vec::new()
        }
        Overlay::Tenants { selected } => {
            let count = app.session.memberships.len();
            match key {
                KeyPress::Up | KeyPress::Char('k') if count > 0 => {
                    *selected = (*selected + count - 1) % count;
                    Vec::new()
                }
                KeyPress::Down | KeyPress::Char('j') if count > 0 => {
                    *selected = (*selected + 1) % count;
                    Vec::new()
                }
                KeyPress::Enter => {
                    let choice = app.session.memberships.get(*selected).cloned();
                    app.overlay = Overlay::None;
                    match choice {
                        Some(tenant) => {
                            app.toast_info(format!("Switching to tenant `{}`…", tenant.name));
                            vec![Cmd::SwitchTenant(Some(tenant.name)), Cmd::Refresh]
                        }
                        None => Vec::new(),
                    }
                }
                KeyPress::Escape | KeyPress::Char('q') => {
                    app.overlay = Overlay::None;
                    Vec::new()
                }
                _ => Vec::new(),
            }
        }
        Overlay::ConnectorInspect { scroll } | Overlay::TextViewer { scroll, .. } => {
            let max_scroll = app.viewport.overlay_scroll_max.get();
            match key {
                KeyPress::Up | KeyPress::Char('k') | KeyPress::ScrollUp => {
                    *scroll = scroll.saturating_sub(1)
                }
                KeyPress::Down | KeyPress::Char('j') | KeyPress::ScrollDown => {
                    *scroll = scroll.saturating_add(1).min(max_scroll)
                }
                KeyPress::PageUp => *scroll = scroll.saturating_sub(20),
                KeyPress::PageDown => *scroll = scroll.saturating_add(20).min(max_scroll),
                KeyPress::Home | KeyPress::Char('g') => *scroll = 0,
                KeyPress::End | KeyPress::Char('G') => *scroll = max_scroll,
                KeyPress::Escape | KeyPress::Enter | KeyPress::Char('q') => {
                    app.overlay = Overlay::None;
                }
                _ => {}
            }
            Vec::new()
        }
    }
}

fn on_prompt_key(app: &mut App, key: KeyPress) -> Vec<Cmd> {
    match &mut app.prompt {
        Prompt::Closed => unreachable!("caller checked for an open prompt"),
        Prompt::Command {
            input,
            history_index,
        } => match key {
            KeyPress::Char(character) => {
                input.push(character);
                *history_index = None;
                Vec::new()
            }
            KeyPress::Backspace => {
                if input.pop().is_none() {
                    app.prompt = Prompt::Closed;
                }
                Vec::new()
            }
            KeyPress::Tab => {
                if let Some(completed) = complete(input) {
                    *input = completed;
                    input.push(' ');
                }
                Vec::new()
            }
            KeyPress::Up => {
                let history = &app.command_history;
                if history.is_empty() {
                    return Vec::new();
                }
                let next = match history_index {
                    None => history.len() - 1,
                    Some(index) => index.saturating_sub(1),
                };
                *history_index = Some(next);
                *input = history[next].clone();
                Vec::new()
            }
            KeyPress::Down => {
                let history = &app.command_history;
                if let Some(index) = history_index {
                    if *index + 1 < history.len() {
                        *index += 1;
                        *input = history[*index].clone();
                    } else {
                        *history_index = None;
                        input.clear();
                    }
                }
                Vec::new()
            }
            KeyPress::Enter => {
                let line = input.clone();
                app.prompt = Prompt::Closed;
                if line.trim().is_empty() {
                    return Vec::new();
                }
                app.command_history.push(line.clone());
                run_command_line(app, &line)
            }
            KeyPress::Escape => {
                app.prompt = Prompt::Closed;
                Vec::new()
            }
            _ => Vec::new(),
        },
        Prompt::Filter { input } => match key {
            KeyPress::Char(character) => {
                input.push(character);
                app.filter = input.clone();
                app.reconcile_selection();
                Vec::new()
            }
            KeyPress::Backspace => {
                if input.pop().is_none() {
                    app.prompt = Prompt::Closed;
                } else {
                    app.filter = input.clone();
                    app.reconcile_selection();
                }
                Vec::new()
            }
            KeyPress::Enter => {
                app.prompt = Prompt::Closed;
                Vec::new()
            }
            KeyPress::Escape => {
                app.filter.clear();
                app.prompt = Prompt::Closed;
                app.reconcile_selection();
                Vec::new()
            }
            _ => Vec::new(),
        },
    }
}

fn run_command_line(app: &mut App, line: &str) -> Vec<Cmd> {
    match parse(line) {
        Ok(command) => run_command(app, command),
        Err(error) => {
            app.toast_error(error.to_string());
            Vec::new()
        }
    }
}

fn run_command(app: &mut App, command: Command) -> Vec<Cmd> {
    match command {
        Command::Start { pipeline } => {
            lifecycle(app, pipeline, |pipeline| Action::Start { pipeline })
        }
        Command::Pause { pipeline } => {
            lifecycle(app, pipeline, |pipeline| Action::Pause { pipeline })
        }
        Command::Resume { pipeline } => {
            lifecycle(app, pipeline, |pipeline| Action::Resume { pipeline })
        }
        Command::Stop { pipeline, force } => lifecycle(app, pipeline, move |pipeline| {
            Action::Stop { pipeline, force }
        }),
        Command::Restart { pipeline } => {
            lifecycle(app, pipeline, |pipeline| Action::Restart { pipeline })
        }
        Command::Clear { pipeline } => {
            lifecycle(app, pipeline, |pipeline| Action::Clear { pipeline })
        }
        Command::Delete { pipeline } => {
            lifecycle(app, pipeline, |pipeline| Action::Delete { pipeline })
        }
        Command::Profile => set_view(app, View::Profiler),
        Command::Sql => set_view(app, View::Sql),
        Command::Samply { duration_secs } => start_samply(app, duration_secs),
        Command::Bundle => start_bundle(app),
        Command::BundleSettings => {
            app.overlay = Overlay::BundleSettings { cursor: 0 };
            Vec::new()
        }
        Command::ConnectorPause => connector_action(app, true),
        Command::ConnectorResume => connector_action(app, false),
        Command::Tenant { name } => switch_tenant(app, name),
        Command::RefreshEvery { seconds } => {
            let seconds = seconds.clamp(1, 60);
            app.refresh_secs = seconds;
            app.toast_info(format!("Refreshing every {seconds}s."));
            vec![Cmd::SetRefreshSecs(seconds)]
        }
        Command::Filter { text } => {
            app.filter = text;
            app.reconcile_selection();
            Vec::new()
        }
        Command::Sort { column } => {
            match SortColumn::parse(&column) {
                Some(sort) => {
                    app.sort = sort;
                    app.toast_info(format!("Sorting by {}.", sort.title()));
                }
                None => app.toast_error(format!("unknown sort column `{column}`")),
            }
            Vec::new()
        }
        Command::Help => {
            app.overlay = Overlay::Help;
            Vec::new()
        }
        Command::Quit => vec![Cmd::Quit],
    }
}

/// Resolve the action's targets: an explicit name, else the whole
/// multi-selection, else the selected pipeline.
fn lifecycle(
    app: &mut App,
    pipeline: Option<String>,
    build: impl Fn(String) -> Action,
) -> Vec<Cmd> {
    if let Some(target) = pipeline {
        if !app.pipelines.iter().any(|row| row.name == target) {
            app.toast_error(format!("no pipeline named `{target}`"));
            return Vec::new();
        }
        return request_actions(app, vec![build(target)]);
    }
    if !app.marked.is_empty() {
        // Visible order keeps batches deterministic.
        let targets: Vec<String> = app
            .visible_rows()
            .iter()
            .filter(|row| app.marked.contains(&row.name))
            .map(|row| row.name.clone())
            .collect();
        let actions = targets.into_iter().map(build).collect();
        return request_actions(app, actions);
    }
    let Some(target) = app.selected_name.clone() else {
        app.toast_error("Select a pipeline first.");
        return Vec::new();
    };
    request_actions(app, vec![build(target)])
}

fn request_action(app: &mut App, action: Action) -> Vec<Cmd> {
    request_actions(app, vec![action])
}

fn request_actions(app: &mut App, actions: Vec<Action>) -> Vec<Cmd> {
    if actions.is_empty() {
        return Vec::new();
    }
    let must_confirm = actions.iter().any(|action| {
        action.always_confirms() || (app.ask_before_destructive && action.needs_confirmation())
    });
    if must_confirm {
        app.overlay = Overlay::Confirm { actions };
        return Vec::new();
    }
    let count = actions.len();
    let verb = actions[0].verb();
    let mut commands = Vec::new();
    for action in actions {
        commands.extend(execute_or_queue(app, action));
    }
    if count > 1 {
        app.toast_info(format!("Requested {verb} for {count} pipelines."));
    }
    commands
}

/// Run the action now, or queue it behind the pipeline's current activity.
fn execute_or_queue(app: &mut App, action: Action) -> Vec<Cmd> {
    let pipeline = action.pipeline().to_string();
    if action.is_lifecycle() {
        let busy_with_actions = app.inflight.contains_key(&pipeline)
            || app
                .queued
                .get(&pipeline)
                .is_some_and(|queue| !queue.is_empty());
        let row = app
            .pipelines
            .iter()
            .find(|row| row.name == pipeline)
            .cloned();
        // Pause and resume reach the running process, which accepts them
        // from any queryable state; start and clear need a settled server.
        let waiting_for_steady = requires_steady_state(&action)
            && row.as_ref().is_some_and(|row| {
                row.is_transitioning()
                    && !(matches!(action, Action::Pause { .. } | Action::Resume { .. })
                        && row.is_queryable())
            });
        if busy_with_actions || waiting_for_steady {
            enqueue(app, pipeline, action);
            return Vec::new();
        }
        // An action whose goal already holds (clear on cleared storage,
        // start on a running pipeline) is a local no-op, not a server call.
        if let Some(row) = row
            && !row.is_transitioning()
            && let Disposition::Satisfied(reason) = classify(&action, &row)
        {
            app.toast_info(format!("Skipped {}: {reason}.", action.describe()));
            return Vec::new();
        }
    }
    run_now(app, action)
}

/// How long the storage-burn animation is guaranteed to show for a clear.
const CLEAR_FLASH_MILLIS: i64 = 2_100;

fn run_now(app: &mut App, action: Action) -> Vec<Cmd> {
    if action.is_lifecycle() {
        app.inflight
            .insert(action.pipeline().to_string(), action.clone());
    }
    app.pending_action_count += 1;
    vec![Cmd::Run(action)]
}

const MAX_QUEUED_PER_PIPELINE: usize = 4;

/// Queued actions older than this are dropped instead of run: firing a
/// destructive step many minutes after it was requested would surprise.
const QUEUE_EXPIRY_MILLIS: i64 = 10 * 60 * 1_000;

fn enqueue(app: &mut App, pipeline: String, action: Action) {
    let now_millis = app.now_millis;
    let queue = app.queued.entry(pipeline.clone()).or_default();
    if queue.iter().any(|queued| queued.action == action) {
        app.toast_info(format!("Already queued: {}.", action.describe()));
        return;
    }
    if queue.len() >= MAX_QUEUED_PER_PIPELINE {
        app.toast_error(format!(
            "Queue for `{pipeline}` is full ({MAX_QUEUED_PER_PIPELINE} actions)."
        ));
        return;
    }
    let description = action.describe();
    queue.push_back(crate::app::state::QueuedAction {
        action,
        queued_at_millis: now_millis,
    });
    app.toast_info(format!(
        "Queued: {description}; runs when `{pipeline}` settles."
    ));
}

/// Actions that only make sense once the pipeline reached a steady state.
/// Stop and restart may interrupt a transition, so they run immediately.
fn requires_steady_state(action: &Action) -> bool {
    matches!(
        action,
        Action::Start { .. } | Action::Clear { .. } | Action::Pause { .. } | Action::Resume { .. }
    )
}

/// What a settled pipeline state means for a queued action.
enum Disposition {
    /// The state matches the action's precondition: run it.
    Run,
    /// The action's goal already holds; running it would fail or be a no-op.
    Satisfied(&'static str),
    /// Neither: keep waiting. Intermediate steady states are normal (a
    /// checkpointing stop passes through `Suspended` before `Stopped`), so
    /// waiting, not dropping, is the default.
    Wait,
}

fn classify(action: &Action, row: &crate::model::pipeline::PipelineRow) -> Disposition {
    let status = row.deployment_status.as_str();
    match action {
        Action::Start { .. } => match status {
            // The server refuses a start while storage is being cleared.
            "Stopped" if row.storage_status != "Clearing" => Disposition::Run,
            "Running" => Disposition::Satisfied("already running"),
            _ => Disposition::Wait,
        },
        Action::Pause { .. } => match status {
            "Paused" => Disposition::Satisfied("already paused"),
            _ if row.is_queryable() => Disposition::Run,
            _ => Disposition::Wait,
        },
        Action::Resume { .. } => match status {
            "Running" => Disposition::Satisfied("already running"),
            _ if row.is_queryable() => Disposition::Run,
            _ => Disposition::Wait,
        },
        Action::Stop { .. } => match status {
            // A start the server accepted but has not acted on yet still
            // reads `Stopped`; the desired status tells the two apart.
            "Stopped"
                if !row
                    .desired_status
                    .as_deref()
                    .is_some_and(|desired| matches!(desired, "Running" | "Paused")) =>
            {
                Disposition::Satisfied("already stopped")
            }
            _ => Disposition::Run,
        },
        Action::Clear { .. } => {
            if row.storage_status == "Cleared" {
                Disposition::Satisfied("storage already cleared")
            } else if status == "Stopped" {
                Disposition::Run
            } else {
                Disposition::Wait
            }
        }
        Action::Delete { .. } => {
            // The server deletes only pipelines whose resources are stopped,
            // whose desired status is stopped, and whose storage is cleared,
            // so a queued `stop, clear, delete` chain tears down.
            let desired_stopped = row
                .desired_status
                .as_deref()
                .is_none_or(|desired| desired == "Stopped");
            if status == "Stopped" && desired_stopped && row.storage_status == "Cleared" {
                Disposition::Run
            } else {
                Disposition::Wait
            }
        }
        _ => Disposition::Run,
    }
}

/// Launch every queued action whose pipeline settled into the right state.
/// Actions wait through intermediate states, are skipped once their goal
/// already holds, and expire after [`QUEUE_EXPIRY_MILLIS`].
fn drain_queues(app: &mut App) -> Vec<Cmd> {
    let mut commands = Vec::new();
    let now_millis = app.now_millis;
    let pipelines: Vec<String> = app.queued.keys().cloned().collect();
    for pipeline in pipelines {
        if app.inflight.contains_key(&pipeline) {
            continue;
        }
        let Some(row) = app
            .pipelines
            .iter()
            .find(|row| row.name == pipeline)
            .cloned()
        else {
            if let Some(queue) = app.queued.remove(&pipeline)
                && !queue.is_empty()
            {
                // After a delete, vanishing is the success condition.
                if app
                    .inflight
                    .get(&pipeline)
                    .is_some_and(|action| matches!(action, Action::Delete { .. }))
                {
                    app.toast_info(format!("`{pipeline}` deleted."));
                } else {
                    app.toast_error(format!("Dropped queued actions: `{pipeline}` is gone."));
                }
            }
            continue;
        };
        loop {
            let Some(front) = app
                .queued
                .get(&pipeline)
                .and_then(|queue| queue.front())
                .cloned()
            else {
                break;
            };
            // Only actions the server would refuse mid-transition wait for
            // a steady state; a queued stop goes out at the next refresh.
            if row.is_transitioning() && requires_steady_state(&front.action) {
                break;
            }
            let pop = |app: &mut App| {
                app.queued
                    .get_mut(&pipeline)
                    .expect("queue exists")
                    .pop_front();
            };
            if now_millis.saturating_sub(front.queued_at_millis) > QUEUE_EXPIRY_MILLIS {
                pop(app);
                app.toast_error(format!(
                    "Dropped queued {}: waited over {} minutes.",
                    front.action.describe(),
                    QUEUE_EXPIRY_MILLIS / 60_000
                ));
                continue;
            }
            match classify(&front.action, &row) {
                Disposition::Run => {
                    pop(app);
                    app.toast_info(format!("Running queued: {}.", front.action.describe()));
                    commands.extend(run_now(app, front.action));
                    break;
                }
                Disposition::Satisfied(reason) => {
                    pop(app);
                    app.toast_info(format!(
                        "Skipped queued {}: {reason}.",
                        front.action.describe()
                    ));
                }
                Disposition::Wait => break,
            }
        }
    }
    app.queued.retain(|_, queue| !queue.is_empty());
    commands
}

fn connector_action(app: &mut App, pause: bool) -> Vec<Cmd> {
    let Some(pipeline) = app.selected_name.clone() else {
        app.toast_error("Select a pipeline first.");
        return Vec::new();
    };
    let Some(connector) = app.selected_connector() else {
        app.toast_error("No connector selected; open the Connectors view.");
        return Vec::new();
    };
    if connector.kind != ConnectorKind::Input {
        app.toast_error("Only input connectors can be paused or resumed.");
        return Vec::new();
    }
    let table = connector.relation.clone();
    let name = connector.endpoint_name.clone();
    let action = if pause {
        Action::ConnectorPause {
            pipeline,
            table,
            connector: name,
        }
    } else {
        Action::ConnectorResume {
            pipeline,
            table,
            connector: name,
        }
    };
    request_action(app, action)
}

fn switch_tenant(app: &mut App, name: Option<String>) -> Vec<Cmd> {
    match name {
        Some(name) => {
            let known = app.session.memberships.is_empty()
                || app
                    .session
                    .memberships
                    .iter()
                    .any(|membership| membership.name == name);
            if !known {
                app.toast_error(format!("not a member of tenant `{name}`"));
                return Vec::new();
            }
            app.toast_info(format!("Switching to tenant `{name}`…"));
            vec![Cmd::SwitchTenant(Some(name)), Cmd::Refresh]
        }
        None => {
            open_tenant_picker(app);
            Vec::new()
        }
    }
}

fn open_tenant_picker(app: &mut App) {
    if !app.session.has_selectable_tenants() {
        app.toast_info("This identity can act in only one tenant.");
        return;
    }
    let active = app.session.tenant_label();
    let selected = app
        .session
        .memberships
        .iter()
        .position(|membership| membership.name == active)
        .unwrap_or(0);
    app.overlay = Overlay::Tenants { selected };
}

/// Record a Samply CPU profile of every marked pipeline, else the selected one.
fn start_samply(app: &mut App, duration_secs: u64) -> Vec<Cmd> {
    let targets = capture_targets(app);
    let mut commands = Vec::new();
    for pipeline in targets {
        let queryable = app
            .pipelines
            .iter()
            .find(|row| row.name == pipeline)
            .is_some_and(|row| row.is_queryable());
        if !queryable {
            app.toast_error(format!("`{pipeline}` is not running; start it first."));
            continue;
        }
        if app.samply.get(&pipeline).is_some_and(Capture::is_active) {
            app.toast_error(format!(
                "samply `{pipeline}`: a capture is already running."
            ));
            continue;
        }
        app.samply.insert(
            pipeline.clone(),
            Capture::record(pipeline.clone(), duration_secs, app.now_millis),
        );
        app.pending_action_count += 1;
        app.toast_info(format!(
            "samply `{pipeline}`: recording for {duration_secs}s; the profile saves when done."
        ));
        commands.push(Cmd::Run(Action::Samply {
            pipeline,
            duration_secs,
        }));
    }
    commands
}

/// Download a support bundle of every marked pipeline, else the selected
/// one. Any deployment state qualifies: the server bundles what it has.
fn start_bundle(app: &mut App) -> Vec<Cmd> {
    let bundle = app.bundle_options;
    let targets = capture_targets(app);
    let mut commands = Vec::new();
    for pipeline in targets {
        if app.bundles.get(&pipeline).is_some_and(Capture::is_active) {
            app.toast_error(format!(
                "bundle `{pipeline}`: a download is already running."
            ));
            continue;
        }
        app.bundles.insert(
            pipeline.clone(),
            Capture::download(
                DownloadKind::SupportBundle,
                pipeline.clone(),
                bundle,
                app.now_millis,
            ),
        );
        app.toast_info(format!(
            "bundle `{pipeline}`: collecting the support bundle; it saves when done."
        ));
        commands.push(Cmd::FetchDownload {
            kind: DownloadKind::SupportBundle,
            pipeline,
            bundle,
        });
    }
    commands
}

/// The marked pipelines in visible order, else the selected one; toasts
/// when there is neither.
fn capture_targets(app: &mut App) -> Vec<String> {
    if !app.marked.is_empty() {
        return app
            .visible_rows()
            .iter()
            .filter(|row| app.marked.contains(&row.name))
            .map(|row| row.name.clone())
            .collect();
    }
    match app.selected_name.clone() {
        Some(pipeline) => vec![pipeline],
        None => {
            app.toast_error("Select a pipeline first.");
            Vec::new()
        }
    }
}

/// The server answered the request that starts a recording.
fn on_samply_started(
    app: &mut App,
    pipeline: &str,
    duration_secs: u64,
    result: Result<(), crate::http::ApiError>,
) -> Vec<Cmd> {
    let now_millis = app.now_millis;
    let Some(capture) = app.samply.get_mut(pipeline) else {
        return Vec::new();
    };
    match result {
        Ok(()) => {
            capture.accept();
            app.toast_success(format!(
                "samply `{pipeline}`: profiler attached, recording {duration_secs}s."
            ));
        }
        Err(error) => {
            capture.fail(error.to_string(), now_millis);
            app.toast_error(format!("samply `{pipeline}`: {error}"));
        }
    }
    Vec::new()
}

/// Open a pipeline's saved Samply profile in the browser; `None` picks the
/// capture the status line shows.
/// Show a pipeline's saved download: a profile opens in the browser through
/// `samply load`, a support bundle is revealed in the file manager.
fn open_capture(app: &mut App, kind: DownloadKind, pipeline: String) -> Vec<Cmd> {
    let tag = kind.tag();
    let noun = kind.noun();
    let phase = app
        .captures(kind)
        .get(&pipeline)
        .map(|capture| capture.phase.clone());
    match phase {
        Some(CapturePhase::Saved { path, .. }) => match kind {
            DownloadKind::SamplyProfile => {
                app.toast_info(format!("Opening {path} with samply load…"));
                vec![Cmd::OpenSamply { path }]
            }
            DownloadKind::SupportBundle => {
                app.toast_info(format!("Showing {path} in the file manager…"));
                vec![Cmd::RevealFile { path }]
            }
        },
        Some(CapturePhase::Failed { .. }) => {
            app.toast_error(format!(
                "{tag} `{pipeline}`: the last download failed; nothing to show."
            ));
            Vec::new()
        }
        Some(_) => {
            app.toast_info(format!("{tag} `{pipeline}`: the {noun} is not ready yet."));
            Vec::new()
        }
        None => {
            app.toast_error(format!("No {noun} for `{pipeline}`; run :{tag} first."));
            Vec::new()
        }
    }
}

/// `open_capture` for the selected pipeline, or a nudge when there is none.
fn open_selected_capture(app: &mut App, kind: DownloadKind) -> Vec<Cmd> {
    match app.selected_name.clone() {
        Some(pipeline) => open_capture(app, kind, pipeline),
        None => {
            app.toast_error("Select a pipeline first.");
            Vec::new()
        }
    }
}

/// Keys shared by all views. Returns `None` when the key is view-specific.
fn on_global_key(app: &mut App, key: KeyPress) -> Option<Vec<Cmd>> {
    match key {
        KeyPress::Click { row, column } => {
            // The status line names a saved profile; clicking it opens the
            // profile. Other clicks there mean nothing.
            if app.viewport.status_line_row.get() == Some(row) {
                let saved = app.capture_spotlight().and_then(|capture| {
                    matches!(capture.phase, CapturePhase::Saved { .. })
                        .then(|| (capture.kind, capture.pipeline.clone()))
                });
                return Some(match saved {
                    Some((kind, pipeline)) => open_capture(app, kind, pipeline),
                    None => Vec::new(),
                });
            }
            // The bottom tab bar switches views from anywhere; every other
            // click belongs to the active view.
            let view = app.viewport.view_at(row, column)?;
            Some(set_view(app, view))
        }
        KeyPress::Char(':') => {
            app.prompt = Prompt::Command {
                input: String::new(),
                history_index: None,
            };
            Some(Vec::new())
        }
        KeyPress::Char('/') => {
            app.prompt = Prompt::Filter {
                input: app.filter.clone(),
            };
            Some(Vec::new())
        }
        KeyPress::Char('?') => {
            app.overlay = Overlay::Help;
            Some(Vec::new())
        }
        KeyPress::Char('r') => Some(vec![Cmd::Refresh]),
        KeyPress::Char('T') => {
            open_tenant_picker(app);
            Some(Vec::new())
        }
        KeyPress::Char(digit @ '1'..='7') => {
            View::from_digit(digit).map(|view| set_view(app, view))
        }
        KeyPress::Char('l') => Some(set_view(app, View::Logs)),
        KeyPress::Tab => Some(set_view(app, app.view.next(false))),
        KeyPress::BackTab => Some(set_view(app, app.view.next(true))),
        KeyPress::Char('q') => {
            if app.view == View::Pipelines {
                Some(vec![Cmd::Quit])
            } else {
                Some(set_view(app, View::Pipelines))
            }
        }
        KeyPress::Escape => {
            if !app.marked.is_empty() {
                clear_marks(app);
                Some(Vec::new())
            } else if app.view != View::Pipelines {
                Some(set_view(app, View::Pipelines))
            } else if !app.filter.is_empty() {
                app.filter.clear();
                app.reconcile_selection();
                Some(Vec::new())
            } else {
                Some(Vec::new())
            }
        }
        _ => None,
    }
}

/// Switch views, kicking off any fetch the target view depends on and
/// stopping the log stream when the Logs view is left.
pub fn set_view(app: &mut App, view: View) -> Vec<Cmd> {
    let previous = app.view;
    app.view = view;
    let mut commands = Vec::new();
    if previous == View::Logs && view != View::Logs {
        app.logs.streaming = false;
        app.logs.retry_at_millis = i64::MAX;
        commands.push(Cmd::StopLogs);
    }
    match view {
        View::Sql => commands.extend(ensure_sql(app)),
        View::Profiler => commands.extend(ensure_report(app)),
        View::Logs => commands.extend(ensure_logs(app)),
        _ => {}
    }
    commands
}

/// Point the Logs view at the selected pipeline: stream its logs and fetch
/// its diagnostics, reusing whatever is already current.
fn ensure_logs(app: &mut App) -> Vec<Cmd> {
    let Some(selected) = app.selected_name.clone() else {
        return Vec::new();
    };
    let mut commands = Vec::new();
    if app.logs.pipeline.as_deref() != Some(selected.as_str()) || !app.logs.streaming {
        if app.logs.pipeline.as_deref() != Some(selected.as_str()) {
            app.logs.reset(selected.clone());
        }
        commands.push(start_log_stream(app));
    }
    let diagnostics_fresh = app.logs.diagnostics_pipeline.as_deref() == Some(selected.as_str());
    if !diagnostics_fresh && !app.logs.loading_diagnostics {
        app.logs.loading_diagnostics = true;
        app.logs.diagnostics.clear();
        commands.push(Cmd::FetchDiagnostics(selected));
    }
    commands
}

/// (Re)connect the stream for the pipeline the Logs view points at. The
/// server replays retained history on connect, so the buffer starts fresh.
fn start_log_stream(app: &mut App) -> Cmd {
    app.logs.lines.clear();
    app.logs.streaming = true;
    app.logs.retry_at_millis = app.now_millis + 3_000;
    Cmd::StartLogs(app.logs.pipeline.clone().expect("logs view has a pipeline"))
}

fn on_logs_key(app: &mut App, key: KeyPress) -> Vec<Cmd> {
    let last_row = app.logs.row_count().saturating_sub(1);
    let current = app.logs.scroll.unwrap_or(last_row);
    match key {
        KeyPress::Up | KeyPress::Char('k') | KeyPress::ScrollUp => {
            app.logs.scroll = Some(current.saturating_sub(1));
        }
        KeyPress::Down | KeyPress::Char('j') | KeyPress::ScrollDown => {
            let next = (current + 1).min(last_row);
            // Scrolling back to the end re-arms follow mode.
            app.logs.scroll = (next < last_row).then_some(next);
        }
        KeyPress::PageUp => app.logs.scroll = Some(current.saturating_sub(20)),
        KeyPress::PageDown => {
            let next = (current + 20).min(last_row);
            app.logs.scroll = (next < last_row).then_some(next);
        }
        KeyPress::Home | KeyPress::Char('g') => app.logs.scroll = Some(0),
        KeyPress::End | KeyPress::Char('G') => app.logs.scroll = None,
        _ => {}
    }
    Vec::new()
}

fn ensure_sql(app: &mut App) -> Vec<Cmd> {
    let Some(selected) = app.selected_name.clone() else {
        return Vec::new();
    };
    let fresh = app
        .sql
        .as_ref()
        .is_some_and(|document| document.pipeline == selected);
    if fresh || app.loading_sql {
        return Vec::new();
    }
    app.loading_sql = true;
    vec![Cmd::FetchSql(selected)]
}

fn ensure_report(app: &mut App) -> Vec<Cmd> {
    let Some(selected) = app.selected_name.clone() else {
        return Vec::new();
    };
    let fresh = app.report_pipeline.as_deref() == Some(selected.as_str());
    if fresh || app.loading_report {
        return Vec::new();
    }
    fetch_report(app, selected)
}

fn fetch_report(app: &mut App, pipeline: String) -> Vec<Cmd> {
    let queryable = app
        .pipelines
        .iter()
        .any(|row| row.name == pipeline && row.is_queryable());
    if !queryable {
        app.toast_error(format!(
            "`{pipeline}` is not running; profiles need a live circuit."
        ));
        return Vec::new();
    }
    app.loading_report = true;
    // SQL is needed alongside the report for the heat view.
    let mut commands = vec![Cmd::FetchHotspots(pipeline)];
    commands.extend(ensure_sql(app));
    commands
}

/// Move the cursor by `delta` rows and watch the pipeline it lands on. Any
/// move without shift ends a shift run; the marks it made stay.
fn move_and_watch(app: &mut App, delta: isize) -> Vec<Cmd> {
    app.mark_anchor = None;
    let before = app.selected_name.clone();
    app.move_selection(delta);
    watch_if_changed(app, before)
}

fn watch_if_changed(app: &App, before: Option<String>) -> Vec<Cmd> {
    if app.selected_name != before {
        vec![Cmd::Watch(app.selected_name.clone())]
    } else {
        Vec::new()
    }
}

fn clear_marks(app: &mut App) {
    app.marked.clear();
    app.mark_anchor = None;
}

/// Shift plus arrow: move the cursor and rewrite the run's span with
/// spreadsheet semantics. The anchor is where shift was first pressed and
/// the span is every row between it and the cursor, so reversing direction
/// shrinks it and crossing the anchor flips it. Only the span is rewritten;
/// marks made elsewhere with space stay, which is how a keyboard-only
/// console builds a selection that is not one contiguous block.
fn extend_range(app: &mut App, delta: isize) -> Vec<Cmd> {
    // A refresh or filter can drop the anchor row; the run restarts here.
    let anchor = app
        .mark_anchor
        .take()
        .filter(|anchor| app.visible_rows().iter().any(|row| &row.name == anchor))
        .or_else(|| app.selected_name.clone());
    let Some(anchor) = anchor else {
        return Vec::new();
    };
    let previous_span = app
        .selected_name
        .as_deref()
        .map(|cursor| app.range_between(&anchor, cursor))
        .unwrap_or_default();
    let commands = move_and_watch(app, delta);
    let span = app
        .selected_name
        .as_deref()
        .map(|cursor| app.range_between(&anchor, cursor))
        .unwrap_or_default();
    app.marked.retain(|name| !previous_span.contains(name));
    app.marked.extend(span);
    app.mark_anchor = Some(anchor);
    commands
}

/// One page of movement: the height the table actually rendered with, or a
/// sane default before the first frame.
fn page_size(viewport: (usize, usize)) -> isize {
    let (_, height) = viewport;
    if height == 0 { 10 } else { height as isize }
}

fn select_edge(app: &mut App, top: bool) -> Vec<Cmd> {
    app.mark_anchor = None;
    let names: Vec<String> = app
        .visible_rows()
        .iter()
        .map(|row| row.name.clone())
        .collect();
    let target = if top {
        names.first().cloned()
    } else {
        names.last().cloned()
    };
    let Some(target) = target else {
        return Vec::new();
    };
    let before = app.selected_name.clone();
    app.select_pipeline(target);
    watch_if_changed(app, before)
}

/// Map a click at terminal `row` to an absolute table index, given the
/// `(offset, height)` viewport the table rendered with. `None` when the
/// click hit chrome or empty space.
fn clicked_index(row: u16, count: usize, viewport: (usize, usize)) -> Option<usize> {
    let (offset, height) = viewport;
    let visible = row.checked_sub(FIRST_TABLE_ROW)? as usize;
    let index = offset + visible;
    (visible < height && index < count).then_some(index)
}

/// Toggle the mark on the currently selected pipeline.
fn toggle_mark(app: &mut App) {
    let Some(selected) = app.selected_name.clone() else {
        return;
    };
    if !app.marked.remove(&selected) {
        app.marked.insert(selected);
    }
}

/// The pipeline under a click at terminal `row`, if the click hit a table
/// row rather than chrome or empty space.
fn clicked_name(app: &App, row: u16) -> Option<String> {
    let names: Vec<&str> = app
        .visible_rows()
        .iter()
        .map(|visible| visible.name.as_str())
        .collect();
    let index = clicked_index(row, names.len(), app.viewport.pipelines.get())?;
    Some(names[index].to_string())
}

/// A double click is a second click on the same row within this window; the
/// tick cadence adds up to 150ms of jitter to the timestamps.
const DOUBLE_CLICK_MILLIS: i64 = 500;

fn on_pipelines_key(app: &mut App, key: KeyPress) -> Vec<Cmd> {
    match key {
        KeyPress::Up | KeyPress::Char('k') => move_and_watch(app, -1),
        KeyPress::Down | KeyPress::Char('j') => move_and_watch(app, 1),
        KeyPress::ScrollUp => move_and_watch(app, -1),
        KeyPress::ScrollDown => move_and_watch(app, 1),
        KeyPress::Home | KeyPress::Char('g') => select_edge(app, true),
        KeyPress::End | KeyPress::Char('G') => select_edge(app, false),
        KeyPress::PageUp => {
            let page = page_size(app.viewport.pipelines.get());
            move_and_watch(app, -page)
        }
        KeyPress::PageDown => {
            let page = page_size(app.viewport.pipelines.get());
            move_and_watch(app, page)
        }
        KeyPress::Char(' ') => {
            // Toggle in place, like k9s; the mark survives plain movement.
            toggle_mark(app);
            Vec::new()
        }
        KeyPress::ShiftDown => extend_range(app, 1),
        KeyPress::ShiftUp => extend_range(app, -1),
        KeyPress::Click { row, column } => {
            // A click on the header row sorts by that column, toggling the
            // direction on repeat clicks.
            if row == FIRST_TABLE_ROW - 1 {
                app.last_click = None;
                if let Some(sort) = app.viewport.sort_at(column) {
                    if app.sort == sort {
                        app.sort_descending = !app.sort_descending;
                    } else {
                        app.sort = sort;
                        app.sort_descending = false;
                    }
                    app.toast_info(format!(
                        "Sorting by {} {}.",
                        app.sort.title(),
                        if app.sort_descending {
                            "descending"
                        } else {
                            "ascending"
                        }
                    ));
                }
                return Vec::new();
            }
            let Some(name) = clicked_name(app, row) else {
                app.last_click = None;
                return Vec::new();
            };
            let double = app.last_click.is_some_and(|(last_row, at)| {
                last_row == row && app.now_millis.saturating_sub(at) <= DOUBLE_CLICK_MILLIS
            });
            app.mark_anchor = None;
            let before = app.selected_name.clone();
            app.select_pipeline(name);
            let mut commands = watch_if_changed(app, before);
            if double {
                app.last_click = None;
                if let Some(target) = app.viewport.target_at(column) {
                    use crate::app::state::ClickTarget;
                    let view = match target {
                        ClickTarget::Sql => Some(View::Sql),
                        ClickTarget::Metrics => Some(View::Metrics),
                        ClickTarget::Logs => Some(View::Logs),
                        ClickTarget::Samply | ClickTarget::Bundle => None,
                    };
                    let kind = match target {
                        ClickTarget::Bundle => DownloadKind::SupportBundle,
                        _ => DownloadKind::SamplyProfile,
                    };
                    match (view, app.selected_name.clone()) {
                        (Some(view), _) => commands.extend(set_view(app, view)),
                        (None, Some(pipeline)) => {
                            commands.extend(open_capture(app, kind, pipeline));
                        }
                        (None, None) => {}
                    }
                }
            } else {
                app.last_click = Some((row, app.now_millis));
            }
            commands
        }
        KeyPress::Enter => {
            clear_marks(app);
            // A broken, undeployed pipeline has no metrics; its story is in
            // the logs and compile errors.
            let broken = app.selected_pipeline().is_some_and(|row| {
                row.health() == crate::model::pipeline::Health::Bad && !row.is_queryable()
            });
            if broken {
                set_view(app, View::Logs)
            } else {
                set_view(app, View::Metrics)
            }
        }
        KeyPress::Char('s') => lifecycle(app, None, |pipeline| Action::Start { pipeline }),
        KeyPress::Char('p') => lifecycle(app, None, |pipeline| Action::Pause { pipeline }),
        KeyPress::Char('P') => lifecycle(app, None, |pipeline| Action::Resume { pipeline }),
        KeyPress::Char('x') => lifecycle(app, None, |pipeline| Action::Stop {
            pipeline,
            force: false,
        }),
        KeyPress::Char('X') => lifecycle(app, None, |pipeline| Action::Stop {
            pipeline,
            force: true,
        }),
        KeyPress::Char('C') => lifecycle(app, None, |pipeline| Action::Clear { pipeline }),
        KeyPress::Char('f') => set_view(app, View::Profiler),
        KeyPress::Char('v') => set_view(app, View::Sql),
        KeyPress::Char('b') => open_selected_capture(app, DownloadKind::SamplyProfile),
        KeyPress::Char('B') => open_selected_capture(app, DownloadKind::SupportBundle),
        KeyPress::Char('o') => {
            app.sort = app.sort.cycle();
            app.toast_info(format!("Sorting by {}.", app.sort.title()));
            Vec::new()
        }
        KeyPress::Char('O') => {
            app.sort_descending = !app.sort_descending;
            Vec::new()
        }
        KeyPress::Char('e') => {
            let error = app
                .selected_pipeline()
                .and_then(|row| row.deployment_error.clone());
            match error {
                Some(body) => {
                    app.overlay = Overlay::TextViewer {
                        title: "DEPLOYMENT ERROR".to_string(),
                        body,
                        scroll: 0,
                    };
                }
                None => app.toast_info("No deployment error on this pipeline."),
            }
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn on_metrics_key(app: &mut App, key: KeyPress) -> Vec<Cmd> {
    match key {
        KeyPress::Up | KeyPress::Char('k') => move_and_watch(app, -1),
        KeyPress::Down | KeyPress::Char('j') => move_and_watch(app, 1),
        KeyPress::ScrollUp => move_and_watch(app, -1),
        KeyPress::ScrollDown => move_and_watch(app, 1),
        KeyPress::PageUp => {
            let page = page_size(app.viewport.pipelines.get());
            move_and_watch(app, -page)
        }
        KeyPress::PageDown => {
            let page = page_size(app.viewport.pipelines.get());
            move_and_watch(app, page)
        }
        KeyPress::Enter => set_view(app, View::Connectors),
        KeyPress::Char('f') => set_view(app, View::Profiler),
        _ => Vec::new(),
    }
}

fn on_connectors_key(app: &mut App, key: KeyPress) -> Vec<Cmd> {
    let count = app.connectors().len();
    match key {
        KeyPress::Up | KeyPress::Char('k') if count > 0 => {
            app.selected_connector = (app.selected_connector + count - 1) % count;
            Vec::new()
        }
        KeyPress::Down | KeyPress::Char('j') if count > 0 => {
            app.selected_connector = (app.selected_connector + 1) % count;
            Vec::new()
        }
        KeyPress::ScrollUp if count > 0 => {
            app.selected_connector = app.selected_connector.saturating_sub(1);
            Vec::new()
        }
        KeyPress::ScrollDown if count > 0 => {
            app.selected_connector = (app.selected_connector + 1).min(count - 1);
            Vec::new()
        }
        KeyPress::PageUp if count > 0 => {
            let page = page_size(app.viewport.connectors.get()) as usize;
            app.selected_connector = app.selected_connector.saturating_sub(page);
            Vec::new()
        }
        KeyPress::PageDown if count > 0 => {
            let page = page_size(app.viewport.connectors.get()) as usize;
            app.selected_connector = (app.selected_connector + page).min(count - 1);
            Vec::new()
        }
        KeyPress::Click { row, .. } => {
            if let Some(index) = clicked_index(row, count, app.viewport.connectors.get()) {
                app.selected_connector = index;
            }
            Vec::new()
        }
        KeyPress::Char('p') => connector_action(app, true),
        KeyPress::Char('P') => connector_action(app, false),
        KeyPress::Enter if count > 0 => {
            app.overlay = Overlay::ConnectorInspect { scroll: 0 };
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn on_profiler_key(app: &mut App, key: KeyPress) -> Vec<Cmd> {
    let count = app
        .report
        .as_ref()
        .map_or(0, |report| report.hotspots.len());
    match key {
        KeyPress::Up | KeyPress::Char('k') if count > 0 => {
            app.selected_hotspot = (app.selected_hotspot + count - 1) % count;
            Vec::new()
        }
        KeyPress::Down | KeyPress::Char('j') if count > 0 => {
            app.selected_hotspot = (app.selected_hotspot + 1) % count;
            Vec::new()
        }
        KeyPress::ScrollUp if count > 0 => {
            app.selected_hotspot = app.selected_hotspot.saturating_sub(1);
            Vec::new()
        }
        KeyPress::ScrollDown if count > 0 => {
            app.selected_hotspot = (app.selected_hotspot + 1).min(count - 1);
            Vec::new()
        }
        KeyPress::PageUp if count > 0 => {
            let page = page_size(app.viewport.hotspots.get()) as usize;
            app.selected_hotspot = app.selected_hotspot.saturating_sub(page);
            Vec::new()
        }
        KeyPress::PageDown if count > 0 => {
            let page = page_size(app.viewport.hotspots.get()) as usize;
            app.selected_hotspot = (app.selected_hotspot + page).min(count - 1);
            Vec::new()
        }
        KeyPress::Click { row, .. } => {
            if let Some(index) = clicked_index(row, count, app.viewport.hotspots.get()) {
                app.selected_hotspot = index;
            }
            Vec::new()
        }
        KeyPress::Char('m') => {
            app.metric = app.metric.next();
            app.selected_hotspot = 0;
            Vec::new()
        }
        KeyPress::Char('f') => {
            let Some(selected) = app.selected_name.clone() else {
                return Vec::new();
            };
            if app.loading_report {
                return Vec::new();
            }
            fetch_report(app, selected)
        }
        KeyPress::Enter if count > 0 => {
            let target = app.report.as_ref().and_then(|report| {
                let order = report.ranking(app.metric);
                let index = *order.get(app.selected_hotspot)?;
                let hotspot = &report.hotspots[index];
                let first = hotspot.first_line()?;
                let last = hotspot
                    .spans
                    .iter()
                    .map(|span| span.end_line)
                    .max()
                    .unwrap_or(first);
                Some((first, last))
            });
            match target {
                Some((first, last)) => {
                    app.sql_focus = Some((first, last));
                    app.sql_scroll = first.saturating_sub(4);
                    set_view(app, View::Sql)
                }
                None => {
                    app.toast_info("This operator has no SQL position information.");
                    Vec::new()
                }
            }
        }
        _ => Vec::new(),
    }
}

fn on_sql_key(app: &mut App, key: KeyPress) -> Vec<Cmd> {
    let line_count = app.sql.as_ref().map_or(0, SqlDocument::line_count);
    let max_scroll = line_count.saturating_sub(1);
    match key {
        KeyPress::Up | KeyPress::Char('k') | KeyPress::ScrollUp => {
            app.sql_scroll = app.sql_scroll.saturating_sub(1)
        }
        KeyPress::Down | KeyPress::Char('j') | KeyPress::ScrollDown => {
            app.sql_scroll = (app.sql_scroll + 1).min(max_scroll)
        }
        KeyPress::PageUp => app.sql_scroll = app.sql_scroll.saturating_sub(20),
        KeyPress::PageDown => app.sql_scroll = (app.sql_scroll + 20).min(max_scroll),
        KeyPress::Home | KeyPress::Char('g') => app.sql_scroll = 0,
        KeyPress::End | KeyPress::Char('G') => app.sql_scroll = max_scroll,
        KeyPress::Char('f') => return set_view(app, View::Profiler),
        _ => {}
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::{set_view, update};
    use crate::app::capture::{Capture, FINISHED_TTL_MILLIS};
    use crate::app::msg::{Cmd, KeyPress, Msg};
    use crate::app::state::{App, Connection, Overlay, Prompt, SortColumn, View};
    use crate::gateway::{Action, BundleOptions, DownloadKind, PipelineDetail};
    use crate::http::ApiError;
    use crate::http::Download;
    use crate::model::dataflow::{DataflowIndex, HotspotReport};
    use crate::model::profile::CircuitProfile;
    use crate::testutil::{overview_with, stats_with_processed};
    use serde_json::json;

    fn app_with_overview(names: &[&str]) -> App {
        let mut app = App::new("http://x".to_string(), 2);
        update(
            &mut app,
            Msg::Overview {
                result: Ok(overview_with(names)),
                latency_millis: 5,
                now_millis: 1_000,
            },
        );
        app
    }

    fn press(app: &mut App, key: KeyPress) -> Vec<Cmd> {
        update(app, Msg::Key(key))
    }

    fn marked(app: &App) -> Vec<&str> {
        let mut names: Vec<&str> = app.marked.iter().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    fn anchor(app: &App) -> Option<&str> {
        app.mark_anchor.as_deref()
    }

    fn type_command(app: &mut App, line: &str) -> Vec<Cmd> {
        press(app, KeyPress::Char(':'));
        for character in line.chars() {
            press(app, KeyPress::Char(character));
        }
        press(app, KeyPress::Enter)
    }

    /// Complete the in-flight action so the next one runs immediately.
    fn finish(app: &mut App, action: Action) -> Vec<Cmd> {
        update(
            app,
            Msg::ActionFinished {
                action,
                result: Ok(()),
            },
        )
    }

    fn detail_msg(pipeline: &str, processed: i64, now_millis: i64) -> Msg {
        Msg::Detail {
            pipeline: pipeline.to_string(),
            result: Ok(PipelineDetail {
                stats: stats_with_processed(processed),
                series: Default::default(),
            }),
            now_millis,
        }
    }

    #[test]
    fn the_first_overview_selects_and_watches_a_pipeline() {
        let mut app = App::new("http://x".to_string(), 2);
        let commands = update(
            &mut app,
            Msg::Overview {
                result: Ok(overview_with(&["a", "b"])),
                latency_millis: 9,
                now_millis: 50,
            },
        );
        assert_eq!(commands, vec![Cmd::Watch(Some("a".to_string()))]);
        assert_eq!(app.connection, Connection::Online { latency_millis: 9 });
        assert!(app.first_overview_seen);
    }

    #[test]
    fn overview_failures_mark_the_connection() {
        let mut app = App::new("http://x".to_string(), 2);
        update(
            &mut app,
            Msg::Overview {
                result: Err(ApiError::Transport("refused".to_string())),
                latency_millis: 0,
                now_millis: 1,
            },
        );
        assert!(matches!(app.connection, Connection::Lost { .. }));

        // A rejected request (not transport) is also fatal for the dashboard.
        let mut app = App::new("http://x".to_string(), 2);
        update(
            &mut app,
            Msg::Overview {
                result: Err(ApiError::Status {
                    status: 401,
                    message: "bad key".to_string(),
                }),
                latency_millis: 0,
                now_millis: 1,
            },
        );
        assert!(matches!(app.connection, Connection::Lost { .. }));

        // Recovery announces itself.
        update(
            &mut app,
            Msg::Overview {
                result: Ok(overview_with(&["a"])),
                latency_millis: 2,
                now_millis: 5,
            },
        );
        assert_eq!(app.toast.as_ref().unwrap().message, "Reconnected.");
    }

    #[test]
    fn removed_pipelines_lose_their_live_rows() {
        let mut app = app_with_overview(&["a", "b"]);
        update(
            &mut app,
            Msg::RowStats {
                pipeline: "b".to_string(),
                stats: stats_with_processed(5),
                now_millis: 2_000,
            },
        );
        assert!(app.live.contains_key("b"));
        update(
            &mut app,
            Msg::Overview {
                result: Ok(overview_with(&["a"])),
                latency_millis: 1,
                now_millis: 3_000,
            },
        );
        assert!(!app.live.contains_key("b"));
    }

    #[test]
    fn navigation_keys_move_the_selection_and_rewatch() {
        let mut app = app_with_overview(&["a", "b", "c"]);
        let commands = press(&mut app, KeyPress::Char('j'));
        assert_eq!(commands, vec![Cmd::Watch(Some("b".to_string()))]);
        let commands = press(&mut app, KeyPress::Char('G'));
        assert_eq!(commands, vec![Cmd::Watch(Some("c".to_string()))]);
        let commands = press(&mut app, KeyPress::Char('g'));
        assert_eq!(commands, vec![Cmd::Watch(Some("a".to_string()))]);
        assert!(press(&mut app, KeyPress::Char('g')).is_empty());
        // The ends hold: no wrap-around, so nothing to rewatch.
        assert!(press(&mut app, KeyPress::Up).is_empty());
        assert_eq!(app.selected_name.as_deref(), Some("a"));
    }

    #[test]
    fn every_action_runs_without_dialogs_by_default() {
        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Stopped".to_string();
        let commands = press(&mut app, KeyPress::Char('s'));
        assert_eq!(
            commands,
            vec![Cmd::Run(Action::Start {
                pipeline: "a".to_string()
            })]
        );
        assert_eq!(app.pending_action_count, 1);
        finish(
            &mut app,
            Action::Start {
                pipeline: "a".to_string(),
            },
        );

        app.pipelines[0].deployment_status = "Running".to_string();
        let commands = press(&mut app, KeyPress::Char('x'));
        assert_eq!(
            commands,
            vec![Cmd::Run(Action::Stop {
                pipeline: "a".to_string(),
                force: false
            })]
        );
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn the_ask_flag_gates_destructive_actions_behind_a_dialog() {
        let mut app = app_with_overview(&["a"]);
        app.ask_before_destructive = true;

        // Start is not destructive: still no dialog.
        app.pipelines[0].deployment_status = "Stopped".to_string();
        let commands = press(&mut app, KeyPress::Char('s'));
        assert_eq!(commands.len(), 1);
        assert_eq!(app.overlay, Overlay::None);
        finish(
            &mut app,
            Action::Start {
                pipeline: "a".to_string(),
            },
        );

        app.pipelines[0].deployment_status = "Running".to_string();
        let commands = press(&mut app, KeyPress::Char('x'));
        assert!(commands.is_empty());
        assert!(matches!(app.overlay, Overlay::Confirm { .. }));
        let commands = press(&mut app, KeyPress::Char('y'));
        assert_eq!(
            commands,
            vec![Cmd::Run(Action::Stop {
                pipeline: "a".to_string(),
                force: false
            })]
        );
        assert_eq!(app.overlay, Overlay::None);

        // Declining leaves a toast and runs nothing.
        press(&mut app, KeyPress::Char('X'));
        let commands = press(&mut app, KeyPress::Char('n'));
        assert!(commands.is_empty());
        assert_eq!(app.toast.as_ref().unwrap().message, "Cancelled.");

        // Restart counts as destructive too.
        type_command(&mut app, "restart");
        assert!(matches!(app.overlay, Overlay::Confirm { .. }));
        press(&mut app, KeyPress::Escape);
    }

    #[test]
    fn wheel_scrolling_clamps_at_the_list_ends() {
        let mut app = app_with_overview(&["a", "b", "c"]);
        // At the top already: no wrap to the bottom.
        assert!(press(&mut app, KeyPress::ScrollUp).is_empty());
        assert_eq!(app.selected_name.as_deref(), Some("a"));
        let commands = press(&mut app, KeyPress::ScrollDown);
        assert_eq!(commands, vec![Cmd::Watch(Some("b".to_string()))]);
        // Overscrolling parks at the bottom instead of cycling.
        for _ in 0..10 {
            press(&mut app, KeyPress::ScrollDown);
        }
        assert_eq!(app.selected_name.as_deref(), Some("c"));

        // The metrics view scrolls pipelines the same way.
        app.view = View::Metrics;
        press(&mut app, KeyPress::ScrollUp);
        assert_eq!(app.selected_name.as_deref(), Some("b"));
    }

    #[test]
    fn wheel_scrolling_reaches_lists_and_documents_everywhere() {
        let mut app = app_with_overview(&["a"]);
        update(&mut app, detail_msg("a", 10, 1_000));
        app.view = View::Connectors;
        press(&mut app, KeyPress::ScrollDown);
        assert_eq!(app.selected_connector, 0, "single connector clamps");
        press(&mut app, KeyPress::ScrollUp);
        assert_eq!(app.selected_connector, 0);

        app.view = View::Sql;
        app.sql = Some(crate::app::state::SqlDocument {
            pipeline: "a".to_string(),
            text: "l1\nl2\nl3".to_string(),
        });
        press(&mut app, KeyPress::ScrollDown);
        assert_eq!(app.sql_scroll, 1);
        press(&mut app, KeyPress::ScrollUp);
        assert_eq!(app.sql_scroll, 0);

        app.overlay = Overlay::TextViewer {
            title: "T".to_string(),
            body: "b".to_string(),
            scroll: 0,
        };
        app.viewport.overlay_scroll_max.set(1);
        press(&mut app, KeyPress::ScrollDown);
        assert_eq!(viewer_scroll(&app), 1);
    }

    fn viewer_scroll(app: &App) -> u16 {
        match &app.overlay {
            Overlay::TextViewer { scroll, .. } | Overlay::ConnectorInspect { scroll } => *scroll,
            other => panic!("no text overlay open: {other:?}"),
        }
    }

    #[test]
    fn text_overlays_scroll_no_further_than_the_renderer_reports() {
        let mut app = app_with_overview(&["a"]);
        app.overlay = Overlay::TextViewer {
            title: "T".to_string(),
            body: "b".to_string(),
            scroll: 0,
        };
        // Before the first frame nothing is known to overflow.
        press(&mut app, KeyPress::Down);
        assert_eq!(viewer_scroll(&app), 0);
        app.viewport.overlay_scroll_max.set(3);
        press(&mut app, KeyPress::PageDown);
        assert_eq!(viewer_scroll(&app), 3);
        press(&mut app, KeyPress::Down);
        assert_eq!(viewer_scroll(&app), 3);
        press(&mut app, KeyPress::Up);
        assert_eq!(viewer_scroll(&app), 2);
        press(&mut app, KeyPress::Home);
        assert_eq!(viewer_scroll(&app), 0);
        press(&mut app, KeyPress::End);
        assert_eq!(viewer_scroll(&app), 3);
    }

    #[test]
    fn profiler_wheel_scrolling_clamps_over_hotspots() {
        let mut app = app_with_overview(&["a"]);
        update(
            &mut app,
            Msg::HotspotsLoaded {
                pipeline: "a".to_string(),
                result: Ok(loaded_report()),
            },
        );
        app.view = View::Profiler;
        press(&mut app, KeyPress::ScrollDown);
        assert_eq!(app.selected_hotspot, 0, "one hotspot clamps");
        press(&mut app, KeyPress::ScrollUp);
        assert_eq!(app.selected_hotspot, 0);
    }

    #[test]
    fn restart_runs_directly_without_the_ask_flag() {
        let mut app = app_with_overview(&["a"]);
        assert_eq!(
            type_command(&mut app, "restart"),
            vec![Cmd::Run(Action::Restart {
                pipeline: "a".to_string()
            })]
        );
        // The same restart requested again queues behind the running one,
        // and requesting it a third time is refused as a duplicate.
        assert_eq!(type_command(&mut app, "restart a"), Vec::new());
        assert!(app.toast.as_ref().unwrap().message.contains("Queued"));
        type_command(&mut app, "restart a");
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("Already queued")
        );
    }

    #[test]
    fn the_command_bar_completes_parses_and_remembers() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char(':'));
        for character in "prof".chars() {
            press(&mut app, KeyPress::Char(character));
        }
        press(&mut app, KeyPress::Tab);
        let Prompt::Command { input, .. } = &app.prompt else {
            panic!("command prompt open");
        };
        assert_eq!(input, "profile ");
        press(&mut app, KeyPress::Backspace);
        press(&mut app, KeyPress::Enter);
        assert_eq!(app.view, View::Profiler);
        assert_eq!(app.command_history, vec!["profile".to_string()]);

        // History recall via arrow keys.
        press(&mut app, KeyPress::Char(':'));
        press(&mut app, KeyPress::Up);
        let Prompt::Command { input, .. } = &app.prompt else {
            panic!("command prompt open");
        };
        assert_eq!(input, "profile");
        press(&mut app, KeyPress::Down);
        let Prompt::Command { input, .. } = &app.prompt else {
            panic!("command prompt open");
        };
        assert!(input.is_empty());
        press(&mut app, KeyPress::Escape);
        assert_eq!(app.prompt, Prompt::Closed);
    }

    #[test]
    fn bad_commands_toast_instead_of_acting() {
        let mut app = app_with_overview(&["a"]);
        type_command(&mut app, "warp 9");
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("unknown command")
        );
        type_command(&mut app, "start missing");
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("no pipeline named")
        );
    }

    #[test]
    fn filtering_is_live_and_escape_clears_it() {
        let mut app = app_with_overview(&["orders", "users"]);
        press(&mut app, KeyPress::Char('/'));
        press(&mut app, KeyPress::Char('u'));
        assert_eq!(app.filter, "u");
        assert_eq!(app.selected_name.as_deref(), Some("users"));
        press(&mut app, KeyPress::Enter);
        assert_eq!(app.prompt, Prompt::Closed);
        assert_eq!(app.filter, "u");

        press(&mut app, KeyPress::Char('/'));
        press(&mut app, KeyPress::Escape);
        assert!(app.filter.is_empty());

        // Esc on the dashboard clears a persisted filter too.
        app.filter = "ord".to_string();
        press(&mut app, KeyPress::Escape);
        assert!(app.filter.is_empty());
    }

    #[test]
    fn detail_updates_history_and_errors_stay_local() {
        let mut app = app_with_overview(&["a"]);
        update(&mut app, detail_msg("a", 100, 1_000));
        update(&mut app, detail_msg("a", 200, 2_000));
        assert!(app.detail.is_some());
        assert!(!app.history.is_empty());
        assert!(app.live.contains_key("a"));

        update(
            &mut app,
            Msg::Detail {
                pipeline: "a".to_string(),
                result: Err(ApiError::Status {
                    status: 503,
                    message: "not deployed".to_string(),
                }),
                now_millis: 3_000,
            },
        );
        assert!(app.detail.is_none());
        assert!(app.detail_error.as_ref().unwrap().contains("not deployed"));
        // Stale detail for an unselected pipeline is ignored.
        update(&mut app, detail_msg("other", 5, 4_000));
        assert!(app.detail.is_none());
    }

    fn download_fetched(pipeline: &str, result: Result<Download, ApiError>) -> Msg {
        Msg::DownloadFetched {
            kind: DownloadKind::SamplyProfile,
            pipeline: pipeline.to_string(),
            result,
        }
    }

    #[test]
    fn a_samply_capture_runs_from_command_to_saved_profile() {
        let mut app = app_with_overview(&["a"]);
        let recording = Action::Samply {
            pipeline: "a".to_string(),
            duration_secs: 2,
        };
        assert_eq!(
            type_command(&mut app, "samply 2"),
            vec![Cmd::Run(recording.clone())]
        );
        assert!(app.is_busy());
        assert_eq!(app.samply["a"].label(1_000), "▄▂▁▂ 0s/2s");

        // The window cannot close before the server accepted the request.
        assert!(update(&mut app, Msg::Tick { now_millis: 3_500 }).is_empty());
        finish(&mut app, recording);
        assert!(app.toast.as_ref().unwrap().message.contains("recording 2s"));
        assert_eq!(
            update(&mut app, Msg::Tick { now_millis: 3_600 }),
            vec![Cmd::FetchDownload {
                kind: DownloadKind::SamplyProfile,
                pipeline: "a".to_string(),
                bundle: BundleOptions::default(),
            }]
        );
        assert_eq!(app.samply["a"].label(3_600), "⇣ ▰▱▱▱");

        // Not ready yet: ask again once Retry-After passed.
        let pending = Download::Pending {
            retry_after_secs: 2,
        };
        assert!(update(&mut app, download_fetched("a", Ok(pending))).is_empty());
        assert!(update(&mut app, Msg::Tick { now_millis: 4_000 }).is_empty());
        assert_eq!(
            update(&mut app, Msg::Tick { now_millis: 5_600 }),
            vec![Cmd::FetchDownload {
                kind: DownloadKind::SamplyProfile,
                pipeline: "a".to_string(),
                bundle: BundleOptions::default(),
            }]
        );
        assert_eq!(
            update(
                &mut app,
                download_fetched("a", Ok(Download::Ready(vec![1, 2])))
            ),
            vec![Cmd::SaveDownload {
                kind: DownloadKind::SamplyProfile,
                pipeline: "a".to_string(),
                bytes: vec![1, 2]
            }]
        );
        update(
            &mut app,
            Msg::FileSaved {
                kind: DownloadKind::SamplyProfile,
                pipeline: "a".to_string(),
                path: "./a-samply-1.json.gz".to_string(),
                result: Ok(()),
            },
        );
        assert_eq!(app.samply["a"].label(5_600), "✓ saved");
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("./a-samply-1.json.gz")
        );
        assert!(!app.is_busy());
        update(
            &mut app,
            Msg::Tick {
                now_millis: 5_600 + FINISHED_TTL_MILLIS,
            },
        );
        assert!(app.samply.is_empty(), "finished captures fade");
    }

    #[test]
    fn samply_refuses_stopped_pipelines_and_duplicate_captures() {
        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Stopped".to_string();
        assert!(type_command(&mut app, "samply 5").is_empty());
        assert!(app.toast.as_ref().unwrap().message.contains("not running"));
        assert!(app.samply.is_empty());

        app.pipelines[0].deployment_status = "Running".to_string();
        assert_eq!(type_command(&mut app, "samply 5").len(), 1);
        assert!(type_command(&mut app, "samply 5").is_empty());
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("already running")
        );
        assert_eq!(app.pending_action_count, 1);
    }

    #[test]
    fn a_rejected_recording_fails_the_capture() {
        let mut app = app_with_overview(&["a"]);
        type_command(&mut app, "samply 5");
        update(
            &mut app,
            Msg::ActionFinished {
                action: Action::Samply {
                    pipeline: "a".to_string(),
                    duration_secs: 5,
                },
                result: Err(ApiError::Status {
                    status: 409,
                    message: "already in progress".to_string(),
                }),
            },
        );
        assert_eq!(app.samply["a"].label(0), "✖ failed");
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("already in progress")
        );
        assert_eq!(app.pending_action_count, 0);
        // A failed capture can be replaced right away.
        assert_eq!(type_command(&mut app, "samply 5").len(), 1);
    }

    /// An app whose one-second capture of `a` has closed its window and asked
    /// for the profile.
    fn app_fetching_a_profile() -> App {
        let mut app = app_with_overview(&["a"]);
        type_command(&mut app, "samply 1");
        finish(
            &mut app,
            Action::Samply {
                pipeline: "a".to_string(),
                duration_secs: 1,
            },
        );
        assert_eq!(
            update(&mut app, Msg::Tick { now_millis: 2_000 }),
            vec![Cmd::FetchDownload {
                kind: DownloadKind::SamplyProfile,
                pipeline: "a".to_string(),
                bundle: BundleOptions::default(),
            }]
        );
        app
    }

    #[test]
    fn samply_fetch_errors_and_silence_fail_the_capture() {
        let mut app = app_fetching_a_profile();
        let rejected = ApiError::Status {
            status: 500,
            message: "failed to profile the pipeline using samply".to_string(),
        };
        assert!(update(&mut app, download_fetched("a", Err(rejected))).is_empty());
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("failed to profile")
        );
        assert!(!app.samply["a"].is_active());

        let mut app = app_fetching_a_profile();
        update(
            &mut app,
            Msg::Tick {
                now_millis: 10 * 60 * 1_000,
            },
        );
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("never produced")
        );
        assert!(!app.samply["a"].is_active());
    }

    #[test]
    fn samply_applies_to_every_marked_pipeline() {
        let mut app = app_with_overview(&["a", "b", "c"]);
        press(&mut app, KeyPress::Char(' '));
        press(&mut app, KeyPress::Char('j'));
        press(&mut app, KeyPress::Char('j'));
        press(&mut app, KeyPress::Char(' '));
        assert_eq!(type_command(&mut app, "samply 10").len(), 2);
        assert!(app.samply.contains_key("a"));
        assert!(app.samply.contains_key("c"));
        assert!(!app.samply.contains_key("b"));
    }

    #[test]
    fn captures_of_vanished_pipelines_are_dropped_on_refresh() {
        let mut app = app_with_overview(&["a", "b"]);
        type_command(&mut app, "samply 5");
        update(
            &mut app,
            Msg::Overview {
                result: Ok(overview_with(&["b"])),
                latency_millis: 1,
                now_millis: 2_000,
            },
        );
        assert!(app.samply.is_empty());
    }

    #[test]
    fn b_opens_the_saved_profile_or_explains_why_not() {
        let mut app = app_with_overview(&["a"]);
        assert!(press(&mut app, KeyPress::Char('b')).is_empty());
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("run :samply first")
        );

        type_command(&mut app, "samply 5");
        assert!(press(&mut app, KeyPress::Char('b')).is_empty());
        assert!(app.toast.as_ref().unwrap().message.contains("not ready"));

        let path = "/tmp/a-samply-1.json.gz".to_string();
        app.samply.get_mut("a").unwrap().saved(path.clone(), 2_000);
        assert_eq!(
            press(&mut app, KeyPress::Char('b')),
            vec![Cmd::OpenSamply { path: path.clone() }]
        );

        update(
            &mut app,
            Msg::SamplyOpened {
                path: path.clone(),
                result: Ok("Firefox".to_string()),
            },
        );
        assert!(app.toast.as_ref().unwrap().message.contains("serving"));
        update(
            &mut app,
            Msg::SamplyOpened {
                path,
                result: Err("no samply on PATH".to_string()),
            },
        );
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("no samply on PATH")
        );

        app.samply
            .get_mut("a")
            .unwrap()
            .fail("boom".to_string(), 3_000);
        assert!(press(&mut app, KeyPress::Char('b')).is_empty());
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("nothing to show")
        );

        let mut empty = App::new("http://x".to_string(), 2);
        assert!(press(&mut empty, KeyPress::Char('b')).is_empty());
        assert!(
            empty
                .toast
                .as_ref()
                .unwrap()
                .message
                .contains("Select a pipeline")
        );
    }

    #[test]
    fn clicks_on_the_status_line_and_the_samply_cell_open_the_profile() {
        let mut app = app_with_overview(&["a"]);
        app.viewport.status_line_row.set(Some(43));
        // Nothing saved yet: the click is swallowed.
        assert!(
            press(
                &mut app,
                KeyPress::Click {
                    row: 43,
                    column: 10
                }
            )
            .is_empty()
        );
        let mut capture = Capture::record("a".to_string(), 1, 0);
        capture.saved("/tmp/a.json.gz".to_string(), 1);
        app.samply.insert("a".to_string(), capture);
        let open = vec![Cmd::OpenSamply {
            path: "/tmp/a.json.gz".to_string(),
        }];
        assert_eq!(
            press(
                &mut app,
                KeyPress::Click {
                    row: 43,
                    column: 10
                }
            ),
            open
        );

        // A double click on the SAMPLY cell does the same instead of jumping.
        app.viewport.pipelines.set((0, 20));
        use crate::app::state::{ClickTarget, ColumnHit};
        *app.viewport.click_columns.borrow_mut() = vec![ColumnHit {
            x_start: 1,
            x_end: 30,
            target: ClickTarget::Samply,
            sort: None,
        }];
        app.now_millis = 1_000;
        let row = crate::app::state::FIRST_TABLE_ROW;
        press(&mut app, KeyPress::Click { row, column: 5 });
        assert_eq!(press(&mut app, KeyPress::Click { row, column: 5 }), open);
        assert_eq!(app.view, View::Pipelines);
    }

    #[test]
    fn a_support_bundle_downloads_saves_and_reveals() {
        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Stopped".to_string();
        let fetch = Cmd::FetchDownload {
            kind: DownloadKind::SupportBundle,
            pipeline: "a".to_string(),
            bundle: BundleOptions::default(),
        };
        assert_eq!(type_command(&mut app, "bundle"), vec![fetch]);
        assert_eq!(app.bundles["a"].label(1_000), "⇣ ▱▱▰▱");
        assert!(app.is_busy());
        assert!(type_command(&mut app, "bundle").is_empty(), "one at a time");
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("already running")
        );
        assert!(
            press(&mut app, KeyPress::Char('B')).is_empty(),
            "nothing to reveal yet"
        );
        assert!(app.toast.as_ref().unwrap().message.contains("not ready"));

        let commands = update(
            &mut app,
            Msg::DownloadFetched {
                kind: DownloadKind::SupportBundle,
                pipeline: "a".to_string(),
                result: Ok(Download::Ready(vec![b'P', b'K'])),
            },
        );
        assert_eq!(
            commands,
            vec![Cmd::SaveDownload {
                kind: DownloadKind::SupportBundle,
                pipeline: "a".to_string(),
                bytes: vec![b'P', b'K'],
            }]
        );
        let path = "/tmp/a-support-bundle-1.zip".to_string();
        update(
            &mut app,
            Msg::FileSaved {
                kind: DownloadKind::SupportBundle,
                pipeline: "a".to_string(),
                path: path.clone(),
                result: Ok(()),
            },
        );
        assert_eq!(app.bundles["a"].label(1_000), "✓ saved");
        assert!(app.samply.is_empty(), "profiles and bundles live apart");
        assert!(
            app.capture_spotlight()
                .unwrap()
                .headline(1_000)
                .contains("support bundle saved to")
        );

        // B, the status line, and the BUNDLE cell all reveal the zip.
        let reveal = vec![Cmd::RevealFile { path: path.clone() }];
        assert_eq!(press(&mut app, KeyPress::Char('B')), reveal);
        app.viewport.status_line_row.set(Some(43));
        assert_eq!(
            press(&mut app, KeyPress::Click { row: 43, column: 3 }),
            reveal
        );
        app.viewport.pipelines.set((0, 20));
        use crate::app::state::{ClickTarget, ColumnHit};
        *app.viewport.click_columns.borrow_mut() = vec![ColumnHit {
            x_start: 1,
            x_end: 30,
            target: ClickTarget::Bundle,
            sort: None,
        }];
        let row = crate::app::state::FIRST_TABLE_ROW;
        press(&mut app, KeyPress::Click { row, column: 5 });
        assert_eq!(press(&mut app, KeyPress::Click { row, column: 5 }), reveal);

        update(
            &mut app,
            Msg::FileRevealed {
                path: path.clone(),
                result: Ok(()),
            },
        );
        assert!(app.toast.as_ref().unwrap().message.contains("file manager"));
        update(
            &mut app,
            Msg::FileRevealed {
                path,
                result: Err("no xdg-open".to_string()),
            },
        );
        assert!(app.toast.as_ref().unwrap().message.contains("no xdg-open"));
    }

    #[test]
    fn bundle_failures_and_ticks_speak_of_bundles() {
        let mut app = app_with_overview(&["a"]);
        type_command(&mut app, "bundle");
        update(
            &mut app,
            Msg::DownloadFetched {
                kind: DownloadKind::SupportBundle,
                pipeline: "a".to_string(),
                result: Err(ApiError::Status {
                    status: 500,
                    message: "collector crashed".to_string(),
                }),
            },
        );
        let toast = app.toast.as_ref().unwrap().message.clone();
        assert!(toast.starts_with("bundle `a`:"), "{toast}");
        assert!(toast.contains("collector crashed"), "{toast}");
        assert!(press(&mut app, KeyPress::Char('B')).is_empty());
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("nothing to show")
        );

        let mut app = app_with_overview(&["a"]);
        type_command(&mut app, "bundle");
        update(
            &mut app,
            Msg::Tick {
                now_millis: 10 * 60 * 1_000,
            },
        );
        let toast = app.toast.as_ref().unwrap().message.clone();
        assert!(
            toast.contains("never produced the support bundle"),
            "{toast}"
        );

        let mut empty = App::new("http://x".to_string(), 2);
        assert!(type_command(&mut empty, "bundle").is_empty());
        assert!(press(&mut empty, KeyPress::Char('B')).is_empty());
        assert!(
            empty
                .toast
                .as_ref()
                .unwrap()
                .message
                .contains("Select a pipeline")
        );
    }

    #[test]
    fn quotas_come_with_the_status_payload() {
        let mut app = app_with_overview(&["a", "b"]);
        let mut overview = overview_with(&["a", "b"]);
        overview.pipelines[0].storage_quota_bytes = Some(9);
        update(
            &mut app,
            Msg::Overview {
                result: Ok(overview),
                latency_millis: 1,
                now_millis: 2_000,
            },
        );
        assert_eq!(app.storage_quotas.get("a"), Some(&9));
        assert_eq!(app.storage_quotas.get("b"), None);
        // An edit that removes the quota removes the coloring with it.
        update(
            &mut app,
            Msg::Overview {
                result: Ok(overview_with(&["a", "b"])),
                latency_millis: 1,
                now_millis: 3_000,
            },
        );
        assert!(app.storage_quotas.is_empty());
    }

    #[test]
    fn a_stop_queued_behind_a_start_fires_while_still_provisioning() {
        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Stopped".to_string();
        let start = Action::Start {
            pipeline: "a".to_string(),
        };
        assert_eq!(
            press(&mut app, KeyPress::Char('s')),
            vec![Cmd::Run(start.clone())]
        );
        // The start is still in flight, so the stop waits behind it.
        assert!(press(&mut app, KeyPress::Char('x')).is_empty());
        assert_eq!(app.queued_label("a").as_deref(), Some("stop"));
        finish(&mut app, start);

        // The next overview finds the pipeline provisioning; the server takes
        // a stop in any state, so the queued stop goes out at once.
        let mut overview = overview_with(&["a"]);
        overview.pipelines[0].deployment_status = "Provisioning".to_string();
        overview.pipelines[0].desired_status = Some("Running".to_string());
        let commands = update(
            &mut app,
            Msg::Overview {
                result: Ok(overview),
                latency_millis: 1,
                now_millis: 3_000,
            },
        );
        assert!(commands.contains(&Cmd::Run(Action::Stop {
            pipeline: "a".to_string(),
            force: false,
        })));
        assert!(app.queued_label("a").is_none());
    }

    #[test]
    fn a_queued_stop_is_not_skipped_when_a_start_was_accepted_but_not_acted_on() {
        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Stopped".to_string();
        let start = Action::Start {
            pipeline: "a".to_string(),
        };
        press(&mut app, KeyPress::Char('s'));
        press(&mut app, KeyPress::Char('x'));
        finish(&mut app, start);
        // Still `Stopped`, but the desired status shows the accepted start.
        let mut overview = overview_with(&["a"]);
        overview.pipelines[0].deployment_status = "Stopped".to_string();
        overview.pipelines[0].desired_status = Some("Running".to_string());
        let commands = update(
            &mut app,
            Msg::Overview {
                result: Ok(overview),
                latency_millis: 1,
                now_millis: 3_000,
            },
        );
        assert!(commands.contains(&Cmd::Run(Action::Stop {
            pipeline: "a".to_string(),
            force: false,
        })));
    }

    #[test]
    fn queued_deletes_and_starts_wait_for_what_the_server_demands() {
        use super::{Disposition, classify};
        use crate::model::pipeline::PipelineRow;
        let row = |desired: Option<&str>, storage: &str| PipelineRow {
            name: "a".to_string(),
            deployment_status: "Stopped".to_string(),
            desired_status: desired.map(str::to_string),
            storage_status: storage.to_string(),
            ..Default::default()
        };
        let delete = Action::Delete {
            pipeline: "a".to_string(),
        };
        // A start the server accepted still reads Stopped; deleting now
        // would be refused, so the queued delete keeps waiting.
        assert!(matches!(
            classify(&delete, &row(Some("Running"), "Cleared")),
            Disposition::Wait
        ));
        assert!(matches!(
            classify(&delete, &row(Some("Stopped"), "Cleared")),
            Disposition::Run
        ));
        assert!(matches!(
            classify(&delete, &row(None, "Cleared")),
            Disposition::Run
        ));
        let start = Action::Start {
            pipeline: "a".to_string(),
        };
        assert!(matches!(
            classify(&start, &row(None, "Clearing")),
            Disposition::Wait
        ));
        assert!(matches!(
            classify(&start, &row(None, "Cleared")),
            Disposition::Run
        ));
    }

    #[test]
    fn the_bundle_dialog_edits_options_and_can_start_the_download() {
        let mut app = app_with_overview(&["a"]);
        assert!(type_command(&mut app, "bundle settings").is_empty());
        assert_eq!(app.overlay, Overlay::BundleSettings { cursor: 0 });
        press(&mut app, KeyPress::Char(' '));
        assert!(!app.bundle_options.collect, "space toggles the row");
        press(&mut app, KeyPress::Char('j'));
        press(&mut app, KeyPress::Right);
        press(&mut app, KeyPress::Right);
        assert_eq!(app.bundle_options.limit, Some(3), "arrows step the limit");
        press(&mut app, KeyPress::Left);
        assert_eq!(app.bundle_options.limit, Some(2));
        press(&mut app, KeyPress::Char(' '));
        assert_eq!(app.bundle_options.limit, Some(3), "space steps it too");
        press(&mut app, KeyPress::Char('k'));
        press(&mut app, KeyPress::Char('k'));
        assert_eq!(
            app.overlay,
            Overlay::BundleSettings { cursor: 11 },
            "wraps to Download"
        );

        let expected = BundleOptions {
            collect: false,
            limit: Some(3),
            ..Default::default()
        };
        let commands = press(&mut app, KeyPress::Enter);
        assert_eq!(app.overlay, Overlay::None);
        assert_eq!(
            commands,
            vec![Cmd::FetchDownload {
                kind: DownloadKind::SupportBundle,
                pipeline: "a".to_string(),
                bundle: expected,
            }]
        );
        assert_eq!(
            app.bundles["a"].bundle, expected,
            "the capture remembers its options"
        );

        // A broken download retries with the same options.
        let interrupted = ApiError::Transport("download interrupted".to_string());
        update(
            &mut app,
            Msg::DownloadFetched {
                kind: DownloadKind::SupportBundle,
                pipeline: "a".to_string(),
                result: Err(interrupted),
            },
        );
        let retry = update(&mut app, Msg::Tick { now_millis: 60_000 });
        assert_eq!(
            retry,
            vec![Cmd::FetchDownload {
                kind: DownloadKind::SupportBundle,
                pipeline: "a".to_string(),
                bundle: expected,
            }]
        );

        // Escape closes without downloading; the settings stay for the session.
        type_command(&mut app, "bundle settings");
        assert!(press(&mut app, KeyPress::Escape).is_empty());
        assert_eq!(app.overlay, Overlay::None);
        assert!(!app.bundle_options.collect);
        assert!(type_command(&mut app, "bundle now").is_empty());
        assert!(app.toast.as_ref().unwrap().message.contains("usage"));
    }

    #[test]
    fn an_accepted_request_shows_its_desired_status_at_once() {
        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Stopped".to_string();
        app.pipelines[0].desired_status = Some("Stopped".to_string());
        let start = Action::Start {
            pipeline: "a".to_string(),
        };
        press(&mut app, KeyPress::Char('s'));
        finish(&mut app, start);
        assert_eq!(app.pipelines[0].desired_status.as_deref(), Some("Running"));
        assert!(app.pipelines[0].is_transitioning());

        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].desired_status = Some("Running".to_string());
        let pause = Action::Pause {
            pipeline: "a".to_string(),
        };
        press(&mut app, KeyPress::Char('p'));
        finish(&mut app, pause);
        assert_eq!(app.pipelines[0].desired_status.as_deref(), Some("Paused"));
    }

    #[test]
    fn pause_reaches_an_initializing_pipeline_but_start_waits_for_a_settled_one() {
        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Initializing".to_string();
        assert_eq!(
            press(&mut app, KeyPress::Char('p')),
            vec![Cmd::Run(Action::Pause {
                pipeline: "a".to_string()
            })]
        );

        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Provisioning".to_string();
        assert!(press(&mut app, KeyPress::Char('s')).is_empty());
        assert_eq!(app.queued_label("a").as_deref(), Some("start"));
    }

    #[test]
    fn entering_sql_and_profiler_fetches_once() {
        let mut app = app_with_overview(&["a"]);
        let commands = press(&mut app, KeyPress::Char('v'));
        assert_eq!(commands, vec![Cmd::FetchSql("a".to_string())]);
        assert!(app.loading_sql);
        // Re-entering while loading does not refetch.
        assert!(set_view(&mut app, View::Sql).is_empty());

        update(
            &mut app,
            Msg::SqlLoaded {
                pipeline: "a".to_string(),
                result: Ok("SELECT 1;".to_string()),
            },
        );
        assert!(!app.loading_sql);
        assert_eq!(app.sql.as_ref().unwrap().text, "SELECT 1;");
        assert!(
            set_view(&mut app, View::Sql).is_empty(),
            "cached SQL is kept"
        );

        let commands = press(&mut app, KeyPress::Char('f'));
        assert_eq!(commands, vec![Cmd::FetchHotspots("a".to_string())]);
        assert!(app.loading_report);
    }

    #[test]
    fn profiler_refuses_stopped_pipelines() {
        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Stopped".to_string();
        let commands = press(&mut app, KeyPress::Char('f'));
        assert_eq!(commands, Vec::new());
        assert!(app.toast.as_ref().unwrap().message.contains("not running"));
        assert_eq!(app.view, View::Profiler);
    }

    fn loaded_report() -> HotspotReport {
        let profile = CircuitProfile::from_json(&json!({
            "worker_profiles": [{"metadata": {"n0_1": [
                {"metric_id": "runtime_seconds",
                 "value": {"type": "duration", "value": {"secs": 2, "nanos": 0}}},
                {"metric_id": "persistent_id", "value": {"type": "string", "value": "op"}}
            ]}}]
        }));
        let dataflow = DataflowIndex::from_json(&json!({
            "mir": {"m": {"operation": "join", "persistent_id": "op", "view": "v",
                "positions": [{"start_line_number": 3, "start_column": 1,
                               "end_line_number": 4, "end_column": 2}]}}
        }));
        HotspotReport::build(&profile, &dataflow)
    }

    #[test]
    fn hotspot_enter_jumps_into_the_sql() {
        let mut app = app_with_overview(&["a"]);
        update(
            &mut app,
            Msg::HotspotsLoaded {
                pipeline: "a".to_string(),
                result: Ok(loaded_report()),
            },
        );
        assert!(app.toast.as_ref().unwrap().message.contains("Profiled"));
        app.view = View::Profiler;
        press(&mut app, KeyPress::Enter);
        assert_eq!(app.view, View::Sql);
        assert_eq!(app.sql_focus, Some((3, 4)));
        assert_eq!(app.sql_scroll, 0);
    }

    #[test]
    fn profiler_keys_cycle_selection_and_metric() {
        let mut app = app_with_overview(&["a"]);
        update(
            &mut app,
            Msg::HotspotsLoaded {
                pipeline: "a".to_string(),
                result: Ok(loaded_report()),
            },
        );
        app.view = View::Profiler;
        press(&mut app, KeyPress::Char('j'));
        assert_eq!(app.selected_hotspot, 0, "one hotspot wraps to itself");
        press(&mut app, KeyPress::Char('m'));
        assert_eq!(app.metric, crate::model::dataflow::CostMetric::Memory);
    }

    #[test]
    fn empty_reports_get_a_helpful_toast() {
        let mut app = app_with_overview(&["a"]);
        update(
            &mut app,
            Msg::HotspotsLoaded {
                pipeline: "a".to_string(),
                result: Ok(HotspotReport::default()),
            },
        );
        assert!(app.toast.as_ref().unwrap().message.contains("no cost data"));
    }

    #[test]
    fn sql_scrolling_clamps_to_the_document() {
        let mut app = app_with_overview(&["a"]);
        app.sql = Some(crate::app::state::SqlDocument {
            pipeline: "a".to_string(),
            text: "l1\nl2\nl3".to_string(),
        });
        app.view = View::Sql;
        press(&mut app, KeyPress::PageDown);
        assert_eq!(app.sql_scroll, 2);
        press(&mut app, KeyPress::Char('g'));
        assert_eq!(app.sql_scroll, 0);
        press(&mut app, KeyPress::Char('G'));
        assert_eq!(app.sql_scroll, 2);
        press(&mut app, KeyPress::Char('k'));
        assert_eq!(app.sql_scroll, 1);
        press(&mut app, KeyPress::PageUp);
        assert_eq!(app.sql_scroll, 0);
    }

    #[test]
    fn finished_actions_toast_and_refresh() {
        let mut app = app_with_overview(&["a"]);
        app.pending_action_count = 1;
        let commands = update(
            &mut app,
            Msg::ActionFinished {
                action: Action::Start {
                    pipeline: "a".to_string(),
                },
                result: Ok(()),
            },
        );
        assert_eq!(commands, vec![Cmd::Refresh]);
        assert_eq!(app.pending_action_count, 0);

        let commands = update(
            &mut app,
            Msg::ActionFinished {
                action: Action::Pause {
                    pipeline: "a".to_string(),
                },
                result: Err(ApiError::Status {
                    status: 400,
                    message: "already paused".to_string(),
                }),
            },
        );
        assert!(commands.is_empty());
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("already paused")
        );
    }

    #[test]
    fn save_failures_fail_the_capture_and_stray_answers_are_ignored() {
        let mut app = app_fetching_a_profile();
        assert_eq!(
            update(
                &mut app,
                download_fetched("a", Ok(Download::Ready(vec![9])))
            ),
            vec![Cmd::SaveDownload {
                kind: DownloadKind::SamplyProfile,
                pipeline: "a".to_string(),
                bytes: vec![9]
            }]
        );
        update(
            &mut app,
            Msg::FileSaved {
                kind: DownloadKind::SamplyProfile,
                pipeline: "a".to_string(),
                path: "x.json.gz".to_string(),
                result: Err("disk full".to_string()),
            },
        );
        assert!(app.toast.as_ref().unwrap().message.contains("disk full"));
        assert_eq!(app.samply["a"].label(0), "✖ failed");

        // Answers for pipelines without a capture are ignored.
        assert!(
            update(
                &mut app,
                download_fetched("ghost", Ok(Download::Ready(vec![1])))
            )
            .is_empty()
        );
        assert!(
            update(
                &mut app,
                Msg::FileSaved {
                    kind: DownloadKind::SamplyProfile,
                    pipeline: "ghost".to_string(),
                    path: "x".to_string(),
                    result: Ok(()),
                },
            )
            .is_empty()
        );
        assert!(!app.samply.contains_key("ghost"));
    }

    #[test]
    fn tenant_picker_flows_from_key_to_switch() {
        let mut app = app_with_overview(&["a"]);
        app.session = crate::model::header::Session::from_json(&json!({
            "tenant_name": "one",
            "memberships": [
                {"tenant_id": "1", "name": "one", "role": "admin"},
                {"tenant_id": "2", "name": "two", "role": "read"}
            ]
        }));
        press(&mut app, KeyPress::Char('T'));
        assert!(matches!(app.overlay, Overlay::Tenants { selected: 0 }));
        press(&mut app, KeyPress::Char('j'));
        let commands = press(&mut app, KeyPress::Enter);
        assert_eq!(
            commands,
            vec![Cmd::SwitchTenant(Some("two".to_string())), Cmd::Refresh]
        );

        // Direct command with validation.
        let commands = type_command(&mut app, "tenant nowhere");
        assert!(commands.is_empty());
        assert!(app.toast.as_ref().unwrap().message.contains("not a member"));
        let commands = type_command(&mut app, "tenant two");
        assert_eq!(
            commands,
            vec![Cmd::SwitchTenant(Some("two".to_string())), Cmd::Refresh]
        );
    }

    #[test]
    fn single_tenant_identities_skip_the_picker() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('T'));
        assert_eq!(app.overlay, Overlay::None);
        assert!(app.toast.as_ref().unwrap().message.contains("one tenant"));
    }

    #[test]
    fn connector_actions_validate_direction_and_selection() {
        let mut app = app_with_overview(&["a"]);
        type_command(&mut app, "connector-pause");
        assert!(app.toast.as_ref().unwrap().message.contains("No connector"));

        update(&mut app, detail_msg("a", 10, 1_000));
        app.view = View::Connectors;
        let commands = press(&mut app, KeyPress::Char('p'));
        assert_eq!(
            commands,
            vec![Cmd::Run(Action::ConnectorPause {
                pipeline: "a".to_string(),
                table: "orders".to_string(),
                connector: "orders_in".to_string()
            })]
        );
        let commands = press(&mut app, KeyPress::Char('P'));
        assert_eq!(
            commands,
            vec![Cmd::Run(Action::ConnectorResume {
                pipeline: "a".to_string(),
                table: "orders".to_string(),
                connector: "orders_in".to_string()
            })]
        );
        press(&mut app, KeyPress::Enter);
        assert!(matches!(app.overlay, Overlay::ConnectorInspect { .. }));
        press(&mut app, KeyPress::Char('j'));
        press(&mut app, KeyPress::Escape);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn views_switch_by_digit_tab_and_back_keys() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('3'));
        assert_eq!(app.view, View::Connectors);
        press(&mut app, KeyPress::Tab);
        assert_eq!(app.view, View::Profiler);
        press(&mut app, KeyPress::BackTab);
        assert_eq!(app.view, View::Connectors);
        press(&mut app, KeyPress::Char('q'));
        assert_eq!(app.view, View::Pipelines);
        let commands = press(&mut app, KeyPress::Char('q'));
        assert_eq!(commands, vec![Cmd::Quit]);
        assert_eq!(type_command(&mut app, "q"), vec![Cmd::Quit]);
    }

    #[test]
    fn sort_keys_cycle_and_reverse() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('o'));
        assert_eq!(app.sort, SortColumn::Status);
        press(&mut app, KeyPress::Char('O'));
        assert!(app.sort_descending);
        type_command(&mut app, "sort rps");
        assert_eq!(app.sort, SortColumn::Throughput);
        type_command(&mut app, "sort sideways");
        assert!(app.toast.as_ref().unwrap().message.contains("unknown sort"));
    }

    #[test]
    fn error_viewer_opens_only_when_there_is_an_error() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('e'));
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("No deployment error")
        );
        app.pipelines[0].deployment_error = Some("worker died".to_string());
        press(&mut app, KeyPress::Char('e'));
        assert!(matches!(app.overlay, Overlay::TextViewer { .. }));
        press(&mut app, KeyPress::PageDown);
        press(&mut app, KeyPress::Char('q'));
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn help_opens_from_key_and_closes_on_any_key() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('?'));
        assert_eq!(app.overlay, Overlay::Help);
        press(&mut app, KeyPress::Char('z'));
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn metrics_view_navigates_and_descends() {
        let mut app = app_with_overview(&["a", "b"]);
        press(&mut app, KeyPress::Enter);
        assert_eq!(app.view, View::Metrics);
        let commands = press(&mut app, KeyPress::Char('j'));
        assert_eq!(commands, vec![Cmd::Watch(Some("b".to_string()))]);
        press(&mut app, KeyPress::Enter);
        assert_eq!(app.view, View::Connectors);
    }

    #[test]
    fn refresh_cadence_command_reaches_the_poller() {
        let mut app = app_with_overview(&["a"]);
        let commands = type_command(&mut app, "refresh-every 7");
        assert_eq!(commands, vec![Cmd::SetRefreshSecs(7)]);
        assert_eq!(app.refresh_secs, 7);
    }

    #[test]
    fn sql_and_profile_fetch_errors_toast() {
        let mut app = app_with_overview(&["a"]);
        update(
            &mut app,
            Msg::SqlLoaded {
                pipeline: "a".to_string(),
                result: Err(ApiError::Status {
                    status: 404,
                    message: "gone".to_string(),
                }),
            },
        );
        assert!(app.toast.as_ref().unwrap().message.contains("SQL"));
        update(
            &mut app,
            Msg::HotspotsLoaded {
                pipeline: "a".to_string(),
                result: Err(ApiError::Status {
                    status: 503,
                    message: "not running".to_string(),
                }),
            },
        );
        assert!(app.toast.as_ref().unwrap().message.contains("profile"));
        assert!(!app.loading_report);
    }

    #[test]
    fn tenant_overlay_wraps_and_closes() {
        let mut app = app_with_overview(&["a"]);
        app.session = crate::model::header::Session::from_json(&json!({
            "tenant_name": "one",
            "memberships": [
                {"tenant_id": "1", "name": "one", "role": "admin"},
                {"tenant_id": "2", "name": "two", "role": "read"}
            ]
        }));
        press(&mut app, KeyPress::Char('T'));
        press(&mut app, KeyPress::Char('k'));
        assert!(matches!(app.overlay, Overlay::Tenants { selected: 1 }));
        press(&mut app, KeyPress::Char('x'));
        assert!(
            matches!(app.overlay, Overlay::Tenants { .. }),
            "unknown keys ignored"
        );
        press(&mut app, KeyPress::Escape);
        assert_eq!(app.overlay, Overlay::None);

        // Enter on an impossible selection closes without switching.
        app.overlay = Overlay::Tenants { selected: 99 };
        assert!(press(&mut app, KeyPress::Enter).is_empty());
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn text_viewer_scrolls_in_both_directions() {
        let mut app = app_with_overview(&["a"]);
        app.overlay = Overlay::TextViewer {
            title: "T".to_string(),
            body: "b".to_string(),
            scroll: 5,
        };
        press(&mut app, KeyPress::Up);
        press(&mut app, KeyPress::PageUp);
        let Overlay::TextViewer { scroll, .. } = &app.overlay else {
            panic!("viewer still open");
        };
        assert_eq!(*scroll, 0);
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::PageDown);
        press(&mut app, KeyPress::Home);
        let Overlay::TextViewer { scroll, .. } = &app.overlay else {
            panic!("viewer still open");
        };
        assert_eq!(*scroll, 0);
        press(&mut app, KeyPress::Enter);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn command_prompt_edge_cases() {
        let mut app = app_with_overview(&["a"]);
        // Backspace on an empty prompt closes it.
        press(&mut app, KeyPress::Char(':'));
        press(&mut app, KeyPress::Backspace);
        assert_eq!(app.prompt, Prompt::Closed);

        // History Up with no history is inert; blank Enter runs nothing.
        press(&mut app, KeyPress::Char(':'));
        press(&mut app, KeyPress::Up);
        press(&mut app, KeyPress::Enter);
        assert!(app.command_history.is_empty());

        // Two history entries: Up twice reaches the older one, Down returns.
        type_command(&mut app, "help");
        press(&mut app, KeyPress::Char('x'));
        type_command(&mut app, "sql");
        press(&mut app, KeyPress::Char(':'));
        press(&mut app, KeyPress::Up);
        press(&mut app, KeyPress::Up);
        let Prompt::Command { input, .. } = &app.prompt else {
            panic!("prompt open");
        };
        assert_eq!(input, "help");
        press(&mut app, KeyPress::Down);
        let Prompt::Command { input, .. } = &app.prompt else {
            panic!("prompt open");
        };
        assert_eq!(input, "sql");
        // An exact command completes to itself plus a space.
        press(&mut app, KeyPress::Tab);
        let Prompt::Command { input, .. } = &app.prompt else {
            panic!("prompt open");
        };
        assert_eq!(input, "sql ");
        // Unknown keys and ambiguous completion leave the input alone.
        press(&mut app, KeyPress::Backspace);
        press(&mut app, KeyPress::Backspace);
        press(&mut app, KeyPress::Backspace);
        press(&mut app, KeyPress::Backspace);
        press(&mut app, KeyPress::Char('s'));
        press(&mut app, KeyPress::Home);
        press(&mut app, KeyPress::Tab);
        let Prompt::Command { input, .. } = &app.prompt else {
            panic!("prompt open");
        };
        assert_eq!(input, "s");
        press(&mut app, KeyPress::Escape);
    }

    #[test]
    fn filter_prompt_backspace_and_stray_keys() {
        let mut app = app_with_overview(&["orders", "users"]);
        press(&mut app, KeyPress::Char('/'));
        press(&mut app, KeyPress::Char('o'));
        press(&mut app, KeyPress::Char('r'));
        press(&mut app, KeyPress::Backspace);
        assert_eq!(app.filter, "o");
        press(&mut app, KeyPress::Home);
        press(&mut app, KeyPress::Backspace);
        assert_eq!(app.filter, "");
        press(&mut app, KeyPress::Backspace);
        assert_eq!(app.prompt, Prompt::Closed);
    }

    #[test]
    fn lifecycle_commands_via_the_bar_cover_every_verb() {
        let mut app = app_with_overview(&["a"]);
        app.ask_before_destructive = true;
        assert_eq!(
            type_command(&mut app, "pause"),
            vec![Cmd::Run(Action::Pause {
                pipeline: "a".to_string()
            })]
        );
        finish(
            &mut app,
            Action::Pause {
                pipeline: "a".to_string(),
            },
        );
        app.pipelines[0].deployment_status = "Paused".to_string();
        assert_eq!(
            type_command(&mut app, "resume a"),
            vec![Cmd::Run(Action::Resume {
                pipeline: "a".to_string()
            })]
        );
        finish(
            &mut app,
            Action::Resume {
                pipeline: "a".to_string(),
            },
        );
        type_command(&mut app, "stop");
        assert!(matches!(app.overlay, Overlay::Confirm { .. }));
        press(&mut app, KeyPress::Escape);
        type_command(&mut app, "clear");
        assert!(matches!(app.overlay, Overlay::Confirm { .. }));
        press(&mut app, KeyPress::Escape);
        assert_eq!(
            type_command(&mut app, "sql"),
            vec![Cmd::FetchSql("a".to_string())]
        );
        assert_eq!(type_command(&mut app, "filter zz"), Vec::new());
        assert_eq!(app.filter, "zz");
        app.filter.clear();
        app.reconcile_selection();
        type_command(&mut app, "help");
        assert_eq!(app.overlay, Overlay::Help);
        press(&mut app, KeyPress::Enter);
    }

    #[test]
    fn samply_needs_a_selection_then_runs() {
        let mut app = App::new("http://x".to_string(), 2);
        type_command(&mut app, "samply 5");
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("Select a pipeline")
        );

        let mut app = app_with_overview(&["a"]);
        let commands = type_command(&mut app, "samply 5");
        assert_eq!(
            commands,
            vec![Cmd::Run(Action::Samply {
                pipeline: "a".to_string(),
                duration_secs: 5
            })]
        );
        assert_eq!(app.pending_action_count, 1);
    }

    #[test]
    fn connector_resume_command_matches_the_key() {
        let mut app = app_with_overview(&["a"]);
        update(&mut app, detail_msg("a", 10, 1_000));
        let commands = type_command(&mut app, "connector-resume");
        assert_eq!(
            commands,
            vec![Cmd::Run(Action::ConnectorResume {
                pipeline: "a".to_string(),
                table: "orders".to_string(),
                connector: "orders_in".to_string()
            })]
        );
    }

    #[test]
    fn actions_without_any_selection_toast() {
        let mut app = App::new("http://x".to_string(), 2);
        assert!(press(&mut app, KeyPress::Char('s')).is_empty());
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("Select a pipeline")
        );
        assert!(type_command(&mut app, "samply").is_empty());
        assert!(press(&mut app, KeyPress::Char('f')).is_empty());
        assert_eq!(app.view, View::Profiler);
    }

    #[test]
    fn a_queued_clear_runs_after_the_stop_settles() {
        let mut app = app_with_overview(&["a"]);
        // Stop starts immediately; clear queues behind it.
        assert_eq!(press(&mut app, KeyPress::Char('x')).len(), 1);
        assert!(press(&mut app, KeyPress::Char('C')).is_empty());
        assert!(app.is_clear_queued("a"));
        assert_eq!(
            app.queued_label("a"),
            None,
            "clears badge in the storage column"
        );
        assert!(app.toast.as_ref().unwrap().message.contains("Queued"));

        // The stop request finished, but the pipeline is still Stopping:
        // the queued clear must wait.
        app.pipelines[0].deployment_status = "Stopping".to_string();
        let commands = finish(
            &mut app,
            Action::Stop {
                pipeline: "a".to_string(),
                force: false,
            },
        );
        assert!(!commands.contains(&Cmd::Run(Action::Clear {
            pipeline: "a".to_string()
        })));
        assert!(app.is_clear_queued("a"));

        // The next refresh reports Stopped: the clear fires.
        let mut overview = overview_with(&["a"]);
        overview.pipelines[0].deployment_status = "Stopped".to_string();
        let commands = update(
            &mut app,
            Msg::Overview {
                result: Ok(overview),
                latency_millis: 1,
                now_millis: 5_000,
            },
        );
        assert!(commands.contains(&Cmd::Run(Action::Clear {
            pipeline: "a".to_string()
        })));
        assert!(!app.is_clear_queued("a"));
        assert!(app.inflight.contains_key("a"));
    }

    #[test]
    fn a_queued_clear_waits_through_the_suspended_leg_of_a_stop() {
        // The user's exact flow: Running, stop (with checkpoint), queue
        // clear. A checkpointing stop passes through the steady-looking
        // `Suspended` state; the clear must wait there, not fail or drop.
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('x'));
        press(&mut app, KeyPress::Char('C'));
        finish(
            &mut app,
            Action::Stop {
                pipeline: "a".to_string(),
                force: false,
            },
        );
        for status in ["Running", "Suspended", "Stopping"] {
            let mut overview = overview_with(&["a"]);
            overview.pipelines[0].deployment_status = status.to_string();
            let commands = update(
                &mut app,
                Msg::Overview {
                    result: Ok(overview),
                    latency_millis: 1,
                    now_millis: 5_000,
                },
            );
            assert!(
                commands
                    .iter()
                    .all(|command| !matches!(command, Cmd::Run(_))),
                "clear must wait while the pipeline is {status}"
            );
            assert!(app.is_clear_queued("a"));
        }
        let mut overview = overview_with(&["a"]);
        overview.pipelines[0].deployment_status = "Stopped".to_string();
        let commands = update(
            &mut app,
            Msg::Overview {
                result: Ok(overview),
                latency_millis: 1,
                now_millis: 6_000,
            },
        );
        assert!(commands.contains(&Cmd::Run(Action::Clear {
            pipeline: "a".to_string()
        })));
    }

    #[test]
    fn queued_actions_expire_instead_of_firing_much_later() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('x'));
        // Queue a pause; the pipeline ends up Stopped where pause waits,
        // and after ten minutes it expires.
        assert!(press(&mut app, KeyPress::Char('p')).is_empty());
        finish(
            &mut app,
            Action::Stop {
                pipeline: "a".to_string(),
                force: false,
            },
        );
        let mut overview = overview_with(&["a"]);
        overview.pipelines[0].deployment_status = "Stopped".to_string();
        update(
            &mut app,
            Msg::Overview {
                result: Ok(overview.clone()),
                latency_millis: 1,
                now_millis: 5_000,
            },
        );
        assert_eq!(
            app.queued_label("a").as_deref(),
            Some("pause"),
            "still waiting"
        );

        let commands = update(
            &mut app,
            Msg::Overview {
                result: Ok(overview),
                latency_millis: 1,
                now_millis: 5_000 + 11 * 60 * 1_000,
            },
        );
        assert!(
            commands
                .iter()
                .all(|command| !matches!(command, Cmd::Run(_)))
        );
        assert_eq!(app.queued_label("a"), None);
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("waited over 10 minutes")
        );
    }

    #[test]
    fn only_successful_clears_flash_the_burn_animation() {
        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Stopped".to_string();
        app.now_millis = 1_000;
        press(&mut app, KeyPress::Char('C'));
        assert!(
            app.clearing_flash.is_empty(),
            "no burn while the outcome is unknown"
        );
        finish(
            &mut app,
            Action::Clear {
                pipeline: "a".to_string(),
            },
        );
        assert!(
            app.clearing_flash
                .get("a")
                .is_some_and(|until| *until > 1_000)
        );
        update(&mut app, Msg::Tick { now_millis: 10_000 });
        assert!(app.clearing_flash.is_empty());

        // A rejected clear never burns.
        press(&mut app, KeyPress::Char('C'));
        update(
            &mut app,
            Msg::ActionFinished {
                action: Action::Clear {
                    pipeline: "a".to_string(),
                },
                result: Err(ApiError::Status {
                    status: 400,
                    message: "pipeline is running".to_string(),
                }),
            },
        );
        assert!(app.clearing_flash.is_empty());
    }

    #[test]
    fn satisfied_requests_skip_locally_without_a_server_call() {
        let mut app = app_with_overview(&["a"]);
        // Start on a running pipeline is already achieved.
        assert!(press(&mut app, KeyPress::Char('s')).is_empty());
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("already running")
        );
        assert_eq!(app.pending_action_count, 0);

        // Clear on already-cleared storage: no call, no burn, no queue.
        app.pipelines[0].deployment_status = "Stopped".to_string();
        app.pipelines[0].storage_status = "Cleared".to_string();
        assert!(press(&mut app, KeyPress::Char('C')).is_empty());
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("storage already cleared")
        );
        assert!(app.clearing_flash.is_empty());
        assert!(!app.is_clear_queued("a"));
    }

    #[test]
    fn satisfied_queued_actions_are_skipped_not_run() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('x'));
        // Queue another stop and a clear behind the first stop.
        press(&mut app, KeyPress::Char('X'));
        press(&mut app, KeyPress::Char('C'));
        finish(
            &mut app,
            Action::Stop {
                pipeline: "a".to_string(),
                force: false,
            },
        );
        // The pipeline arrives Stopped with storage already cleared: the
        // queued force-stop and clear are both already satisfied.
        let mut overview = overview_with(&["a"]);
        overview.pipelines[0].deployment_status = "Stopped".to_string();
        overview.pipelines[0].storage_status = "Cleared".to_string();
        let commands = update(
            &mut app,
            Msg::Overview {
                result: Ok(overview),
                latency_millis: 1,
                now_millis: 5_000,
            },
        );
        assert!(
            commands
                .iter()
                .all(|command| !matches!(command, Cmd::Run(_)))
        );
        assert_eq!(app.queued_label("a"), None);
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("Skipped queued")
        );
    }

    #[test]
    fn actions_queue_while_a_pipeline_transitions() {
        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Provisioning".to_string();
        // Start must wait for a steady state.
        assert!(press(&mut app, KeyPress::Char('s')).is_empty());
        assert_eq!(app.queued_label("a").as_deref(), Some("start"));
        // Stop interrupts a transition, so it runs immediately even now
        // (nothing in flight; only the transition is in the way).
        app.queued.clear();
        assert_eq!(press(&mut app, KeyPress::Char('x')).len(), 1);
    }

    #[test]
    fn a_failed_action_drops_its_queue() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('x'));
        press(&mut app, KeyPress::Char('C'));
        update(
            &mut app,
            Msg::ActionFinished {
                action: Action::Stop {
                    pipeline: "a".to_string(),
                    force: false,
                },
                result: Err(ApiError::Status {
                    status: 500,
                    message: "boom".to_string(),
                }),
            },
        );
        assert_eq!(app.queued_label("a"), None);
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("dropped 1 queued")
        );
    }

    #[test]
    fn queues_cap_and_deduplicate() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('x'));
        press(&mut app, KeyPress::Char('C'));
        press(&mut app, KeyPress::Char('C'));
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("Already queued")
        );
        press(&mut app, KeyPress::Char('s'));
        press(&mut app, KeyPress::Char('p'));
        press(&mut app, KeyPress::Char('P'));
        assert_eq!(app.queued.get("a").unwrap().len(), 4);
        type_command(&mut app, "restart");
        assert!(app.toast.as_ref().unwrap().message.contains("full"));
    }

    #[test]
    fn queues_vanish_with_their_pipeline() {
        let mut app = app_with_overview(&["a", "b"]);
        press(&mut app, KeyPress::Char('x'));
        press(&mut app, KeyPress::Char('C'));
        finish(
            &mut app,
            Action::Stop {
                pipeline: "a".to_string(),
                force: false,
            },
        );
        update(
            &mut app,
            Msg::Overview {
                result: Ok(overview_with(&["b"])),
                latency_millis: 1,
                now_millis: 5_000,
            },
        );
        assert!(app.queued.is_empty());
        assert!(app.toast.as_ref().unwrap().message.contains("gone"));
    }

    #[test]
    fn clicks_select_table_rows_and_ignore_chrome() {
        let mut app = app_with_overview(&["a", "b", "c"]);
        // Before any render the viewport is empty and clicks are inert.
        assert!(
            press(
                &mut app,
                KeyPress::Click {
                    row: crate::app::state::FIRST_TABLE_ROW,
                    column: 5,
                },
            )
            .is_empty()
        );
        app.viewport.pipelines.set((0, 20));
        let commands = press(
            &mut app,
            KeyPress::Click {
                row: crate::app::state::FIRST_TABLE_ROW + 2,
                column: 5,
            },
        );
        assert_eq!(commands, vec![Cmd::Watch(Some("c".to_string()))]);
        // Clicking the already selected row does nothing.
        assert!(
            press(
                &mut app,
                KeyPress::Click {
                    row: crate::app::state::FIRST_TABLE_ROW + 2,
                    column: 5,
                },
            )
            .is_empty()
        );
        // Header and empty area clicks are inert.
        assert!(press(&mut app, KeyPress::Click { row: 0, column: 5 }).is_empty());
        assert!(press(&mut app, KeyPress::Click { row: 40, column: 5 }).is_empty());
        assert_eq!(app.selected_name.as_deref(), Some("c"));
    }

    #[test]
    fn clicks_select_connectors_and_hotspots() {
        let mut app = app_with_overview(&["a"]);
        update(&mut app, detail_msg("a", 10, 1_000));
        app.viewport.connectors.set((0, 20));
        app.viewport.hotspots.set((0, 20));
        app.view = View::Connectors;
        press(
            &mut app,
            KeyPress::Click {
                row: crate::app::state::FIRST_TABLE_ROW,
                column: 5,
            },
        );
        assert_eq!(app.selected_connector, 0);

        update(
            &mut app,
            Msg::HotspotsLoaded {
                pipeline: "a".to_string(),
                result: Ok(loaded_report()),
            },
        );
        app.view = View::Profiler;
        press(
            &mut app,
            KeyPress::Click {
                row: crate::app::state::FIRST_TABLE_ROW,
                column: 5,
            },
        );
        assert_eq!(app.selected_hotspot, 0);
    }

    #[test]
    fn the_logs_view_streams_diagnoses_and_follows() {
        let mut app = app_with_overview(&["a"]);
        let commands = press(&mut app, KeyPress::Char('l'));
        assert_eq!(app.view, View::Logs);
        assert!(commands.contains(&Cmd::StartLogs("a".to_string())));
        assert!(commands.contains(&Cmd::FetchDiagnostics("a".to_string())));
        assert!(app.logs.streaming);

        for index in 0..3 {
            update(
                &mut app,
                Msg::LogLine {
                    pipeline: "a".to_string(),
                    line: format!("\u{1b}[32mINFO\u{1b}[0m line{index}"),
                },
            );
        }
        assert_eq!(app.logs.lines.len(), 3);
        assert_eq!(app.logs.lines[0], "INFO line0");
        // Stale lines for other pipelines are ignored.
        update(
            &mut app,
            Msg::LogLine {
                pipeline: "other".to_string(),
                line: "nope".to_string(),
            },
        );
        assert_eq!(app.logs.lines.len(), 3);

        update(
            &mut app,
            Msg::DiagnosticsLoaded {
                pipeline: "a".to_string(),
                result: Ok(vec![crate::model::diagnostics::DiagnosticSection {
                    title: "RUST COMPILATION FAILED (exit 101)".to_string(),
                    severity: crate::model::diagnostics::Severity::Error,
                    body: "error[E0432]: unresolved import".to_string(),
                }]),
            },
        );
        assert_eq!(app.logs.diagnostics.len(), 1);
        assert_eq!(app.logs.row_count(), 3 + 2 + 1);

        // Scrolling unpins; G re-follows.
        press(&mut app, KeyPress::Char('k'));
        assert!(app.logs.scroll.is_some());
        press(&mut app, KeyPress::Char('G'));
        assert!(app.logs.scroll.is_none());
        press(&mut app, KeyPress::Char('g'));
        assert_eq!(app.logs.scroll, Some(0));
        // Scrolling to the last row re-arms follow mode.
        press(&mut app, KeyPress::End);
        assert!(app.logs.scroll.is_none());
    }

    #[test]
    fn ended_streams_retry_only_when_it_helps() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('l'));
        // A failed stream on a live pipeline retries via the tick.
        update(
            &mut app,
            Msg::LogsClosed {
                pipeline: "a".to_string(),
                result: Err(ApiError::Transport("reset".to_string())),
            },
        );
        assert!(!app.logs.streaming);
        let later = app.now_millis + 10_000;
        let commands = update(&mut app, Msg::Tick { now_millis: later });
        assert_eq!(commands, vec![Cmd::StartLogs("a".to_string())]);

        // A graceful end on a stopped pipeline is final.
        app.pipelines[0].deployment_status = "Stopped".to_string();
        update(
            &mut app,
            Msg::LogsClosed {
                pipeline: "a".to_string(),
                result: Ok(()),
            },
        );
        let later = app.now_millis + 60_000;
        let commands = update(&mut app, Msg::Tick { now_millis: later });
        assert!(commands.is_empty());
    }

    #[test]
    fn leaving_the_logs_view_stops_the_stream() {
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Char('l'));
        let commands = press(&mut app, KeyPress::Char('1'));
        assert!(commands.contains(&Cmd::StopLogs));
        assert!(!app.logs.streaming);
        // No retries fire once the view is left.
        let later = app.now_millis + 60_000;
        let commands = update(&mut app, Msg::Tick { now_millis: later });
        assert!(commands.is_empty());
    }

    #[test]
    fn enter_on_a_broken_stopped_pipeline_opens_its_logs() {
        let mut app = app_with_overview(&["a"]);
        app.pipelines[0].deployment_status = "Stopped".to_string();
        app.pipelines[0].program_status = "RustError".to_string();
        let commands = press(&mut app, KeyPress::Enter);
        assert_eq!(app.view, View::Logs);
        assert!(commands.contains(&Cmd::StartLogs("a".to_string())));

        // A healthy pipeline still opens its metrics.
        let mut app = app_with_overview(&["a"]);
        press(&mut app, KeyPress::Enter);
        assert_eq!(app.view, View::Metrics);
    }

    #[test]
    fn switching_pipelines_inside_the_logs_view_restarts_the_stream() {
        let mut app = app_with_overview(&["a", "b"]);
        press(&mut app, KeyPress::Char('l'));
        app.logs.push_line("old line");
        // The selected pipeline disappears; reconciliation moves to `b` and
        // the stream follows.
        let commands = update(
            &mut app,
            Msg::Overview {
                result: Ok(overview_with(&["b"])),
                latency_millis: 1,
                now_millis: 5_000,
            },
        );
        assert!(commands.contains(&Cmd::StartLogs("b".to_string())));
        assert_eq!(app.logs.pipeline.as_deref(), Some("b"));
        assert!(app.logs.lines.is_empty());
    }

    #[test]
    fn space_toggles_marks_that_survive_plain_movement() {
        let mut app = app_with_overview(&["a", "b", "c", "d"]);
        press(&mut app, KeyPress::Char(' '));
        assert_eq!(marked(&app), ["a"]);
        assert_eq!(app.selected_name.as_deref(), Some("a"));
        press(&mut app, KeyPress::Char(' '));
        assert!(app.marked.is_empty());
        press(&mut app, KeyPress::Char(' '));
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Char(' '));
        assert_eq!(marked(&app), ["a", "b"]);

        // Esc clears the marks before anything else.
        press(&mut app, KeyPress::Escape);
        assert!(app.marked.is_empty());
    }

    #[test]
    fn arrows_stop_at_the_ends_instead_of_wrapping() {
        let mut app = app_with_overview(&["a", "b", "c"]);
        assert!(press(&mut app, KeyPress::Up).is_empty());
        assert_eq!(app.selected_name.as_deref(), Some("a"));
        press(&mut app, KeyPress::End);
        assert_eq!(app.selected_name.as_deref(), Some("c"));
        assert!(press(&mut app, KeyPress::Down).is_empty());
        assert_eq!(app.selected_name.as_deref(), Some("c"));
        press(&mut app, KeyPress::PageDown);
        assert_eq!(app.selected_name.as_deref(), Some("c"));
        press(&mut app, KeyPress::ShiftDown);
        assert_eq!(marked(&app), ["c"]);
    }

    #[test]
    fn shift_arrows_grow_and_shrink_the_range_from_its_anchor() {
        let mut app = app_with_overview(&["a", "b", "c", "d"]);
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::ShiftDown);
        press(&mut app, KeyPress::ShiftDown);
        assert_eq!(marked(&app), ["b", "c", "d"]);
        // Reversing shrinks the range instead of extending it upward.
        press(&mut app, KeyPress::ShiftUp);
        assert_eq!(marked(&app), ["b", "c"]);
        assert_eq!(app.selected_name.as_deref(), Some("c"));
        press(&mut app, KeyPress::ShiftUp);
        assert_eq!(marked(&app), ["b"]);
        // Crossing the anchor flips the range to the other side.
        press(&mut app, KeyPress::ShiftUp);
        assert_eq!(marked(&app), ["a", "b"]);
        assert_eq!(app.selected_name.as_deref(), Some("a"));
        // The top edge holds the range steady.
        assert!(press(&mut app, KeyPress::ShiftUp).is_empty());
        assert_eq!(marked(&app), ["a", "b"]);
    }

    #[test]
    fn moving_without_shift_ends_the_run_but_keeps_its_marks() {
        let mut app = app_with_overview(&["a", "b", "c", "d"]);
        press(&mut app, KeyPress::ShiftDown);
        assert_eq!(marked(&app), ["a", "b"]);
        press(&mut app, KeyPress::Down);
        assert_eq!(marked(&app), ["a", "b"]);
        assert_eq!(anchor(&app), None);
        // The next run starts at the cursor and adds to what is there.
        press(&mut app, KeyPress::ShiftDown);
        assert_eq!(marked(&app), ["a", "b", "c", "d"]);
        assert_eq!(anchor(&app), Some("c"));
        // Shrinking the new run leaves the older marks alone.
        press(&mut app, KeyPress::ShiftUp);
        assert_eq!(marked(&app), ["a", "b", "c"]);
        // The wheel, home/end, and a click end the run the same way.
        press(&mut app, KeyPress::ScrollUp);
        assert_eq!(anchor(&app), None);
        assert_eq!(marked(&app), ["a", "b", "c"]);
        press(&mut app, KeyPress::ShiftDown);
        press(&mut app, KeyPress::End);
        assert_eq!(anchor(&app), None);
        press(&mut app, KeyPress::ShiftUp);
        app.viewport.pipelines.set((0, 20));
        press(
            &mut app,
            KeyPress::Click {
                row: crate::app::state::FIRST_TABLE_ROW,
                column: 5,
            },
        );
        assert_eq!(anchor(&app), None);
        assert_eq!(marked(&app), ["a", "b", "c", "d"]);
        assert_eq!(app.selected_name.as_deref(), Some("a"));
    }

    #[test]
    fn a_shift_run_rewrites_only_its_own_span() {
        let mut app = app_with_overview(&["a", "b", "c", "d", "e"]);
        press(&mut app, KeyPress::Char(' '));
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::ShiftDown);
        assert_eq!(marked(&app), ["a", "c", "d"]);
        press(&mut app, KeyPress::ShiftDown);
        press(&mut app, KeyPress::ShiftUp);
        assert_eq!(marked(&app), ["a", "c", "d"]);
        // Unmarking inside the span with space holds until the span moves
        // over that row again.
        press(&mut app, KeyPress::Char(' '));
        assert_eq!(marked(&app), ["a", "c"]);
        press(&mut app, KeyPress::ShiftUp);
        assert_eq!(marked(&app), ["a", "c"]);
        press(&mut app, KeyPress::ShiftDown);
        assert_eq!(marked(&app), ["a", "c", "d"]);
    }

    #[test]
    fn clicking_a_bottom_tab_switches_the_view_from_anywhere() {
        let mut app = app_with_overview(&["a"]);
        // Before the first frame no tab bar is known and clicks fall through.
        assert!(press(&mut app, KeyPress::Click { row: 44, column: 3 }).is_empty());
        assert_eq!(app.view, View::Pipelines);
        app.viewport.tab_bar_row.set(Some(44));
        *app.viewport.tabs.borrow_mut() = vec![
            crate::app::state::TabHit {
                x_start: 0,
                x_end: 13,
                view: View::Pipelines,
            },
            crate::app::state::TabHit {
                x_start: 14,
                x_end: 25,
                view: View::Metrics,
            },
        ];
        press(
            &mut app,
            KeyPress::Click {
                row: 44,
                column: 20,
            },
        );
        assert_eq!(app.view, View::Metrics);
        // The gap between tabs and other rows are not tabs.
        press(
            &mut app,
            KeyPress::Click {
                row: 44,
                column: 13,
            },
        );
        assert_eq!(app.view, View::Metrics);
        press(&mut app, KeyPress::Click { row: 43, column: 3 });
        assert_eq!(app.view, View::Metrics);
        press(&mut app, KeyPress::Click { row: 44, column: 3 });
        assert_eq!(app.view, View::Pipelines);
    }

    #[test]
    fn a_shift_run_restarts_when_its_anchor_disappears() {
        let mut app = app_with_overview(&["a", "b", "c", "d"]);
        press(&mut app, KeyPress::ShiftDown);
        assert_eq!(anchor(&app), Some("a"));
        update(
            &mut app,
            Msg::Overview {
                result: Ok(overview_with(&["b", "c", "d"])),
                latency_millis: 1,
                now_millis: 2_000,
            },
        );
        press(&mut app, KeyPress::ShiftDown);
        assert_eq!(marked(&app), ["b", "c"]);
        assert_eq!(anchor(&app), Some("b"));
    }

    #[test]
    fn enter_collapses_the_multi_selection_and_drills_in() {
        let mut app = app_with_overview(&["a", "b"]);
        press(&mut app, KeyPress::Char(' '));
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Char(' '));
        assert_eq!(app.marked.len(), 2);
        press(&mut app, KeyPress::Enter);
        assert!(app.marked.is_empty());
        assert_eq!(app.view, View::Metrics);
    }

    #[test]
    fn actions_apply_to_every_marked_pipeline() {
        let mut app = app_with_overview(&["a", "b", "c"]);
        press(&mut app, KeyPress::Char(' '));
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Char(' '));
        let commands = press(&mut app, KeyPress::Char('x'));
        assert_eq!(
            commands,
            vec![
                Cmd::Run(Action::Stop {
                    pipeline: "a".to_string(),
                    force: false
                }),
                Cmd::Run(Action::Stop {
                    pipeline: "b".to_string(),
                    force: false
                }),
            ]
        );
        assert!(app.toast.as_ref().unwrap().message.contains("2 pipelines"));
        // An explicit command argument overrides the marks.
        finish(
            &mut app,
            Action::Stop {
                pipeline: "c".to_string(),
                force: false,
            },
        );
        let commands = type_command(&mut app, "pause c");
        assert_eq!(
            commands,
            vec![Cmd::Run(Action::Pause {
                pipeline: "c".to_string()
            })]
        );
    }

    #[test]
    fn batch_confirmation_lists_and_runs_all_actions() {
        let mut app = app_with_overview(&["a", "b"]);
        app.ask_before_destructive = true;
        press(&mut app, KeyPress::Char(' '));
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Char(' '));
        let commands = press(&mut app, KeyPress::Char('x'));
        assert!(commands.is_empty());
        let Overlay::Confirm { actions } = &app.overlay else {
            panic!("batch confirm expected");
        };
        assert_eq!(actions.len(), 2);
        let commands = press(&mut app, KeyPress::Char('y'));
        assert_eq!(commands.len(), 2);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn delete_always_confirms_and_chains_behind_stop_and_clear() {
        let mut app = app_with_overview(&["a"]);
        // Even without --ask, delete asks.
        assert!(type_command(&mut app, "delete").is_empty());
        assert!(matches!(app.overlay, Overlay::Confirm { .. }));
        let commands = press(&mut app, KeyPress::Char('y'));
        // With nothing in flight the delete runs directly; the server is
        // the judge of whether the pipeline is deletable.
        assert_eq!(commands.len(), 1);
        finish(
            &mut app,
            Action::Delete {
                pipeline: "a".to_string(),
            },
        );

        // The teardown chain: stop, clear, delete.
        press(&mut app, KeyPress::Char('x'));
        press(&mut app, KeyPress::Char('C'));
        type_command(&mut app, "delete");
        press(&mut app, KeyPress::Char('y'));
        assert!(app.is_clear_queued("a"));
        finish(
            &mut app,
            Action::Stop {
                pipeline: "a".to_string(),
                force: false,
            },
        );
        // Stopped with storage still in use: only the clear fires.
        let mut overview = overview_with(&["a"]);
        overview.pipelines[0].deployment_status = "Stopped".to_string();
        overview.pipelines[0].storage_status = "InUse".to_string();
        let commands = update(
            &mut app,
            Msg::Overview {
                result: Ok(overview),
                latency_millis: 1,
                now_millis: 5_000,
            },
        );
        assert_eq!(
            commands
                .iter()
                .filter(|command| matches!(command, Cmd::Run(_)))
                .count(),
            1
        );
        finish(
            &mut app,
            Action::Clear {
                pipeline: "a".to_string(),
            },
        );
        // Once cleared, the delete fires.
        let mut overview = overview_with(&["a"]);
        overview.pipelines[0].deployment_status = "Stopped".to_string();
        overview.pipelines[0].storage_status = "Cleared".to_string();
        let commands = update(
            &mut app,
            Msg::Overview {
                result: Ok(overview),
                latency_millis: 1,
                now_millis: 6_000,
            },
        );
        assert!(commands.contains(&Cmd::Run(Action::Delete {
            pipeline: "a".to_string()
        })));
    }

    #[test]
    fn double_clicks_jump_to_the_clicked_columns_view() {
        let mut app = app_with_overview(&["a"]);
        app.viewport.pipelines.set((0, 20));
        use crate::app::state::{ClickTarget, ColumnHit};
        *app.viewport.click_columns.borrow_mut() = vec![
            ColumnHit {
                x_start: 1,
                x_end: 20,
                target: ClickTarget::Sql,
                sort: Some(SortColumn::Name),
            },
            ColumnHit {
                x_start: 21,
                x_end: 40,
                target: ClickTarget::Metrics,
                sort: Some(SortColumn::Throughput),
            },
            ColumnHit {
                x_start: 41,
                x_end: 50,
                target: ClickTarget::Logs,
                sort: None,
            },
        ];
        app.now_millis = 1_000;
        let row = crate::app::state::FIRST_TABLE_ROW;
        // First click selects; second within the window jumps.
        press(&mut app, KeyPress::Click { row, column: 25 });
        assert_eq!(app.view, View::Pipelines);
        press(&mut app, KeyPress::Click { row, column: 25 });
        assert_eq!(app.view, View::Metrics);

        // A slow second click is a fresh selection, not a jump.
        app.view = View::Pipelines;
        press(&mut app, KeyPress::Click { row, column: 5 });
        app.now_millis += 2_000;
        press(&mut app, KeyPress::Click { row, column: 5 });
        assert_eq!(app.view, View::Pipelines);
        // A fast second one on the name column opens the SQL.
        press(&mut app, KeyPress::Click { row, column: 5 });
        assert_eq!(app.view, View::Sql);
    }

    #[test]
    fn page_keys_move_by_the_rendered_page_height() {
        let names: Vec<String> = (0..30).map(|index| format!("p{index:02}")).collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let mut app = app_with_overview(&name_refs);
        assert_eq!(app.selected_name.as_deref(), Some("p00"));

        // The renderer reported a 12-row viewport.
        app.viewport.pipelines.set((0, 12));
        let commands = press(&mut app, KeyPress::PageDown);
        assert_eq!(app.selected_name.as_deref(), Some("p12"));
        assert_eq!(commands, vec![Cmd::Watch(Some("p12".to_string()))]);
        press(&mut app, KeyPress::PageDown);
        assert_eq!(app.selected_name.as_deref(), Some("p24"));
        // Clamps at the end instead of wrapping.
        press(&mut app, KeyPress::PageDown);
        assert_eq!(app.selected_name.as_deref(), Some("p29"));
        press(&mut app, KeyPress::PageUp);
        assert_eq!(app.selected_name.as_deref(), Some("p17"));

        // Before the first render a default page of 10 applies.
        app.viewport.pipelines.set((0, 0));
        press(&mut app, KeyPress::PageUp);
        assert_eq!(app.selected_name.as_deref(), Some("p07"));

        // The metrics view pages through pipelines the same way.
        app.viewport.pipelines.set((0, 12));
        app.view = View::Metrics;
        press(&mut app, KeyPress::PageDown);
        assert_eq!(app.selected_name.as_deref(), Some("p19"));
    }

    #[test]
    fn page_keys_page_connectors_and_hotspots_too() {
        let mut app = app_with_overview(&["a"]);
        update(&mut app, detail_msg("a", 10, 1_000));
        app.viewport.connectors.set((0, 5));
        app.view = View::Connectors;
        press(&mut app, KeyPress::PageDown);
        assert_eq!(app.selected_connector, 0, "one connector clamps");
        press(&mut app, KeyPress::PageUp);
        assert_eq!(app.selected_connector, 0);

        update(
            &mut app,
            Msg::HotspotsLoaded {
                pipeline: "a".to_string(),
                result: Ok(loaded_report()),
            },
        );
        app.viewport.hotspots.set((0, 5));
        app.view = View::Profiler;
        press(&mut app, KeyPress::PageDown);
        assert_eq!(app.selected_hotspot, 0, "one hotspot clamps");
        press(&mut app, KeyPress::PageUp);
        assert_eq!(app.selected_hotspot, 0);
    }

    #[test]
    fn header_clicks_sort_and_toggle_direction() {
        use crate::app::state::{ClickTarget, ColumnHit};
        let mut app = app_with_overview(&["a", "b"]);
        app.viewport.pipelines.set((0, 20));
        *app.viewport.click_columns.borrow_mut() = vec![
            ColumnHit {
                x_start: 1,
                x_end: 20,
                target: ClickTarget::Sql,
                sort: Some(SortColumn::Name),
            },
            ColumnHit {
                x_start: 21,
                x_end: 40,
                target: ClickTarget::Metrics,
                sort: Some(SortColumn::Memory),
            },
            ColumnHit {
                x_start: 41,
                x_end: 50,
                target: ClickTarget::Metrics,
                sort: None,
            },
        ];
        let header = crate::app::state::FIRST_TABLE_ROW - 1;
        // A new column sorts ascending.
        press(
            &mut app,
            KeyPress::Click {
                row: header,
                column: 25,
            },
        );
        assert_eq!(app.sort, SortColumn::Memory);
        assert!(!app.sort_descending);
        // The same column again flips to descending, and back.
        press(
            &mut app,
            KeyPress::Click {
                row: header,
                column: 25,
            },
        );
        assert!(app.sort_descending);
        press(
            &mut app,
            KeyPress::Click {
                row: header,
                column: 25,
            },
        );
        assert!(!app.sort_descending);
        // Another sortable column resets to ascending.
        app.sort_descending = true;
        press(
            &mut app,
            KeyPress::Click {
                row: header,
                column: 5,
            },
        );
        assert_eq!(app.sort, SortColumn::Name);
        assert!(!app.sort_descending);
        // Unsortable columns are inert, and header clicks never select rows.
        press(
            &mut app,
            KeyPress::Click {
                row: header,
                column: 45,
            },
        );
        assert_eq!(app.sort, SortColumn::Name);
        assert_eq!(app.selected_name.as_deref(), Some("a"));
    }

    #[test]
    fn clicks_use_the_rendered_scroll_offset() {
        let mut app = app_with_overview(&["a", "b", "c", "d", "e"]);
        // The renderer reported a window starting at the third row.
        app.viewport.pipelines.set((2, 2));
        let row = crate::app::state::FIRST_TABLE_ROW + 1;
        press(&mut app, KeyPress::Click { row, column: 5 });
        assert_eq!(app.selected_name.as_deref(), Some("d"));
        // Below the rendered window: inert even though more rows exist.
        press(
            &mut app,
            KeyPress::Click {
                row: row + 1,
                column: 5,
            },
        );
        assert_eq!(app.selected_name.as_deref(), Some("d"));
    }

    #[test]
    fn refresh_key_requests_a_poll() {
        let mut app = app_with_overview(&["a"]);
        assert_eq!(press(&mut app, KeyPress::Char('r')), vec![Cmd::Refresh]);
    }
}
