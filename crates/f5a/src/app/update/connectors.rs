//! Keys and actions of the Connectors view: tree navigation, marks that
//! work like the pipelines table, batch start and pause, sorting, and the
//! jump to a connector's declaration in the SQL.

use crate::gateway::{Action, ConnectorTarget};
use crate::model::sql_locate::{LineSpan, Located, locate};
use crate::model::stats::{ConnectorKind, ConnectorRow};

use super::super::connector_tree::{ConnectorNode, RecentErrors};
use super::super::msg::{Cmd, KeyPress};
use super::super::state::{App, FIRST_TABLE_ROW, SqlJump, View};
use super::{DOUBLE_CLICK_MILLIS, clicked_index, open_sql_from_here, page_size, run_now};

pub(super) fn on_connectors_key(app: &mut App, key: KeyPress) -> Vec<Cmd> {
    if app.connector_pane.is_detail_focused
        && let Some(commands) = on_detail_key(app, key)
    {
        return commands;
    }
    match key {
        KeyPress::Up | KeyPress::Char('k') | KeyPress::ScrollUp => move_cursor(app, -1),
        KeyPress::Down | KeyPress::Char('j') | KeyPress::ScrollDown => move_cursor(app, 1),
        KeyPress::PageUp => move_cursor(app, -page_size(app.viewport.connectors.get())),
        KeyPress::PageDown => move_cursor(app, page_size(app.viewport.connectors.get())),
        KeyPress::Home | KeyPress::Char('g') => move_to_edge(app, true),
        KeyPress::End | KeyPress::Char('G') => move_to_edge(app, false),
        KeyPress::Char(' ') => {
            toggle_mark(app);
            Vec::new()
        }
        KeyPress::ShiftUp => extend_range(app, -1),
        KeyPress::ShiftDown => extend_range(app, 1),
        KeyPress::Left => collapse_or_ascend(app),
        KeyPress::Right => {
            if let Some(node @ ConnectorNode::Relation { .. }) = app.connector_pane.cursor.clone() {
                app.connector_pane.collapsed.remove(&node);
            }
            Vec::new()
        }
        // Enter folds a relation, or moves the keys into a connector's errors.
        KeyPress::Enter => {
            match app.connector_pane.cursor.clone() {
                Some(node @ ConnectorNode::Relation { .. }) => {
                    if !app.connector_pane.collapsed.remove(&node) {
                        app.connector_pane.collapsed.insert(node);
                    }
                }
                Some(ConnectorNode::Endpoint(_)) => {
                    let has_errors = app
                        .selected_connector()
                        .is_some_and(ConnectorRow::has_errors);
                    app.connector_pane.is_detail_focused = has_errors;
                }
                None => {}
            }
            Vec::new()
        }
        // The pipelines table's keys: `s` starts, `p` pauses, `P` resumes,
        // and `x` stops; a connector's stop is its pause.
        KeyPress::Char('s') | KeyPress::Char('P') => connector_actions(app, false),
        KeyPress::Char('x') | KeyPress::Char('p') => connector_actions(app, true),
        KeyPress::Char('v') => jump_to_sql(app),
        KeyPress::Char('o') => {
            app.connector_pane.sort = app.connector_pane.sort.cycle();
            app.toast_info(format!(
                "Sorting connectors by {}.",
                app.connector_pane.sort.title()
            ));
            Vec::new()
        }
        KeyPress::Char('O') => {
            app.connector_pane.sort_descending = !app.connector_pane.sort_descending;
            Vec::new()
        }
        KeyPress::Click { row, column } => on_click(app, row, column),
        _ => Vec::new(),
    }
}

/// Keys while the error list has focus: they walk it one error at a time,
/// and `enter` returns to the tree. `None` hands the key to the tree.
fn on_detail_key(app: &mut App, key: KeyPress) -> Option<Vec<Cmd>> {
    let page = page_size(app.viewport.connector_detail.get());
    let delta = match key {
        KeyPress::Up | KeyPress::Char('k') | KeyPress::ScrollUp => -1,
        KeyPress::Down | KeyPress::Char('j') | KeyPress::ScrollDown => 1,
        KeyPress::PageUp => -page,
        KeyPress::PageDown => page,
        KeyPress::Home | KeyPress::Char('g') => isize::MIN,
        KeyPress::End | KeyPress::Char('G') => isize::MAX,
        KeyPress::Enter => {
            app.connector_pane.is_detail_focused = false;
            return Some(Vec::new());
        }
        // A click on the tree hands the keys back to it.
        KeyPress::Click { column, .. } if app.viewport.is_on_connector_tree(column) => {
            app.connector_pane.is_detail_focused = false;
            return None;
        }
        KeyPress::Click { .. } => return Some(Vec::new()),
        _ => return None,
    };
    let endpoint_name = app.selected_connector()?.endpoint_name.clone();
    app.connector_pane
        .step_selected_error(&endpoint_name, delta);
    Some(Vec::new())
}

/// Move the cursor `delta` rows, parking at the ends. Any move without
/// shift ends a shift run; its marks stay.
fn move_cursor(app: &mut App, delta: isize) -> Vec<Cmd> {
    app.connector_pane.mark_anchor = None;
    step_cursor(app, delta);
    ensure_recent_errors(app)
}

fn move_to_edge(app: &mut App, is_top: bool) -> Vec<Cmd> {
    let row_count = app.connector_tree().rows.len() as isize;
    move_cursor(app, if is_top { -row_count } else { row_count })
}

fn step_cursor(app: &mut App, delta: isize) {
    let next = {
        let tree = app.connector_tree();
        if tree.rows.is_empty() {
            return;
        }
        let current = app
            .connector_pane
            .cursor
            .as_ref()
            .and_then(|node| tree.position(node))
            .unwrap_or(0) as isize;
        let index = current
            .saturating_add(delta)
            .clamp(0, tree.rows.len() as isize - 1);
        tree.rows[index as usize].node.clone()
    };
    app.connector_pane.set_cursor(Some(next));
}

/// Endpoint names the cursor row stands for: a relation row covers all of
/// its connectors.
fn endpoints_at_cursor(app: &App) -> Vec<String> {
    let Some(cursor) = &app.connector_pane.cursor else {
        return Vec::new();
    };
    app.connector_tree()
        .connectors_under(cursor)
        .iter()
        .map(|connector| connector.endpoint_name.clone())
        .collect()
}

fn toggle_mark(app: &mut App) {
    let endpoints = endpoints_at_cursor(app);
    app.connector_pane.toggle_marks(endpoints);
}

/// Shift plus arrow, with the pipelines table's spreadsheet semantics: the
/// span runs from the anchor to the cursor, and only the span is rewritten.
fn extend_range(app: &mut App, delta: isize) -> Vec<Cmd> {
    let anchor = {
        let tree = app.connector_tree();
        app.connector_pane
            .mark_anchor
            .clone()
            .filter(|anchor| tree.position(anchor).is_some())
            .or_else(|| app.connector_pane.cursor.clone())
    };
    let Some(anchor) = anchor else {
        return Vec::new();
    };
    let span_to_cursor = |app: &App| {
        app.connector_pane
            .cursor
            .as_ref()
            .map(|cursor| app.connector_tree().endpoints_between(&anchor, cursor))
            .unwrap_or_default()
    };
    let previous_span = span_to_cursor(app);
    step_cursor(app, delta);
    let span = span_to_cursor(app);
    let pane = &mut app.connector_pane;
    pane.marked.retain(|name| !previous_span.contains(name));
    pane.marked.extend(span);
    pane.mark_anchor = Some(anchor);
    ensure_recent_errors(app)
}

/// `←` folds a relation, or climbs from a connector to its relation.
fn collapse_or_ascend(app: &mut App) -> Vec<Cmd> {
    match app.connector_pane.cursor.clone() {
        Some(node @ ConnectorNode::Relation { .. }) => {
            app.connector_pane.collapsed.insert(node);
        }
        Some(node @ ConnectorNode::Endpoint(_)) => {
            let parent = app
                .connector_tree()
                .group_of(&node)
                .map(|group| group.node());
            if parent.is_some() {
                app.connector_pane.set_cursor(parent);
            }
        }
        None => {}
    }
    Vec::new()
}

/// Start (or pause) every marked connector, else the ones the cursor row
/// stands for. Only inputs can be paused; outputs in the selection are
/// skipped with a note. Repeating a pause or start is harmless on the
/// server, so stale stats never cause a request to be dropped.
pub(super) fn connector_actions(app: &mut App, pause: bool) -> Vec<Cmd> {
    let Some(pipeline) = app.selected_name.clone() else {
        app.toast_error("Select a pipeline first.");
        return Vec::new();
    };
    let (actions, output_count) = {
        let tree = app.connector_tree();
        let targets = if app.connector_pane.marked.is_empty() {
            app.connector_pane
                .cursor
                .as_ref()
                .map(|cursor| tree.connectors_under(cursor))
                .unwrap_or_default()
        } else {
            tree.all_connectors()
                .filter(|connector| app.connector_pane.marked.contains(&connector.endpoint_name))
                .collect()
        };
        let output_count = targets
            .iter()
            .filter(|connector| connector.kind == ConnectorKind::Output)
            .count();
        if targets.is_empty() {
            app.toast_error("No connector selected; open the Connectors view.");
            return Vec::new();
        }
        let actions: Vec<Action> = targets
            .iter()
            .filter(|connector| connector.kind == ConnectorKind::Input)
            .map(|connector| {
                let pipeline = pipeline.clone();
                let table = connector.relation.clone();
                let connector = connector.connector_name().to_string();
                if pause {
                    Action::ConnectorPause {
                        pipeline,
                        table,
                        connector,
                    }
                } else {
                    Action::ConnectorResume {
                        pipeline,
                        table,
                        connector,
                    }
                }
            })
            .collect();
        (actions, output_count)
    };
    if actions.is_empty() {
        app.toast_error("Only input connectors can be paused or started.");
        return Vec::new();
    }
    let count = actions.len();
    let mut commands = Vec::new();
    for action in actions {
        commands.extend(run_now(app, action));
    }
    let verb = if pause { "pause" } else { "start" };
    let skipped = match output_count {
        0 => String::new(),
        1 => "; skipped 1 output connector".to_string(),
        many => format!("; skipped {many} output connectors"),
    };
    if count > 1 || output_count > 0 {
        let noun = if count == 1 {
            "connector"
        } else {
            "connectors"
        };
        app.toast_info(format!("Requested {verb} for {count} {noun}{skipped}."));
    }
    commands
}

/// Open the SQL view at the cursor's declaration: a connector's object in
/// its relation's `connectors` property, or a relation's whole property.
/// When the program is not loaded yet, the jump waits for it.
pub(super) fn jump_to_sql(app: &mut App) -> Vec<Cmd> {
    let Some(pipeline) = app.selected_name.clone() else {
        app.toast_error("Select a pipeline first.");
        return Vec::new();
    };
    let jump = {
        let tree = app.connector_tree();
        match &app.connector_pane.cursor {
            Some(ConnectorNode::Relation { name, .. }) => Some(SqlJump {
                pipeline,
                relation: name.clone(),
                connector: None,
            }),
            Some(ConnectorNode::Endpoint(endpoint)) => {
                tree.find_connector(endpoint).map(|connector| SqlJump {
                    pipeline,
                    relation: connector.relation.clone(),
                    connector: Some(connector.connector_name().to_string()),
                })
            }
            None => None,
        }
    };
    let Some(jump) = jump else {
        app.toast_error("No connector selected.");
        return Vec::new();
    };
    app.pending_sql_jump = Some(jump);
    let commands = open_sql_from_here(app);
    apply_pending_sql_jump(app);
    commands
}

/// Resolve a waiting jump once the SQL of its pipeline is loaded.
pub(super) fn apply_pending_sql_jump(app: &mut App) {
    let Some(jump) = app.pending_sql_jump.clone() else {
        return;
    };
    let Some(document) = app
        .sql
        .as_ref()
        .filter(|document| document.pipeline == jump.pipeline)
    else {
        return;
    };
    let located = locate(&document.text, &jump.relation, jump.connector.as_deref());
    app.pending_sql_jump = None;
    let relation = &jump.relation;
    let focus = match (located, jump.connector.as_deref()) {
        (Some(Located::Connector(span)), _) | (Some(Located::Connectors(span)), None) => span,
        (Some(Located::Connectors(span)), Some(connector)) => {
            app.toast_info(format!(
                "`{connector}` has no entry of its own in `{relation}`; showing all its connectors."
            ));
            span
        }
        (Some(Located::Relation(span)), _) => {
            app.toast_info(format!(
                "`{relation}` declares no `connectors` property; showing its CREATE statement."
            ));
            span
        }
        (None, _) => {
            app.toast_error(format!(
                "`{relation}` is not declared in this program's SQL."
            ));
            return;
        }
    };
    focus_sql(app, focus);
}

fn focus_sql(app: &mut App, span: LineSpan) {
    app.sql_focus = Some((span.first, span.last));
    app.sql_scroll = span.first.saturating_sub(4);
}

fn on_click(app: &mut App, row: u16, column: u16) -> Vec<Cmd> {
    // A click on the detail pane focuses it when there are errors to scroll.
    if !app.viewport.is_on_connector_tree(column) {
        if app
            .selected_connector()
            .is_some_and(ConnectorRow::has_errors)
        {
            app.connector_pane.is_detail_focused = true;
        }
        return Vec::new();
    }
    if row == FIRST_TABLE_ROW - 1 {
        app.last_click = None;
        if let Some(sort) = app.viewport.connector_sort_at(column) {
            let pane = &mut app.connector_pane;
            if pane.sort == sort {
                pane.sort_descending = !pane.sort_descending;
            } else {
                pane.sort = sort;
                pane.sort_descending = false;
            }
            let direction = if pane.sort_descending {
                "descending"
            } else {
                "ascending"
            };
            let title = pane.sort.title();
            app.toast_info(format!("Sorting connectors by {title} {direction}."));
        }
        return Vec::new();
    }
    let clicked = {
        let tree = app.connector_tree();
        clicked_index(row, tree.rows.len(), app.viewport.connectors.get())
            .map(|index| tree.rows[index].node.clone())
    };
    let Some(node) = clicked else {
        app.last_click = None;
        return Vec::new();
    };
    let is_double = app.last_click.is_some_and(|(last_row, at_millis)| {
        last_row == row && app.now_millis.saturating_sub(at_millis) <= DOUBLE_CLICK_MILLIS
    });
    app.connector_pane.mark_anchor = None;
    app.connector_pane.set_cursor(Some(node));
    if is_double {
        // Like a double click on a pipeline's name, it opens the SQL.
        app.last_click = None;
        return jump_to_sql(app);
    }
    app.last_click = Some((row, app.now_millis));
    ensure_recent_errors(app)
}

/// Fetch the recent errors of the connector under the cursor while the
/// Connectors view shows it, when it has errors the pane has not listed yet.
pub(super) fn ensure_recent_errors(app: &mut App) -> Vec<Cmd> {
    if app.view != View::Connectors || app.connector_pane.is_loading_errors {
        return Vec::new();
    }
    let Some(pipeline) = app.selected_name.clone() else {
        return Vec::new();
    };
    let Some(connector) = app.selected_connector() else {
        return Vec::new();
    };
    let is_listed = app
        .connector_pane
        .recent_errors
        .as_ref()
        .is_some_and(|listed| {
            listed.endpoint_name == connector.endpoint_name
                && listed.error_count == connector.error_count
        });
    if connector.error_count == 0 || is_listed {
        return Vec::new();
    }
    let command = Cmd::FetchConnectorStatus {
        pipeline,
        endpoint_name: connector.endpoint_name.clone(),
        target: ConnectorTarget {
            kind: connector.kind,
            relation: connector.relation.clone(),
            connector: connector.connector_name().to_string(),
        },
    };
    app.connector_pane.is_loading_errors = true;
    vec![command]
}

/// Store a fetched status's recent errors; a failure is kept too, so the
/// pane explains it and does not refetch until the error count moves.
pub(super) fn on_connector_status(
    app: &mut App,
    pipeline: String,
    endpoint_name: String,
    result: Result<Box<crate::model::stats::ConnectorRow>, crate::http::ApiError>,
) -> Vec<Cmd> {
    // A reset for another pipeline already cleared the loading flag.
    if app.selected_name.as_deref() != Some(pipeline.as_str()) {
        return Vec::new();
    }
    app.connector_pane.is_loading_errors = false;
    let current_count = app
        .connector_tree()
        .find_connector(&endpoint_name)
        .map_or(0, |connector| connector.error_count);
    app.connector_pane.recent_errors = Some(match result {
        Ok(status) => RecentErrors {
            endpoint_name,
            error_count: status.error_count,
            result: Ok(status.recent_errors),
        },
        Err(error) => RecentErrors {
            endpoint_name,
            error_count: current_count,
            result: Err(error.to_string()),
        },
    });
    // The cursor may have moved on while the request ran.
    ensure_recent_errors(app)
}

#[cfg(test)]
mod tests {
    use super::super::update;
    use crate::app::connector_tree::{ConnectorNode, ConnectorSort};
    use crate::app::msg::{Cmd, KeyPress, Msg};
    use crate::app::state::{App, FIRST_TABLE_ROW, SqlDocument, View};
    use crate::gateway::{Action, ConnectorTarget, PipelineDetail};
    use crate::http::ApiError;
    use crate::model::stats::{ConnectorKind, ConnectorRow, PipelineStats};
    use crate::testutil::overview_with;
    use serde_json::json;

    /// Table `orders` with two inputs, table `users` with one, and view
    /// `totals` with one output.
    fn stats(kafka_errors: i64) -> PipelineStats {
        PipelineStats::from_json(&json!({
            "inputs": [
                {"endpoint_name": "orders.kafka", "config": {"stream": "orders"},
                 "metrics": {"total_records": 500, "num_parse_errors": kafka_errors}},
                {"endpoint_name": "orders.http", "config": {"stream": "orders"}, "paused": true,
                 "metrics": {"total_records": 100}},
                {"endpoint_name": "users.unnamed-0", "config": {"stream": "users"},
                 "metrics": {"total_records": 50}}
            ],
            "outputs": [
                {"endpoint_name": "totals.delta", "config": {"stream": "totals"},
                 "metrics": {"transmitted_records": 70}}
            ]
        }))
    }

    fn detail(app: &mut App, stats: PipelineStats, now_millis: i64) -> Vec<Cmd> {
        update(
            app,
            Msg::Detail {
                pipeline: "p".to_string(),
                result: Ok(PipelineDetail {
                    stats,
                    series: Default::default(),
                }),
                now_millis,
            },
        )
    }

    fn connectors_app() -> App {
        let mut app = App::new("http://x".to_string(), 2);
        update(
            &mut app,
            Msg::Overview {
                result: Ok(overview_with(&["p"])),
                latency_millis: 1,
                now_millis: 1_000,
            },
        );
        detail(&mut app, stats(0), 1_000);
        update(&mut app, Msg::Key(KeyPress::Char('3')));
        app
    }

    fn press(app: &mut App, key: KeyPress) -> Vec<Cmd> {
        update(app, Msg::Key(key))
    }

    fn cursor(app: &App) -> String {
        match app.connector_pane.cursor.as_ref().expect("a cursor") {
            ConnectorNode::Relation { name, .. } => format!("[{name}]"),
            ConnectorNode::Endpoint(endpoint) => endpoint.clone(),
        }
    }

    fn marked(app: &App) -> Vec<&str> {
        let mut names: Vec<&str> = app
            .connector_pane
            .marked
            .iter()
            .map(String::as_str)
            .collect();
        names.sort_unstable();
        names
    }

    fn pause(table: &str, connector: &str) -> Cmd {
        Cmd::Run(Action::ConnectorPause {
            pipeline: "p".to_string(),
            table: table.to_string(),
            connector: connector.to_string(),
        })
    }

    fn start(table: &str, connector: &str) -> Cmd {
        Cmd::Run(Action::ConnectorResume {
            pipeline: "p".to_string(),
            table: table.to_string(),
            connector: connector.to_string(),
        })
    }

    #[test]
    fn the_cursor_walks_the_tree_and_parks_at_the_ends() {
        let mut app = connectors_app();
        assert_eq!(
            cursor(&app),
            "[orders]",
            "a detail refresh seeds the cursor"
        );
        press(&mut app, KeyPress::Down);
        assert_eq!(cursor(&app), "orders.http");
        press(&mut app, KeyPress::Up);
        press(&mut app, KeyPress::Up);
        assert_eq!(cursor(&app), "[orders]");
        press(&mut app, KeyPress::End);
        assert_eq!(cursor(&app), "totals.delta");
        press(&mut app, KeyPress::ScrollDown);
        assert_eq!(cursor(&app), "totals.delta");
        app.viewport.connectors.set((0, 3));
        press(&mut app, KeyPress::PageUp);
        assert_eq!(cursor(&app), "[users]");
        press(&mut app, KeyPress::Char('g'));
        assert_eq!(cursor(&app), "[orders]");
    }

    #[test]
    fn arrows_and_enter_fold_relations_without_leaving_the_tab() {
        let mut app = connectors_app();
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Left);
        assert_eq!(cursor(&app), "[orders]", "left climbs to the relation");
        press(&mut app, KeyPress::Left);
        press(&mut app, KeyPress::Down);
        assert_eq!(cursor(&app), "[users]", "folded connectors are skipped");
        press(&mut app, KeyPress::Up);
        press(&mut app, KeyPress::Right);
        press(&mut app, KeyPress::Down);
        assert_eq!(cursor(&app), "orders.http");
        assert!(press(&mut app, KeyPress::Enter).is_empty());
        assert!(
            !app.connector_pane.is_detail_focused,
            "no errors, nothing to browse"
        );
        press(&mut app, KeyPress::Up);
        press(&mut app, KeyPress::Enter);
        assert_eq!(
            relation_rows(&app)[..2],
            ["[orders]", "[users]"],
            "enter folds"
        );
        press(&mut app, KeyPress::Enter);
        assert_eq!(relation_rows(&app)[1], "orders.http", "and unfolds");
        assert_eq!(app.view, View::Connectors, "enter never leaves tab 3");
    }

    /// `users` only reached end of input and `totals` failed; neither has an
    /// Active connector.
    fn mostly_idle_stats() -> PipelineStats {
        let mut stats = stats(0);
        stats.inputs[2].end_of_input = true;
        stats.outputs[0].fatal_error = Some("disk full".to_string());
        stats
    }

    fn relation_rows(app: &App) -> Vec<String> {
        app.connector_tree()
            .rows
            .iter()
            .map(|row| match &row.node {
                ConnectorNode::Relation { name, .. } => format!("[{name}]"),
                ConnectorNode::Endpoint(endpoint) => endpoint.clone(),
            })
            .collect()
    }

    #[test]
    fn the_first_visit_expands_only_relations_with_active_connectors() {
        let mut app = App::new("http://x".to_string(), 2);
        update(
            &mut app,
            Msg::Overview {
                result: Ok(overview_with(&["p", "q"])),
                latency_millis: 1,
                now_millis: 1_000,
            },
        );
        detail(&mut app, mostly_idle_stats(), 1_000);
        assert!(!app.connector_pane.has_default_folds, "not shown yet");
        press(&mut app, KeyPress::Char('3'));
        let folded = vec![
            "[orders]",
            "orders.http",
            "orders.kafka",
            "[users]",
            "[totals]",
        ];
        assert_eq!(relation_rows(&app), folded);

        // The user's folds survive leaving and returning.
        press(&mut app, KeyPress::End);
        press(&mut app, KeyPress::Right);
        press(&mut app, KeyPress::Char('1'));
        detail(&mut app, mostly_idle_stats(), 3_000);
        press(&mut app, KeyPress::Char('3'));
        assert!(relation_rows(&app).contains(&"totals.delta".to_string()));

        // Another pipeline gets its own first look, also when its stats
        // arrive after the view opened.
        app.select_pipeline("q".to_string());
        update(
            &mut app,
            Msg::Detail {
                pipeline: "q".to_string(),
                result: Ok(PipelineDetail {
                    stats: mostly_idle_stats(),
                    series: Default::default(),
                }),
                now_millis: 5_000,
            },
        );
        assert_eq!(relation_rows(&app), folded);
    }

    #[test]
    fn reaching_eoi_folds_the_relation_under_the_cursor() {
        let mut app = connectors_app();
        app.connector_pane.cursor = Some(ConnectorNode::Endpoint("users.unnamed-0".to_string()));
        detail(&mut app, mostly_idle_stats(), 3_000);
        assert_eq!(cursor(&app), "[users]");
        press(&mut app, KeyPress::Right);
        detail(&mut app, mostly_idle_stats(), 5_000);
        assert!(relation_rows(&app).contains(&"users.unnamed-0".to_string()));
    }

    /// Five parse errors for `orders.kafka`, newest first.
    fn kafka_errors(app: &mut App) {
        let errors = (1..=5)
            .rev()
            .map(|index| {
                json!({"timestamp": "2026-01-02T03:04:05Z", "index": index,
                                 "message": format!("bad row {index}")})
            })
            .collect::<Vec<_>>();
        let status = ConnectorRow::from_json(
            &json!({"endpoint_name": "orders.kafka", "metrics": {"num_parse_errors": 2},
                    "parse_errors": errors}),
            ConnectorKind::Input,
        );
        status_loaded(app, Ok(status));
    }

    fn selected_index(app: &App) -> Option<i64> {
        app.connector_pane.selected_error.map(|(_, index)| index)
    }

    #[test]
    fn enter_browses_a_connectors_errors_one_at_a_time() {
        let mut app = connectors_app();
        detail(&mut app, stats(2), 3_000);
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Down);
        assert_eq!(cursor(&app), "orders.kafka");
        kafka_errors(&mut app);
        app.viewport.connector_detail.set((0, 3));
        assert!(press(&mut app, KeyPress::Enter).is_empty());
        assert!(app.connector_pane.is_detail_focused);
        assert_eq!(selected_index(&app), None, "the newest, until a key moves");

        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Char('j'));
        assert_eq!(selected_index(&app), Some(3), "one error per key");
        assert_eq!(cursor(&app), "orders.kafka", "the tree cursor stays");
        press(&mut app, KeyPress::Char('G'));
        press(&mut app, KeyPress::ScrollDown);
        assert_eq!(selected_index(&app), Some(1), "parks at the oldest");
        press(&mut app, KeyPress::PageUp);
        assert_eq!(selected_index(&app), Some(4), "a page is the pane's height");
        press(&mut app, KeyPress::Char('g'));
        assert_eq!(selected_index(&app), Some(5));

        press(&mut app, KeyPress::Enter);
        assert!(
            !app.connector_pane.is_detail_focused,
            "enter returns to the tree"
        );
        press(&mut app, KeyPress::Enter);
        press(&mut app, KeyPress::Escape);
        assert!(!app.connector_pane.is_detail_focused, "so does esc");
        assert_eq!(app.view, View::Connectors);
        press(&mut app, KeyPress::Enter);
        press(&mut app, KeyPress::Char('1'));
        press(&mut app, KeyPress::Char('3'));
        assert!(
            !app.connector_pane.is_detail_focused,
            "tab 3 opens on the tree"
        );
    }

    #[test]
    fn leaving_the_row_or_clicking_the_tree_returns_the_keys() {
        let mut app = connectors_app();
        detail(&mut app, stats(2), 3_000);
        app.viewport.connectors.set((0, 20));
        app.viewport.connector_tree_right.set(Some(60));
        app.viewport.connector_detail.set((0, 10));
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Down);
        kafka_errors(&mut app);
        press(&mut app, KeyPress::Enter);
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Left);
        assert_eq!(cursor(&app), "[orders]");
        assert!(
            !app.connector_pane.is_detail_focused,
            "another row, another pane"
        );
        assert_eq!(app.connector_pane.selected_error, None);

        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Down);
        let detail_pane = KeyPress::Click {
            row: FIRST_TABLE_ROW + 3,
            column: 70,
        };
        press(&mut app, detail_pane);
        assert!(
            app.connector_pane.is_detail_focused,
            "a click focuses the errors"
        );
        press(
            &mut app,
            KeyPress::Click {
                row: FIRST_TABLE_ROW + 1,
                column: 5,
            },
        );
        assert!(!app.connector_pane.is_detail_focused);
        assert_eq!(cursor(&app), "orders.http", "the click also selects");
        press(&mut app, detail_pane);
        assert!(
            !app.connector_pane.is_detail_focused,
            "nothing to scroll without errors"
        );
    }

    #[test]
    fn a_collapse_moves_a_hidden_cursor_to_its_relation() {
        let mut app = connectors_app();
        app.connector_pane.cursor = Some(ConnectorNode::Endpoint("users.unnamed-0".to_string()));
        app.connector_pane
            .collapsed
            .insert(ConnectorNode::Relation {
                kind: ConnectorKind::Input,
                name: "users".to_string(),
            });
        detail(&mut app, stats(0), 3_000);
        assert_eq!(cursor(&app), "[users]");
        let mut vanished = stats(0);
        vanished.inputs.retain(|input| input.relation != "users");
        detail(&mut app, vanished, 5_000);
        assert_eq!(
            cursor(&app),
            "[orders]",
            "a vanished row falls back to the first"
        );
    }

    #[test]
    fn space_and_shift_arrows_mark_like_the_pipelines_table() {
        let mut app = connectors_app();
        press(&mut app, KeyPress::Char(' '));
        assert_eq!(
            marked(&app),
            vec!["orders.http", "orders.kafka"],
            "relation marks all"
        );
        press(&mut app, KeyPress::Char(' '));
        assert!(marked(&app).is_empty());

        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::ShiftDown);
        assert_eq!(marked(&app), vec!["orders.http", "orders.kafka"]);
        press(&mut app, KeyPress::ShiftDown);
        assert_eq!(
            marked(&app),
            vec!["orders.http", "orders.kafka", "users.unnamed-0"]
        );
        press(&mut app, KeyPress::ShiftUp);
        press(&mut app, KeyPress::ShiftUp);
        assert_eq!(
            marked(&app),
            vec!["orders.http"],
            "reversing shrinks the span"
        );

        press(&mut app, KeyPress::Escape);
        assert!(marked(&app).is_empty(), "esc clears connector marks first");
        assert_eq!(app.view, View::Connectors);
        press(&mut app, KeyPress::Escape);
        assert_eq!(app.view, View::Pipelines);
    }

    #[test]
    fn lifecycle_keys_start_and_pause_every_marked_input() {
        let mut app = connectors_app();
        press(&mut app, KeyPress::Char(' '));
        press(&mut app, KeyPress::End);
        press(&mut app, KeyPress::Char(' '));
        let commands = press(&mut app, KeyPress::Char('x'));
        assert_eq!(
            commands,
            vec![pause("orders", "http"), pause("orders", "kafka")],
            "display order; the output connector is skipped"
        );
        let toast = &app.toast.as_ref().expect("a summary").message;
        assert_eq!(
            toast,
            "Requested pause for 2 connectors; skipped 1 output connector."
        );
        assert_eq!(
            press(&mut app, KeyPress::Char('s')),
            vec![start("orders", "http"), start("orders", "kafka")]
        );
        assert_eq!(press(&mut app, KeyPress::Char('p')).len(), 2);
        assert_eq!(press(&mut app, KeyPress::Char('P')).len(), 2);
    }

    #[test]
    fn without_marks_actions_follow_the_cursor_row() {
        let mut app = connectors_app();
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Down);
        assert_eq!(
            press(&mut app, KeyPress::Char('p')),
            vec![pause("orders", "kafka")]
        );
        assert!(
            app.toast.is_none(),
            "a single request toasts when it finishes"
        );
        press(&mut app, KeyPress::End);
        assert!(press(&mut app, KeyPress::Char('s')).is_empty());
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("Only input connectors")
        );
        app.detail = None;
        app.reconcile_connector_cursor();
        assert!(press(&mut app, KeyPress::Char('s')).is_empty());
        assert!(app.toast.as_ref().unwrap().message.contains("No connector"));
    }

    #[test]
    fn sorting_cycles_by_key_and_by_header_click() {
        let mut app = connectors_app();
        press(&mut app, KeyPress::Char('o'));
        assert_eq!(app.connector_pane.sort, ConnectorSort::Status);
        press(&mut app, KeyPress::Char('O'));
        assert!(app.connector_pane.sort_descending);

        app.viewport
            .connector_columns
            .borrow_mut()
            .push(crate::app::state::SortHit {
                x_start: 30,
                x_end: 40,
                sort: Some(ConnectorSort::Records),
            });
        let header = KeyPress::Click {
            row: FIRST_TABLE_ROW - 1,
            column: 32,
        };
        press(&mut app, header);
        assert_eq!(app.connector_pane.sort, ConnectorSort::Records);
        assert!(!app.connector_pane.sort_descending);
        press(&mut app, header);
        assert!(app.connector_pane.sort_descending);
        assert!(app.toast.as_ref().unwrap().message.contains("descending"));

        update(&mut app, Msg::Key(KeyPress::Char(':')));
        for character in "sort errors".chars() {
            press(&mut app, KeyPress::Char(character));
        }
        press(&mut app, KeyPress::Enter);
        assert_eq!(app.connector_pane.sort, ConnectorSort::Errors);
        assert_eq!(
            app.sort,
            crate::app::state::SortColumn::Name,
            "the pipelines sort is untouched"
        );
    }

    #[test]
    fn clicks_select_rows_and_double_clicks_open_the_sql() {
        let mut app = connectors_app();
        app.viewport.connectors.set((0, 20));
        app.viewport.connector_tree_right.set(Some(60));
        let row = FIRST_TABLE_ROW + 2;
        press(&mut app, KeyPress::Click { row, column: 5 });
        assert_eq!(cursor(&app), "orders.kafka");
        press(
            &mut app,
            KeyPress::Click {
                row: FIRST_TABLE_ROW + 4,
                column: 70,
            },
        );
        assert_eq!(
            cursor(&app),
            "orders.kafka",
            "the detail pane ignores clicks"
        );
        press(
            &mut app,
            KeyPress::Click {
                row: FIRST_TABLE_ROW + 30,
                column: 5,
            },
        );
        assert_eq!(app.last_click, None, "empty space resets double clicks");
        press(&mut app, KeyPress::Click { row, column: 5 });
        let commands = press(&mut app, KeyPress::Click { row, column: 5 });
        assert_eq!(app.view, View::Sql);
        assert_eq!(commands, vec![Cmd::FetchSql("p".to_string())]);
    }

    const PROGRAM: &str = "CREATE TABLE orders (id INT) WITH ('connectors' = '[\n  {\"name\": \"http\"},\n  {\"name\": \"kafka\"}\n]');\nCREATE TABLE users (id INT) WITH ('connectors' = '[{}]');\nCREATE VIEW totals AS SELECT 1;";

    fn load_sql(app: &mut App, text: &str) -> Vec<Cmd> {
        update(
            app,
            Msg::SqlLoaded {
                pipeline: "p".to_string(),
                result: Ok(text.to_string()),
            },
        )
    }

    #[test]
    fn v_jumps_to_the_connector_once_the_sql_arrives() {
        let mut app = connectors_app();
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Down);
        assert_eq!(
            press(&mut app, KeyPress::Char('v')),
            vec![Cmd::FetchSql("p".to_string())]
        );
        assert_eq!(app.view, View::Sql);
        assert!(app.pending_sql_jump.is_some());
        load_sql(&mut app, PROGRAM);
        assert_eq!(app.sql_focus, Some((3, 3)));
        assert!(app.pending_sql_jump.is_none());
        press(&mut app, KeyPress::Escape);
        assert_eq!(app.view, View::Connectors, "esc returns to the connector");
        assert_eq!(cursor(&app), "orders.kafka");
        press(&mut app, KeyPress::Char('v'));

        // Loaded SQL resolves at once; unnamed connectors go by position.
        press(&mut app, KeyPress::Char('3'));
        press(&mut app, KeyPress::Home);
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Down);
        assert_eq!(cursor(&app), "users.unnamed-0");
        assert!(press(&mut app, KeyPress::Char('v')).is_empty());
        assert_eq!(app.sql_focus, Some((5, 5)));
        assert_eq!(app.sql_scroll, 1);
    }

    #[test]
    fn sql_jumps_explain_what_they_could_not_find() {
        let mut app = connectors_app();
        app.sql = Some(SqlDocument {
            pipeline: "p".to_string(),
            text: PROGRAM.to_string(),
        });
        press(&mut app, KeyPress::Char('v'));
        assert_eq!(
            app.sql_focus,
            Some((1, 4)),
            "a relation shows all its connectors"
        );
        assert!(app.toast.is_none());

        press(&mut app, KeyPress::Char('3'));
        press(&mut app, KeyPress::End);
        press(&mut app, KeyPress::Char('v'));
        assert_eq!(app.sql_focus, Some((6, 6)));
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("no `connectors`")
        );

        app.sql = Some(SqlDocument {
            pipeline: "p".to_string(),
            text: "CREATE TABLE orders (id INT) WITH ('connectors' = '[]');".to_string(),
        });
        press(&mut app, KeyPress::Char('3'));
        press(&mut app, KeyPress::Home);
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Char('v'));
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("no entry of its own")
        );

        app.sql = Some(SqlDocument {
            pipeline: "p".to_string(),
            text: "SELECT 1;".to_string(),
        });
        app.sql_focus = None;
        press(&mut app, KeyPress::Char('3'));
        press(&mut app, KeyPress::Char('v'));
        assert_eq!(app.sql_focus, None);
        assert!(app.toast.as_ref().unwrap().message.contains("not declared"));
    }

    #[test]
    fn failed_sql_loads_drop_the_waiting_jump() {
        let mut app = connectors_app();
        press(&mut app, KeyPress::Char('v'));
        update(
            &mut app,
            Msg::SqlLoaded {
                pipeline: "p".to_string(),
                result: Err(ApiError::Transport("down".to_string())),
            },
        );
        assert!(app.pending_sql_jump.is_none());
        app.selected_name = None;
        assert!(press(&mut app, KeyPress::Char('3')).is_empty());
        press(&mut app, KeyPress::Char('v'));
        assert!(
            app.toast
                .as_ref()
                .unwrap()
                .message
                .contains("Select a pipeline")
        );
    }

    fn kafka_target() -> ConnectorTarget {
        ConnectorTarget {
            kind: ConnectorKind::Input,
            relation: "orders".to_string(),
            connector: "kafka".to_string(),
        }
    }

    fn fetch_kafka() -> Cmd {
        Cmd::FetchConnectorStatus {
            pipeline: "p".to_string(),
            endpoint_name: "orders.kafka".to_string(),
            target: kafka_target(),
        }
    }

    fn status_loaded(app: &mut App, result: Result<ConnectorRow, ApiError>) -> Vec<Cmd> {
        update(
            app,
            Msg::ConnectorStatusLoaded {
                pipeline: "p".to_string(),
                endpoint_name: "orders.kafka".to_string(),
                result: result.map(Box::new),
            },
        )
    }

    #[test]
    fn recent_errors_load_for_an_erroring_connector_under_the_cursor() {
        let mut app = connectors_app();
        detail(&mut app, stats(2), 3_000);
        press(&mut app, KeyPress::Down);
        assert_eq!(press(&mut app, KeyPress::Down), vec![fetch_kafka()]);
        assert!(
            detail(&mut app, stats(2), 5_000).is_empty(),
            "one request at a time"
        );
        let status = ConnectorRow::from_json(
            &json!({
                "endpoint_name": "orders.kafka",
                "metrics": {"num_parse_errors": 2},
                "parse_errors": [{"message": "bad row"}]
            }),
            ConnectorKind::Input,
        );
        assert!(status_loaded(&mut app, Ok(status)).is_empty());
        let listed = app.connector_pane.recent_errors.as_ref().unwrap();
        assert_eq!(listed.result.as_ref().unwrap()[0].message, "bad row");
        assert!(
            detail(&mut app, stats(2), 7_000).is_empty(),
            "unchanged counts reuse the list"
        );
        assert_eq!(detail(&mut app, stats(3), 9_000), vec![fetch_kafka()]);
        status_loaded(&mut app, Err(ApiError::Transport("gone".to_string())));
        let listed = app.connector_pane.recent_errors.as_ref().unwrap();
        assert_eq!(listed.result, Err("connection failed: gone".to_string()));
        assert!(
            detail(&mut app, stats(3), 11_000).is_empty(),
            "no retry loop"
        );
    }

    #[test]
    fn recent_errors_wait_for_the_view_and_ignore_other_pipelines() {
        let mut app = connectors_app();
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Down);
        press(&mut app, KeyPress::Char('1'));
        assert!(
            detail(&mut app, stats(4), 3_000).is_empty(),
            "not on screen"
        );
        assert_eq!(press(&mut app, KeyPress::Char('3')), vec![fetch_kafka()]);
        app.selected_name = Some("other".to_string());
        assert!(status_loaded(&mut app, Ok(ConnectorRow::default())).is_empty());
        assert!(
            app.connector_pane.is_loading_errors,
            "left for the other pipeline"
        );
    }
}
