//! Whole-screen render tests: every view and overlay draws into a test
//! backend, and the buffer must show the load-bearing content.

use ratatui::Terminal;
use ratatui::backend::TestBackend;
use serde_json::json;

use crate::app::state::{App, Connection, Overlay, Prompt, View};
use crate::app::update::update;
use crate::app::{KeyPress, Msg};
use crate::gateway::{Action, PipelineDetail};
use crate::model::dataflow::{DataflowIndex, HotspotReport};
use crate::model::profile::CircuitProfile;
use crate::model::timeseries::{TimePoint, TimeSeries};
use crate::testutil::{overview_with, stats_with_processed};

use super::render;

fn draw(app: &App, width: u16, height: u16) -> String {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|frame| render(frame, app)).unwrap();
    let buffer = terminal.backend().buffer();
    let mut text = String::new();
    for y in 0..buffer.area.height {
        for x in 0..buffer.area.width {
            text.push_str(buffer[(x, y)].symbol());
        }
        text.push('\n');
    }
    text
}

fn populated_app() -> App {
    let mut app = App::new("http://127.0.0.1:8080".to_string(), 2);
    update(
        &mut app,
        Msg::Overview {
            result: Ok(overview_with(&["orders_pipeline", "inventory"])),
            latency_millis: 4,
            now_millis: 10_000,
        },
    );
    update(
        &mut app,
        Msg::RowStats {
            pipeline: "inventory".to_string(),
            stats: stats_with_processed(1_000),
            now_millis: 10_000,
        },
    );
    update(
        &mut app,
        Msg::RowStats {
            pipeline: "inventory".to_string(),
            stats: stats_with_processed(2_000),
            now_millis: 11_000,
        },
    );
    // Rows sort by name, so "inventory" is selected first; move to the
    // pipeline the detail fixtures describe.
    update(&mut app, Msg::Key(KeyPress::Down));
    update(
        &mut app,
        Msg::Detail {
            pipeline: "orders_pipeline".to_string(),
            result: Ok(PipelineDetail {
                stats: stats_with_processed(5_000),
                series: TimeSeries {
                    points: (0..30)
                        .map(|index| TimePoint {
                            at_millis: 10_000 + index * 1_000,
                            processed_records: index * 500,
                            memory_bytes: 1_000_000 + index * 1_000,
                            storage_bytes: 2_000_000,
                        })
                        .collect(),
                },
            }),
            now_millis: 40_000,
        },
    );
    app
}

fn loaded_report() -> HotspotReport {
    let profile = CircuitProfile::from_json(&json!({
        "worker_profiles": [{"metadata": {"n0_1": [
            {"metric_id": "runtime_seconds",
             "value": {"type": "duration", "value": {"secs": 3, "nanos": 0}}},
            {"metric_id": "used_memory_bytes", "value": {"type": "bytes", "value": 4096}},
            {"metric_id": "persistent_id", "value": {"type": "string", "value": "op"}}
        ]}}],
        "graph": {"nodes": {"id": "", "label": "", "nodes": [
            {"Simple": {"id": "n0_1", "label": "join"}}
        ]}, "edges": []}
    }));
    let dataflow = DataflowIndex::from_json(&json!({
        "mir": {"m": {"operation": "join", "persistent_id": "op", "view": "hot_view",
            "positions": [{"start_line_number": 2, "start_column": 1,
                           "end_line_number": 2, "end_column": 20}]}}
    }));
    HotspotReport::build(&profile, &dataflow)
}

#[test]
fn the_dashboard_shows_rows_metrics_and_the_logo() {
    let app = populated_app();
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("orders_pipeline"));
    assert!(screen.contains("inventory"));
    assert!(screen.contains("Running"));
    assert!(screen.contains("RPS"));
    assert!(screen.contains("1 Pipelines"));
    assert!(screen.contains("███"), "logo renders on wide terminals");
    assert!(screen.contains("ok (4ms)"));
}

#[test]
fn wide_terminals_show_tags_runtime_and_full_names() {
    let mut app = populated_app();
    app.pipelines[0].tags = vec!["prod".to_string(), "billing".to_string()];
    app.pipelines[0].platform_version = "0.338.0+enterprise".to_string();
    app.pipelines[1].name = "a_very_long_pipeline_name_that_should_not_be_cut".to_string();
    let screen = draw(&app, 200, 45);
    assert!(screen.contains("TAGS"));
    assert!(screen.contains("prod,billing"));
    assert!(screen.contains("RUNTIME"));
    assert!(screen.contains("0.338.0+ent"));
    assert!(screen.contains("a_very_long_pipeline_name_that_should_not_be_cut"));
}

#[test]
fn transitioning_pipelines_animate_and_show_their_target() {
    let mut app = populated_app();
    app.pipelines[1].deployment_status = "Running".to_string();
    app.pipelines[1].desired_status = Some("Stopped".to_string());
    app.spinner_frame = 0;
    let first = draw(&app, 160, 45);
    assert!(first.contains("Running⇢Stopped"));
    app.spinner_frame = 1;
    let second = draw(&app, 160, 45);
    assert_ne!(
        first, second,
        "the transition marker animates across frames"
    );
}

#[test]
fn the_header_shows_the_full_host_role_and_tenant_when_wide() {
    let mut app = populated_app();
    app.host = "https://very-long-instance-name.staging.feldera.example.com:8443".to_string();
    app.session = crate::model::header::Session::from_json(&json!({
        "tenant_name": "an-extremely-long-tenant-name-corp",
        "role": "admin",
        "memberships": []
    }));
    let screen = draw(&app, 220, 45);
    assert!(screen.contains("https://very-long-instance-name.staging.feldera.example.com:8443"));
    assert!(screen.contains("an-extremely-long-tenant-name-corp"));
    assert!(screen.contains("Role:"));
    assert!(screen.contains("admin"));
}

#[test]
fn narrow_terminals_drop_the_logo_but_keep_the_table() {
    let app = populated_app();
    let screen = draw(&app, 74, 30);
    assert!(!screen.contains("███████╗"));
    assert!(
        !screen.contains("▛▀▘"),
        "even the small mark yields to the table"
    );
    assert!(screen.contains("orders_pipeline"));
    let screen = draw(&app, 90, 30);
    assert!(
        screen.contains("▛▀▘ ▛▀▀ ▞▀▚"),
        "mid-width terminals get the small mark"
    );
}

#[test]
fn tiny_terminals_get_a_size_hint() {
    let app = populated_app();
    let screen = draw(&app, 40, 10);
    assert!(screen.contains("needs at least"));
}

#[test]
fn the_metrics_view_renders_charts_and_the_metric_table() {
    let mut app = populated_app();
    app.view = View::Metrics;
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("METRICS · orders_pipeline"));
    assert!(screen.contains("THROUGHPUT"));
    assert!(screen.contains("MEMORY"));
    assert!(screen.contains("ALL GLOBAL METRICS"));
    assert!(screen.contains("rss_bytes"));
}

#[test]
fn metrics_without_data_explain_themselves() {
    let mut app = populated_app();
    app.view = View::Metrics;
    app.detail = None;
    app.detail_error = Some("HTTP 503: not deployed".to_string());
    let screen = draw(&app, 120, 40);
    assert!(screen.contains("not deployed"));

    app.selected_name = None;
    let screen = draw(&app, 120, 40);
    assert!(screen.contains("Select a pipeline"));
}

#[test]
fn the_connectors_view_lists_endpoints() {
    let mut app = populated_app();
    app.view = View::Connectors;
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("orders_in"));
    assert!(screen.contains("orders"));
    assert!(screen.contains("Active"));
    assert!(screen.contains("ENDPOINT"));
}

#[test]
fn the_profiler_view_ranks_hotspots() {
    let mut app = populated_app();
    app.view = View::Profiler;
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("Press f"));

    update(
        &mut app,
        Msg::HotspotsLoaded {
            pipeline: "orders_pipeline".to_string(),
            result: Ok(loaded_report()),
        },
    );
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("hot_view"));
    assert!(screen.contains("join"));
    assert!(screen.contains("100.0%"));
    assert!(screen.contains("L2"));
    assert!(screen.contains("1 mapped to SQL"));
}

#[test]
fn the_sql_view_highlights_and_shows_heat() {
    let mut app = populated_app();
    app.view = View::Sql;
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("open this tab"));

    update(
        &mut app,
        Msg::SqlLoaded {
            pipeline: "orders_pipeline".to_string(),
            result: Ok(
                "CREATE TABLE orders (id INT);\nCREATE VIEW hot_view AS SELECT 1;".to_string(),
            ),
        },
    );
    update(
        &mut app,
        Msg::HotspotsLoaded {
            pipeline: "orders_pipeline".to_string(),
            result: Ok(loaded_report()),
        },
    );
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("CREATE"));
    assert!(screen.contains("heat: TIME"));
    assert!(screen.contains("line 1-2 of 2"));
    assert!(screen.contains("▌"), "hot line gets a heat gutter");
}

#[test]
fn a_samply_capture_shows_on_the_row_and_in_the_status_line() {
    let mut app = populated_app();
    let screen = draw(&app, 160, 45);
    assert!(!screen.contains("SAMPLY"), "no column without a capture");
    for key in ":samply 30"
        .chars()
        .map(KeyPress::Char)
        .chain([KeyPress::Enter])
    {
        update(&mut app, Msg::Key(key));
    }
    app.toast = None;
    let screen = draw(&app, 160, 45);
    assert!(
        screen.contains("SAMPLY"),
        "the column appears with the capture"
    );
    assert!(screen.contains("▄▆█▆ 0s/30s"), "{screen}");
    assert!(
        screen.find("SAMPLY").unwrap() > screen.find("AGE").unwrap(),
        "SAMPLY is the last column"
    );
    assert!(screen.contains("samply `orders_pipeline`: starting the profiler"));

    update(
        &mut app,
        Msg::ActionFinished {
            action: Action::Samply {
                pipeline: "orders_pipeline".to_string(),
                duration_secs: 30,
            },
            result: Ok(()),
        },
    );
    update(&mut app, Msg::Tick { now_millis: 55_000 });
    app.toast = None;
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("▄▂▁▂ 15s/30s"), "{screen}");
    update(&mut app, Msg::Tick { now_millis: 55_150 });
    app.toast = None;
    let next_frame = draw(&app, 160, 45);
    assert!(
        next_frame.contains("▂▁▂▄ 15s/30s"),
        "the waveform scrolls each frame"
    );
    assert!(screen.contains("recording ▰▰▰▰▰▰▰▰▰▰▱▱▱▱▱▱▱▱▱▱ 15s/30s"));
    assert!(
        screen.contains("? help"),
        "wide terminals keep the key hints"
    );

    app.samply
        .get_mut("orders_pipeline")
        .unwrap()
        .saved("./orders_pipeline-samply-1.json.gz".to_string(), 70_000);
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("✓ saved"));
    assert!(screen.contains("profile saved to ./orders_pipeline-samply-1.json.gz"));

    // Narrow terminals give the path priority over the key hints.
    let screen = draw(&app, 90, 30);
    assert!(screen.contains("profile saved to ./orders_pipeline-samply-1.json.gz"));
    assert!(!screen.contains("? help"));
    // The status line reports its row so a click on the path can open it.
    assert_eq!(app.viewport.status_line_row.get(), Some(28));
}

#[test]
fn profiler_states_cover_loading_empty_and_metric_cycling() {
    let mut app = populated_app();
    app.view = View::Profiler;
    app.loading_report = true;
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("Profiling the circuit"));

    app.loading_report = false;
    app.report = Some(HotspotReport::default());
    app.report_pipeline = Some("orders_pipeline".to_string());
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("no cost data"));

    update(
        &mut app,
        Msg::HotspotsLoaded {
            pipeline: "orders_pipeline".to_string(),
            result: Ok(loaded_report()),
        },
    );
    update(&mut app, Msg::Key(KeyPress::Char('m')));
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("by MEMORY"));
    assert!(screen.contains("4.0 KiB"));
    update(&mut app, Msg::Key(KeyPress::Char('m')));
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("by STATE RECORDS"));
}

#[test]
fn a_support_bundle_gets_its_own_trailing_column() {
    use crate::gateway::DownloadKind;
    use crate::http::Download;
    let mut app = populated_app();
    for key in ":bundle"
        .chars()
        .map(KeyPress::Char)
        .chain([KeyPress::Enter])
    {
        update(&mut app, Msg::Key(key));
    }
    app.toast = None;
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("BUNDLE"), "{screen}");
    assert!(
        !screen.contains("SAMPLY"),
        "no profile capture, no SAMPLY column"
    );
    assert!(screen.find("BUNDLE").unwrap() > screen.find("AGE").unwrap());
    assert!(screen.contains("⇣ "), "the download marquee shows");
    assert!(screen.contains("bundle `orders_pipeline`: collecting the support bundle"));

    update(
        &mut app,
        Msg::DownloadFetched {
            kind: DownloadKind::SupportBundle,
            pipeline: "orders_pipeline".to_string(),
            result: Ok(Download::Ready(vec![1])),
        },
    );
    update(
        &mut app,
        Msg::FileSaved {
            kind: DownloadKind::SupportBundle,
            pipeline: "orders_pipeline".to_string(),
            path: "./orders_pipeline-support-bundle-1.zip".to_string(),
            result: Ok(()),
        },
    );
    app.toast = None;
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("✓ saved"));
    assert!(screen.contains("support bundle saved to ./orders_pipeline-support-bundle-1.zip"));
}

#[test]
fn an_action_in_flight_badges_the_row_at_once() {
    let mut app = populated_app();
    // orders_pipeline is selected; only a stopped pipeline has a start to send.
    app.pipelines[0].deployment_status = "Stopped".to_string();
    update(&mut app, Msg::Key(KeyPress::Char('s')));
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("orders_pipeline ↻start"), "{screen}");
    update(
        &mut app,
        Msg::ActionFinished {
            action: Action::Start {
                pipeline: "orders_pipeline".to_string(),
            },
            result: Ok(()),
        },
    );
    let screen = draw(&app, 160, 45);
    assert!(
        !screen.contains("↻start"),
        "the badge leaves with the request"
    );
}

#[test]
fn the_bundle_dialog_renders_its_rows_and_the_current_limit() {
    let mut app = populated_app();
    for key in ":bundle settings"
        .chars()
        .map(KeyPress::Char)
        .chain([KeyPress::Enter])
    {
        update(&mut app, Msg::Key(key));
    }
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("SUPPORT BUNDLE"));
    assert!(screen.contains("[x] Collect fresh data before downloading"));
    assert!(screen.contains("current only"));
    assert!(screen.contains("▶ Download now"));
    update(&mut app, Msg::Key(KeyPress::Char(' ')));
    update(&mut app, Msg::Key(KeyPress::Char('j')));
    update(&mut app, Msg::Key(KeyPress::Right));
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("[ ] Collect fresh data before downloading"));
    assert!(screen.contains("last 2"));
}

#[test]
fn an_accepted_start_shows_the_destination_before_the_next_overview() {
    let mut app = populated_app();
    app.pipelines[0].deployment_status = "Stopped".to_string();
    app.pipelines[0].desired_status = Some("Stopped".to_string());
    update(&mut app, Msg::Key(KeyPress::Char('s')));
    update(
        &mut app,
        Msg::ActionFinished {
            action: Action::Start {
                pipeline: "orders_pipeline".to_string(),
            },
            result: Ok(()),
        },
    );
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("Stopped⇢Running"), "{screen}");
}

#[test]
fn the_logs_view_shows_diagnostics_stream_and_state() {
    let mut app = populated_app();
    let screen = draw_view(&mut app, View::Logs);
    assert!(screen.contains("LOGS · orders_pipeline"));
    assert!(screen.contains("FOLLOW"));

    update(
        &mut app,
        Msg::DiagnosticsLoaded {
            pipeline: "orders_pipeline".to_string(),
            result: Ok(vec![crate::model::diagnostics::DiagnosticSection {
                title: "RUST COMPILATION FAILED (exit 101)".to_string(),
                severity: crate::model::diagnostics::Severity::Error,
                body: "error[E0432]: unresolved import `dbsp::SetHandle`".to_string(),
            }]),
        },
    );
    for line in ["INFO pipeline started", "ERROR worker lost", "plain output"] {
        update(
            &mut app,
            Msg::LogLine {
                pipeline: "orders_pipeline".to_string(),
                line: line.to_string(),
            },
        );
    }
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("RUST COMPILATION FAILED"));
    assert!(screen.contains("unresolved import"));
    assert!(screen.contains("ERROR worker lost"));
    assert!(screen.contains("● streaming"));
    assert!(screen.contains("3 lines") || screen.contains("lines"));

    update(
        &mut app,
        Msg::LogsClosed {
            pipeline: "orders_pipeline".to_string(),
            result: Err(crate::http::ApiError::Transport("reset".to_string())),
        },
    );
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("reset"));
}

fn draw_view(app: &mut App, view: View) -> String {
    let commands = crate::app::update::set_view(app, view);
    let _ = commands;
    draw(app, 160, 45)
}

#[test]
fn critical_memory_pressure_shows_fire_in_table_and_metrics() {
    let mut app = populated_app();
    let hot_stats = crate::model::stats::PipelineStats::from_json(&json!({
        "global_metrics": {
            "state": "Running",
            "total_processed_records": 9_000,
            "rss_bytes": 950_000_000i64,
            "memory_pressure": "critical"
        }
    }));
    update(
        &mut app,
        Msg::RowStats {
            pipeline: "inventory".to_string(),
            stats: hot_stats.clone(),
            now_millis: 50_000,
        },
    );
    let screen = draw(&app, 175, 45);
    let row = screen
        .lines()
        .find(|line| line.contains("inventory"))
        .unwrap()
        .to_string();
    assert!(row.contains("🔥"), "table burns under pressure: {row}");

    // The metrics view spells it out.
    update(&mut app, Msg::Key(KeyPress::Home));
    update(
        &mut app,
        Msg::Detail {
            pipeline: "inventory".to_string(),
            result: Ok(PipelineDetail {
                stats: hot_stats,
                series: TimeSeries::default(),
            }),
            now_millis: 51_000,
        },
    );
    app.view = View::Metrics;
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("PRESSURE"));
    assert!(screen.contains("critical🔥"));
}

#[test]
fn storage_pressure_escalates_against_the_quota() {
    let mut app = populated_app();
    // 2.0 KiB of storage against a quota it nearly fills; the quota arrives
    // with the status payload.
    let mut overview = overview_with(&["orders_pipeline", "inventory"]);
    overview.pipelines[1].storage_quota_bytes = Some(2_100);
    update(
        &mut app,
        Msg::Overview {
            result: Ok(overview),
            latency_millis: 4,
            now_millis: 41_000,
        },
    );
    let screen = draw(&app, 175, 45);
    let row = screen
        .lines()
        .find(|line| line.contains("inventory"))
        .unwrap()
        .to_string();
    assert!(
        row.contains("2.0 KiB🔥"),
        "storage burns near its quota: {row}"
    );

    // Without a quota there is no pressure to report.
    app.storage_quotas.remove("inventory");
    let screen = draw(&app, 175, 45);
    assert!(!screen.contains("2.0 KiB🔥"));

    // The metrics view shows usage against the quota.
    app.storage_quotas
        .insert("orders_pipeline".to_string(), 2_100);
    app.view = View::Metrics;
    let screen = draw(&app, 160, 45);
    // Wide emoji occupy a continuation cell in the buffer, so match parts.
    assert!(screen.contains("2.0 KiB🔥"));
    assert!(screen.contains("/ 2.1 KiB"));
}

#[test]
fn marked_rows_wear_a_check_glyph() {
    let mut app = populated_app();
    update(&mut app, Msg::Key(KeyPress::Home));
    update(&mut app, Msg::Key(KeyPress::Char(' ')));
    let screen = draw(&app, 175, 45);
    let row = screen
        .lines()
        .find(|line| line.contains("inventory"))
        .unwrap()
        .to_string();
    assert!(row.contains("✓"), "marked row shows a check: {row}");
    // The selection stayed put, so the same row is still under the cursor.
    assert_eq!(app.selected_name.as_deref(), Some("inventory"));
}

#[test]
fn stopped_pipelines_show_no_stale_runtime_metrics() {
    let mut app = populated_app();
    // "inventory" has cached live metrics from when it ran; it then stops
    // and its storage is cleared.
    let inventory = app
        .pipelines
        .iter()
        .position(|row| row.name == "inventory")
        .unwrap();
    app.pipelines[inventory].deployment_status = "Stopped".to_string();
    app.pipelines[inventory].storage_status = "Cleared".to_string();
    let screen = draw(&app, 170, 45);
    let inventory_row = screen
        .lines()
        .find(|line| line.contains("inventory"))
        .unwrap()
        .to_string();
    assert!(
        !inventory_row.contains("K/s") && !inventory_row.contains("MiB"),
        "no stale rates or sizes on a cleared stopped pipeline: {inventory_row}"
    );

    // With storage still in use, the last-known footprint stays visible.
    app.pipelines[inventory].storage_status = "InUse".to_string();
    let screen = draw(&app, 170, 45);
    let inventory_row = screen
        .lines()
        .find(|line| line.contains("inventory"))
        .unwrap()
        .to_string();
    assert!(
        inventory_row.contains("2.0 KiB"),
        "last-known storage shows while InUse: {inventory_row}"
    );
    assert!(
        !inventory_row.contains("K/s"),
        "rates stay blank for stopped pipelines: {inventory_row}"
    );
}

#[test]
fn clearing_storage_burns_the_size_to_dust() {
    let mut app = populated_app();
    let inventory = app
        .pipelines
        .iter()
        .position(|row| row.name == "inventory")
        .unwrap();
    app.pipelines[inventory].deployment_status = "Stopped".to_string();
    app.pipelines[inventory].storage_status = "Clearing".to_string();
    app.spinner_frame = 6;
    let screen = draw(&app, 170, 45);
    let row = screen
        .lines()
        .find(|line| line.contains("inventory"))
        .unwrap()
        .to_string();
    assert!(!row.contains("2.0 KiB"), "the size is mid-burn: {row}");
    assert!(
        row.contains('▒') || row.contains('·'),
        "dust visible: {row}"
    );
    // Frames differ, so the burn animates.
    app.spinner_frame = 12;
    let later = draw(&app, 170, 45);
    assert_ne!(screen, later);
}

#[test]
fn queued_clears_badge_the_storage_column_and_other_verbs_the_name() {
    let mut app = populated_app();
    update(&mut app, Msg::Key(KeyPress::Char('x')));
    update(&mut app, Msg::Key(KeyPress::Char('C')));
    update(&mut app, Msg::Key(KeyPress::Char('s')));
    app.view = View::Pipelines;
    let screen = draw(&app, 175, 45);
    let row = screen
        .lines()
        .find(|line| line.contains("orders_pipeline"))
        .unwrap()
        .to_string();
    assert!(
        row.contains("⧗ clear"),
        "clear badge in storage column: {row}"
    );
    assert!(
        row.contains("⧗start"),
        "other queued verbs stay on the name: {row}"
    );
    let name_part = &row[..row.find("STATUS").map_or(60, |_| 60).min(row.len())];
    assert!(
        !name_part.contains("clear"),
        "no clear badge next to the name: {name_part}"
    );
}

#[test]
fn long_lists_scroll_to_the_selection_with_a_scrollbar() {
    let mut app = App::new("http://x".to_string(), 2);
    let names: Vec<String> = (0..40).map(|index| format!("pipe_{index:02}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    update(
        &mut app,
        Msg::Overview {
            result: Ok(crate::testutil::overview_with(&name_refs)),
            latency_millis: 1,
            now_millis: 1_000,
        },
    );
    app.select_pipeline("pipe_39".to_string());
    let screen = draw(&app, 160, 30);
    assert!(
        screen.contains("pipe_39"),
        "the selected last row is scrolled into view"
    );
    assert!(!screen.contains("pipe_00"), "the first rows scrolled out");
    assert!(
        screen.contains('█') || screen.contains('║'),
        "a scrollbar thumb renders"
    );
    let (offset, height) = app.viewport.pipelines.get();
    assert!(offset > 0);
    assert!(height > 0);
    assert!(!app.viewport.click_columns.borrow().is_empty());
}

#[test]
fn marked_rows_show_in_the_title_and_batch_confirm_lists_names() {
    let mut app = populated_app();
    update(&mut app, Msg::Key(KeyPress::Home));
    update(&mut app, Msg::Key(KeyPress::Char(' ')));
    update(&mut app, Msg::Key(KeyPress::Down));
    update(&mut app, Msg::Key(KeyPress::Char(' ')));
    let screen = draw(&app, 170, 45);
    assert!(screen.contains("2 marked"));

    app.overlay = Overlay::Confirm {
        actions: vec![
            Action::Stop {
                pipeline: "inventory".to_string(),
                force: false,
            },
            Action::Stop {
                pipeline: "orders_pipeline".to_string(),
                force: false,
            },
        ],
    };
    let screen = draw(&app, 170, 45);
    assert!(screen.contains("stop 2 pipelines?"));
    assert!(screen.contains("inventory, orders_pipeline"));
}

#[test]
fn clicked_rows_match_the_rendered_table_offset() {
    let app = populated_app();
    let screen = draw(&app, 160, 45);
    // The click handler assumes the first data row lands at FIRST_TABLE_ROW;
    // this pins the layout contract.
    let rows: Vec<&str> = screen.lines().collect();
    let first_data_row = rows[crate::app::state::FIRST_TABLE_ROW as usize];
    assert!(
        first_data_row.contains("inventory"),
        "expected the first pipeline at row {}, got: {first_data_row}",
        crate::app::state::FIRST_TABLE_ROW
    );
}

#[test]
fn overlays_draw_over_the_active_view() {
    let mut app = populated_app();
    app.overlay = Overlay::Help;
    let screen = draw(&app, 140, 45);
    assert!(screen.contains("PIPELINE LIFECYCLE"));
    assert!(screen.contains("shift-↕"));
    assert!(
        screen.contains("press any key to close"),
        "the help shows every entry through its last line"
    );

    app.overlay = Overlay::Confirm {
        actions: vec![Action::Stop {
            pipeline: "orders_pipeline".to_string(),
            force: true,
        }],
    };
    let screen = draw(&app, 140, 45);
    assert!(screen.contains("force-stop"));
    assert!(screen.contains("confirm"));

    app.session = crate::model::header::Session::from_json(&json!({
        "tenant_name": "one",
        "memberships": [
            {"tenant_id": "1", "name": "one", "role": "admin"},
            {"tenant_id": "2", "name": "two", "role": "read"}
        ]
    }));
    app.overlay = Overlay::Tenants { selected: 1 };
    let screen = draw(&app, 140, 45);
    assert!(screen.contains("SWITCH TENANT"));
    assert!(screen.contains("two"));

    app.overlay = Overlay::ConnectorInspect { scroll: 0 };
    let screen = draw(&app, 140, 45);
    assert!(screen.contains("CONNECTOR · orders_in"));
    assert!(screen.contains("endpoint_name"));

    app.overlay = Overlay::TextViewer {
        title: "DEPLOYMENT ERROR".to_string(),
        body: "worker exploded".to_string(),
        scroll: 0,
    };
    let screen = draw(&app, 140, 45);
    assert!(screen.contains("worker exploded"));
}

/// The glyphs down the right border of the popup whose title row is `top`,
/// corners excluded; a scrollbar thumb shows up here and nowhere else.
fn right_border_column(rows: &[&str], top: usize) -> Vec<char> {
    let title_row: Vec<char> = rows[top].chars().collect();
    let left = title_row.iter().position(|glyph| *glyph == '╭').unwrap();
    let right = left
        + title_row[left..]
            .iter()
            .position(|glyph| *glyph == '╮')
            .unwrap();
    let bottom = rows[top..]
        .iter()
        .position(|row| row.chars().nth(left) == Some('╰'))
        .unwrap()
        + top;
    rows[top + 1..bottom]
        .iter()
        .map(|row| row.chars().nth(right).unwrap())
        .collect()
}

#[test]
fn the_text_viewer_fits_its_text_and_scrolls_when_it_overflows() {
    let mut app = populated_app();
    app.overlay = Overlay::TextViewer {
        title: "DEPLOYMENT ERROR".to_string(),
        body: "worker exploded".to_string(),
        scroll: 0,
    };
    let screen = draw(&app, 140, 45);
    let rows: Vec<&str> = screen.lines().collect();
    let top = rows
        .iter()
        .position(|row| row.contains("╭ DEPLOYMENT ERROR "))
        .expect("the viewer draws its title");
    // One line of text needs three rows: border, text, border.
    assert!(rows[top + 1].contains("worker exploded"));
    assert!(rows[top + 2].contains('╰'));
    assert_eq!(app.viewport.overlay_scroll_max.get(), 0);
    // Centered on the screen, and no wider than the text needs.
    let left = rows[top].find('╭').unwrap();
    let box_width = rows[top][left..].find('╮').unwrap();
    let left_columns = rows[top][..left].chars().count();
    let box_columns = rows[top][left..left + box_width].chars().count() + 1;
    assert!(box_columns < 40, "box is {box_columns} columns wide");
    assert!(
        (40..=80).contains(&left_columns),
        "box starts at column {left_columns}"
    );
    assert!(
        !right_border_column(&rows, top).contains(&'█'),
        "nothing to scroll, no scrollbar"
    );

    let body: Vec<String> = (1..=100).map(|number| format!("line {number}")).collect();
    app.overlay = Overlay::TextViewer {
        title: "DEPLOYMENT ERROR".to_string(),
        body: body.join("\n"),
        scroll: 0,
    };
    let screen = draw(&app, 140, 45);
    assert!(screen.contains("line 1 "));
    assert!(!screen.contains("line 100"));
    let rows: Vec<&str> = screen.lines().collect();
    let top = rows
        .iter()
        .position(|row| row.contains("╭ DEPLOYMENT ERROR "))
        .unwrap();
    assert!(
        right_border_column(&rows, top).contains(&'█'),
        "overflow shows a scrollbar thumb"
    );
    // 45 rows minus the 4-row margin and 2 borders leaves 39 lines visible.
    assert_eq!(app.viewport.overlay_scroll_max.get(), 61);

    // Scrolling past the end shows the last line, not empty space.
    app.overlay = Overlay::TextViewer {
        title: "DEPLOYMENT ERROR".to_string(),
        body: body.join("\n"),
        scroll: 500,
    };
    let screen = draw(&app, 140, 45);
    assert!(screen.contains("line 100"));
    assert!(screen.contains("line 62 "));
}

#[test]
fn prompts_and_toasts_take_over_the_status_line() {
    let mut app = populated_app();
    app.prompt = Prompt::Command {
        input: "sta".to_string(),
        history_index: None,
    };
    let screen = draw(&app, 140, 40);
    assert!(screen.contains(":sta"));

    app.prompt = Prompt::Filter {
        input: "ord".to_string(),
    };
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("/ord"));

    app.prompt = Prompt::Closed;
    app.toast_error("something failed");
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("something failed"));

    app.toast = None;
    app.filter = "inv".to_string();
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("filter: inv"));
}

#[test]
fn connection_states_render_in_header_and_empty_dashboard() {
    let mut app = App::new("http://gone.test".to_string(), 2);
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("Connecting"));

    app.connection = Connection::Lost {
        error: "connection failed: refused".to_string(),
        since_millis: 0,
    };
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("Cannot reach the instance"));
    assert!(screen.contains("LOST"));

    app.connection = Connection::Online { latency_millis: 2 };
    app.filter = "zzz".to_string();
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("No pipeline matches"));

    app.filter.clear();
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("No pipelines yet"));
}

#[test]
fn every_view_renders_with_an_empty_app() {
    for view in View::ALL {
        let mut app = App::new("http://x".to_string(), 2);
        app.view = view;
        draw(&app, 120, 40);
        draw(&app, 71, 17);
    }
}

#[test]
fn the_tab_bar_reports_where_each_tab_is() {
    let mut app = populated_app();
    let screen = draw(&app, 140, 45);
    let tab_row = screen.lines().nth(44).unwrap();
    let metrics_column = tab_row.find("2 Metrics").unwrap() as u16;
    assert_eq!(app.viewport.tab_bar_row.get(), Some(44));
    assert_eq!(app.viewport.tabs.borrow().len(), View::ALL.len());
    assert_eq!(
        app.viewport.view_at(44, metrics_column),
        Some(View::Metrics)
    );
    update(
        &mut app,
        Msg::Key(crate::app::msg::KeyPress::Click {
            row: 44,
            column: metrics_column,
        }),
    );
    assert_eq!(app.view, View::Metrics);
    // A narrow terminal hides the trailing tabs, so they get no hit range.
    draw(&app, 70, 20);
    assert!(app.viewport.tabs.borrow().len() < View::ALL.len());
}
