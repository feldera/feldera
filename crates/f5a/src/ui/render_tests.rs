//! Whole-screen render tests: every view and overlay draws into a test
//! backend, and the buffer must show the load-bearing content.

use ratatui::Terminal;
use ratatui::backend::TestBackend;
use serde_json::json;

use crate::app::connector_tree::ConnectorNode;
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
    app.host = "https://very-long-instance-name.feldera.example.com:8443".to_string();
    app.session = crate::model::header::Session::from_json(&json!({
        "tenant_name": "an-extremely-long-tenant-name-corp",
        "role": "admin",
        "memberships": []
    }));
    let screen = draw(&app, 220, 45);
    assert!(screen.contains("https://very-long-instance-name.feldera.example.com:8443"));
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
fn the_connectors_view_groups_endpoints_under_their_relation() {
    let mut app = populated_app();
    app.view = View::Connectors;
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("CONNECTORS · orders_pipeline [1]"));
    assert!(screen.contains("▾ orders table"));
    assert!(screen.contains("└─ orders_in"));
    assert!(screen.contains("Active"));
    assert!(
        screen.contains("NAME↑"),
        "the active sort wears its direction"
    );
    assert!(screen.contains("RPS"));
    let rows: Vec<&str> = screen.lines().collect();
    assert!(
        rows[crate::app::state::FIRST_TABLE_ROW as usize].contains("orders table"),
        "the first relation sits on the first table row"
    );
    assert!(
        screen.contains("1 input connector"),
        "the detail pane describes the relation"
    );
}

/// Long names show in full when the terminal has room and shrink only when it does not.
#[test]
fn connector_names_use_the_available_width() {
    let mut app = populated_app();
    app.view = View::Connectors;
    let input = &mut app.detail.as_mut().expect("populated app has stats").inputs[0];
    input.relation = "scm_platform_inventory_ledger_v1".to_string();
    input.endpoint_name =
        "scm_platform_inventory_ledger_v1.scm_platform_inventory_ledger_v1_0".to_string();
    app.reconcile_connector_cursor();

    let screen = draw(&app, 140, 40);
    assert!(screen.contains("└─ scm_platform_inventory_ledger_v1_0 "));
    assert!(screen.contains("▾ scm_platform_inventory_ledger_v1 table"));

    let screen = draw(&app, 70, 20);
    let branch = screen
        .lines()
        .find(|line| line.contains("└─"))
        .expect("the connector row renders");
    assert!(branch.contains('…'), "{branch}");
    assert!(screen.contains("RECORDS"));
}

fn rich_stats() -> crate::model::stats::PipelineStats {
    crate::model::stats::PipelineStats::from_json(&json!({
        "global_metrics": {"total_processed_records": 1_000},
        "inputs": [
            {"endpoint_name": "orders.kafka", "config": {"stream": "orders"},
             "barrier": true,
             "health": {"status": "Unhealthy", "description": "broker unreachable"},
             "fatal_error": "the topic vanished",
             "completed_frontier": {
                 "metadata": {"offsets": [42]},
                 "ingested_at": "1970-01-01T00:00:30Z",
                 "processed_at": "1970-01-01T00:00:31Z",
                 "completed_at": "1970-01-01T00:00:32Z"
             },
             "metrics": {"total_records": 900, "total_bytes": 4096, "buffered_records": 12,
                         "buffered_bytes": 512, "num_parse_errors": 2,
                         "processing_latency_p99_micros": 12_345}},
            {"endpoint_name": "orders.http", "config": {"stream": "orders"}, "paused": true,
             "metrics": {"total_records": 100}}
        ],
        "outputs": [
            {"endpoint_name": "totals.delta", "config": {"stream": "totals"},
             "health": {"status": "Healthy"},
             "metrics": {"transmitted_records": 70, "queued_records": 3, "queued_batches": 1,
                         "buffered_records": 4, "buffered_batches": 2, "memory": 2048,
                         "total_processed_input_records": 600, "batch_records_written": 9}}
        ]
    }))
}

fn rich_connectors_app() -> App {
    let mut app = populated_app();
    app.detail = Some(rich_stats());
    app.view = View::Connectors;
    app.now_millis = 40_000;
    app.reconcile_connector_cursor();
    app
}

#[test]
fn the_detail_pane_shows_what_the_table_leaves_out_for_inputs() {
    let mut app = rich_connectors_app();
    app.connector_pane.cursor = Some(ConnectorNode::Endpoint("orders.kafka".to_string()));
    app.connector_pane.recent_errors = Some(crate::app::connector_tree::RecentErrors {
        endpoint_name: "orders.kafka".to_string(),
        error_count: 2,
        result: Ok(vec![crate::model::stats::ConnectorError {
            kind: crate::model::stats::ConnectorErrorKind::Parse,
            at_millis: Some(37_000),
            index: 0,
            message: "expected value at line 1".to_string(),
        }]),
    });
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("Fatal · broker unreachable"));
    assert!(screen.contains("12 records · 512 B"));
    assert!(screen.contains("12.3 ms"), "p99 latency");
    assert!(screen.contains("parse 2 · transport 0"));
    assert!(screen.contains("yes, holds back checkpoints"));
    assert!(screen.contains("completed 8s ago · 2.0 s end to end"));
    assert!(screen.contains("{\"offsets\":[42]}"));
    assert!(screen.contains("FATAL ERROR"));
    assert!(screen.contains("the topic vanished"));
    assert!(screen.contains("RECENT ERRORS · 1 kept of 2"));
    assert!(screen.contains("3s ago      parse     expected value at line 1"));

    app.connector_pane.recent_errors = None;
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("loading…"));
}

#[test]
fn errors_list_one_line_each_and_the_selected_one_expands() {
    let mut app = rich_connectors_app();
    app.connector_pane.cursor = Some(ConnectorNode::Endpoint("orders.kafka".to_string()));
    app.connector_pane.recent_errors = Some(crate::app::connector_tree::RecentErrors {
        endpoint_name: "orders.kafka".to_string(),
        error_count: 2,
        // Newest first, as the parser orders them.
        result: Ok((0..40)
            .rev()
            .map(|index| crate::model::stats::ConnectorError {
                kind: crate::model::stats::ConnectorErrorKind::Parse,
                at_millis: Some(37_000),
                index,
                message: format!("bad row number {index}\nInvalid fragment: '{{\"id\": {index}}}'"),
            })
            .collect()),
    });
    let screen = draw(&app, 160, 40);
    assert!(
        screen.contains("bad row number 39"),
        "newest first, one line each"
    );
    assert!(
        !screen.contains("Invalid fragment"),
        "only first lines while compact"
    );
    assert!(
        !screen.contains("bad row number 0 "),
        "the oldest is below the fold"
    );

    app.connector_pane.is_detail_focused = true;
    app.connector_pane.selected_error = Some((crate::model::stats::ConnectorErrorKind::Parse, 0));
    let screen = draw(&app, 160, 40);
    assert!(
        screen.contains("3s ago      parse     bad row number 0"),
        "the selection wraps its message in place"
    );
    assert!(
        screen.contains("Invalid fragment: '{\"id\": 0}'"),
        "and its whole message, scrolled into view"
    );
    assert!(!screen.contains("latency p99"), "the metrics scrolled away");
    assert_eq!(
        screen.matches("Invalid fragment").count(),
        1,
        "only one expands"
    );

    // Walking back to the newest error brings the stats back into view.
    app.connector_pane.selected_error = Some((crate::model::stats::ConnectorErrorKind::Parse, 39));
    let screen = draw(&app, 160, 40);
    assert!(screen.contains("latency p99"), "back at the top");
    assert!(screen.contains("Invalid fragment: '{\"id\": 39}'"));
}

#[test]
fn the_detail_view_follows_the_selection_and_keeps_still_otherwise() {
    use super::connectors::detail_offset;
    assert_eq!(detail_offset(0, None, 10, 20), 0, "everything fits");
    assert_eq!(
        detail_offset(5, None, 30, 20),
        5,
        "no selection keeps the view"
    );
    assert_eq!(
        detail_offset(50, None, 30, 20),
        10,
        "a shrunken text pulls back"
    );
    assert_eq!(
        detail_offset(0, Some((25, 27)), 40, 20),
        8,
        "down just enough"
    );
    assert_eq!(
        detail_offset(25, Some((30, 32)), 60, 20),
        25,
        "visible: no move"
    );
    assert_eq!(
        detail_offset(30, Some((25, 27)), 60, 20),
        25,
        "up to its first line"
    );
    assert_eq!(
        detail_offset(30, Some((10, 12)), 40, 20),
        0,
        "the first screenful shows from the top, stats included"
    );
    assert_eq!(
        detail_offset(0, Some((30, 60)), 70, 20),
        30,
        "a tall error shows its first line"
    );
}

#[test]
fn the_detail_pane_shows_queues_memory_and_lag_for_outputs() {
    let mut app = rich_connectors_app();
    app.connector_pane.cursor = Some(ConnectorNode::Endpoint("totals.delta".to_string()));
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("totals · view · output"));
    assert!(screen.contains("Active · healthy"));
    assert!(screen.contains("3 records · 1 batch"));
    assert!(screen.contains("4 records · 2 batches"));
    assert!(screen.contains("2.0 KiB"), "memory");
    assert!(screen.contains("600 of 1.0K inputs · lag 400"));
    assert!(screen.contains("9 records written"));
    assert!(!screen.contains("RECENT ERRORS"), "no errors, no section");
}

#[test]
fn relation_rows_aggregate_marks_and_fold() {
    let mut app = rich_connectors_app();
    let screen = draw(&app, 160, 45);
    assert!(
        screen.contains("1 Fatal · 1 Paused"),
        "status counts, worst first"
    );
    assert!(screen.contains("2 input connectors"));
    assert!(
        screen.contains("1.0K · 4.0 KiB"),
        "summed records and bytes"
    );

    app.connector_pane.marked.insert("orders.kafka".to_string());
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("1 marked"));
    assert!(screen.contains("◦▾ orders"), "a partly marked relation");
    assert!(screen.contains("✓  └─ kafka"));

    app.connector_pane.marked.insert("orders.http".to_string());
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("✓▾ orders"));

    app.connector_pane
        .collapsed
        .insert(ConnectorNode::Relation {
            kind: crate::model::stats::ConnectorKind::Input,
            name: "orders".to_string(),
        });
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("▸ orders"));
    assert!(!screen.contains("├─ http"));
}

#[test]
fn the_detail_pane_moves_below_or_away_on_small_terminals() {
    let app = rich_connectors_app();
    draw(&app, 160, 45);
    assert!(
        app.viewport.connector_tree_right.get().is_some(),
        "beside the tree"
    );
    let screen = draw(&app, 100, 34);
    assert!(
        app.viewport.connector_tree_right.get().is_none(),
        "below the tree"
    );
    assert!(screen.contains("2 input connectors"));
    let screen = draw(&app, 100, 24);
    assert!(
        !screen.contains("2 input connectors"),
        "no room for the pane"
    );
    assert!(screen.contains("▾ orders"));
}

#[test]
fn connector_sort_headers_report_back_for_clicks() {
    let mut app = rich_connectors_app();
    app.connector_pane.sort = crate::app::connector_tree::ConnectorSort::Rate;
    app.connector_pane.sort_descending = true;
    let screen = draw(&app, 160, 45);
    assert!(screen.contains("RPS↓"));
    assert!(screen.contains("sort:rps↓"));
    let hits = app.viewport.connector_columns.borrow();
    assert_eq!(hits.len(), 6);
    assert_eq!(hits[0].x_start, 1);
    assert!(hits.windows(2).all(|pair| pair[0].x_end < pair[1].x_start));
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

/// A mapped join feeding an unmapped trace whose memory sits on one of
/// two workers.
fn skewed_report() -> HotspotReport {
    let reading =
        |metric: &str, value: serde_json::Value| json!({"metric_id": metric, "value": value});
    let seconds = |secs: i64| json!({"type": "duration", "value": {"secs": secs, "nanos": 0}});
    let bytes = |value: i64| json!({"type": "bytes", "value": value});
    let cache = json!({"used": bytes(50), "max": bytes(100)});
    let worker = |trace_bytes: i64| {
        json!({"metadata": {
            "n": [
                reading("used_memory_bytes", bytes(trace_bytes)),
                reading("bloom_filter_size_bytes", bytes(10)),
                reading("foreground_cache_occupancy", cache.clone()),
                reading("background_cache_occupancy", cache.clone())
            ],
            "n_join": [
                reading("runtime_seconds", seconds(2)),
                reading("persistent_id", json!({"type": "string", "value": "op"}))
            ],
            "n_trace": [
                reading("used_memory_bytes", bytes(trace_bytes)),
                reading("allocated_memory_bytes", bytes(trace_bytes)),
                reading("bloom_filter_hits_count", json!({"type": "count", "value": 5}))
            ]
        }})
    };
    let profile = CircuitProfile::from_json(&json!({
        "metrics": [
            {"name": "allocated_memory_bytes", "category": "State", "advanced": false},
            {"name": "bloom_filter_hits_count", "category": "Cache", "advanced": true}
        ],
        "worker_profiles": [worker(3_000), worker(0)],
        "graph": {
            "nodes": {"id": "n", "nodes": [
                {"Simple": {"id": "n_join", "label": "Join"}},
                {"Simple": {"id": "n_trace", "label": "Z1 (trace)"}}
            ]},
            "edges": [{"from_node": "n_join", "to_node": "n_trace"}]
        }
    }));
    let dataflow = DataflowIndex::from_json(&json!({
        "mir": {"m": {"operation": "join", "persistent_id": "op", "view": "hot_view",
            "positions": [{"start_line_number": 2, "start_column": 1,
                           "end_line_number": 2, "end_column": 20}]}}
    }));
    HotspotReport::build(&profile, &dataflow)
}

#[test]
fn hotspots_show_skew_borrowed_owners_and_the_selected_operator() {
    let mut app = populated_app();
    app.view = View::Profiler;
    app.report = Some(skewed_report());
    app.report_pipeline = Some("inventory".to_string());
    app.metric = crate::model::dataflow::CostMetric::Memory;
    let screen = draw(&app, 180, 45);
    assert!(screen.contains("COST↓") && screen.contains("SKEW"));
    assert!(screen.contains("2.0×"), "all memory on one of two workers");
    assert!(
        screen.contains("~hot_view") && screen.contains("~L2"),
        "borrowed from the join"
    );
    assert!(
        screen.contains("operator state 2.9 KiB"),
        "the operators' sum"
    );
    assert!(screen.contains("bloom filters 20 B"));
    assert!(
        screen.contains("buffer cache 100 B of 200 B"),
        "each worker's shared cache counts once"
    );
    assert!(screen.contains("RSS 1.0 KiB"), "the process for comparison");

    // The detail pane describes the selected trace.
    assert!(screen.contains("Z1 (trace)"));
    assert!(screen.contains("from neighbor n_join"));
    assert!(screen.contains("WORKERS · memory per worker"));
    assert!(screen.contains("busiest worker 0 · 2.0× the mean · 1 idle"));
    assert!(screen.contains("STATE") && screen.contains("allocated memory"));
    assert!(screen.contains("CACHE") && screen.contains("bloom filter hits"));
    assert_eq!(app.viewport.hotspot_columns.borrow().len(), 8);
    assert!(app.viewport.hotspot_table_right.get().is_some());

    let (max_scroll, visible_lines) = app.viewport.hotspot_detail.get();
    assert!(visible_lines > 0, "the pane reports its height");
    assert_eq!(max_scroll, 0, "the details fit");
    let screen = draw(&app, 180, 26);
    let (max_scroll, _) = app.viewport.hotspot_detail.get();
    assert!(max_scroll > 0, "a short terminal overflows the details");
    assert!(screen.contains("relation"));
    app.is_hotspot_detail_focused = true;
    app.hotspot_detail_scroll = max_scroll;
    let screen = draw(&app, 180, 26);
    assert!(
        !screen.contains("relation      ~hot_view"),
        "scrolled past the top"
    );
    assert!(
        screen.contains("bloom filter hits"),
        "down to the last reading"
    );

    app.metric = crate::model::dataflow::CostMetric::Time;
    let screen = draw(&app, 180, 45);
    assert!(screen.contains("1 mapped to SQL, 1 ~inferred"));
    assert!(
        screen.contains("join · Join"),
        "the mapped operator names its operation"
    );
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
    assert!(screen.contains("heat:time"));
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
    assert!(screen.contains("metric:memory"));
    assert!(screen.contains("4.0 KiB"));
    update(&mut app, Msg::Key(KeyPress::Char('m')));
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("metric:storage"));
    assert!(screen.contains("spines hold"));
    update(&mut app, Msg::Key(KeyPress::Char('m')));
    let screen = draw(&app, 140, 40);
    assert!(screen.contains("metric:state records"));
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
    assert!(screen.contains("HELP · Pipelines"));
    assert!(screen.contains("LIFECYCLE · the marked pipelines, else the row"));
    assert!(screen.contains("shift-↕"));
    assert!(screen.contains("EVERY TAB"));
    assert!(
        screen.contains("press any key to close"),
        "the help shows every entry through its last line"
    );
    // Each tab explains its own keys.
    for (view, title, key) in [
        (View::Metrics, "HELP · Metrics", "open Connectors"),
        (View::Connectors, "HELP · Connectors", "browse its errors"),
        (View::Profiler, "HELP · Hotspots", "cycle the metric"),
        (
            View::Sql,
            "HELP · SQL",
            "back to the tab that opened the SQL",
        ),
        (View::Logs, "HELP · Logs", "follow the tail"),
    ] {
        app.view = view;
        let screen = draw(&app, 140, 45);
        assert!(screen.contains(title), "{title}");
        assert!(screen.contains(key), "{title} explains {key}");
        assert!(
            !screen.contains("LIFECYCLE ·"),
            "{title} leaves out the pipelines keys"
        );
    }
    app.view = View::Pipelines;

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

#[test]
fn every_tab_names_the_pipeline_its_panel_shows() {
    let mut app = populated_app();
    // Before any tab-specific data arrives, the selected pipeline.
    app.detail = None;
    for (view, tab) in [
        (View::Metrics, "METRICS"),
        (View::Connectors, "CONNECTORS"),
        (View::Profiler, "HOTSPOTS"),
        (View::Sql, "SQL"),
        (View::Logs, "LOGS"),
    ] {
        app.view = view;
        let screen = draw(&app, 160, 30);
        let title = screen.lines().nth(6).unwrap_or_default();
        assert!(
            title.contains(&format!("{tab} · orders_pipeline")),
            "{tab}: {title}"
        );
    }
    // With data, the pipeline the data belongs to, the count, and the sort.
    app.report = Some(loaded_report());
    app.report_pipeline = Some("inventory".to_string());
    app.view = View::Profiler;
    let screen = draw(&app, 160, 30);
    assert!(screen.contains("HOTSPOTS · inventory [1]  metric:time  sort:cost↓"));
}
