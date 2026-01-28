#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "dash",
#   "pandas",
#   "plotly",
# ]
# ///
"""Dash dashboard for Feldera benchmark CSV output."""

from __future__ import annotations

import base64
import os
import re
from difflib import SequenceMatcher
from io import StringIO
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, ctx, dcc, html, no_update

DEFAULT_DATA_PATH = str(Path(__file__).resolve().parents[0] / "bench_results.csv")
VERSION_COLUMN = "platform_version"
METRICS: List[Tuple[str, str]] = [
    ("throughput_value", "Throughput [records/s]"),
    ("storage_value", "Storage [bytes]"),
    ("memory_value", "Memory [bytes]"),
    ("buffered_input_records_value", "Buffered Input Records"),
    ("state_amplification_value", "State Amplification"),
]
BYTE_METRICS = {"storage_value", "memory_value"}
ACCENT_COLOR = "#C533B9"
ACCENT_COLOR_ALT = "#FCAF4F"
GRID_COLOR = "#D9D9D9"
TEXT_COLOR = "#1E1E1E"
GAUGE_GREEN_MAX = 800 * 1024 * 1024  # 800 MiB
GAUGE_YELLOW_MAX = int(1.6 * 1024 * 1024 * 1024)  # 1.6 GiB
GAUGE_RED_MAX = 3 * 1024 * 1024 * 1024  # 3 GiB


def load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def coerce_numeric(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "pipeline_name" not in df.columns:
        df["pipeline_name"] = "u64"
    df[VERSION_COLUMN] = df[VERSION_COLUMN].astype(str)

    numeric_columns = [
        "pipeline_workers",
        "payload_bytes",
        "duration_s",
        "throughput_value",
        "storage_value",
        "memory_value",
        "buffered_input_records_value",
        "state_amplification_value",
    ]
    df = coerce_numeric(df, numeric_columns)

    if "payload_bytes" in df.columns:
        df["payload_kib"] = df["payload_bytes"] / 1024.0
    else:
        df["payload_kib"] = pd.NA

    return df


def aggregate_metric(df: pd.DataFrame, metric_key: str) -> pd.DataFrame:
    agg = (
        df.groupby("pipeline_workers", dropna=False)[metric_key]
        .agg(["mean", "min", "max"])
        .reset_index()
        .rename(columns={"mean": "value_mean", "min": "value_min", "max": "value_max"})
    )
    return agg


def extract_program_sql(program_df: pd.DataFrame) -> str:
    if "program_sql" not in program_df.columns:
        return "No SQL available in CSV."
    df = program_df.copy()
    sort_cols = [
        col
        for col in ["pipeline_workers", "payload_bytes", "run_id"]
        if col in df.columns
    ]
    if sort_cols:
        df = df.sort_values(sort_cols)
    sql_values = df["program_sql"].dropna().tolist()
    if not sql_values:
        return "No SQL available in CSV."
    first_sql_raw = sql_values[0]
    if not isinstance(first_sql_raw, str) or not first_sql_raw:
        return "No SQL available in CSV."
    first_sql = first_sql_raw
    if len(sql_values) == 1:
        return first_sql.replace("\\n", "\n")

    last_sql = sql_values[-1]
    if not isinstance(last_sql, str) or not last_sql:
        return first_sql.replace("\\n", "\n")

    parameterized = parameterize_sql(first_sql, last_sql)
    return parameterized.replace("\\n", "\n")


TOKEN_PATTERN = re.compile(r"\s+|\d+|[A-Za-z_]+|[^A-Za-z0-9_\s]")


def tokenize_sql(sql: str) -> list[str]:
    return TOKEN_PATTERN.findall(sql)


def parameterize_sql(first_sql: str, last_sql: str, placeholder: str = "$VARIABLE") -> str:
    first_tokens = tokenize_sql(first_sql)
    last_tokens = tokenize_sql(last_sql)

    matcher = SequenceMatcher(None, first_tokens, last_tokens, autojunk=False)
    parts: list[str] = []
    last_was_placeholder = False

    for tag, i1, i2, _j1, _j2 in matcher.get_opcodes():
        if tag == "equal":
            parts.extend(first_tokens[i1:i2])
            last_was_placeholder = False
        else:
            if not last_was_placeholder:
                parts.append(placeholder)
                last_was_placeholder = True

    return "".join(parts)


def payload_values_for_program(program_df: pd.DataFrame) -> tuple[list[float], list[str]]:
    if "payload_bytes" in program_df.columns:
        values = (
            program_df["payload_bytes"].dropna().sort_values().unique().tolist()
        )
        labels = [format_bytes_binary(v) for v in values]
        return values, labels
    if "payload_kib" in program_df.columns:
        values = (
            program_df["payload_kib"].dropna().sort_values().unique().tolist()
        )
        labels = [f"{v:.3f} KiB" for v in values]
        return values, labels
    return [], []


def build_write_gauge(df: pd.DataFrame) -> go.Figure | None:
    if "storage_value" not in df.columns or "duration_s" not in df.columns:
        return None
    bytes_per_sec = df["storage_value"] / df["duration_s"]
    bytes_per_sec = bytes_per_sec.dropna()
    if bytes_per_sec.empty:
        return None

    value_bytes = float(bytes_per_sec.mean())

    # Use GiB for values >= 1 GiB, otherwise MiB to keep ticks readable.
    use_gib = value_bytes >= 1024**3
    unit_factor = 1024.0**3 if use_gib else 1024.0**2
    unit_suffix = " GiB/s" if use_gib else " MiB/s"

    value_scaled = value_bytes / unit_factor
    green_max = GAUGE_GREEN_MAX / unit_factor
    yellow_max = GAUGE_YELLOW_MAX / unit_factor
    red_max = GAUGE_RED_MAX / unit_factor
    axis_max = max(red_max, value_scaled * 1.1)
    tick_fmt = ".2f" if use_gib else ".0f"
    number_fmt = ".2f" if use_gib else ".0f"
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=value_scaled,
            number={
                "valueformat": number_fmt,
                "suffix": unit_suffix,
                "font": {"size": 16, "color": TEXT_COLOR},
            },
            title={"text": "IO Writes", "font": {"size": 12, "color": TEXT_COLOR}},
            gauge={
                "axis": {
                    "range": [0, axis_max],
                    "tickformat": tick_fmt,
                    "tickprefix": "",
                    "ticksuffix": unit_suffix,
                    "tickfont": {"size": 10},
                },
                "bar": {"color": ACCENT_COLOR, "thickness": 0.2},
                "steps": [
                    {"range": [0, green_max], "color": "#9EE7A4"},
                    {"range": [green_max, yellow_max], "color": "#F4E19E"},
                    {"range": [yellow_max, red_max], "color": "#F7A1A1"},
                ],
                "threshold": {
                    "line": {"color": "#6A1B9A", "width": 3},
                    "thickness": 0.65,
                    "value": value_scaled,
                },
            },
        )
    )
    fig.update_layout(
        margin=dict(l=36, r=36, t=24, b=12),
        height=210,
        paper_bgcolor="#FFFFFF",
        font=dict(family="DM Sans, Segoe UI, sans-serif", color=TEXT_COLOR),
    )
    return fig


def build_gauge_card(
    fig: go.Figure | None,
    worker_marks: dict[int, str],
    slider_value: int,
    slider_max: int,
    disabled: bool,
    animation_delay: int = 0,
) -> html.Div:
    gauge_content = (
        dcc.Graph(
            figure=fig,
            config={"displayModeBar": False, "responsive": True},
            className="graph",
            style={"height": "210px", "width": "100%"},
        )
        if fig is not None
        else html.Div("IO write data unavailable.", className="card empty")
    )
    return html.Div(
        className="card",
        style={"animationDelay": f"{animation_delay}ms"},
        children=[
            html.Div(
                [
                    "IO Writes",
                    html.Span(
                        "?",
                        **{
                            "data-tooltip": (
                                "Calculated as total storage used divided by experiment duration in seconds; "
                                "not continuous disk IO monitoring."
                            )
                        },
                        className="info-tooltip",
                        style={
                            "display": "inline-block",
                            "marginLeft": "6px",
                            "width": "16px",
                            "height": "16px",
                            "lineHeight": "16px",
                            "textAlign": "center",
                            "borderRadius": "50%",
                            "border": "1px solid #9aa0a6",
                            "color": "#6b7280",
                            "fontSize": "11px",
                            "cursor": "default",
                            "fontWeight": 600,
                        },
                    ),
                ],
                className="card-title",
            ),
            gauge_content,
            html.Div(
                className="gauge-worker-select",
                children=[
                    html.Label(
                        "Workers",
                        className="control-label control-label-small",
                    ),
                    dcc.Slider(
                        id="gauge-worker-slider",
                        min=0,
                        max=slider_max,
                        step=1,
                        value=slider_value,
                        marks=worker_marks,
                        included=False,
                        disabled=disabled,
                        tooltip={"placement": "bottom", "always_visible": False},
                    ),
                ],
            ),
        ],
    )


def format_bytes_binary(value: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if value < 1:
        return f"{value:.0f} B"
    unit_idx = 0
    while value >= 1024.0 and unit_idx < len(units) - 1:
        value /= 1024.0
        unit_idx += 1
    if value >= 100:
        return f"{value:.0f} {units[unit_idx]}"
    if value >= 10:
        return f"{value:.1f} {units[unit_idx]}"
    return f"{value:.2f} {units[unit_idx]}"


def binary_tick_values(max_val: float, ticks: int = 5) -> tuple[list[float], list[str]]:
    if max_val <= 0:
        return [0.0], ["0 B"]
    step = max_val / max(ticks - 1, 1)
    vals = [round(step * idx, 6) for idx in range(ticks)]
    labels = [format_bytes_binary(val) for val in vals]
    return vals, labels


def build_metric_figure(agg: pd.DataFrame, label: str, metric_key: str) -> go.Figure:
    if agg.empty:
        fig = go.Figure()
        fig.update_layout(
            title=None,
            xaxis_title="Pipeline workers",
            yaxis_title=label,
            font=dict(family="DM Sans, Segoe UI, sans-serif", color=TEXT_COLOR),
            paper_bgcolor="#FFFFFF",
            plot_bgcolor="#FFFFFF",
            margin=dict(l=40, r=20, t=20, b=40),
            height=320,
            autosize=True,
            showlegend=False,
        )
        return fig

    if metric_key == "state_amplification_value":
        hover_fmt = ".3f"
    else:
        hover_fmt = ".3s"
    is_bytes = metric_key in BYTE_METRICS

    agg_sorted = agg.sort_values("pipeline_workers")
    err_plus = agg_sorted["value_max"] - agg_sorted["value_mean"]
    err_minus = agg_sorted["value_mean"] - agg_sorted["value_min"]
    customdata = None
    hovertemplate = f"Workers=%{{x}}<br>Mean=%{{y:{hover_fmt}}}<extra></extra>"
    if is_bytes:
        customdata = [format_bytes_binary(v) for v in agg_sorted["value_mean"]]
        hovertemplate = "Workers=%{x}<br>Mean=%{customdata}<extra></extra>"
    fig = go.Figure(
        data=[
            go.Scatter(
                x=agg_sorted["pipeline_workers"],
                y=agg_sorted["value_mean"],
                mode="lines+markers",
                name=label,
                line=dict(color=ACCENT_COLOR, width=2),
                marker=dict(size=7, color=ACCENT_COLOR),
                customdata=customdata,
                error_y=dict(
                    type="data",
                    symmetric=False,
                    array=err_plus,
                    arrayminus=err_minus,
                    thickness=1.2,
                    width=3,
                ),
                hovertemplate=hovertemplate,
            )
        ]
    )
    if metric_key == "throughput_value" and not agg_sorted.empty:
        baseline_workers = agg_sorted["pipeline_workers"].min()
        baseline_row = agg_sorted[agg_sorted["pipeline_workers"] == baseline_workers]
        if not baseline_row.empty and baseline_workers:
            baseline_value = baseline_row["value_mean"].iloc[0]
            ideal = baseline_value * (
                agg_sorted["pipeline_workers"] / baseline_workers
            )
            fig.add_trace(
                go.Scatter(
                    x=agg_sorted["pipeline_workers"],
                    y=ideal,
                    mode="lines",
                    name="Ideal speedup",
                    line=dict(color=ACCENT_COLOR_ALT, width=2, dash="dash"),
                    hovertemplate=(
                        f"Workers=%{{x}}<br>Ideal=%{{y:{hover_fmt}}}<extra></extra>"
                    ),
                )
            )
    fig.update_layout(
        title=None,
        xaxis_title="Pipeline workers",
        yaxis_title=label,
        font=dict(family="DM Sans, Segoe UI, sans-serif", color=TEXT_COLOR),
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        margin=dict(l=40, r=20, t=20, b=40),
        height=320,
        autosize=True,
        showlegend=metric_key == "throughput_value",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.08,
            xanchor="left",
            x=0,
            font=dict(size=11),
        ),
    )
    fig.update_xaxes(showgrid=True, gridcolor=GRID_COLOR, zeroline=False)
    fig.update_yaxes(
        showgrid=True, gridcolor=GRID_COLOR, zeroline=False, rangemode="tozero"
    )
    if is_bytes:
        max_val = agg_sorted["value_max"].max()
        tickvals, ticktext = binary_tick_values(max_val)
        fig.update_yaxes(tickvals=tickvals, ticktext=ticktext)
    return fig


def load_inline_css() -> str:
    css_path = Path(__file__).with_name("dashboard.css")
    try:
        return css_path.read_text(encoding="utf-8")
    except OSError:
        return ""


app = Dash(__name__)
app.title = "Feldera Bench Dashboard"
app.config.suppress_callback_exceptions = True
app.index_string = f"""<!DOCTYPE html>
<html>
    <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        {{%favicon%}}
        {{%css%}}
        <style>{load_inline_css()}</style>
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
    </body>
</html>"""

app.layout = html.Div(
    className="app",
    children=[
        dcc.Store(id="data-store"),
        html.Div(
            className="app-header",
            children=[
                html.Div(
                    className="header-left",
                    children=[
                        html.Img(
                            src="https://www.feldera.com/images/Logomark.png",
                            alt="Feldera",
                            className="brand-logo",
                        ),
                        html.Div(
                            children=[
                                html.H1("Benchmark Dashboard", className="app-title"),
                                html.P(
                                    "Explore throughput, memory, storage, buffered input, and state amplification.",
                                    className="app-subtitle",
                                ),
                            ],
                        ),
                    ]
                ),
                html.Div(
                    className="header-right",
                    children=[
                        html.Div(id="load-status", className="status-pill"),
                    ],
                ),
            ],
        ),
        html.Div(
            className="layout",
            children=[
                html.Div(
                    className="controls",
                    children=[
                        html.Div(
                            className="control-group",
                            children=[
                                html.Label("Data Source", className="control-label"),
                                html.Button(
                                    "Load default CSV",
                                    id="load-button",
                                    className="button",
                                ),
                                dcc.Upload(
                                    id="csv-upload",
                                    className="upload",
                                    children=html.Button(
                                        "Upload CSV",
                                        className="button button-outline",
                                    ),
                                    multiple=False,
                                ),
                                html.Div(
                                    "Uses the default CSV unless you upload a file.",
                                    className="control-help",
                                ),
                            ],
                        ),
                        html.Div(
                            className="control-group",
                            children=[
                                html.Label("Version", className="control-label"),
                                dcc.Dropdown(
                                    id="version-dropdown",
                                    className="control-dropdown",
                                    clearable=False,
                                ),
                                html.Div(
                                    "Filter results by platform version.",
                                    className="control-help",
                                ),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    className="content",
                    children=[
                        html.Div(
                            className="tabs-card",
                            children=[dcc.Tabs(id="program-tabs", className="tabs")],
                        ),
                        html.Div(
                            id="sql-block",
                            className="sql-card",
                            children=[
                                html.Div("Program SQL", className="card-title"),
                                dcc.Markdown(
                                    "```sql\nLoading...\n```",
                                    id="sql-content",
                                    className="sql-markdown",
                                ),
                                html.Div(
                                    id="payload-slider-wrapper",
                                    className="payload-slider",
                                    children=[
                                        html.Label(
                                            "Payload Record Size [Bytes]",
                                            className="control-label control-label-small",
                                        ),
                                        dcc.Slider(
                                            id="payload-slider",
                                            min=0,
                                            max=0,
                                            step=1,
                                            value=0,
                                            marks={0: "0"},
                                            included=False,
                                            disabled=True,
                                            tooltip={"placement": "bottom", "always_visible": False},
                                        ),
                                    ],
                                ),
                            ],
                        ),
                        html.Div(
                            id="metric-graphs",
                            className="grid",
                            children=[
                                html.Div(
                                    className="card",
                                    children=[
                                        html.Div("IO Writes", className="card-title"),
                                        html.Div("Loading...", className="card empty"),
                                        html.Div(
                                            className="gauge-worker-select",
                                            children=[
                                                html.Label(
                                                    "Workers",
                                                    className="control-label control-label-small",
                                                ),
                                                dcc.Slider(
                                                    id="gauge-worker-slider",
                                                    min=0,
                                                    max=0,
                                                    step=1,
                                                    value=0,
                                                    marks={0: "0"},
                                                    included=False,
                                                    disabled=True,
                                                ),
                                            ],
                                        ),
                                    ],
                                )
                            ],
                        ),
                    ],
                ),
            ],
        ),
    ],
)


@app.callback(
    Output("data-store", "data"),
    Output("load-status", "children"),
    Input("load-button", "n_clicks"),
    Input("csv-upload", "contents"),
    State("csv-upload", "filename"),
    prevent_initial_call=False,
)
def handle_load(_n_clicks, upload_contents, upload_filename):
    if ctx.triggered_id == "csv-upload" and upload_contents:
        try:
            _content_type, content_string = upload_contents.split(",", 1)
            decoded = base64.b64decode(content_string)
            df = prepare_data(pd.read_csv(StringIO(decoded.decode("utf-8"))))
        except Exception as exc:
            return None, f"Failed to load upload: {exc}"
        label = upload_filename or "uploaded file"
        return df.to_json(orient="split"), f"Loaded {len(df):,} rows from {label}."
    path = DEFAULT_DATA_PATH
    if not os.path.exists(path):
        return None, f"Missing file: {path}"
    try:
        df = prepare_data(load_csv(path))
    except Exception as exc:
        return None, f"Failed to load CSV: {exc}"

    return df.to_json(orient="split"), f"Loaded {len(df):,} rows."


@app.callback(
    Output("version-dropdown", "options"),
    Output("version-dropdown", "value"),
    Input("data-store", "data"),
)
def update_version_dropdown(data):
    if not data:
        return [], None
    df = pd.read_json(StringIO(data), orient="split")
    versions = sorted(df[VERSION_COLUMN].dropna().unique().tolist())
    if not versions:
        return [], None
    options = [{"label": version, "value": version} for version in versions]
    return options, options[0]["value"]


@app.callback(
    Output("program-tabs", "children"),
    Output("program-tabs", "value"),
    Input("data-store", "data"),
    Input("version-dropdown", "value"),
)
def update_program_tabs(data, version):
    if not data:
        return [], None
    df = pd.read_json(StringIO(data), orient="split")
    if version:
        df = df[df[VERSION_COLUMN] == version]
    programs = sorted(df["pipeline_name"].dropna().unique().tolist())
    tabs = [dcc.Tab(label=program, value=program) for program in programs]
    selected = programs[0] if programs else None
    return tabs, selected


@app.callback(
    Output("payload-slider", "min"),
    Output("payload-slider", "max"),
    Output("payload-slider", "marks"),
    Output("payload-slider", "value"),
    Output("payload-slider", "step"),
    Output("payload-slider", "disabled"),
    Output("payload-slider-wrapper", "style"),
    Input("program-tabs", "value"),
    Input("version-dropdown", "value"),
    Input("data-store", "data"),
)
def update_payload_slider(program, version, data):
    if not data or not program:
        return 0, 0, {0: "0"}, 0, None, True, {"display": "none"}

    df = pd.read_json(StringIO(data), orient="split")
    if version:
        df = df[df[VERSION_COLUMN] == version]
    program_df = df[df["pipeline_name"] == program].copy()
    values, labels = payload_values_for_program(program_df)
    if not values:
        return 0, 0, {0: "0"}, 0, None, True, {"display": "none"}
    if len(values) == 1:
        return 0, 0, {0: labels[0]}, 0, None, True, {"display": "none"}

    marks = {idx: label for idx, label in enumerate(labels)}
    slider_max = max(len(values) - 1, 0)
    return 0, slider_max, marks, 0, 1, False, {}


@app.callback(
    Output("sql-content", "children"),
    Output("metric-graphs", "children"),
    Input("program-tabs", "value"),
    Input("payload-slider", "value"),
    Input("gauge-worker-slider", "value"),
    Input("version-dropdown", "value"),
    Input("data-store", "data"),
)
def update_graphs(program, payload_idx, gauge_worker_idx, version, data):
    if not data:
        empty_gauge = build_gauge_card(
            fig=None,
            worker_marks={0: "0"},
            slider_value=0,
            slider_max=0,
            disabled=True,
        )
        return (
            "No SQL available.",
            html.Div(
                [
                    html.Div("No data loaded.", className="card empty"),
                    empty_gauge,
                ],
                className="grid",
            ),
        )
    df = pd.read_json(StringIO(data), orient="split")
    if version:
        df = df[df[VERSION_COLUMN] == version]
    if not program:
        return (
            "Select a program tab.",
            html.Div(
                [
                    html.Div("Select a program tab.", className="card empty"),
                    build_gauge_card(
                        fig=None,
                        worker_marks={0: "0"},
                        slider_value=0,
                        slider_max=0,
                        disabled=True,
                    ),
                ],
                className="grid",
            ),
        )

    program_df_all = df[df["pipeline_name"] == program].copy()
    program_df = program_df_all
    if payload_idx is not None:
        values, _labels = payload_values_for_program(program_df)
        if values and isinstance(payload_idx, (int, float)):
            idx = int(payload_idx)
            if 0 <= idx < len(values):
                payload_value = values[idx]
                if "payload_bytes" in program_df.columns:
                    if payload_value in program_df["payload_bytes"].unique():
                        program_df = program_df[program_df["payload_bytes"] == payload_value]
                elif "payload_kib" in program_df.columns:
                    program_df = program_df[program_df["payload_kib"] == payload_value]

    workers: list[float] = []
    worker_marks: dict[int, str] = {}
    selected_idx: int | None = None
    if "pipeline_workers" in program_df.columns:
        workers = (
            program_df["pipeline_workers"].dropna().sort_values().unique().tolist()
        )
        worker_marks = {idx: str(int(w)) for idx, w in enumerate(workers)}
        if workers:
            if isinstance(gauge_worker_idx, int) and 0 <= gauge_worker_idx < len(workers):
                selected_idx = gauge_worker_idx
            else:
                selected_idx = 0

    gauge_df = program_df
    selected_worker = None
    if selected_idx is not None and workers:
        selected_worker = workers[selected_idx]
        gauge_df = program_df[program_df["pipeline_workers"] == selected_worker]

    sql = extract_program_sql(program_df_all)
    write_gauge_fig = build_write_gauge(gauge_df)

    graphs = []
    gauge_added = False

    slider_disabled = not workers
    slider_max = max(len(workers) - 1, 0)
    slider_value = selected_idx if selected_idx is not None else 0

    for idx, (metric_key, label) in enumerate(METRICS):
        if metric_key not in program_df.columns:
            graphs.append(
                html.Div(
                    f"Missing metric: {metric_key}",
                    className="card empty",
                    style={"animationDelay": f"{len(graphs) * 60}ms"},
                )
            )
            continue
        agg = aggregate_metric(program_df, metric_key)
        fig = build_metric_figure(agg, label, metric_key)
        graphs.append(
            html.Div(
                className="card",
                style={"animationDelay": f"{len(graphs) * 60}ms"},
                children=[
                    html.Div(label, className="card-title"),
                    dcc.Graph(
                        figure=fig,
                        config={"displayModeBar": False, "responsive": True},
                        className="graph",
                        style={"height": "320px", "width": "100%"},
                    )
                ],
            )
        )
        if metric_key == "storage_value" and write_gauge_fig is not None:
            graphs.append(
                build_gauge_card(
                    write_gauge_fig,
                    worker_marks,
                    slider_value,
                    slider_max,
                    slider_disabled,
                    animation_delay=len(graphs) * 60,
                )
            )
            gauge_added = True

    if not gauge_added:
        graphs.append(
            build_gauge_card(
                write_gauge_fig,
                worker_marks,
                slider_value,
                slider_max,
                slider_disabled,
                animation_delay=len(graphs) * 60,
            )
        )

    return f"```sql\n{sql}\n```", graphs


if __name__ == "__main__":
    app.run(debug=True)
