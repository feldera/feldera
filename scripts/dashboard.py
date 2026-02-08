#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "dash",
#   "pandas",
#   "plotly",
# ]
# ///
"""
Dashboard for Feldera benchmark CSV output.

Run with (needs uv):

$ ./dashboard.py -h

See `scalebench.py` to generate the CSV data for it.
"""

from __future__ import annotations

import argparse
import base64
import os
import re
from difflib import SequenceMatcher
from io import StringIO
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, dcc, html

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
IO_WRITE_WARN_MIB = 1600
IO_WRITE_MAX_MIB = 3000
MEM_BW_WARN_GBS = 160
MEM_BW_MAX_GBS = 200


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
        "mem_bw_read_min",
        "mem_bw_read_max",
        "mem_bw_read_mean",
        "mem_bw_read_stdev",
        "mem_bw_write_min",
        "mem_bw_write_max",
        "mem_bw_write_mean",
        "mem_bw_write_stdev",
        "mem_bw_total_min",
        "mem_bw_total_max",
        "mem_bw_total_mean",
        "mem_bw_total_stdev",
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


def parameterize_sql(
    first_sql: str, last_sql: str, placeholder: str = "$VARIABLE"
) -> str:
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


def payload_values_for_program(
    program_df: pd.DataFrame,
) -> tuple[list[float], list[str]]:
    if "payload_bytes" in program_df.columns:
        values = program_df["payload_bytes"].dropna().sort_values().unique().tolist()
        labels = [format_bytes_binary(v) for v in values]
        return values, labels
    if "payload_kib" in program_df.columns:
        values = program_df["payload_kib"].dropna().sort_values().unique().tolist()
        labels = [f"{v:.3f} KiB" for v in values]
        return values, labels
    return [], []


def payload_values_for_versions(
    primary_df: pd.DataFrame, compare_df: pd.DataFrame | None = None
) -> tuple[list[float], list[str]]:
    values, labels = payload_values_for_program(primary_df)
    if compare_df is None:
        return values, labels
    compare_values, _ = payload_values_for_program(compare_df)
    if not compare_values:
        return [], []
    compare_set = set(compare_values)
    label_map = {value: label for value, label in zip(values, labels)}
    common_values = [value for value in values if value in compare_set]
    common_labels = [label_map[value] for value in common_values]
    return common_values, common_labels


def build_write_barplot(
    primary_df: pd.DataFrame,
    compare_df: pd.DataFrame | None,
    primary_label: str,
    compare_label: str | None = None,
) -> go.Figure | None:
    if (
        "storage_value" not in primary_df.columns
        or "duration_s" not in primary_df.columns
    ):
        return None

    def agg_mib_per_sec(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["pipeline_workers", "mib_s"])
        values = (df["storage_value"] / df["duration_s"]).dropna() / (1024.0**2)
        data = df.loc[values.index, "pipeline_workers"].to_frame()
        data["mib_s"] = values
        return (
            data.groupby("pipeline_workers", dropna=False)["mib_s"].mean().reset_index()
        )

    primary_agg = agg_mib_per_sec(primary_df)
    if primary_agg.empty:
        return None

    compare_agg = None
    if compare_df is not None and compare_label:
        compare_agg = agg_mib_per_sec(compare_df)
        if not compare_agg.empty:
            common_workers = sorted(
                set(primary_agg["pipeline_workers"])
                & set(compare_agg["pipeline_workers"])
            )
            if common_workers:
                primary_agg = primary_agg[
                    primary_agg["pipeline_workers"].isin(common_workers)
                ]
                compare_agg = compare_agg[
                    compare_agg["pipeline_workers"].isin(common_workers)
                ]
            compare_agg = compare_agg.sort_values("pipeline_workers")
        else:
            compare_agg = None

    fig = go.Figure()
    primary_agg = primary_agg.sort_values("pipeline_workers")
    max_val = primary_agg["mib_s"].max()
    primary_x = [str(int(v)) for v in primary_agg["pipeline_workers"]]
    fig.add_trace(
        go.Bar(
            x=primary_x,
            y=primary_agg["mib_s"],
            name=primary_label,
            marker_color=ACCENT_COLOR,
            offsetgroup="primary",
            text=[f"{v:.0f} MiB/s" for v in primary_agg["mib_s"]],
            textposition="outside",
            cliponaxis=False,
            hovertemplate=(
                f"{primary_label}<br>Workers=%{{x}}<br>"
                "IO Writes=%{y:.1f} MiB/s<extra></extra>"
            ),
        )
    )
    if compare_agg is not None and compare_label:
        max_val = max(max_val, compare_agg["mib_s"].max())
        fig.add_trace(
            go.Bar(
                x=[str(int(v)) for v in compare_agg["pipeline_workers"]],
                y=compare_agg["mib_s"],
                name=compare_label,
                marker_color=ACCENT_COLOR_ALT,
                offsetgroup="compare",
                text=[f"{v:.0f} MiB/s" for v in compare_agg["mib_s"]],
                textposition="outside",
                cliponaxis=False,
                hovertemplate=(
                    f"{compare_label}<br>Workers=%{{x}}<br>"
                    "IO Writes=%{y:.1f} MiB/s<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        barmode="group",
        margin=dict(l=40, r=16, t=10, b=40),
        height=260,
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font=dict(family="DM Sans, Segoe UI, sans-serif", color=TEXT_COLOR),
        showlegend=True,
    )
    range_max = max(IO_WRITE_MAX_MIB * 1.05, max_val * 1.15)
    fig.update_yaxes(
        title="MiB/s",
        showgrid=True,
        gridcolor=GRID_COLOR,
        zeroline=False,
        range=[0, range_max],
    )
    worker_values = sorted(primary_agg["pipeline_workers"].dropna().unique().tolist())
    worker_labels = [str(int(v)) for v in worker_values]
    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        title="Pipeline workers",
        tickmode="array",
        tickvals=worker_labels,
        ticktext=worker_labels,
        type="category",
    )

    fig.add_shape(
        type="line",
        xref="paper",
        x0=0,
        x1=1,
        y0=IO_WRITE_WARN_MIB,
        y1=IO_WRITE_WARN_MIB,
        line=dict(color="#F4E19E", width=2, dash="dash"),
    )
    fig.add_shape(
        type="line",
        xref="paper",
        x0=0,
        x1=1,
        y0=IO_WRITE_MAX_MIB,
        y1=IO_WRITE_MAX_MIB,
        line=dict(color="#F7A1A1", width=2, dash="dash"),
    )
    fig.add_annotation(
        x=1.02,
        y=IO_WRITE_WARN_MIB,
        xref="paper",
        yref="y",
        text=f"{IO_WRITE_WARN_MIB} MiB/s",
        showarrow=False,
        font=dict(size=10, color="#8A7A2E"),
    )
    fig.add_annotation(
        x=1.02,
        y=IO_WRITE_MAX_MIB,
        xref="paper",
        yref="y",
        text=f"{IO_WRITE_MAX_MIB} MiB/s",
        showarrow=False,
        font=dict(size=10, color="#9A2C2C"),
    )
    return fig


def build_gauge_card(
    figures: list[go.Figure | None],
    animation_delay: int = 0,
    title: str = "IO Writes",
    graph_height: str = "260px",
) -> html.Div:
    graphs = []
    for fig in figures:
        if fig is None:
            continue
        graphs.append(
            dcc.Graph(
                figure=fig,
                config={"displayModeBar": False, "responsive": True},
                className="graph",
                style={"height": graph_height, "width": "100%"},
            )
        )
    gauge_content = (
        html.Div(graphs, className="gauge-grid")
        if graphs
        else html.Div("IO write data unavailable.", className="card empty")
    )
    return html.Div(
        className="card",
        style={"animationDelay": f"{animation_delay}ms"},
        children=[
            html.Div(
                [
                    title,
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


def build_metric_figure(
    series: list[dict],
    label: str,
    metric_key: str,
    show_ideal: bool = True,
) -> go.Figure:
    if metric_key == "state_amplification_value":
        hover_fmt = ".3f"
    else:
        hover_fmt = ".3s"
    is_bytes = metric_key in BYTE_METRICS

    fig = go.Figure()
    has_data = False
    max_val = None

    for entry in series:
        agg = entry["agg"]
        if agg.empty:
            continue
        has_data = True
        agg_sorted = agg.sort_values("pipeline_workers")
        err_plus = agg_sorted["value_max"] - agg_sorted["value_mean"]
        err_minus = agg_sorted["value_mean"] - agg_sorted["value_min"]
        customdata = None
        hovertemplate = f"{entry['name']}<br>Workers=%{{x}}<br>Mean=%{{y:{hover_fmt}}}<extra></extra>"
        if is_bytes:
            customdata = [format_bytes_binary(v) for v in agg_sorted["value_mean"]]
            hovertemplate = f"{entry['name']}<br>Workers=%{{x}}<br>Mean=%{{customdata}}<extra></extra>"
            max_val = (
                agg_sorted["value_max"].max()
                if max_val is None
                else max(max_val, agg_sorted["value_max"].max())
            )
        fig.add_trace(
            go.Scatter(
                x=agg_sorted["pipeline_workers"],
                y=agg_sorted["value_mean"],
                mode="lines+markers",
                name=entry["name"],
                line=dict(color=entry["color"], width=2),
                marker=dict(size=7, color=entry["color"]),
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
        )

    if not has_data:
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

    if metric_key == "throughput_value" and show_ideal:
        primary_series = next(
            (entry for entry in series if not entry["agg"].empty), None
        )
        if primary_series is not None:
            agg_sorted = primary_series["agg"].sort_values("pipeline_workers")
            baseline_workers = agg_sorted["pipeline_workers"].min()
            baseline_row = agg_sorted[
                agg_sorted["pipeline_workers"] == baseline_workers
            ]
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
                            f"Ideal speedup<br>Workers=%{{x}}<br>"
                            f"Ideal=%{{y:{hover_fmt}}}<extra></extra>"
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
        showlegend=len(fig.data) > 1,
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
    if is_bytes and max_val is not None:
        tickvals, ticktext = binary_tick_values(max_val)
        fig.update_yaxes(tickvals=tickvals, ticktext=ticktext)
    return fig


def build_mem_bw_figure(
    primary_df: pd.DataFrame,
    compare_df: pd.DataFrame | None,
    primary_label: str,
    compare_label: str | None = None,
) -> go.Figure | None:
    metric_defs = [
        ("mem_bw_read_mean", "Read", "#3B82F6"),
        ("mem_bw_write_mean", "Write", "#F59E0B"),
        ("mem_bw_total_mean", "Total", "#10B981"),
    ]

    if not all(key in primary_df.columns for key, _label, _color in metric_defs):
        return None

    fig = go.Figure()
    worker_labels: list[str] = []
    has_data = False
    max_val = None

    for metric_key, metric_label, color in metric_defs:
        primary_agg = aggregate_metric(primary_df, metric_key)
        if primary_agg.empty:
            continue
        primary_agg = primary_agg.sort_values("pipeline_workers")

        compare_agg = None
        if compare_df is not None and compare_label:
            if metric_key in compare_df.columns:
                compare_agg = aggregate_metric(compare_df, metric_key)
                if compare_agg.empty:
                    compare_agg = None
                else:
                    compare_agg = compare_agg.sort_values("pipeline_workers")

        if compare_agg is not None:
            common_workers = sorted(
                set(primary_agg["pipeline_workers"])
                & set(compare_agg["pipeline_workers"])
            )
            if common_workers:
                primary_agg = primary_agg[
                    primary_agg["pipeline_workers"].isin(common_workers)
                ]
                compare_agg = compare_agg[
                    compare_agg["pipeline_workers"].isin(common_workers)
                ]

        x_primary = [str(int(v)) for v in primary_agg["pipeline_workers"]]
        if not worker_labels:
            worker_labels = x_primary

        err_plus = primary_agg["value_max"] - primary_agg["value_mean"]
        err_minus = primary_agg["value_mean"] - primary_agg["value_min"]
        fig.add_trace(
            go.Bar(
                x=x_primary,
                y=primary_agg["value_mean"],
                name=f"{primary_label} {metric_label}",
                marker_color=color,
                offsetgroup=f"{metric_label}-primary",
                error_y=dict(
                    type="data",
                    symmetric=False,
                    array=err_plus,
                    arrayminus=err_minus,
                    thickness=1.2,
                    width=3,
                ),
                hovertemplate=(
                    f"{primary_label} {metric_label}<br>"
                    "Workers=%{x}<br>"
                    "Mean=%{y:.2f} GB/s<extra></extra>"
                ),
            )
        )
        has_data = True
        max_val = (
            primary_agg["value_max"].max()
            if max_val is None
            else max(max_val, primary_agg["value_max"].max())
        )

        if compare_agg is not None and compare_label:
            x_compare = [str(int(v)) for v in compare_agg["pipeline_workers"]]
            err_plus = compare_agg["value_max"] - compare_agg["value_mean"]
            err_minus = compare_agg["value_mean"] - compare_agg["value_min"]
            fig.add_trace(
                go.Bar(
                    x=x_compare,
                    y=compare_agg["value_mean"],
                    name=f"{compare_label} {metric_label}",
                    marker_color=color,
                    offsetgroup=f"{metric_label}-compare",
                    opacity=0.55,
                    error_y=dict(
                        type="data",
                        symmetric=False,
                        array=err_plus,
                        arrayminus=err_minus,
                        thickness=1.2,
                        width=3,
                    ),
                    hovertemplate=(
                        f"{compare_label} {metric_label}<br>"
                        "Workers=%{x}<br>"
                        "Mean=%{y:.2f} GB/s<extra></extra>"
                    ),
                )
            )
            max_val = max(max_val, compare_agg["value_max"].max())

    if not has_data:
        return None

    fig.update_layout(
        barmode="group",
        margin=dict(l=40, r=16, t=20, b=40),
        height=320,
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font=dict(family="DM Sans, Segoe UI, sans-serif", color=TEXT_COLOR),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.08,
            xanchor="left",
            x=0,
            font=dict(size=11),
        ),
    )
    fig.update_yaxes(
        title="GB/s",
        showgrid=True,
        gridcolor=GRID_COLOR,
        zeroline=False,
        rangemode="tozero",
    )
    if max_val is not None:
        range_max = max(max_val * 1.2, MEM_BW_MAX_GBS * 1.05)
        fig.update_yaxes(range=[0, range_max])

    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        title="Pipeline workers",
        tickmode="array",
        tickvals=worker_labels,
        ticktext=worker_labels,
        type="category",
    )
    fig.add_shape(
        type="line",
        xref="paper",
        x0=0,
        x1=1,
        y0=MEM_BW_WARN_GBS,
        y1=MEM_BW_WARN_GBS,
        line=dict(color="#F4E19E", width=2, dash="dash"),
    )
    fig.add_shape(
        type="line",
        xref="paper",
        x0=0,
        x1=1,
        y0=MEM_BW_MAX_GBS,
        y1=MEM_BW_MAX_GBS,
        line=dict(color="#F7A1A1", width=2, dash="dash"),
    )
    fig.add_annotation(
        x=1.02,
        y=MEM_BW_WARN_GBS,
        xref="paper",
        yref="y",
        text=f"{MEM_BW_WARN_GBS} GB/s",
        showarrow=False,
        font=dict(size=10, color="#8A7A2E"),
    )
    fig.add_annotation(
        x=1.02,
        y=MEM_BW_MAX_GBS,
        xref="paper",
        yref="y",
        text=f"{MEM_BW_MAX_GBS} GB/s",
        showarrow=False,
        font=dict(size=10, color="#9A2C2C"),
    )
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
                    ],
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
                                dcc.Upload(
                                    id="csv-upload",
                                    className="upload",
                                    children=html.Button(
                                        "Add CSV results",
                                        className="button button-outline",
                                    ),
                                    multiple=False,
                                ),
                                html.Div(
                                    "Default CSV always loads; uploaded CSV is appended.",
                                    className="control-help",
                                ),
                            ],
                        ),
                        html.Div(
                            className="control-group",
                            children=[
                                html.Label("Versions", className="control-label"),
                                html.Div(
                                    className="control-subgroup",
                                    children=[
                                        html.Label(
                                            "Primary",
                                            className="control-label control-label-small",
                                        ),
                                        dcc.Dropdown(
                                            id="version-primary-dropdown",
                                            className="control-dropdown",
                                            clearable=False,
                                        ),
                                    ],
                                ),
                                html.Div(
                                    className="control-subgroup",
                                    children=[
                                        html.Label(
                                            "Compare",
                                            className="control-label control-label-small",
                                        ),
                                        dcc.Dropdown(
                                            id="version-compare-dropdown",
                                            className="control-dropdown",
                                            clearable=True,
                                            placeholder="None",
                                        ),
                                    ],
                                ),
                                html.Div(
                                    "Compare metrics across two platform versions.",
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
                                            tooltip={
                                                "placement": "bottom",
                                                "always_visible": False,
                                            },
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
    Input("csv-upload", "contents"),
    State("csv-upload", "filename"),
    prevent_initial_call=False,
)
def handle_load(upload_contents, upload_filename):
    path = DEFAULT_DATA_PATH
    if not os.path.exists(path):
        return None, f"Missing file: {path}"
    try:
        default_df = load_csv(path)
    except Exception as exc:
        return None, f"Failed to load default CSV: {exc}"

    if upload_contents:
        try:
            _content_type, content_string = upload_contents.split(",", 1)
            decoded = base64.b64decode(content_string)
            upload_df = pd.read_csv(StringIO(decoded.decode("utf-8")))
        except Exception as exc:
            return None, f"Failed to load upload: {exc}"
        combined = pd.concat([default_df, upload_df], ignore_index=True)
        df = prepare_data(combined)
        label = upload_filename or "uploaded file"
        return (
            df.to_json(orient="split"),
            f"Loaded {len(default_df):,} rows + {len(upload_df):,} rows from {label}.",
        )

    df = prepare_data(default_df)
    return df.to_json(orient="split"), f"Loaded {len(df):,} rows."


@app.callback(
    Output("version-primary-dropdown", "options"),
    Output("version-primary-dropdown", "value"),
    Output("version-compare-dropdown", "options"),
    Output("version-compare-dropdown", "value"),
    Input("data-store", "data"),
    State("version-primary-dropdown", "value"),
    State("version-compare-dropdown", "value"),
)
def update_version_dropdown(data, primary_value, compare_value):
    if not data:
        return [], None, [], None
    df = pd.read_json(StringIO(data), orient="split")
    versions = sorted(df[VERSION_COLUMN].dropna().unique().tolist())
    if not versions:
        return [], None, [], None
    options = [{"label": version, "value": version} for version in versions]
    primary = primary_value if primary_value in versions else versions[0]
    compare_options = [{"label": "None", "value": ""}] + options
    compare = compare_value if compare_value in versions else None
    if compare == primary:
        compare = None
    return options, primary, compare_options, compare or ""


@app.callback(
    Output("program-tabs", "children"),
    Output("program-tabs", "value"),
    Input("data-store", "data"),
    Input("version-primary-dropdown", "value"),
    Input("version-compare-dropdown", "value"),
)
def update_program_tabs(data, primary_version, compare_version):
    if not data or not primary_version:
        return [], None
    df = pd.read_json(StringIO(data), orient="split")
    primary_df = df[df[VERSION_COLUMN] == primary_version]
    programs = set(primary_df["pipeline_name"].dropna().unique().tolist())
    if compare_version and compare_version != primary_version:
        compare_df = df[df[VERSION_COLUMN] == compare_version]
        compare_programs = set(compare_df["pipeline_name"].dropna().unique().tolist())
        programs = programs & compare_programs
    programs = sorted(programs)
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
    Input("version-primary-dropdown", "value"),
    Input("version-compare-dropdown", "value"),
    Input("data-store", "data"),
)
def update_payload_slider(program, primary_version, compare_version, data):
    if not data or not program or not primary_version:
        return 0, 0, {0: "0"}, 0, None, True, {"display": "none"}

    df = pd.read_json(StringIO(data), orient="split")
    primary_df = df[
        (df[VERSION_COLUMN] == primary_version) & (df["pipeline_name"] == program)
    ].copy()
    compare_df = None
    if compare_version and compare_version != primary_version:
        compare_df = df[
            (df[VERSION_COLUMN] == compare_version) & (df["pipeline_name"] == program)
        ].copy()
    values, labels = payload_values_for_versions(primary_df, compare_df)
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
    Input("version-primary-dropdown", "value"),
    Input("version-compare-dropdown", "value"),
    Input("data-store", "data"),
)
def update_graphs(program, payload_idx, primary_version, compare_version, data):
    if not data:
        empty_gauge = build_gauge_card(
            figures=[None],
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
    if not program or not primary_version:
        return (
            "Select a program tab.",
            html.Div(
                [
                    html.Div("Select a program tab.", className="card empty"),
                    build_gauge_card(
                        figures=[None],
                    ),
                ],
                className="grid",
            ),
        )

    compare_active = compare_version and compare_version != primary_version
    program_df_all = df[
        (df[VERSION_COLUMN] == primary_version) & (df["pipeline_name"] == program)
    ].copy()
    compare_df_all = None
    if compare_active:
        compare_df_all = df[
            (df[VERSION_COLUMN] == compare_version) & (df["pipeline_name"] == program)
        ].copy()

    program_df = program_df_all
    compare_df = compare_df_all

    if payload_idx is not None:
        values, _labels = payload_values_for_versions(program_df_all, compare_df_all)
        if values and isinstance(payload_idx, (int, float)):
            idx = int(payload_idx)
            if 0 <= idx < len(values):
                payload_value = values[idx]
                if "payload_bytes" in program_df.columns:
                    if payload_value in program_df["payload_bytes"].unique():
                        program_df = program_df[
                            program_df["payload_bytes"] == payload_value
                        ]
                        if compare_df is not None:
                            compare_df = compare_df[
                                compare_df["payload_bytes"] == payload_value
                            ]
                elif "payload_kib" in program_df.columns:
                    program_df = program_df[program_df["payload_kib"] == payload_value]
                    if compare_df is not None:
                        compare_df = compare_df[
                            compare_df["payload_kib"] == payload_value
                        ]

    gauge_df = program_df
    compare_gauge_df = compare_df

    sql_primary = extract_program_sql(program_df_all)
    sql_compare = (
        extract_program_sql(compare_df_all) if compare_df_all is not None else None
    )
    sql_block = f"```sql\n{sql_primary}\n```"
    if sql_compare is not None:
        if sql_compare.strip() == sql_primary.strip():
            sql_block = (
                f"```sql\n-- {primary_version} vs {compare_version}\n{sql_primary}\n```"
            )
        else:
            sql_block = (
                f"```sql\n-- {primary_version}\n{sql_primary}\n```\n"
                f"```sql\n-- {compare_version}\n{sql_compare}\n```"
            )

    write_fig = build_write_barplot(
        gauge_df,
        compare_gauge_df if compare_active else None,
        str(primary_version),
        str(compare_version) if compare_active else None,
    )
    write_gauge_figs = [write_fig]

    graphs = []
    gauge_added = False

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
        series = [
            {
                "name": str(primary_version),
                "agg": aggregate_metric(program_df, metric_key),
                "color": ACCENT_COLOR,
            }
        ]
        if compare_active and compare_df is not None:
            series.append(
                {
                    "name": str(compare_version),
                    "agg": aggregate_metric(compare_df, metric_key),
                    "color": ACCENT_COLOR_ALT,
                }
            )
        fig = build_metric_figure(series, label, metric_key, show_ideal=True)
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
                    ),
                ],
            )
        )
        if metric_key == "storage_value" and any(write_gauge_figs):
            graphs.append(
                build_gauge_card(
                    write_gauge_figs,
                    animation_delay=len(graphs) * 60,
                    title="IO Writes",
                )
            )
            gauge_added = True

    if not gauge_added:
        graphs.append(
            build_gauge_card(
                write_gauge_figs,
                animation_delay=len(graphs) * 60,
                title="IO Writes",
            )
        )

    mem_bw_fig = build_mem_bw_figure(
        program_df,
        compare_df if compare_active else None,
        str(primary_version),
        str(compare_version) if compare_active else None,
    )
    if mem_bw_fig is not None:
        graphs.append(
            html.Div(
                className="card",
                style={"animationDelay": f"{len(graphs) * 60}ms"},
                children=[
                    html.Div("Memory Bandwidth [GB/s]", className="card-title"),
                    dcc.Graph(
                        figure=mem_bw_fig,
                        config={"displayModeBar": False, "responsive": True},
                        className="graph",
                        style={"height": "320px", "width": "100%"},
                    ),
                ],
            )
        )

    return sql_block, graphs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Feldera benchmark dashboard.")
    parser.add_argument(
        "--data",
        default=DEFAULT_DATA_PATH,
        help="Path to the benchmark CSV to load by default.",
    )
    args = parser.parse_args()
    DEFAULT_DATA_PATH = str(Path(args.data).expanduser())
    app.run(debug=True)
