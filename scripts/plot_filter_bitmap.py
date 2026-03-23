#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pandas>=2.2",
#   "plotly>=5.24",
#   "kaleido>=0.2.1",
# ]
# ///

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot filter_bitmap.csv comparisons for bloom vs roaring."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("filter_bitmap.csv"),
        help="Input CSV produced by crates/dbsp/benches/filter_bitmap.rs",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("filter_bitmap_plots"),
        help="Directory to write plots into",
    )
    return parser.parse_args()


def format_structure(name: str) -> str:
    return {
        "bloom": "fastbloom",
        "roaring": "roaring",
    }.get(name, name)


def format_num_elements(value: int) -> str:
    return f"{value:,}"


def format_bytes(value: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    unit_index = 0
    while value >= 1024.0 and unit_index + 1 < len(units):
        value /= 1024.0
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"


def format_ns_per_element(value: float) -> str:
    return f"{value:.2f} ns"


def build_metric_figure(
    frame: pd.DataFrame,
    y_column: str,
    y_label: str,
    title: str,
    formatter,
) -> go.Figure:
    fig = go.Figure()
    colors = {
        "bloom": "#0f766e",
        "roaring": "#c2410c",
    }

    ordered_sizes = sorted(frame["num_elements"].unique())
    x_labels = [format_num_elements(value) for value in ordered_sizes]

    for structure in ["bloom", "roaring"]:
        structure_frame = (
            frame[frame["structure"] == structure]
            .sort_values("num_elements")
            .set_index("num_elements")
        )
        if structure_frame.empty:
            continue

        y_values = [float(structure_frame.loc[size, y_column]) for size in ordered_sizes]
        text_values = [formatter(value) for value in y_values]
        fig.add_trace(
            go.Bar(
                name=format_structure(structure),
                x=x_labels,
                y=y_values,
                text=text_values,
                textposition="outside",
                cliponaxis=False,
                marker_color=colors[structure],
            )
        )

    fig.update_layout(
        title=title,
        barmode="group",
        template="plotly_white",
        width=max(950, 180 * len(ordered_sizes)),
        height=650,
        legend_title_text="Structure",
        margin=dict(t=90, r=30, b=80, l=80),
    )
    fig.update_xaxes(title_text="Input Size (num_elements)")
    fig.update_yaxes(title_text=y_label, type="log")
    return fig


def build_summary_figure(frame: pd.DataFrame) -> go.Figure:
    fig = make_subplots(
        rows=1,
        cols=3,
        subplot_titles=("Insert Time", "Lookup Time", "Memory Usage"),
        horizontal_spacing=0.08,
    )

    colors = {
        "bloom": "#0f766e",
        "roaring": "#c2410c",
    }
    ordered_sizes = sorted(frame["num_elements"].unique())
    x_labels = [format_num_elements(value) for value in ordered_sizes]
    metrics = [
        ("insert_ns_per_element_avg", format_ns_per_element, "Insert Time (ns/element)"),
        ("lookup_ns_per_element_avg", format_ns_per_element, "Lookup Time (ns/element)"),
        ("bytes_used", format_bytes, "Memory Usage (bytes)"),
    ]

    for col_index, (metric, formatter, y_label) in enumerate(metrics, start=1):
        for structure in ["bloom", "roaring"]:
            structure_frame = (
                frame[frame["structure"] == structure]
                .sort_values("num_elements")
                .set_index("num_elements")
            )
            if structure_frame.empty:
                continue

            y_values = [float(structure_frame.loc[size, metric]) for size in ordered_sizes]
            text_values = [formatter(value) for value in y_values]
            fig.add_trace(
                go.Bar(
                    name=format_structure(structure),
                    x=x_labels,
                    y=y_values,
                    text=text_values,
                    textposition="outside",
                    cliponaxis=False,
                    marker_color=colors[structure],
                    showlegend=col_index == 1,
                ),
                row=1,
                col=col_index,
            )
        fig.update_yaxes(title_text=y_label, type="log", row=1, col=col_index)
        fig.update_xaxes(title_text="Input Size (num_elements)", row=1, col=col_index)

    fig.update_layout(
        title="filter_bitmap Summary",
        barmode="group",
        template="plotly_white",
        width=max(1600, 420 * len(ordered_sizes)),
        height=700,
        legend_title_text="Structure",
        margin=dict(t=100, r=30, b=90, l=70),
    )
    return fig


def write_figure(fig: go.Figure, base_path: Path) -> None:
    fig.write_html(base_path.with_suffix(".html"))
    fig.write_image(base_path.with_suffix(".png"), scale=2)


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise SystemExit(f"input CSV not found: {args.input}")

    frame = pd.read_csv(args.input)
    if frame.empty:
        raise SystemExit(f"input CSV is empty: {args.input}")

    required_columns = {
        "structure",
        "num_elements",
        "insert_ns_per_element_avg",
        "lookup_ns_per_element_avg",
        "bytes_used",
    }
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise SystemExit(
            f"input CSV is missing required columns: {', '.join(missing_columns)}"
        )

    frame = frame.copy().sort_values(["num_elements", "structure"])
    args.output_dir.mkdir(parents=True, exist_ok=True)

    insert_figure = build_metric_figure(
        frame,
        "insert_ns_per_element_avg",
        "Insert Time (ns/element)",
        "filter_bitmap: Insert Time",
        format_ns_per_element,
    )
    write_figure(insert_figure, args.output_dir / "filter_bitmap_insert_ns_per_element")

    lookup_figure = build_metric_figure(
        frame,
        "lookup_ns_per_element_avg",
        "Lookup Time (ns/element)",
        "filter_bitmap: Lookup Time",
        format_ns_per_element,
    )
    write_figure(lookup_figure, args.output_dir / "filter_bitmap_lookup_ns_per_element")

    memory_figure = build_metric_figure(
        frame,
        "bytes_used",
        "Memory Usage (bytes)",
        "filter_bitmap: Memory Usage",
        format_bytes,
    )
    write_figure(memory_figure, args.output_dir / "filter_bitmap_memory_bytes")

    summary_figure = build_summary_figure(frame)
    write_figure(summary_figure, args.output_dir / "filter_bitmap_summary")

    print(f"wrote plots to {args.output_dir}")


if __name__ == "__main__":
    main()
