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
import math
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots


KEY_TYPE_ORDER = ["u32", "u64"]
KEY_SPACE_ORDER = ["consecutive", "full_range", "half_normal"]
STRUCTURE_ORDER = ["bloom", "roaring"]
METRICS = [
    (
        "insert_ns_per_element_avg",
        "insert_ns_per_element_min",
        "insert_ns_per_element_max",
        "Insert Time",
        "Insert Time (ns/element)",
        "ns",
    ),
    (
        "lookup_ns_per_element_avg",
        "lookup_ns_per_element_min",
        "lookup_ns_per_element_max",
        "Lookup Time",
        "Lookup Time (ns/element)",
        "ns",
    ),
    ("bytes_used", None, None, "Memory Usage", "Memory Usage (bytes)", "bytes"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot filter_bitmap.csv comparisons for bloom vs roaring."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("crates/dbsp/filter_bitmap.csv"),
        help="Input CSV produced by crates/dbsp/benches/filter_bitmap.rs",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("filter_bitmap_plots"),
        help="Directory to write plots into",
    )
    parser.add_argument(
        "--write-png",
        action="store_true",
        help="Also export PNG images with Kaleido. Requires a working non-snap Chrome/Chromium.",
    )
    return parser.parse_args()


def format_structure(name: str) -> str:
    return {
        "bloom": "fastbloom",
        "roaring": "roaring",
    }.get(name, name)


def format_key_type(name: str) -> str:
    return {
        "u32": "u32 Keys",
        "u64": "u64 Keys",
    }.get(name, name)


def format_key_space(name: str, key_type: str) -> str:
    if name == "consecutive":
        return "K={0..N}"
    if name == "full_range":
        max_label = "2^32" if key_type == "u32" else "2^64"
        return f"K={{0..{max_label}}}"
    if name == "half_normal":
        return "Half-normal K={0..2^32}"
    return name


def format_distribution_key_space(name: str) -> str:
    if name == "half_normal":
        return "Half-normal K={0..2^32}"
    return name.replace("_", " ").title()


def format_num_elements(value: int) -> str:
    return f"{value:,}"


def format_key_eps(value: float) -> str:
    return f"{value:g}"


def format_bytes(value: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    unit_index = 0
    while value >= 1024.0 and unit_index + 1 < len(units):
        value /= 1024.0
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"


def format_ns_per_element(value: float) -> str:
    return f"{value:.2f} ns"


def format_ratio(value: float) -> str:
    return f"{value:.2f}x"


def metric_formatter(kind: str):
    if kind == "bytes":
        return format_bytes
    return format_ns_per_element


def ordered_values(values: pd.Series, preferred_order: list[str]) -> list[str]:
    present = {str(value) for value in values.dropna().unique()}
    ordered = [value for value in preferred_order if value in present]
    extras = sorted(present - set(preferred_order))
    return ordered + extras


def prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()

    if "key_type" not in frame.columns:
        frame["key_type"] = "u32"
    if "key_space" not in frame.columns:
        frame["key_space"] = "consecutive"
    if "key_eps" not in frame.columns:
        frame["key_eps"] = pd.NA

    numeric_columns = [
        "key_eps",
        "num_elements",
        "lookup_count",
        "false_positive_lookup_count",
        "repetitions",
        "insert_seed",
        "lookup_seed",
        "key_space_seed",
        "bloom_false_positive_rate_target_percent",
        "bloom_seed",
        "bloom_expected_items",
        "bytes_used",
        "bytes_per_element",
        "bits_per_element",
        "insert_ns_per_element_min",
        "insert_ns_per_element_avg",
        "insert_ns_per_element_max",
        "insert_ns_per_element_stddev",
        "lookup_ns_per_element_min",
        "lookup_ns_per_element_avg",
        "lookup_ns_per_element_max",
        "lookup_ns_per_element_stddev",
        "false_positive_rate_percent_min",
        "false_positive_rate_percent_avg",
        "false_positive_rate_percent_max",
        "false_positive_rate_percent_stddev",
    ]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    group_columns = ["structure", "key_type", "key_space", "key_eps", "num_elements"]
    agg_spec: dict[str, str] = {}
    for column in frame.columns:
        if column in group_columns:
            continue
        if not pd.api.types.is_numeric_dtype(frame[column]):
            agg_spec[column] = "first"
        elif column.endswith("_min"):
            agg_spec[column] = "min"
        elif column.endswith("_max"):
            agg_spec[column] = "max"
        elif column.endswith("_avg") or column.endswith("_stddev"):
            agg_spec[column] = "mean"
        elif column in {
            "bytes_used",
            "bytes_per_element",
            "bits_per_element",
            "bloom_false_positive_rate_target_percent",
        }:
            agg_spec[column] = "mean"
        else:
            agg_spec[column] = "first"

    frame = frame.groupby(group_columns, as_index=False, dropna=False).agg(agg_spec)
    return frame.sort_values(group_columns)


def build_category_order(
    frame: pd.DataFrame,
    key_spaces: list[str],
) -> list[tuple[int, str]]:
    ordered_sizes = sorted(int(value) for value in frame["num_elements"].unique())
    categories: list[tuple[int, str]] = []
    for size in ordered_sizes:
        for key_space in key_spaces:
            if ((frame["num_elements"] == size) & (frame["key_space"] == key_space)).any():
                categories.append((size, key_space))
    return categories


def category_axis(categories: list[tuple[int, str]], key_type: str) -> list[list[str]]:
    return [
        [format_num_elements(size) for size, _ in categories],
        [format_key_space(key_space, key_type) for _, key_space in categories],
    ]


def build_metric_figure(
    frame: pd.DataFrame,
    y_column: str,
    y_min_column: str | None,
    y_max_column: str | None,
    y_label: str,
    title: str,
    formatter,
) -> go.Figure:
    key_types = ordered_values(frame["key_type"], KEY_TYPE_ORDER)
    key_spaces = ordered_values(frame["key_space"], KEY_SPACE_ORDER)
    colors = {
        "bloom": "#0f766e",
        "roaring": "#c2410c",
    }

    fig = make_subplots(
        rows=max(1, len(key_types)),
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.18,
        row_titles=[format_key_type(key_type) for key_type in key_types],
    )

    for row_index, key_type in enumerate(key_types, start=1):
        row_frame = frame[frame["key_type"] == key_type]
        categories = build_category_order(row_frame, key_spaces)
        x_axis = category_axis(categories, key_type)

        for structure in STRUCTURE_ORDER:
            structure_frame = (
                row_frame[row_frame["structure"] == structure]
                .set_index(["num_elements", "key_space"])
                .sort_index()
            )
            if structure_frame.empty:
                continue

            y_values = []
            text_values = []
            error_plus = []
            error_minus = []
            for category in categories:
                if category in structure_frame.index:
                    value = float(structure_frame.loc[category, y_column])
                    y_values.append(value)
                    text_values.append(formatter(value))
                    if y_min_column is not None and y_max_column is not None:
                        min_value = float(structure_frame.loc[category, y_min_column])
                        max_value = float(structure_frame.loc[category, y_max_column])
                        error_minus.append(max(0.0, value - min_value))
                        error_plus.append(max(0.0, max_value - value))
                    else:
                        error_minus.append(None)
                        error_plus.append(None)
                else:
                    y_values.append(None)
                    text_values.append("")
                    error_minus.append(None)
                    error_plus.append(None)

            fig.add_trace(
                go.Bar(
                    name=format_structure(structure),
                    x=x_axis,
                    y=y_values,
                    text=text_values,
                    textposition="outside",
                    cliponaxis=False,
                    marker_color=colors[structure],
                    showlegend=row_index == 1,
                    offsetgroup=structure,
                    legendgroup=structure,
                    error_y=(
                        dict(
                            type="data",
                            symmetric=False,
                            array=error_plus,
                            arrayminus=error_minus,
                            thickness=1.2,
                            width=3,
                            color="#334155",
                        )
                        if y_min_column is not None and y_max_column is not None
                        else None
                    ),
                ),
                row=row_index,
                col=1,
            )

        fig.update_xaxes(title_text="Input Size / Key Space", row=row_index, col=1)
        fig.update_yaxes(title_text=y_label, type="log", row=row_index, col=1)

    fig.update_layout(
        title=title,
        barmode="group",
        template="plotly_white",
        width=max(1100, 160 * max(1, len(build_category_order(frame, key_spaces)))),
        height=500 * max(1, len(key_types)),
        legend_title_text="Structure",
        margin=dict(t=90, r=30, b=80, l=80),
    )
    return fig


def relative_frame(frame: pd.DataFrame, metric: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    pivot = (
        frame.pivot_table(
            index=["key_type", "key_space", "key_eps", "num_elements"],
            columns="structure",
            values=metric,
            aggfunc="first",
        )
        .rename(columns={"bloom": "bloom_value", "roaring": "roaring_value"})
        .reset_index()
    )
    if pivot.empty or {"bloom_value", "roaring_value"} - set(pivot.columns):
        return pd.DataFrame()

    pivot = pivot.dropna(subset=["bloom_value", "roaring_value"]).copy()
    if pivot.empty:
        return pivot

    pivot["relative_factor"] = pivot["bloom_value"] / pivot["roaring_value"]
    pivot["log2_relative_factor"] = pivot["relative_factor"].map(math.log2)
    return pivot


def heatmap_tick_values(z_bound: float) -> list[float]:
    step = 0.5 if z_bound <= 2.0 else 1.0
    tick_count = int(round((2 * z_bound) / step))
    return [(-z_bound + step * index) for index in range(tick_count + 1)]


def build_relative_heatmap_figure(
    frame: pd.DataFrame,
    metric: str,
    title: str,
    value_formatter,
    colorbar_title: str,
) -> go.Figure | None:
    ratio_frame = relative_frame(frame, metric)
    if ratio_frame.empty:
        return None

    key_types = ordered_values(ratio_frame["key_type"], KEY_TYPE_ORDER)
    key_spaces = ordered_values(ratio_frame["key_space"], KEY_SPACE_ORDER)
    max_abs_log2 = ratio_frame["log2_relative_factor"].abs().max()
    z_bound = max(0.5, math.ceil(float(max_abs_log2) * 2.0) / 2.0)
    tick_values = heatmap_tick_values(z_bound)
    tick_text = [format_ratio(2**value) for value in tick_values]

    fig = make_subplots(
        rows=max(1, len(key_types)),
        cols=max(1, len(key_spaces)),
        row_titles=[format_key_type(key_type) for key_type in key_types],
        column_titles=[format_distribution_key_space(key_space) for key_space in key_spaces],
        horizontal_spacing=0.08,
        vertical_spacing=0.16,
    )

    max_num_values = 1
    max_eps_values = 1

    for row_index, key_type in enumerate(key_types, start=1):
        for col_index, key_space in enumerate(key_spaces, start=1):
            subplot_frame = ratio_frame[
                (ratio_frame["key_type"] == key_type)
                & (ratio_frame["key_space"] == key_space)
            ]
            if subplot_frame.empty:
                continue

            eps_values = sorted(float(value) for value in subplot_frame["key_eps"].dropna().unique())
            num_values = sorted(int(value) for value in subplot_frame["num_elements"].unique())
            max_num_values = max(max_num_values, len(num_values))
            max_eps_values = max(max_eps_values, len(eps_values))

            log2_table = (
                subplot_frame.pivot(index="key_eps", columns="num_elements", values="log2_relative_factor")
                .reindex(index=eps_values, columns=num_values)
            )
            ratio_table = (
                subplot_frame.pivot(index="key_eps", columns="num_elements", values="relative_factor")
                .reindex(index=eps_values, columns=num_values)
            )
            bloom_table = (
                subplot_frame.pivot(index="key_eps", columns="num_elements", values="bloom_value")
                .reindex(index=eps_values, columns=num_values)
            )
            roaring_table = (
                subplot_frame.pivot(index="key_eps", columns="num_elements", values="roaring_value")
                .reindex(index=eps_values, columns=num_values)
            )

            text = [
                [
                    format_ratio(value) if pd.notna(value) else ""
                    for value in row_values
                ]
                for row_values in ratio_table.values
            ]
            customdata = [
                [
                    [
                        value_formatter(bloom_value) if pd.notna(bloom_value) else "",
                        value_formatter(roaring_value) if pd.notna(roaring_value) else "",
                    ]
                    for bloom_value, roaring_value in zip(bloom_row, roaring_row)
                ]
                for bloom_row, roaring_row in zip(bloom_table.values, roaring_table.values)
            ]

            fig.add_trace(
                go.Heatmap(
                    x=[format_num_elements(value) for value in num_values],
                    y=[format_key_eps(value) for value in eps_values],
                    z=log2_table.values,
                    text=text,
                    customdata=customdata,
                    texttemplate="%{text}",
                    hoverongaps=False,
                    xgap=1,
                    ygap=1,
                    coloraxis="coloraxis",
                    hovertemplate=(
                        "num_elements=%{x}<br>"
                        "key_eps=%{y}<br>"
                        f"{colorbar_title}=%{{text}}<br>"
                        "fastbloom=%{customdata[0]}<br>"
                        "roaring=%{customdata[1]}"
                        "<extra></extra>"
                    ),
                ),
                row=row_index,
                col=col_index,
            )

            fig.update_xaxes(title_text="num_elements", row=row_index, col=col_index)
            fig.update_yaxes(title_text="key_eps", row=row_index, col=col_index)

    fig.update_layout(
        title=title,
        template="plotly_white",
        width=max(950, 280 * len(key_spaces) + 110 * max_num_values * len(key_spaces)),
        height=max(480, 220 * len(key_types) + 70 * max_eps_values * len(key_types)),
        margin=dict(t=110, r=40, b=80, l=90),
        coloraxis=dict(
            colorscale=[
                (0.0, "#b91c1c"),
                (0.5, "#f8fafc"),
                (1.0, "#15803d"),
            ],
            cmin=-z_bound,
            cmax=z_bound,
            colorbar=dict(
                title=colorbar_title,
                tickvals=tick_values,
                ticktext=tick_text,
            ),
        ),
    )
    return fig


def build_summary_figure(frame: pd.DataFrame) -> go.Figure:
    key_types = ordered_values(frame["key_type"], KEY_TYPE_ORDER)
    key_spaces = ordered_values(frame["key_space"], KEY_SPACE_ORDER)
    colors = {
        "bloom": "#0f766e",
        "roaring": "#c2410c",
    }

    fig = make_subplots(
        rows=max(1, len(key_types)),
        cols=3,
        subplot_titles=[
            metric_title
            for _ in key_types
            for _, _, _, metric_title, _, _ in METRICS
        ],
        row_titles=[format_key_type(key_type) for key_type in key_types],
        horizontal_spacing=0.06,
        vertical_spacing=0.18,
    )

    for row_index, key_type in enumerate(key_types, start=1):
        row_frame = frame[frame["key_type"] == key_type]
        categories = build_category_order(row_frame, key_spaces)
        x_axis = category_axis(categories, key_type)

        for col_index, (
            metric,
            metric_min,
            metric_max,
            _metric_title,
            y_label,
            kind,
        ) in enumerate(METRICS, start=1):
            formatter = metric_formatter(kind)
            for structure in STRUCTURE_ORDER:
                structure_frame = (
                    row_frame[row_frame["structure"] == structure]
                    .set_index(["num_elements", "key_space"])
                    .sort_index()
                )
                if structure_frame.empty:
                    continue

                y_values = []
                text_values = []
                error_plus = []
                error_minus = []
                for category in categories:
                    if category in structure_frame.index:
                        value = float(structure_frame.loc[category, metric])
                        y_values.append(value)
                        text_values.append(formatter(value))
                        if metric_min is not None and metric_max is not None:
                            min_value = float(structure_frame.loc[category, metric_min])
                            max_value = float(structure_frame.loc[category, metric_max])
                            error_minus.append(max(0.0, value - min_value))
                            error_plus.append(max(0.0, max_value - value))
                        else:
                            error_minus.append(None)
                            error_plus.append(None)
                    else:
                        y_values.append(None)
                        text_values.append("")
                        error_minus.append(None)
                        error_plus.append(None)

                fig.add_trace(
                    go.Bar(
                        name=format_structure(structure),
                        x=x_axis,
                        y=y_values,
                        text=text_values,
                        textposition="outside",
                        cliponaxis=False,
                        marker_color=colors[structure],
                        showlegend=row_index == 1 and col_index == 1,
                        offsetgroup=structure,
                        legendgroup=structure,
                        error_y=(
                            dict(
                                type="data",
                                symmetric=False,
                                array=error_plus,
                                arrayminus=error_minus,
                                thickness=1.2,
                                width=3,
                                color="#334155",
                            )
                            if metric_min is not None and metric_max is not None
                            else None
                        ),
                    ),
                    row=row_index,
                    col=col_index,
                )

            fig.update_yaxes(title_text=y_label, type="log", row=row_index, col=col_index)
            fig.update_xaxes(
                title_text="Input Size / Key Space",
                row=row_index,
                col=col_index,
            )

    fig.update_layout(
        title="filter_bitmap Summary",
        barmode="group",
        template="plotly_white",
        width=max(1900, 260 * max(1, len(build_category_order(frame, key_spaces)))),
        height=640 * max(1, len(key_types)),
        legend_title_text="Structure",
        margin=dict(t=110, r=30, b=90, l=70),
    )
    return fig


def write_figure(fig: go.Figure, base_path: Path, write_png: bool) -> None:
    fig.write_html(base_path.with_suffix(".html"))
    if write_png:
        try:
            fig.write_image(base_path.with_suffix(".png"), scale=2)
        except Exception as exc:  # pragma: no cover - depends on local browser setup.
            print(f"warning: failed to write {base_path.with_suffix('.png')}: {exc}")


def write_summary_dashboard(
    sections: list[tuple[str, go.Figure]],
    output_path: Path,
) -> None:
    if not sections:
        return

    grouped_summary = next(
        ((title, figure) for title, figure in sections if title == "Grouped Summary"),
        None,
    )
    heatmap_sections = [
        (title, figure) for title, figure in sections if title != "Grouped Summary"
    ]

    html_parts = [
        "<!DOCTYPE html>",
        "<html lang='en'>",
        "<head>",
        "  <meta charset='utf-8'>",
        "  <meta name='viewport' content='width=device-width, initial-scale=1'>",
        "  <title>filter_bitmap Summary</title>",
        "  <style>",
        "    body { font-family: system-ui, sans-serif; margin: 0; background: #f8fafc; color: #0f172a; }",
        "    main { max-width: 2400px; margin: 0 auto; padding: 24px; }",
        "    h1 { margin: 0 0 24px; font-size: 28px; }",
        "    section { background: white; border: 1px solid #e2e8f0; border-radius: 12px; padding: 20px; margin-bottom: 24px; box-shadow: 0 8px 30px rgba(15, 23, 42, 0.05); }",
        "    h2 { margin: 0 0 16px; font-size: 20px; }",
        "    h3 { margin: 0 0 12px; font-size: 16px; }",
        "    .heatmap-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 20px; align-items: start; }",
        "    .plot-card { min-width: 0; }",
        "    .plot-card .js-plotly-plot, .plot-card .plotly-graph-div { width: 100% !important; }",
        "    @media (max-width: 1800px) { .heatmap-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }",
        "    @media (max-width: 1180px) { .heatmap-grid { grid-template-columns: 1fr; } }",
        "  </style>",
        "</head>",
        "<body>",
        "<main>",
        "  <h1>filter_bitmap Summary</h1>",
    ]

    next_plotlyjs_mode = "cdn"

    if grouped_summary is not None:
        title, figure = grouped_summary
        figure_html = pio.to_html(
            figure,
            full_html=False,
            include_plotlyjs=next_plotlyjs_mode,
        )
        next_plotlyjs_mode = False
        html_parts.extend(
            [
                "  <section>",
                f"    <h2>{title}</h2>",
                figure_html,
                "  </section>",
            ]
        )

    if heatmap_sections:
        html_parts.extend(
            [
                "  <section>",
                "    <h2>Roaring Advantage Heatmaps</h2>",
                "    <div class='heatmap-grid'>",
            ]
        )
        for title, figure in heatmap_sections:
            figure_html = pio.to_html(
                figure,
                full_html=False,
                include_plotlyjs=next_plotlyjs_mode,
            )
            next_plotlyjs_mode = False
            html_parts.extend(
                [
                    "      <article class='plot-card'>",
                    f"        <h3>{title}</h3>",
                    figure_html,
                    "      </article>",
                ]
            )
        html_parts.extend(
            [
                "    </div>",
                "  </section>",
            ]
        )

    html_parts.extend(["</main>", "</body>", "</html>"])
    output_path.write_text("\n".join(html_parts), encoding="utf-8")


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

    frame = prepare_frame(frame)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    standard_frame = frame[frame["key_eps"].isna()].copy()
    distribution_frame = frame[frame["key_eps"].notna()].copy()
    summary_sections: list[tuple[str, go.Figure]] = []

    if not standard_frame.empty:
        insert_figure = build_metric_figure(
            standard_frame,
            "insert_ns_per_element_avg",
            "insert_ns_per_element_min",
            "insert_ns_per_element_max",
            "Insert Time (ns/element)",
            "filter_bitmap: Insert Time",
            format_ns_per_element,
        )
        write_figure(
            insert_figure,
            args.output_dir / "filter_bitmap_insert_ns_per_element",
            args.write_png,
        )

        lookup_figure = build_metric_figure(
            standard_frame,
            "lookup_ns_per_element_avg",
            "lookup_ns_per_element_min",
            "lookup_ns_per_element_max",
            "Lookup Time (ns/element)",
            "filter_bitmap: Lookup Time",
            format_ns_per_element,
        )
        write_figure(
            lookup_figure,
            args.output_dir / "filter_bitmap_lookup_ns_per_element",
            args.write_png,
        )

        memory_figure = build_metric_figure(
            standard_frame,
            "bytes_used",
            None,
            None,
            "Memory Usage (bytes)",
            "filter_bitmap: Memory Usage",
            format_bytes,
        )
        write_figure(
            memory_figure,
            args.output_dir / "filter_bitmap_memory_bytes",
            args.write_png,
        )

        summary_figure = build_summary_figure(standard_frame)
        write_figure(
            summary_figure,
            args.output_dir / "filter_bitmap_summary",
            args.write_png,
        )
        summary_sections.append(("Grouped Summary", summary_figure))

    if not distribution_frame.empty:
        insert_heatmap = build_relative_heatmap_figure(
            distribution_frame,
            "insert_ns_per_element_avg",
            "filter_bitmap: Roaring Insert Advantage Heatmap",
            format_ns_per_element,
            "fastbloom / roaring",
        )
        if insert_heatmap is not None:
            write_figure(
                insert_heatmap,
                args.output_dir / "filter_bitmap_insert_advantage_heatmap",
                args.write_png,
            )
            summary_sections.append(("Roaring Insert Advantage Heatmap", insert_heatmap))

        lookup_heatmap = build_relative_heatmap_figure(
            distribution_frame,
            "lookup_ns_per_element_avg",
            "filter_bitmap: Roaring Lookup Advantage Heatmap",
            format_ns_per_element,
            "fastbloom / roaring",
        )
        if lookup_heatmap is not None:
            write_figure(
                lookup_heatmap,
                args.output_dir / "filter_bitmap_lookup_advantage_heatmap",
                args.write_png,
            )
            summary_sections.append(("Roaring Lookup Advantage Heatmap", lookup_heatmap))

        memory_heatmap = build_relative_heatmap_figure(
            distribution_frame,
            "bytes_used",
            "filter_bitmap: Roaring Memory Advantage Heatmap",
            format_bytes,
            "fastbloom / roaring",
        )
        if memory_heatmap is not None:
            write_figure(
                memory_heatmap,
                args.output_dir / "filter_bitmap_memory_advantage_heatmap",
                args.write_png,
            )
            summary_sections.append(("Roaring Memory Advantage Heatmap", memory_heatmap))

    write_summary_dashboard(
        summary_sections,
        args.output_dir / "filter_bitmap_summary.html",
    )

    print(f"wrote plots to {args.output_dir}")


if __name__ == "__main__":
    main()
