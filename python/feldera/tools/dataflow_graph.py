import argparse
import json
import sys
from pathlib import Path

try:
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    import networkx as nx
except ImportError:
    sys.exit("Missing dependencies. Install with:\n  pip install matplotlib networkx pygraphviz")


OP_COLOURS: dict[str, str] = {
    "source_multiset": "#4CAF50",
    "inspect": "#2196F3",
    "constant": "#9E9E9E",
    "differentiate": "#FF9800",
    "integrate": "#FF9800",
    "map": "#00BCD4",
    "map_index": "#00BCD4",
    "neg": "#F44336",
    "aggregate_linear_postprocess": "#9C27B0",
    "atomic_sum": "#673AB7",
}
DEFAULT_COLOUR = "#BDBDBD"

COLOUR_ONLY_A = "#EF9A9A"
COLOUR_ONLY_B = "#A5D6A7"
COLOUR_SHARED = "#90CAF9"


def best_layout(G: nx.DiGraph) -> dict:
    """Use graphviz dot (hierarchical) if available, else fall back to layered."""
    try:
        return nx.nx_agraph.graphviz_layout(G, prog="dot")
    except Exception:  # noqa: BLE001 S110
        pass
    try:
        layers: dict[str, int] = {}
        for node in nx.topological_sort(G):
            preds = list(G.predecessors(node))
            layers[node] = max((layers[p] for p in preds), default=-1) + 1
        by_layer: dict[int, list] = {}
        for node, layer in layers.items():
            by_layer.setdefault(layer, []).append(node)
        pos = {}
        max_width = max(len(v) for v in by_layer.values()) if by_layer else 1
        for layer, nodes in by_layer.items():
            n = len(nodes)
            for i, node in enumerate(nodes):
                x = (i - (n - 1) / 2) * (max_width / max(n, 1)) * 2.0
                y = -layer * 2.0
                pos[node] = (x, y)
        return pos
    except nx.NetworkXUnfeasible:
        return nx.spring_layout(G, seed=42)


def load_dataflow(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    if "mir" not in data:
        sys.exit(f"No 'mir' key found in {path}. Is this a valid dataflow JSON?")
    return data


def build_graph(dataflow: dict) -> nx.DiGraph:
    G = nx.DiGraph()
    mir: dict = dataflow["mir"]
    for node_id, node in mir.items():
        operation = node.get("operation")
        if operation is None:
            continue
        label_parts = [f"{node_id}", f"{operation}"]
        if node.get("table"):
            label_parts.append(f"table: {node['table']}")
        if node.get("view"):
            label_parts.append(f"view: {node['view']}")
        G.add_node(
            node_id,
            operation=operation,
            label="\n".join(label_parts),
            table=node.get("table"),
            view=node.get("view"),
            persistent_id=node.get("persistent_id", ""),
        )
        for inp in node.get("inputs", []):
            src = inp.get("node")
            if src:
                G.add_edge(src, node_id)
    return G



def draw_graph(G: nx.DiGraph, title: str, ax: plt.Axes) -> None:
    pos = best_layout(G)
    labels = nx.get_node_attributes(G, "label")
    operations = nx.get_node_attributes(G, "operation")
    node_colours = [
        OP_COLOURS.get(operations.get(n, ""), DEFAULT_COLOUR) for n in G.nodes()
    ]
    nx.draw_networkx_nodes(G, pos, ax=ax, node_color=node_colours, node_size=2500, alpha=0.95)
    nx.draw_networkx_labels(G, pos, labels=labels, ax=ax, font_size=7, font_weight="bold")
    nx.draw_networkx_edges(
        G, pos, ax=ax, arrows=True, arrowstyle="-|>", arrowsize=20,
        edge_color="#444444", width=1.6, connectionstyle="arc3,rad=0.05",
    )
    seen_ops = set(operations.values())
    patches = [
        mpatches.Patch(color=OP_COLOURS.get(op, DEFAULT_COLOUR), label=op)
        for op in sorted(seen_ops)
    ]
    ax.legend(handles=patches, loc="upper left", fontsize=7, framealpha=0.85)
    ax.set_title(title, fontsize=12, fontweight="bold", pad=16)
    ax.axis("off")



def build_diff_graph(
    dataflow_a: dict, dataflow_b: dict
) -> tuple[nx.DiGraph, dict[str, str]]:
    G = nx.DiGraph()
    status: dict[str, str] = {}

    def pid_map(dataflow: dict) -> dict[str, str]:
        return {
            nid: node.get("persistent_id") or nid
            for nid, node in dataflow["mir"].items()
            if node.get("operation") is not None
        }

    pid_a = pid_map(dataflow_a)
    pid_b = pid_map(dataflow_b)
    pids_a = set(pid_a.values())
    pids_b = set(pid_b.values())

    def add_nodes(dataflow: dict, pid_map_: dict, side: str) -> None:
        for nid, node in dataflow["mir"].items():
            op = node.get("operation")
            if op is None:
                continue
            pid = pid_map_[nid]
            if pid not in G:
                label_parts = [nid, op]
                if node.get("table"):
                    label_parts.append(f"table: {node['table']}")
                if node.get("view"):
                    label_parts.append(f"view: {node['view']}")
                G.add_node(pid, label="\n".join(label_parts), operation=op)
                status[pid] = (
                    "shared"
                    if (pid in pids_a and pid in pids_b)
                    else ("only_a" if side == "a" else "only_b")
                )
        for nid, node in dataflow["mir"].items():
            if node.get("operation") is None:
                continue
            src_pid = pid_map_[nid]
            for inp in node.get("inputs", []):
                inp_nid = inp.get("node")
                if inp_nid and inp_nid in pid_map_:
                    G.add_edge(pid_map_[inp_nid], src_pid)

    add_nodes(dataflow_a, pid_a, "a")
    add_nodes(dataflow_b, pid_b, "b")
    for pid in pids_a & pids_b:
        status[pid] = "shared"
    return G, status


def draw_diff(
    G: nx.DiGraph,
    status: dict[str, str],
    title: str,
    ax: plt.Axes,
    label_a: str = "Graph A",
    label_b: str = "Graph B",
) -> None:
    pos = best_layout(G)
    labels = nx.get_node_attributes(G, "label")
    colour_map = {"only_a": COLOUR_ONLY_A, "only_b": COLOUR_ONLY_B, "shared": COLOUR_SHARED}
    node_colours = [colour_map.get(status.get(n, "shared"), COLOUR_SHARED) for n in G.nodes()]
    nx.draw_networkx_nodes(G, pos, ax=ax, node_color=node_colours, node_size=2500, alpha=0.95)
    nx.draw_networkx_labels(G, pos, labels=labels, ax=ax, font_size=7, font_weight="bold")
    nx.draw_networkx_edges(
        G, pos, ax=ax, arrows=True, arrowstyle="-|>", arrowsize=20,
        edge_color="#444444", width=1.6, connectionstyle="arc3,rad=0.05",
    )
    patches = [
        mpatches.Patch(color=COLOUR_ONLY_A, label=f"Only in {label_a} (removed)"),
        mpatches.Patch(color=COLOUR_ONLY_B, label=f"Only in {label_b} (added)"),
        mpatches.Patch(color=COLOUR_SHARED, label="Shared (unchanged)"),
    ]
    ax.legend(handles=patches, loc="upper left", fontsize=8, framealpha=0.85)
    ax.set_title(title, fontsize=12, fontweight="bold", pad=16)
    ax.axis("off")



def print_summary(G: nx.DiGraph, label: str = "") -> None:
    prefix = f"[{label}] " if label else ""
    ops: dict[str, int] = {}
    for _, data in G.nodes(data=True):
        op = data.get("operation", "unknown")
        ops[op] = ops.get(op, 0) + 1
    print(f"{prefix}Nodes: {G.number_of_nodes()}  Edges: {G.number_of_edges()}")
    for op, count in sorted(ops.items()):
        print(f"  {op}: {count}")


def print_diff_summary(status: dict[str, str], label_a: str, label_b: str) -> None:
    only_a = [n for n, s in status.items() if s == "only_a"]
    only_b = [n for n, s in status.items() if s == "only_b"]
    shared = [n for n, s in status.items() if s == "shared"]
    print("\nDiff summary:")
    print(f"  Shared (unchanged): {len(shared)}")
    print(f"  Only in {label_a} (removed): {len(only_a)}")
    for n in only_a:
        print(f"    - {n}")
    print(f"  Only in {label_b} (added):   {len(only_b)}")
    for n in only_b:
        print(f"    + {n}")



def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Visualize or diff Feldera dataflow graphs.")
    p.add_argument("files", nargs="+", metavar="FILE",
                   help="One dataflow JSON (visualize) or two (diff with --diff)")
    p.add_argument("--diff", action="store_true",
                   help="Diff two dataflow graphs (requires exactly 2 files)")
    p.add_argument("--output", metavar="FILE",
                   help="Save figure to this file (e.g. out.png, out.svg, out.pdf)")
    p.add_argument("--title", metavar="TITLE", help="Custom figure title")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.diff:
        if len(args.files) != 2:
            sys.exit("--diff requires exactly 2 files.")
        path_a, path_b = args.files
        label_a, label_b = Path(path_a).stem, Path(path_b).stem
        df_a, df_b = load_dataflow(path_a), load_dataflow(path_b)
        print_summary(build_graph(df_a), label_a)
        print_summary(build_graph(df_b), label_b)
        G_diff, status = build_diff_graph(df_a, df_b)
        print_diff_summary(status, label_a, label_b)
        title = args.title or f"Dataflow diff: {label_a}  vs  {label_b}"
        _fig, ax = plt.subplots(figsize=(16, 10))
        draw_diff(G_diff, status, title, ax, label_a, label_b)
    else:
        if len(args.files) != 1:
            sys.exit("Provide exactly 1 file to visualize (or 2 + --diff).")
        path = args.files[0]
        df = load_dataflow(path)
        G = build_graph(df)
        print_summary(G, Path(path).stem)
        title = args.title or f"Dataflow graph: {Path(path).stem}"
        _fig, ax = plt.subplots(figsize=(14, 9))
        draw_graph(G, title, ax)

    plt.tight_layout()
    if args.output:
        plt.savefig(args.output, dpi=150, bbox_inches="tight")
        print(f"\nSaved to {args.output}")
    else:
        plt.show()


if __name__ == "__main__":
    main()
