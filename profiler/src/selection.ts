import { type SubSet, CompleteSet, ExplicitSubSet, SubList } from "./util.js";
import { CircuitProfile, type NodeId } from "./profile.js";

/** Describes which part of a CircuitProfile to display. */
export class CircuitSelection {
    constructor(
        // Metric to use for coloring nodes.
        readonly metric: string,
        // Subset of worker thread profiles to display.
        readonly workersVisible: SubList,
        // Subset of nodes to display.
        readonly nodesVisible: SubSet<NodeId>,
        // Region nodes that should be expanded
        readonly regionsExpanded: SubSet<NodeId>,
    ) { }
}

/** Contains the information for the user to slice and dice a circuit. */
export class CircuitSelector {
    private workersVisible: Array<boolean>;
    private readonly allMetrics: Set<string>;
    private readonly allNodeIds: Set<NodeId>;
    private readonly regionsExpanded: Set<NodeId>;
    private selectedMetric: string;
    // Quantile threshold for hiding nodes; a value between 0 and 100.
    // The range of the data is mapped to [0, 100], and nodes below the
    // selected quantile are hidden.
    private quantile: number = 0;
    private onChange: () => void = () => { };

    constructor(private readonly circuit: CircuitProfile) {
        this.workersVisible = Array.from({ length: circuit.getWorkerNames().length }, () => true);
        this.allMetrics = new Set<string>();
        for (const node of circuit.simpleNodes.values()) {
            for (const metric of node.measurements.getMetrics()) {
                this.allMetrics.add(metric);
            }
        }
        this.selectedMetric = this.allMetrics.values().next().value || "";
        if (circuit.simpleNodes.size < 100)
            this.regionsExpanded = new Set(circuit.complexNodes.keys());
        else
            this.regionsExpanded = new Set();

        let allKeys = new Set(this.circuit.simpleNodes.keys());
        this.allNodeIds = allKeys.union(new Set(this.circuit.complexNodes.keys()));
    }

    changed(): void {
        this.onChange();
    }

    setOnChange(onChange: () => void): void {
        this.onChange = onChange;
    }

    toggleExpand(node: NodeId) {
        if (this.regionsExpanded.has(node))
            this.regionsExpanded.delete(node);
        else
            this.regionsExpanded.add(node);
        this.changed();
    }

    // Display the tool to select circuit nodes
    display(parent: string): void {
        const el = document.getElementById(parent);
        if (!el) {
            throw new Error(`Cannot find element with id ${parent}`);
        }
        el.innerHTML = "";
        let table = document.createElement("table");
        let row = table.insertRow();
        let cell = row.insertCell(0);
        cell.appendChild(document.createTextNode("Metric"));
        cell = row.insertCell(1);
        let select = document.createElement("select");
        cell.appendChild(select);
        for (const metric of Array.from(this.allMetrics).sort()) {
            let option = document.createElement("option");
            option.value = metric;
            option.text = metric;
            if (metric === this.selectedMetric) {
                option.selected = true;
            }
            select.appendChild(option);
        }

        row = table.insertRow();
        cell = row.insertCell(0);
        let button = document.createElement("button");
        button.textContent = "Workers";
        button.title = "Toggle worker visibility";
        cell.appendChild(button);

        let allCheckboxes = new Array<HTMLInputElement>();
        cell = row.insertCell(1);
        for (let i = 0; i < this.circuit.getWorkerNames().length; i++) {
            let cb = document.createElement("input");
            cb.type = "checkbox";
            cb.checked = true;
            cb.title = i.toString();
            cb.style.margin = "0";
            cb.style.padding = "0";
            cell.appendChild(cb);
            allCheckboxes.push(cb);
            cb.onchange = (ev) => {
                const target = ev.target as HTMLInputElement;
                this.workersVisible[i] = target.checked;
                this.onChange();
            }
        };

        button.onclick = (_) => {
            for (const cb of allCheckboxes) {
                cb.checked = !cb.checked;
                this.workersVisible[Number(cb.title)] = cb.checked;
            }
            this.onChange();
        }

        el.appendChild(table);
        select.onchange = (ev) => {
            const target = ev.target as HTMLSelectElement;
            this.selectedMetric = target.value;
            this.onChange();
        };

        /*
        TODO: enable this part

        row = table.insertRow();
        cell = row.insertCell(0);
        cell.appendChild(document.createTextNode("Range"));
        cell = row.insertCell(1);
        let slider = cell.appendChild(document.createElement("input"));
        slider.type = "range";
        slider.min = "0";
        slider.max = "100";
        slider.value = "0";
        slider.step = "5";
        slider.onmouseup = (_) => {
            this.quantile = Number(slider.value);
            this.onChange();
        }
        */
    }

    getFullSelection(): CircuitSelection {
        return new CircuitSelection(
            "time",
            new SubList(_ => true),
            new CompleteSet(this.allNodeIds),
            new ExplicitSubSet(new Set(this.circuit.complexNodes.keys()), new Set()))
    }

    getSelection(): CircuitSelection {
        let workers = new SubList(i => (this.workersVisible[i] || false));
        return new CircuitSelection(
            this.selectedMetric,
            workers,
            this.circuit.nodesAboveThreshold(this.selectedMetric, workers, this.quantile),
            new ExplicitSubSet(new Set(this.circuit.complexNodes.keys()), this.regionsExpanded));
    }
}