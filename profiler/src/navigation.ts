import { type SubSet, CompleteSet, SubList } from "./util.js";
import { CircuitProfile, type NodeId } from "./profile.js";

/** Describes which part of a CircuitProfile to display. */
export class CircuitSelection {
    constructor(
        // Metric to use for coloring nodes.
        readonly metric: string,
        // Subset of worker thread profiles to display.
        readonly workersVisible: SubList,
        // Subset of nodes to display.
        readonly nodesVisible: SubSet<NodeId>
    ) { }
}

/** Contains the information for the user to slice and dice a circuit. */
export class CircuitSelector {
    private workersVisible: Array<boolean>;
    private readonly allMetrics: Set<string>;
    private selectedMetric: string;
    private rangeValue: number = 0;
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
    }

    changed(): void {
        this.onChange();
    }

    setOnChange(onChange: () => void): void {
        this.onChange = onChange;
    }

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
        cell.appendChild(document.createTextNode("Workers"));
        cell = row.insertCell(1);
        for (let i = 0; i < this.circuit.getWorkerNames().length; i++) {
            let cb = document.createElement("input");
            cb.type = "checkbox";
            cb.checked = true;
            cb.title = i.toString();
            cb.style.margin = "0";
            cb.style.padding = "0";
            cell.appendChild(cb);
            cb.onchange = (ev) => {
                const target = ev.target as HTMLInputElement;
                this.workersVisible[i] = target.checked;
                this.onChange();
            }
        };

        el.appendChild(table);
        select.onchange = (ev) => {
            const target = ev.target as HTMLSelectElement;
            this.selectedMetric = target.value;
            this.onChange();
        };

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
            this.rangeValue = Number(slider.value);
            this.onChange();
        }
    }

    getFullSelection(): CircuitSelection {
        return new CircuitSelection(
            "time",
            new SubList(_ => true),
            new CompleteSet(this.circuit.simpleNodes.keys()))
    }

    getSelection(): CircuitSelection {
        let workers = new SubList(i => (this.workersVisible[i] || false));
        return new CircuitSelection(
            this.selectedMetric,
            workers,
            this.circuit.nodesAboveThreshold(this.selectedMetric, workers, this.rangeValue));
    }
}