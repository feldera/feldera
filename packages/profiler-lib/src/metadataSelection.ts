// Selector tool to decide which circuit metadata to use for visualizations

import { SubList } from "./util.js";
import { CircuitProfile } from "./profile.js";

/** Describes which metadata from a circuit to display. */
export class MetadataSelection {
    constructor(
        // Metric to use for coloring nodes.
        readonly metric: string,
        // Subset of worker thread profiles to display.
        readonly workersVisible: SubList,
    ) { }
}

/** UI elements and state for selecting the profile metadata to display. */
export class MetadataSelector {
    private workersVisible: Array<boolean>;
    private readonly allMetrics: Set<string>;
    private selectedMetric: string;
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

    /** Set the function to call when the metadata selection changes. */
    setOnChange(onChange: () => void): void {
        this.onChange = onChange;
    }

    // Display the html elements used to select circuit metadata
    display(table: HTMLTableElement): void {
        // Drop-down to select the metric to highlight
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

        // Selection tool for workers; will work for a moderate number of workers (e.g. < 50)
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

        // When the "workers" button is clicked toggle the state of all workers.
        button.onclick = (_) => {
            for (const cb of allCheckboxes) {
                cb.checked = !cb.checked;
                this.workersVisible[Number(cb.title)] = cb.checked;
            }
            this.onChange();
        }

        select.onchange = (ev) => {
            const target = ev.target as HTMLSelectElement;
            this.selectedMetric = target.value;
            this.onChange();
        };
    }

    public static getFullSelection(): MetadataSelection {
        return new MetadataSelection("time", new SubList(_ => true))
    }

    getSelection(): MetadataSelection {
        let workers = new SubList(i => (this.workersVisible[i] || false));
        return new MetadataSelection(this.selectedMetric, workers);
    }
}