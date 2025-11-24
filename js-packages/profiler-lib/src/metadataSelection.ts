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

    /**
     * Initialize UI elements with metadata selection controls
     * Uses dependency-injected DOM elements rather than creating them
     *
     * @param metricSelect Pre-created select element for metric dropdown
     * @param workerCheckboxesContainer Pre-created container for worker checkboxes
     * @param toggleWorkersButton Pre-created button to toggle all workers
     */
    display(
        metricSelect: HTMLSelectElement,
        workerCheckboxesContainer: HTMLElement,
        toggleWorkersButton: HTMLButtonElement
    ): void {
        // Populate metric dropdown
        metricSelect.innerHTML = ''; // Clear any existing options
        for (const metric of Array.from(this.allMetrics).sort()) {
            const option = document.createElement("option");
            option.value = metric;
            option.text = metric;
            if (metric === this.selectedMetric) {
                option.selected = true;
            }
            metricSelect.appendChild(option);
        }

        // Wire up metric selection change handler
        metricSelect.onchange = (ev) => {
            const target = ev.target as HTMLSelectElement;
            this.selectedMetric = target.value;
            this.onChange();
        };

        // Create worker checkboxes in provided container
        workerCheckboxesContainer.innerHTML = ''; // Clear any existing checkboxes
        const allCheckboxes = new Array<HTMLInputElement>();

        for (let i = 0; i < this.circuit.getWorkerNames().length; i++) {
            const cb = document.createElement("input");
            cb.type = "checkbox";
            cb.checked = true;
            cb.title = `Worker ${i}`;
            cb.dataset['workerIndex'] = i.toString();
            workerCheckboxesContainer.appendChild(cb);
            allCheckboxes.push(cb);

            cb.onchange = (ev) => {
                const target = ev.target as HTMLInputElement;
                this.workersVisible[i] = target.checked;
                this.onChange();
            };
        }

        // Wire up toggle all workers button
        toggleWorkersButton.onclick = () => {
            // Determine if we should check or uncheck (if any are unchecked, check all)
            const someUnchecked = allCheckboxes.some(cb => !cb.checked);

            for (const cb of allCheckboxes) {
                cb.checked = someUnchecked;
                const index = Number(cb.dataset['workerIndex']);
                this.workersVisible[index] = someUnchecked;
            }
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