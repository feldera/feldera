// Selector tool to decide which circuit metadata to use for visualizations

import { SubList } from "./util.js";
import { CircuitProfile } from "./profile.js";
import type { ProfilerCallbacks, MetricOption, WorkerOption } from "./profiler.js";

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
    private readonly callbacks: ProfilerCallbacks;

    constructor(private readonly circuit: CircuitProfile, callbacks: ProfilerCallbacks) {
        this.callbacks = callbacks;
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
     * Initialize the metadata selector and trigger callbacks for initial state
     */
    initialize(): void {
        // Notify about available metrics
        const metrics: MetricOption[] = Array.from(this.allMetrics).sort().map(metric => ({
            id: metric,
            label: metric
        }));
        this.callbacks.onMetricsChanged(metrics, this.selectedMetric);

        // Notify about available workers
        this.notifyWorkersChanged();
    }

    /**
     * Notify callbacks about current worker state
     */
    private notifyWorkersChanged(): void {
        const workers: WorkerOption[] = this.circuit.getWorkerNames().map((_name, index) => ({
            id: String(index),
            label: `Worker ${index}`,
            checked: this.workersVisible[index] ?? true
        }));
        this.callbacks.onWorkersChanged(workers);
    }

    /**
     * Select a metric by ID
     */
    selectMetric(metricId: string): void {
        if (this.allMetrics.has(metricId)) {
            this.selectedMetric = metricId;
            this.onChange();
        }
    }

    /**
     * Toggle a worker's visibility
     */
    toggleWorker(workerId: string): void {
        const index = Number(workerId);
        if (index >= 0 && index < this.workersVisible.length) {
            this.workersVisible[index] = !this.workersVisible[index];
            this.notifyWorkersChanged();
            this.onChange();
        }
    }

    /**
     * Toggle the display state of all workers.
     */
    toggleAllWorkers(): void {
        for (let i = 0; i < this.workersVisible.length; i++) {
            this.workersVisible[i] = !this.workersVisible[i];
        }
        this.notifyWorkersChanged();
        this.onChange();
    }

    public static getFullSelection(): MetadataSelection {
        return new MetadataSelection("time", new SubList(_ => true))
    }

    getSelection(): MetadataSelection {
        let workers = new SubList(i => (this.workersVisible[i] || false));
        return new MetadataSelection(this.selectedMetric, workers);
    }
}