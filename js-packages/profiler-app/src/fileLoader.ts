// Profiler lifecycle management for the standalone profiler app

import { CircuitProfile, Visualizer, type VisualizerConfig, type NodeAndMetric } from 'profiler-lib';

/** Utility class for managing profiler lifecycle and rendering */
export class ProfileLoader {
    private visualizer: Visualizer;

    constructor(private readonly config: VisualizerConfig) {
        this.visualizer = new Visualizer(config);
    }

    /**
     * Render a circuit profile, handling profiler recreation for proper event handler setup.
     * This is the single point for rendering profiles regardless of data source.
     *
     * @param circuit The circuit profile to render
     */
    renderCircuit(circuit: CircuitProfile): void {
        try {
            // Dispose old profiler and create new one (ensures clean state)
            this.visualizer.dispose();
            this.visualizer = new Visualizer(this.config);
            this.visualizer.render(circuit);
        } catch (e) {
            const message = e instanceof Error ? e.message : String(e);
            this.config.callbacks.onError(`Error displaying circuit profile: ${message}`);
            console.error(e);
        }
    }

    topLevelEvent(e: Event): void {
        this.visualizer.topLevelEvent(e);
    }

    /** Select a metric by ID */
    selectMetric(metricId: string): void {
        this.visualizer.selectMetric(metricId);
    }

    /** Toggle a worker's visibility */
    toggleWorker(workerId: string): void {
        this.visualizer.toggleWorker(workerId);
    }

    /** Toggle all workers on/off */
    toggleAllWorkers(): void {
        this.visualizer.toggleAllWorkers();
    }

    /** Search for a node by ID */
    search(query: string): void {
        this.visualizer.search(query);
    }

    /** Clean up resources */
    dispose(): void {
        this.visualizer.dispose();
    }

    /** Return the top nodes for a specified metric */
    topNodes(metric: string): Array<NodeAndMetric> {
        return this.visualizer.topNodes(metric);
    }
}
