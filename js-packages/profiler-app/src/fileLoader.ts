// Profiler lifecycle management for the standalone profiler app

import { CircuitProfile, Profiler, type ProfilerConfig } from 'profiler-lib';

/** Utility class for managing profiler lifecycle and rendering */
export class ProfileLoader {
    private profiler: Profiler;

    constructor(private readonly config: ProfilerConfig) {
        this.profiler = new Profiler(config);
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
            this.profiler.dispose();
            this.profiler = new Profiler(this.config);

            this.profiler.render(circuit);
        } catch (e) {
            const message = e instanceof Error ? e.message : String(e);
            this.config.callbacks.onError(`Error displaying circuit profile: ${message}`);
            console.error(e);
        }
    }

    /** Select a metric by ID */
    selectMetric(metricId: string): void {
        this.profiler.selectMetric(metricId);
    }

    /** Toggle a worker's visibility */
    toggleWorker(workerId: string): void {
        this.profiler.toggleWorker(workerId);
    }

    /** Toggle all workers on/off */
    toggleAllWorkers(): void {
        this.profiler.toggleAllWorkers();
    }

    /** Search for a node by ID */
    search(query: string): void {
        this.profiler.search(query);
    }

    /** Clean up resources */
    dispose(): void {
        this.profiler.dispose();
    }
}
