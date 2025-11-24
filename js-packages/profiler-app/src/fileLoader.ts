// Profiler lifecycle management for the standalone profiler app

import { CircuitProfile, Profiler, type ProfilerConfig } from 'profiler-lib';

/** Utility class for managing profiler lifecycle and rendering */
export class ProfileLoader {
    private profiler: Profiler;

    constructor(private readonly config: ProfilerConfig) {
        this.profiler = new Profiler(config);
    }

    /** Display an error on the screen */
    private reportError(message: string): void {
        if (this.config.errorContainer) {
            this.config.errorContainer.textContent = message;
            this.config.errorContainer.style.display = 'block';
        }
        console.error(message);
    }

    /**
     * Render a circuit profile, handling profiler recreation for proper event handler setup.
     * This is the single point for rendering profiles regardless of data source.
     *
     * @param circuit The circuit profile to render
     */
    renderCircuit(circuit: CircuitProfile): void {
        try {
            this.profiler.message("Starting visualization...");

            // Dispose old profiler and create new one (tooltips and event handlers require fresh instance)
            this.profiler.dispose();
            this.profiler = new Profiler(this.config);

            this.profiler.render(circuit);
            this.profiler.clearMessage();
        } catch (e) {
            this.reportError(`Error displaying circuit profile: ${e}`);
        }
    }

    /** Clean up resources */
    dispose(): void {
        this.profiler.dispose();
    }
}
