// File loading utilities for the standalone profiler app

import { CircuitProfile, type JsonProfiles, type Dataflow, Profiler, type ProfilerConfig } from 'profiler-lib';

/** Utility class for loading profile data from files and managing profiler lifecycle */
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

    /** Fetch a JSON file from the server */
    private async fetchJson<T = unknown>(url: string): Promise<T | null> {
        try {
            const response = await fetch(url);
            if (!response.ok) {
                this.reportError(`HTTP error: ${response.status}`);
                return null;
            }

            const contentType = response.headers.get('content-type') || '';
            if (!contentType.includes('application/json')) {
                this.reportError('Unexpected content type: ' + contentType);
                return null;
            }

            const text = await response.text();
            if (!text) {
                this.reportError('Empty response body');
                return null;
            }

            return JSON.parse(text) as T;
        } catch (err) {
            this.reportError('Fetch or JSON parse error: ' + err);
            return null;
        }
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

    /**
     * Load profile and dataflow files, then render the visualization.
     *
     * @param directory Directory containing the JSON files
     * @param basename Base name for the files (without .json extension)
     */
    async loadFiles(directory: string, basename: string): Promise<void> {
        const profileUrl = `${directory}/${basename}.json`;
        const dataflowUrl = `${directory}/dataflow-${basename}.json`;

        // Load profile data
        this.profiler.message("Reading profile file...");
        const profileData = await this.fetchJson<JsonProfiles>(profileUrl);
        if (!profileData) {
            return; // Error already reported
        }

        // Load dataflow graph
        this.profiler.message("Reading dataflow graph...");
        const dataflowData = await this.fetchJson<Dataflow>(dataflowUrl);
        if (!dataflowData) {
            return; // Error already reported
        }

        // Parse and integrate the data
        let circuit: CircuitProfile;
        try {
            this.profiler.message("Extracting profiling information...");
            circuit = CircuitProfile.fromJson(profileData);
            circuit.setDataflow(dataflowData);
        } catch (e) {
            this.reportError(`Error decoding JSON profile data: ${e}`);
            return;
        }

        // Render the profile
        this.renderCircuit(circuit);
    }

    /** Clean up resources */
    dispose(): void {
        this.profiler.dispose();
    }
}
