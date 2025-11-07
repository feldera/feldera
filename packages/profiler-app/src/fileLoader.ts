// File loading utilities for the standalone profiler app

import { CircuitProfile, type JsonProfiles, type Dataflow, Profiler, type ProfilerConfig } from 'profiler-lib';

/** Utility class for loading profile data from files */
export class ProfileLoader {
    private profiler: Profiler | null = null;

    constructor(private readonly config: ProfilerConfig) {}

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

    /** Add a stack trace to an error message if possible */
    private addTrace(message: string, e: unknown): string {
        message += e;
        if (e instanceof Error && e.stack) {
            message += "\n" + e.stack;
        }
        return message;
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
        const profileData = await this.fetchJson<JsonProfiles>(profileUrl);
        if (!profileData) {
            return; // Error already reported
        }

        // Load dataflow graph
        const dataflowData = await this.fetchJson<Dataflow>(dataflowUrl);
        if (!dataflowData) {
            return; // Error already reported
        }

        // Parse and integrate the data
        let circuit: CircuitProfile;
        try {
            circuit = CircuitProfile.fromJson(profileData);
            circuit.setDataflow(dataflowData);
        } catch (e) {
            this.reportError(this.addTrace("Error decoding JSON profile data: ", e));
            return;
        }

        // Render the profile
        try {
            // Clean up previous profiler if exists
            if (this.profiler) {
                this.profiler.dispose();
            }

            // Create new profiler and render
            this.profiler = new Profiler(this.config);
            this.profiler.render(circuit);
        } catch (e) {
            this.reportError(this.addTrace("Error displaying circuit profile: ", e));
        }
    }

    /** Clean up resources */
    dispose(): void {
        if (this.profiler) {
            this.profiler.dispose();
            this.profiler = null;
        }
    }
}
