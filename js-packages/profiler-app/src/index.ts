// Entry point for the standalone profiler application

import { ProfileLoader } from './fileLoader.js';
import { setupBundleUpload } from './bundleUpload.js';
import { type ProfilerConfig } from 'profiler-lib';

// Set up the configuration with dependency-injected UI elements
const config: ProfilerConfig = {
    graphContainer: document.getElementById('visualizer')!,
    navigatorContainer: document.getElementById('navigator-parent')!,

    // UI element references for profiler controls
    metricSelector: document.getElementById('metric-selector') as HTMLSelectElement,
    workerCheckboxesContainer: document.getElementById('worker-checkboxes')!,
    toggleWorkersButton: document.getElementById('toggle-workers-btn') as HTMLButtonElement,

    // Optional containers
    tooltipContainer: document.getElementById('tooltip-container') ?? undefined,
    errorContainer: document.getElementById('error-message') ?? undefined,
    messageContainer: document.getElementById('message') ?? undefined,
    searchInput: document.getElementById('search') as HTMLInputElement ?? undefined,
};

// Create loader - this is the single manager for profiler lifecycle
const loader = new ProfileLoader(config);

async function main() {
    // Set up bundle upload button (delegates to loader for rendering)
    setupBundleUpload(loader);

    if (config.messageContainer) {
        config.messageContainer.innerHTML = `
            <h2>Welcome to Feldera Profiler</h2>
            <p>Click the "Load Bundle" button above to select a support bundle (.zip file)<br/> that contains the pipeline profile to visualize.</p>
        `;
        config.messageContainer.style.display = 'block';
    }
}

// Run the application
main().catch(console.error);
