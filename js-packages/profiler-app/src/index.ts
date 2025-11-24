// Entry point for the standalone profiler application

import { ProfileLoader } from './fileLoader.js';
import { setupBundleUpload } from './bundleUpload.js';
import { processBundleInBrowser } from './browserBundleProcessor.js';
import { CircuitProfile, type ProfilerConfig } from 'profiler-lib';

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

    // Check for ?open=<relative-path> query parameter
    const params = new URLSearchParams(window.location.search);
    const openPath = params.get('open');

    if (openPath) {
        // Load bundle from file system using file:/// URL
        console.log(`Loading bundle from: ${openPath}`);

        // Construct file:/// URL relative to the HTML file location
        const baseUrl = window.location.href.substring(0, window.location.href.lastIndexOf('/') + 1);
        const fileUrl = new URL(openPath, baseUrl).href;

        // Trigger bundle load programmatically
        try {
            const response = await fetch(fileUrl);
            const blob = await response.blob();
            const file = new File([blob], openPath.split('/').pop() || 'bundle.zip', { type: 'application/zip' });

            // Process the bundle and render
            const { profile, dataflow, programCode } = await processBundleInBrowser(file);
            const circuit = CircuitProfile.fromJson(profile);
            circuit.setDataflow(dataflow, programCode);
            loader.renderCircuit(circuit);

            console.log('Bundle loaded successfully from file system');
        } catch (error) {
            console.error('Failed to load bundle from file system:', error);
            if (config.errorContainer) {
                config.errorContainer.textContent = `Failed to load bundle: ${error}`;
                config.errorContainer.style.display = 'block';
            }
        }
    } else {
        // No query parameter - show empty UI with instructions
        console.log('No profile loaded. Use "Load Bundle" button or open with ?open=<path>');
        if (config.messageContainer) {
            config.messageContainer.innerHTML = `
                <h2>Welcome to Feldera Profiler</h2>
                <p>Load a profile bundle to visualize your pipeline:</p>
                <p><strong>Option 1:</strong> Click the "Load Bundle" button above</p>
                <p style=""><strong>Option 2:</strong> Open this page with <code>?open=path/to/bundle.zip</code></p>
            `;
            config.messageContainer.style.display = 'block';
        }
    }
}

// Run the application
main().catch(console.error);
