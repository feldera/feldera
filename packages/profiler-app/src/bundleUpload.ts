// Bundle upload UI handler for browser-based bundle loading

import { processBundleInBrowser } from './browserBundleProcessor.js';
import { CircuitProfile, Profiler, type ProfilerConfig } from 'profiler-lib';

/**
 * Set up bundle upload functionality in the UI
 * Adds event listeners to the "Load Bundle" button and file input
 */
export function setupBundleUpload(config: ProfilerConfig, profiler: Profiler) {
    const uploadInput = document.getElementById('bundle-upload') as HTMLInputElement;
    const loadButton = document.getElementById('load-bundle-btn') as HTMLButtonElement;

    if (!uploadInput || !loadButton) {
        console.warn('Bundle upload elements not found');
        return;
    }

    // Click button opens file picker
    loadButton.addEventListener('click', () => {
        uploadInput.click();
    });

    // Handle file selection
    uploadInput.addEventListener('change', async (event) => {
        const file = (event.target as HTMLInputElement).files?.[0];
        if (!file) return;

        try {
            profiler.message('Processing bundle file...');
            console.log(`Processing bundle: ${file.name}`);

            // Process the bundle
            const { profile, dataflow } = await processBundleInBrowser(file);

            // Create circuit profile
            profiler.message('Creating circuit profile...');
            const circuit = CircuitProfile.fromJson(profile);
            circuit.setDataflow(dataflow);

            // Render the profile
            profiler.message('Rendering visualization...');
            profiler.dispose()
            profiler.render(circuit);
            profiler.clearMessage();

            console.log('Bundle loaded successfully');
        } catch (error) {
            const errorMsg = `Failed to process bundle: ${error}`;
            console.error(errorMsg);
            if (config.errorContainer) {
                config.errorContainer.textContent = errorMsg;
                config.errorContainer.style.display = 'block';
            }
            profiler.clearMessage();
        }

        // Clear the input so same file can be loaded again
        uploadInput.value = '';
    });
}
