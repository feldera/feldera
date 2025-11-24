// Bundle upload UI handler for browser-based bundle loading

import { processBundleInBrowser } from './browserBundleProcessor.js';
import { CircuitProfile } from 'profiler-lib';
import type { ProfileLoader } from './fileLoader.js';

/**
 * Set up bundle upload functionality in the UI
 * Adds event listeners to the "Load Bundle" button and file input
 *
 * @param loader The ProfileLoader instance that manages profiler lifecycle
 */
export function setupBundleUpload(loader: ProfileLoader) {
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
            console.log(`Processing bundle: ${file.name}`);

            // Process the bundle
            const { profile, dataflow, programCode } = await processBundleInBrowser(file);

            // Create circuit profile
            const circuit = CircuitProfile.fromJson(profile);
            circuit.setDataflow(dataflow, programCode);

            // Delegate to ProfileLoader for rendering (handles profiler lifecycle)
            loader.renderCircuit(circuit);

            console.log('Bundle loaded successfully');
        } catch (error) {
            const errorMsg = `Failed to process bundle: ${error}`;
            console.error(errorMsg);
            // ProfileLoader's error reporting will be used via renderCircuit
            throw error;
        } finally {
            // Clear the input so same file can be loaded again
            uploadInput.value = '';
        }
    });
}
