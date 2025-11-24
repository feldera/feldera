// Entry point for the standalone profiler application

import { ProfileLoader } from './fileLoader.js';
import { setupBundleUpload } from './bundleUpload.js';
import type { ProfilerConfig } from 'profiler-lib';

// Set up the configuration
const config: ProfilerConfig = {
    graphContainer: document.getElementById('visualizer')!,
    selectorContainer: document.getElementById('selector')!,
    navigatorContainer: document.getElementById('navigator-parent')!,
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

    let dataName = 'rec'; // default

    // Check if bundle was processed (look for profile-config.json)
    try {
        const response = await fetch('data/profile-config.json');
        if (response.ok) {
            const profileConfig = await response.json();
            if (profileConfig.profileName) {
                dataName = profileConfig.profileName;
                console.log(`Loading processed bundle profile: ${dataName}`);
            }
        }
    } catch (error) {
        // Config file doesn't exist, use default
        console.log('Using default profile: rec');
    }

    // Load the profile data files
    await loader.loadFiles("data", dataName);
}

// Run the application
main().catch(console.error);
