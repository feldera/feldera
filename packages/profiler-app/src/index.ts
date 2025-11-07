// Entry point for the standalone profiler application

import { ProfileLoader } from './fileLoader.js';
import type { ProfilerConfig } from 'profiler-lib';

// Set up the configuration
const config: ProfilerConfig = {
    graphContainer: document.getElementById('app')!,
    selectorContainer: document.getElementById('selector')!,
    navigatorContainer: document.getElementById('navigator-parent')!,
    tooltipContainer: document.getElementById('tooltip-container') ?? undefined,
    errorContainer: document.getElementById('error-message') ?? undefined,
};

// Create loader and load the default data files
const loader = new ProfileLoader(config);

// Temporary workaround until we integrate with the UI: load data from local files
loader.loadFiles("data", "rec");
