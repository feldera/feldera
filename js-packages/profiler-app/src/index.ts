// Entry point for the standalone profiler application

import { ProfileLoader } from './fileLoader.js';
import { setupBundleUpload } from './bundleUpload.js';
import {
    type ProfilerConfig,
    type ProfilerCallbacks,
    type TooltipData,
    type Option,
    type MetricOption,
    type WorkerOption,
} from 'profiler-lib';

// UI element references
const metricSelector = document.getElementById('metric-selector') as HTMLSelectElement;
const workerCheckboxesContainer = document.getElementById('worker-checkboxes')!;
const toggleWorkersButton = document.getElementById('toggle-workers-btn') as HTMLButtonElement;
const searchInput = document.getElementById('search') as HTMLInputElement;
const tooltipContainer = document.getElementById('tooltip-container')!;
const errorContainer = document.getElementById('error-message')!;
const messageContainer = document.getElementById('message')!;

// Profiler callbacks for UI updates
const callbacks: ProfilerCallbacks = {
    displayNodeAttributes: (odata: Option<TooltipData>, visible: boolean) => {
        if (!visible || odata.isNone()) {
            tooltipContainer.innerHTML = '';
            tooltipContainer.style.position = '';
            return;
        }

        // Build tooltip HTML from structured data
        const table = document.createElement('table');
        let data = odata.unwrap();

        // Header row
        const thead = document.createElement('thead');
        const headerRow = thead.insertRow();
        headerRow.insertCell(); // Empty corner cell
        for (const column of data.columns) {
            const th = document.createElement('th');
            th.textContent = column;
            headerRow.appendChild(th);
        }
        table.appendChild(thead);

        // Metric rows
        const tbody = document.createElement('tbody');
        for (const row of data.rows) {
            const tr = tbody.insertRow();
            const metricCell = tr.insertCell();
            metricCell.textContent = row.metric;
            if (row.isCurrentMetric) {
                metricCell.style.backgroundColor = 'blue';
            }

            for (const cell of row.cells) {
                const td = tr.insertCell();
                const percent = cell.percentile;
                const color = `rgb(255, ${Math.round((255 * (100 - percent)) / 100)}, ${Math.round((255 * (100 - percent)) / 100)})`;
                td.style.backgroundColor = color;
                td.style.color = 'black';
                td.style.textAlign = 'right';
                td.textContent = cell.value;
            }
        }

        // Sources row
        if (data.sources) {
            const tr = tbody.insertRow();
            const td = tr.insertCell();
            td.textContent = 'sources';
            const sourcesCell = tr.insertCell();
            sourcesCell.colSpan = data.columns.length;
            sourcesCell.style.fontFamily = 'monospace';
            sourcesCell.style.whiteSpace = 'pre-wrap';
            sourcesCell.style.textAlign = 'left';
            sourcesCell.style.minWidth = '80ch';
            sourcesCell.textContent = data.sources;
        }
        table.appendChild(tbody);

        // Additional attributes
        if (data.attributes.size > 0) {
            for (const [key, value] of data.attributes.entries()) {
                const tr = table.insertRow();
                const keyCell = tr.insertCell();
                keyCell.textContent = key;
                keyCell.style.whiteSpace = 'nowrap';
                const valueCell = tr.insertCell();
                valueCell.colSpan = data.columns.length;
                valueCell.style.whiteSpace = 'nowrap';
                valueCell.textContent = value;
            }
        }

        // Wrap table in a div and display
        const wrapper = document.createElement('div');
        wrapper.appendChild(table);
        tooltipContainer.innerHTML = '';
        tooltipContainer.appendChild(wrapper);
        tooltipContainer.style.position = 'fixed';
        tooltipContainer.style.top = '70px';
        tooltipContainer.style.right = '10px';
    },

    onMetricsChanged: (metrics: MetricOption[], selectedMetricId: string) => {
        metricSelector.innerHTML = '';
        for (const metric of metrics) {
            const option = document.createElement('option');
            option.value = metric.id;
            option.textContent = metric.label;
            if (metric.id === selectedMetricId) {
                option.selected = true;
            }
            metricSelector.appendChild(option);
        }
    },

    onWorkersChanged: (workers: WorkerOption[]) => {
        workerCheckboxesContainer.innerHTML = '';
        for (const worker of workers) {
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = worker.checked;
            checkbox.title = worker.label;
            checkbox.dataset['workerId'] = worker.id;
            workerCheckboxesContainer.appendChild(checkbox);
        }
    },

    displayMessage: (msg: Option<string>) => {
        if (msg.isNone()) {
            messageContainer.textContent = "";
            messageContainer.style.display = 'none';
        } else {
            console.log(msg.unwrap());
            messageContainer.textContent = msg.unwrap();
            messageContainer.style.display = 'block';
        }
    },

    onError: (err: string) => {
        console.error(err);
        errorContainer.textContent = err;
        errorContainer.style.display = 'block';
    }
};

// Set up the configuration with callbacks
const config: ProfilerConfig = {
    graphContainer: document.getElementById('visualizer')!,
    navigatorContainer: document.getElementById('navigator-parent')!,
    callbacks
};

// Create loader - this is the single manager for profiler lifecycle
const loader = new ProfileLoader(config);

async function main() {
    // Set up bundle upload button (delegates to loader for rendering)
    setupBundleUpload(loader);

    // Wire up UI event handlers
    metricSelector.addEventListener('change', (e) => {
        loader.selectMetric((e.target as HTMLSelectElement).value);
    });

    toggleWorkersButton.addEventListener('click', () => {
        loader.toggleAllWorkers();
    });

    workerCheckboxesContainer.addEventListener('change', (e) => {
        const target = e.target as HTMLInputElement;
        if (target.type === 'checkbox' && target.dataset['workerId']) {
            loader.toggleWorker(target.dataset['workerId']);
        }
    });

    searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            loader.search(searchInput.value);
        }
    });

    // Show welcome message
    messageContainer.innerHTML = `
        <h2>Welcome to Feldera Profiler</h2>
        <p>Click the "Load Bundle" button above to select a support bundle (.zip file)<br/> that contains the pipeline profile to visualize.</p>
    `;
    messageContainer.style.display = 'block';
}

// Run the application
main().catch(console.error);
