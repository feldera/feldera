// Entry point for the standalone profiler application

import { ProfileLoader } from './fileLoader.js';
import { setupBundleUpload } from './bundleUpload.js';
import {
    type Option,
    type VisualizerConfig,
    type ProfilerCallbacks,
    type TooltipData,
    type MetricOption,
    type WorkerOption,
    shadeOfRed
} from 'profiler-lib';

// UI element references
const graphMetrics = document.getElementById('toplevel-metrics')!;
const metricButton = document.getElementById('top-nodes');
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
        const thead = table.createTHead();
        const headerRow = thead.insertRow();
        headerRow.insertCell(); // Empty corner cell
        for (const column of data.columns) {
            const th = headerRow.insertCell();
            th.textContent = column;
        }

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
                const color = shadeOfRed(percent);
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

    onMetricsChanged: (metrics: MetricOption[], selectedMetric: string) => {
        metricSelector.innerHTML = '';
        for (const metric of metrics) {
            const option = document.createElement('option');
            option.value = metric.id;
            option.textContent = metric.label;
            if (metric.id === selectedMetric) {
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
const config: VisualizerConfig = {
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

    graphMetrics.addEventListener('mouseover', (e) => loader.topLevelEvent(e));
    graphMetrics.addEventListener('click', (e) => loader.topLevelEvent(e));

    searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            loader.search(searchInput.value);
        }
    });

    metricButton?.addEventListener('click', () => {
        const metric = metricSelector.value;
        let nodes = loader.topNodes(metric);
        if (nodes.length === 0) {
            messageContainer.innerHTML = "No graph nodes found";
            messageContainer.style.display = "block";
            return;
        }
        const table = document.createElement("table");

        const title = table.createTHead();
        let row = title.insertRow();
        const cell = row.insertCell();
        cell.colSpan = 2;
        cell.innerText = "Nodes with high values for " + metric;

        row = title.insertRow();
        row.insertCell().innerHTML = "Node";
        row.insertCell().innerHTML = "Value";
        const tBody = table.createTBody();
        for (const m of nodes) {
            const row = tBody.insertRow();
            row.style.cursor = "pointer";
            row.title = "Click to locate";
            row.onclick = () => {
                // Remove table
                tooltipContainer.innerHTML = '';
                loader.search(m.nodeId);
            }
            row.insertCell().innerText = m.nodeId;
            const metric = row.insertCell();
            metric.innerText = m.label;
            metric.style.backgroundColor = shadeOfRed(m.normalizedValue);
            metric.style.color = 'black';
            metric.style.textAlign = 'right';
        }

        // Wrap table in a div and display
        const wrapper = document.createElement('div');
        wrapper.appendChild(table);
        tooltipContainer.innerHTML = '';
        tooltipContainer.appendChild(wrapper);
        tooltipContainer.style.position = 'fixed';
        tooltipContainer.style.top = '70px';
        tooltipContainer.style.right = '10px';
    });

    // Show welcome message
    messageContainer.innerHTML = `
        <h2>Welcome to Feldera's Profile Visualizer</h2>
        <p>Click the "Load Bundle" button above to select a support bundle (.zip file)<br/> that contains the pipeline profile to visualize.</p>
    `;
    messageContainer.style.display = 'block';
}

// Run the application
main().catch(console.error);
