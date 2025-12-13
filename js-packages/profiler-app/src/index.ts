// Entry point for the standalone profiler application

import { ProfileLoader } from './fileLoader.js';
import { setupBundleUpload } from './bundleUpload.js';
import {
    measurementCategory,
    HierarchicalTable,
    HierarchicalTableRow,
    HierarchicalTableCellValue,
    type Option,
    type VisualizerConfig,
    type ProfilerCallbacks,
    type NodeAttributes,
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
    displayNodeAttributes: (odata: Option<NodeAttributes>, isSticky) => {
        if (odata.isNone()) {
            tooltipContainer.innerHTML = '';
            tooltipContainer.style.position = '';
            return;
        }

        let data = odata.unwrap();
        // Build tooltip HTML from structured data
        const table = HierarchicalTable.create(data.columns);

        // Metric rows
        for (const row of data.rows) {
            let values: Array<HierarchicalTableCellValue> = [];
            for (const cell of row.cells) {
                let cv = new HierarchicalTableCellValue(cell.value, cell.value, cell.percentile, {});
                values.push(cv);
            }
            let r = new HierarchicalTableRow(row.metric, values, row.isCurrentMetric);
            let section = measurementCategory(row.metric);
            table.addRow(section, r);
        }

        // Sources row
        if (data.sources) {
            let cell = new HierarchicalTableCellValue(data.sources, "", 0, {
                fontFamily: 'monospace',
                whiteSpace: 'pre-wrap',
                textAlign: 'left',
                minWidth: '80ch'
            });
            let r = new HierarchicalTableRow("sources", [cell], false);
            table.addRow("", r);
        }

        // Additional attributes
        if (data.attributes.size > 0) {
            for (const [key, value] of data.attributes.entries()) {
                let cell = new HierarchicalTableCellValue(value, "", 0, {
                    whiteSpace: 'nowrap',
                    textAlign: 'left'
                });
                let r = new HierarchicalTableRow(key, [cell], false);
                table.addRow("", r);
            }
        }

        // Wrap table in a div and display
        const wrapper = document.createElement('div');
        wrapper.appendChild(table.asHtml());
        tooltipContainer.innerHTML = '';
        tooltipContainer.appendChild(wrapper);
        tooltipContainer.style.position = 'fixed';
        tooltipContainer.style.top = '70px';
        tooltipContainer.style.right = '10px';
        tooltipContainer.style.pointerEvents = isSticky ? 'auto' : 'none';
    },

    displayTopNodes: (odata, isSticky) => {
        if (odata.isNone()) {
            tooltipContainer.innerHTML = '';
            tooltipContainer.style.position = '';
            return;
        }

        const nodes = odata.unwrap()
        const metric = metricSelector.value

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
                loader.hideNodeAttributes(true)
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
        tooltipContainer.style.pointerEvents = isSticky ? 'auto' : 'none';
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

    graphMetrics.addEventListener('mouseover', () => loader.showGlobalMetrics(false));
    graphMetrics.addEventListener('mouseout', () => loader.hideNodeAttributes());
    graphMetrics.addEventListener('click', () => loader.showGlobalMetrics(true));

    searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            loader.search(searchInput.value);
        }
    });

    metricButton?.addEventListener('mouseover', () => loader.showTopNodes(metricSelector.value, 20, false));
    metricButton?.addEventListener('mouseout', () => loader.hideNodeAttributes());
    metricButton?.addEventListener('click', () => loader.showTopNodes(metricSelector.value, 20, true) );

    // Show welcome message
    messageContainer.innerHTML = `
        <h2>Welcome to Feldera's Profile Visualizer</h2>
        <p>Click the "Load Bundle" button above to select a support bundle (.zip file)<br/> that contains the pipeline profile to visualize.</p>
    `;
    messageContainer.style.display = 'block';
}

// Run the application
main().catch(console.error);
