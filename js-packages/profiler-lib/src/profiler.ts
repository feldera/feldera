// Core profiler visualization library
// This module provides the main API for rendering circuit profiles

import { CircuitProfile, NodeAndMetric } from "./profile.js";
import { Cytograph, CytographRendering } from "./cytograph.js";
import { CircuitSelector } from "./selection.js";
import { MetadataSelector } from './metadataSelection.js';
import { Option, shadeOfRed } from "./util.js";

export { NodeAndMetric, shadeOfRed };

/** Represents a selectable metric option */
export interface MetricOption {
    id: string;
    label: string;
}

/** Represents a worker checkbox option */
export interface WorkerOption {
    id: string;
    label: string;
    checked: boolean;
}

/** Represents a cell in the tooltip heatmap */
export interface TooltipCell {
    value: string;
    percentile: number;
}

/** Represents a row in the tooltip heatmap */
export interface TooltipRow {
    metric: string;
    isCurrentMetric: boolean;
    cells: TooltipCell[];
}

/** Tooltip data structure */
export interface NodeAttributes {
    /** Column headers */
    columns: string[];
    /** Rows of metrics with values */
    rows: TooltipRow[];
    /** Source code information */
    sources?: string;
    /** Additional key-value attributes */
    attributes: Map<string, string>;
}

/** Callbacks for profiler to communicate UI updates */
export interface ProfilerCallbacks {
    displayNodeAttributes: (data: Option<NodeAttributes>, isSticky: boolean) => void;

    displayTopNodes: (data: Option<Array<NodeAndMetric>>, isSticky: boolean) => void;

    /** Called when the available metrics change */
    onMetricsChanged: (metrics: MetricOption[], selectedMetric: string) => void;

    /** Called when the workers state changes */
    onWorkersChanged: (workers: WorkerOption[]) => void;

    /** Called when a status message should be displayed; if the message is None, the messages are cleared */
    displayMessage: (message: Option<string>) => void;

    /** Called when an error should be displayed */
    onError: (error: string) => void;

    /** Called when a node is double-clicked. */
    onNodeDoubleClick?: (nodeId: string, type: 'group' | 'leaf') => void;
}

export interface VisualizerConfig {
    /** Container element for the graph visualization */
    graphContainer: HTMLElement;
    /** Container element for the navigator minimap */
    navigatorContainer: HTMLElement;

    /** Callbacks for UI updates */
    callbacks: ProfilerCallbacks;
}

/**
 * Main class that orchestrates the visualization of circuit profiles.
 * This is the primary API for embedding the profiler in other applications.
 */
export class Visualizer {
    private circuitSelector: CircuitSelector | null = null;
    private metadataSelector: MetadataSelector | null = null;
    private rendering: CytographRendering | null = null;
    private profile: CircuitProfile | null = null;

    constructor(private readonly config: VisualizerConfig) {
        this.config = config;
    }

    /** Display an error message */
    reportError(message: string): void {
        this.config.callbacks.onError(message);
        console.error(message);
    }

    /** Display a status message */
    message(message: string): void {
        this.config.callbacks.displayMessage(Option.some(message));
    }

    /** Clear the status message */
    clearMessage(): void {
        this.config.callbacks.displayMessage(Option.none());
    }

    /**
     * Display a circuit profile using interactive visualization.
     * This is the main entry point for displaying the profile data.
     *
     * @param profile The circuit profile to visualize
     */
    render({ profile, rootNodeId }: { profile: CircuitProfile, rootNodeId: string }): void {
        try {
            // Create selectors
            this.profile = profile;
            this.circuitSelector = new CircuitSelector(profile);
            this.metadataSelector = new MetadataSelector(profile, this.config.callbacks);

            // Initialize metadata selector (will trigger callbacks for initial state)
            this.metadataSelector.initialize();

            // Get initial selection and create graph
            const selection = this.circuitSelector.getSelection();
            const cytograph: Cytograph = Cytograph.fromProfile(profile, selection);

            // Create rendering with navigator
            this.rendering = new CytographRendering(
                this.config.graphContainer,
                this.config.navigatorContainer,
                this.config.callbacks,
                cytograph.graph,
                rootNodeId,
                selection,
                MetadataSelector.getFullSelection(),
                this.message.bind(this),
                this.clearMessage.bind(this)
            );
            this.rendering.setEvents({
                onNodeDoubleClick: (node, type) => {
                    if (type === 'group') {
                        this.circuitSelector!.toggleExpand(node)
                    }
                    this.config.callbacks.onNodeDoubleClick?.(node, type)
                }
            });

            // Wire up event handlers
            this.circuitSelector.setOnChange(() => {
                // Called when the circuit has been modified to display changes
                this.message("Recomputing profile graph")
                const graph = Cytograph.fromProfile(profile, selection);
                this.message("Computing graph changes")
                this.rendering!.updateGraph(graph);
                this.rendering!.updateMetadata(profile, this.metadataSelector!.getSelection());
            });

            this.metadataSelector.setOnChange(() => {
                this.rendering!.updateMetadata(profile, this.metadataSelector!.getSelection());
            });

            // Produce the graph visualization
            this.rendering.updateGraph(cytograph);
            this.rendering.updateMetadata(profile, this.metadataSelector.getSelection());

        } catch (e) {
            const message = e instanceof Error ? e.message : String(e);
            this.reportError(`Error displaying circuit profile: ${message}`);
        }
    }

    /**
     * Show global metrics for the top-level graph
     * @param isSticky If true, the metrics will remain visible after mouse out
     */
    public showGlobalMetrics(isSticky?: boolean): void {
        if (!this.rendering || (this.rendering.stickyInformation && !isSticky)) {
            return;
        }

        if (isSticky) {
            // Hide previous tooltip if any
            this.rendering.hideNodeInformation();
        }

        this.rendering.setStickyNodeInformation(Boolean(isSticky));

        // Display the top-level graph metrics
        const rootNode = this.rendering.getRenderedNode(this.rendering.rootNodeId);
        this.rendering.displayNodeAttributes(rootNode);
    }

    public showTopNodes(metric: string, n: number, isSticky?: boolean): void {
        if (!this.rendering || !this.profile || (this.rendering.stickyInformation && !isSticky)) {
            return;
        }

        if (isSticky) {
            // Hide previous node information if any
            this.rendering.hideNodeInformation();
        }

        this.rendering.setStickyNodeInformation(Boolean(isSticky));

        const topNodes = this.rendering.topNodes(this.profile, metric, n)
        this.config.callbacks.displayTopNodes(Option.some(topNodes), Boolean(isSticky))
    }

    /**
     * Hide the currently displayed metrics
     * @param hideSticky If true, hide metrics even if they're sticky. If false (default), only hide non-sticky metrics.
     */
    public hideNodeAttributes(hideSticky?: boolean): void {
        if (!this.rendering) {
            return;
        }

        if (hideSticky) {
            this.rendering.setStickyNodeInformation(false);
            this.rendering.hideNodeInformation();
        } else if (!this.rendering.stickyInformation) {
            // Only hide if not sticky
            this.rendering.hideNodeInformation();
        }
    }

    /**
     * Select a metric by ID
     */
    selectMetric(metric: string): void {
        this.metadataSelector?.selectMetric(metric);
    }

    /**
     * Toggle a worker's visibility
     */
    toggleWorker(workerId: string): void {
        this.metadataSelector?.toggleWorker(workerId);
    }

    /**
     * Toggle all workers on/off
     */
    toggleAllWorkers(): void {
        this.metadataSelector?.toggleAllWorkers();
    }

    /** Return the ids of the nodes that score highest according to the specified metric. */
    public topNodes(metric: string, n: number): Array<NodeAndMetric> {
        if (this.profile === null || this.rendering === null) {
            return [];
        }
        return this.rendering.topNodes(this.profile, metric, n);
    }

    /**
     * Search for a node by ID
     */
    search(query: string): void {
        this.rendering?.search(query);
    }

    /**
     * Clean up resources when the profiler is no longer needed
     */
    dispose(): void {
        // Dispose rendering resources
        if (this.rendering) {
            this.rendering.dispose();
        }

        // Clear references
        this.circuitSelector = null;
        this.metadataSelector = null;
        this.rendering = null;
    }
}
