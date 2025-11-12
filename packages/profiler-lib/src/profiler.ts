// Core profiler visualization library
// This module provides the main API for rendering circuit profiles

import { CircuitProfile } from "./profile.js";
import { Cytograph, CytographRendering } from "./cytograph.js";
import { CircuitSelector } from "./selection.js";
import { MetadataSelector } from './metadataSelection.js';

export interface ProfilerConfig {
    /** Container element for the graph visualization */
    graphContainer: HTMLElement;
    /** Container element for the selector UI controls */
    selectorContainer: HTMLElement;
    /** Container element for the navigator minimap */
    navigatorContainer: HTMLElement;
    /** Optional container element for the tooltip (defaults to document.body if not provided) */
    tooltipContainer?: HTMLElement | undefined;
    /** Optional error message display element */
    errorContainer?: HTMLElement | undefined;
    /** Optional message display element for status messages */
    messageContainer?: HTMLElement | undefined;
    /** Optional search input element for node search */
    searchInput?: HTMLInputElement | undefined;
}

/**
 * Main profiler class that orchestrates the visualization of circuit profiles.
 * This is the primary API for embedding the profiler in other applications.
 */
export class Profiler {
    private readonly tooltip: HTMLElement;
    private readonly config: ProfilerConfig;
    private circuitSelector: CircuitSelector | null = null;
    private metadataSelector: MetadataSelector | null = null;
    private rendering: CytographRendering | null = null;

    constructor(config: ProfilerConfig) {
        this.config = config;

        // Create tooltip element
        this.tooltip = document.createElement('div');
        this.tooltip.style.position = 'absolute';
        this.tooltip.style.padding = '6px 10px';
        this.tooltip.style.background = 'rgba(0, 0, 0, 0.75)';
        this.tooltip.style.color = 'white';
        this.tooltip.style.borderRadius = '4px';
        this.tooltip.style.fontSize = '14px';
        this.tooltip.style.pointerEvents = 'none';
        this.tooltip.style.zIndex = '999';
        this.tooltip.style.display = 'none';

        // Append tooltip to specified container or document.body
        const tooltipContainer = config.tooltipContainer || document.body;
        tooltipContainer.appendChild(this.tooltip);
    }

    /** Get the tooltip element for hover interactions */
    getTooltip(): HTMLElement {
        return this.tooltip;
    }

    /** Display an error message */
    reportError(message: string): void {
        if (this.config.errorContainer) {
            this.config.errorContainer.textContent = message;
            this.config.errorContainer.style.display = 'block';
        }
        console.error(message);
    }

    /** Display a status message */
    message(message: string): void {
        console.log(message);
        if (this.config.messageContainer) {
            this.config.messageContainer.style.display = 'block';
            this.config.messageContainer.innerText = message;
        }
    }

    /** Clear the status message */
    clearMessage(): void {
        if (this.config.messageContainer) {
            this.config.messageContainer.textContent = '';
            this.config.messageContainer.style.display = 'none';
        }
    }

    /**
     * Render a circuit profile with interactive visualization.
     * This is the main entry point for displaying a profile.
     *
     * @param profile The circuit profile to visualize
     */
    render(profile: CircuitProfile): void {
        try {
            // Clear any previous error
            if (this.config.errorContainer) {
                this.config.errorContainer.style.display = 'none';
            }

            // Create selectors
            this.circuitSelector = new CircuitSelector(profile);
            this.metadataSelector = new MetadataSelector(profile);

            // Display metadata selector UI
            const table = this.config.selectorContainer.querySelector('table');
            if (!table) {
                const newTable = document.createElement('table');
                newTable.id = 'selection-tools';
                this.config.selectorContainer.appendChild(newTable);
                this.metadataSelector.display(newTable);
            } else {
                this.metadataSelector.display(table as HTMLTableElement);
            }

            // Get initial selection and create graph
            const selection = this.circuitSelector.getSelection();
            const cytograph = Cytograph.fromProfile(profile, selection);

            // Create rendering with navigator
            this.rendering = new CytographRendering(
                this.config.graphContainer,
                this.config.navigatorContainer,
                this.tooltip,
                this.config.tooltipContainer,
                cytograph.graph, selection,
                MetadataSelector.getFullSelection(),
                this.message.bind(this),
                this.clearMessage.bind(this)
            );
            this.rendering.setEvents(n => this.circuitSelector!.toggleExpand(n));

            // Wire up event handlers
            this.circuitSelector.setOnChange(() => {
                // Called when the circuit to display changes
                this.message("Recomputing profile graph")
                const graph = Cytograph.fromProfile(profile, selection);
                this.message("Computing graph changes")
                this.rendering!.updateGraph(graph);
                this.rendering!.center(selection.trigger);
                this.rendering!.updateMetadata(profile, this.metadataSelector!.getSelection());
            });

            this.metadataSelector.setOnChange(() => {
                this.rendering!.updateMetadata(profile, this.metadataSelector!.getSelection());
            });

            // Wire up search functionality if search input is provided
            if (this.config.searchInput) {
                this.config.searchInput.onkeydown = (e) => {
                    if (e.key === "Enter") {
                        const query = this.config.searchInput!.value;
                        this.rendering!.search(query);
                    }
                };
            }

            // Produce the graph visualization
            this.rendering.updateGraph(cytograph);
            this.rendering.updateMetadata(profile, this.metadataSelector.getSelection());

        } catch (e) {
            const message = e instanceof Error ? e.message : String(e);
            this.reportError(`Error displaying circuit profile: ${message}`);
        }
    }

    /**
     * Clean up resources when the profiler is no longer needed
     */
    dispose(): void {
        // Remove tooltip from DOM
        if (this.tooltip.parentNode) {
            this.tooltip.parentNode.removeChild(this.tooltip);
        }

        // Clear selector UI from DOM
        const table = this.config.selectorContainer.querySelector('table');
        if (table) {
            // Remove all rows
            while (table.rows.length > 0) {
                table.deleteRow(0);
            }
        }

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
