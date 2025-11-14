// Global constants used for the profile visualization.

import { getStack, Option } from './util';
import { CircuitProfile, type JsonProfiles } from "./profile.js";
import { type Dataflow } from "./dataflow.js";
import { Cytograph, CytographRendering } from "./cytograph.js";
import { CircuitSelector } from "./selection.js";
import { MetadataSelector } from './metadataSelection';

export class Globals {
    private static instance: Globals;

    // HTML element where tooltip data is displayed on hover over a graph node.
    public readonly tooltip: HTMLElement;

    private constructor() {
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
        document.body.appendChild(this.tooltip);
    }

    // The unique instance of this class.
    public static getInstance(): Globals {
        if (!Globals.instance) {
            Globals.instance = new Globals();
        }
        return Globals.instance;
    }

    /** Display an error on the screen, usually an assertion failure. */
    reportError(message: string): void {
        const container = document.getElementById('error-message');
        if (container) {
            container.textContent = message;
            container.style.display = 'block';
        }
        console.error(message);
    }

    // Fetch a JSON file from the server, and parse it as the an object of type T;
    // returns None on error.
    async fetchJson<T = unknown>(url: string): Promise<Option<T>> {
        try {
            const response = await fetch(url);
            if (!response.ok) {
                this.reportError(`HTTP error: ${response.status}`);
                return Option.none();
            }

            const contentType = response.headers.get('content-type') || '';
            if (!contentType.includes('application/json')) {
                this.reportError('Unexpected content type: ' + contentType);
                return Option.none();
            }

            const text = await response.text();
            if (!text) {
                this.reportError('Empty response body');
                return Option.none();
            }

            return Option.some(JSON.parse(text) as T);
        } catch (err) {
            this.reportError('Fetch or JSON parse error: ' + err);
            return Option.none();
        }
    }

    /** Add a stack trace to a message if possible. */
    addTrace(message: string, e: any): string {
        message += e;
        if (e instanceof Error) {
            message += "\n" + getStack(e);
        }
        return message;
    }

    // Start running the visualizer by creating the UI and starting the event loop.
    run(profile: CircuitProfile) {
        let table = document.getElementById("selection-tools") as HTMLTableElement;

        // The main event loop.  When the selector changes,
        // it causes the circuit graph to be recomputed and redrawn.
        // The circuit selector currently does not have a visible representation.
        let circuitSelector = new CircuitSelector(profile);
        // Display the metadata selector
        let metadataSelector = new MetadataSelector(profile);
        metadataSelector.display(table);

        let selection = circuitSelector.getSelection();
        let cytograph = Cytograph.fromProfile(profile, selection);
        // Build the data required for rendering, but do not render yet
        let rendering = new CytographRendering(
            cytograph.graph, selection,
            MetadataSelector.getFullSelection());
        rendering.setEvents(n => circuitSelector.toggleExpand(n));

        // Compute and set initial view parameters
        circuitSelector.setOnChange(() => {
            // Called when the circuit to display changes
            Globals.message("Recomputing profile graph")
            let graph = Cytograph.fromProfile(profile, selection);
            Globals.message("Computing graph changes")
            rendering.updateGraph(graph);
            rendering.center(selection.trigger);
            rendering.updateMetadata(profile, metadataSelector.getSelection());
        });
        metadataSelector.setOnChange(() => {
            // When the metadata selection changes, do these:
            rendering.updateMetadata(profile, metadataSelector.getSelection());
        });
        let search = document.getElementById("search") as HTMLInputElement;
        search.onkeydown = (e) => {
            if (e.key === "Enter") {
                const query = search.value;
                rendering.search(query);
            }
        };

        // Produce the graph visualization
        rendering.updateGraph(cytograph);
        rendering.updateMetadata(profile, metadataSelector.getSelection());
    }

    /** Load two files from the specified directory:
     * <basename>.json
     * dataflow-<basename>.json
     * containing the profile and dataflow graph for the program to visualize.
     * Start the visualization based on these two files. */
    loadFiles(directory: string, basename: string): void {
        const profileUrl = directory + "/" + basename + ".json";
        const dataflowUrl = directory + "/dataflow-" + basename + ".json";

        Globals.message("Reading profile file...");
        this.fetchJson<JsonProfiles>(profileUrl)
            .then((data: Option<JsonProfiles>) => {
                if (data.isNone()) {
                    // Error already reported.
                    return;
                }
                Globals.message("Reading dataflow graph...");

                this.fetchJson<Dataflow>(dataflowUrl)
                    .then((dfData: Option<Dataflow>) => {
                        if (dfData.isNone()) {
                            // Error already reported.
                            return;
                        }

                        let circuit = null;
                        try {
                            Globals.message("Extracting profiling information...");
                            circuit = CircuitProfile.fromJson(data.unwrap());
                            circuit.setDataflow(dfData.unwrap());
                        } catch (e) {
                            this.reportError(this.addTrace("Error decoding JSON profile data: ", e));
                            return;
                        }
                        try {
                            Globals.message("Starting visualization...");
                            this.run(circuit);
                        } catch (e) {
                            this.reportError(this.addTrace("Error displaying circuit profile: ", e));
                        }
                    });
            });
    }

    static message(message: string) {
        console.log(message);
        const messageDiv = document.getElementById("message");
        if (messageDiv === null) {
            console.log("Cannot output message");
            return;
        }
        messageDiv.style.display = "block";
        messageDiv.innerText = message;
    }

    static clearMessage() {
        const messageDiv = document.getElementById("message");
        if (messageDiv === null)
            return;
        messageDiv.textContent = "";
        messageDiv.style.display = "none";
    }
}
