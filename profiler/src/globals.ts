import { getStack, Option } from './util';
import { CircuitProfile, type JsonProfiles } from "./profile.js";
import { type Dataflow } from "./dataflow.js";
import { Cytograph, CytographRendering } from "./cytograph.js";
import { CircuitSelector } from "./selection.js";

// Global constants held in a singleton class.
export class Globals {
    private static instance: Globals;

    // Tooltip HTML element, updated on hover over a graph node.
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

    addTrace(message: string, e: any): string {
        message += e;
        if (e instanceof Error) {
            message += "\n" + getStack(e);
        }
        return message;
    }

    /** Load two files from the specified directory:
     * <basename>.json
     * dataflow-<basename>.json
     * containing the profile and dataflow graph for the program to visualize.
     */
    loadFiles(directory: string, basename: string): void {
        const profileUrl = directory + "/" + basename + ".json";
        const dataflowUrl = directory + "/dataflow-" + basename + ".json";

        this.fetchJson<JsonProfiles>(profileUrl)
            .then((data: Option<JsonProfiles>) => {
                if (data.isNone()) {
                    // Error already reported.
                    return;
                }

                this.fetchJson<Dataflow>(dataflowUrl)
                    .then((dfData: Option<Dataflow>) => {
                        if (dfData.isNone()) {
                            // Error already reported.
                            return;
                        }

                        let circuit;
                        try {
                            circuit = CircuitProfile.fromJson(data.unwrap());
                            circuit.setDataflow(dfData.unwrap());
                        } catch (e) {
                            this.reportError(this.addTrace("Error decoding JSON profile data: ", e));
                            return;
                        }
                        try {
                            // The main event loop.  When the selector changes,
                            // it causes the circuit graph to be recomputed and redrawn.
                            let selector = new CircuitSelector(circuit);
                            selector.display("selector");
                            let selection = selector.getSelection();
                            let graph = Cytograph.fromProfile(circuit, selection);
                            let rendering = new CytographRendering(graph.graph, selection);
                            rendering.updateGraph(graph);
                            selector.setOnChange(() => {
                                let graph = Cytograph.fromProfile(circuit, selection);
                                rendering.updateGraph(graph);
                            });
                            rendering.render(n => selector.toggleExpand(n));
                        } catch (e) {
                            this.reportError(this.addTrace("Error displaying circuit profile: ", e));
                        }
                    });
            });
    }
}
