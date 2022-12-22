import { ErrorDisplay } from "./errReporter"
import { IHtmlElement, SpecialChars, removeAllChildren, formatJson } from "./ui"

/*
interface InputFormat {
    name: string,
    config: {
        input_stream: string
    }
}
*/

interface IOConfig {
    transport: JSON,
    max_buffered_records: number,
}

interface IODescription {
    endpoint_name: string,
    fatal_error: boolean | null
    config: IOConfig,
    metrics: Metrics,
}

interface Metrics {
    total_bytes: number,
    total_records: number,
    buffered_bytes: number,
    buffered_records: number,
    num_errors: number,
}

interface DBSPConfig {
    global_config: {
        max_buffering_delay_usecs: number,
        min_batch_size_records: number
    },
    global_metrics: {
        buffered_input_records: number
    },
    inputs: IODescription[],
    outputs: IODescription[],
    metrics: Metrics
}

class IOArray implements IHtmlElement {
    private readonly root: HTMLElement;
    constructor(
        name: string,
        description: IODescription[], 
        private readonly display: ErrorDisplay,
        readonly parent: Pipeline) {
        this.root = document.createElement("div");
        this.root.style.display = "flex";
        this.root.style.flexGrow = "1";
        this.root.style.flexDirection = "column";
        let inputNo = 0;
        for (let desc of description) {
            let block = document.createElement("div");
            block.style.background = "lightgrey";
            block.style.margin = "2px";
            block.style.flexGrow = "1";
            block.id = name + inputNo;
            let text = document.createElement("span");
            if (name == "input") {
                text.textContent = block.id + SpecialChars.rightArrow;
                block.style.textAlign = "right";
            } else {
                text.textContent = SpecialChars.rightArrow + block.id;
                block.style.textAlign = "left";
            }  
            block.appendChild(text);
            this.root.appendChild(block);
            block.onclick = () => {
                parent.setLastClicked(block.id);
                this.display.reportError(formatJson(desc));
            }
        }
    }

    getHTMLRepresentation(): HTMLElement {
        return this.root;
    }
}

/**
 * Represents the visualization of a running DBSP pipeline.
 */
class Pipeline implements IHtmlElement {
    private root: HTMLElement;
    private inputs: IOArray | null;
    private outputs: IOArray | null;
    private body: HTMLElement | null;
    private config: DBSPConfig | null;
    private timer: number;
    /**
     * Id of last HTML element that was clicked.
     */
    private lastClickedID: string | null;

    public constructor(private display: ErrorDisplay) {
        this.root = document.createElement("div");
        this.root.style.display = "flex";
        this.root.style.flexDirection = "row";
        this.root.style.flexWrap = "nowrap";
        this.root.style.margin = "10px";
        this.inputs = null;
        this.outputs = null;
        this.config = null;
        this.body = null;
        this.lastClickedID = null;
        this.timer = setInterval(() => this.status(), 1000);
    }

    /**
     * @param element Set the id of the last element clicked.
     * Every time we get a new status we simulate a click on this
     * element to redisplay the status.
     */
    public setLastClicked(id: string): void {
        this.lastClickedID = id;
    }

    getHTMLRepresentation(): HTMLElement {
        return this.root;
    }

    public get(url: string, continuation: (response: Response) => void): void {
        fetch(url, {
            method: 'GET',
            headers: {
              Accept: 'application/json',
            },
          }).then(response => continuation(response));
    }

    public status(): void {
        this.get("status", response => this.status_received(response));
    }

    response_received(response: Response): void {
        if (response.ok) {
            response.text().then(r => this.show(r));
            return;
        }
        this.error(response);
    }

    shutdown(): void {
        this.get("shutdown", response => this.response_received(response));
    }

    pause(): void {
        this.get("pause", response => this.response_received(response));
    }

    error(response: Response): void {
        this.display.reportError("Error received: " + response.status);
        clearInterval(this.timer);
    }

    status_received(response: Response): void {
        if (response.ok) {
            response.json().then(v => this.status_decoded(v));
            return;
        }
        this.error(response);
    }

    createFiller(): HTMLDivElement {
        const filler = document.createElement("div");
        filler.style.flexGrow = "100";
        return filler;
    }

    showBody(): void {
        if (this.config == null)
            return;
        const data = formatJson(this.config.global_config);
        this.show(data);
    }

    setConfig(config: DBSPConfig | null): void {
        console.log("Received config");
        this.config = config;
        if (config == null) {
            return;
        }
        removeAllChildren(this.root);
        let filler = this.createFiller();
        this.root.appendChild(filler);
        this.inputs = new IOArray("input", config.inputs, this.display, this);
        this.root.appendChild(this.inputs.getHTMLRepresentation());
        this.body = document.createElement("div");
        this.root.appendChild(this.body);
        this.body.innerText = "computation";
        this.body.style.background = "cyan";
        this.body.style.flexGrow = "1";
        this.body.id = "computation";
        this.body.onclick = () => {
            this.setLastClicked(this.body!.id);
            this.showBody();
        }
        this.outputs = new IOArray("output", config.outputs, this.display, this);
        this.root.appendChild(this.outputs.getHTMLRepresentation());
        filler = this.createFiller();
        this.root.appendChild(filler);
        if (this.lastClickedID != null) {
            let element = document.getElementById(this.lastClickedID);
            if (element != null) {
                element.click();
            }
        }
    }

    status_decoded(config: DBSPConfig): void {
        this.setConfig(config);
    }

    show(data: string): void {
        this.display.reportError(data);
    }

    public start(): void {
        this.get("start", r => this.response_received(r));
    }
}

class DBSP {
    private pipeline: Pipeline;
    private display: ErrorDisplay;
    
    constructor() {
        this.display = new ErrorDisplay();
        this.pipeline = new Pipeline(this.display);
        console.log("Created");
    }

    public loaded(): void {
        console.log("DBSP Loaded");
        const body = document.getElementById("body");
        if (!body) {
            console.log("No body");
            return;
        }

        const controlPanel = document.createElement("div");
        controlPanel.style.background = "lightblue";
        body.appendChild(controlPanel);
        controlPanel.style.width = "100%";

        const start = document.createElement("button");
        start.textContent = "start";
        controlPanel.appendChild(start);
        start.onclick = () => this.pipeline.start();

        const pause = document.createElement("button");
        pause.textContent = "pause";
        controlPanel.appendChild(pause);
        pause.onclick = () => this.pipeline.pause();

        const stop = document.createElement("button");
        stop.textContent = "shutdown";
        stop.onclick = () => this.pipeline.shutdown();
        controlPanel.appendChild(stop);

        const status = document.createElement("button");
        status.textContent = "refresh";
        controlPanel.appendChild(status);
        status.onclick = () => this.pipeline.status();

        body.appendChild(this.pipeline.getHTMLRepresentation());
        body.appendChild(this.display.getHTMLRepresentation());
    }
}

// Simulate a global function, to be accessed in index.html
const _global = window as any;
_global.created = function () {
    const dbsp = new DBSP();
    dbsp.loaded();
};


