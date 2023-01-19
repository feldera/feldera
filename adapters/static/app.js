define("ui", ["require", "exports"], function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.beep = exports.removeAllChildren = exports.px = exports.SpecialChars = exports.formatJson = void 0;
    function formatJson(json) {
        return JSON.stringify(json, null, 4);
    }
    exports.formatJson = formatJson;
    /**
     * A list of special unicode character codes.
     */
    class SpecialChars {
    }
    exports.SpecialChars = SpecialChars;
    // Approximation sign.
    SpecialChars.approx = "\u2248";
    SpecialChars.upArrow = "▲";
    SpecialChars.downArrow = "▼";
    SpecialChars.rightArrow = "⇒";
    SpecialChars.ellipsis = "…";
    SpecialChars.downArrowHtml = "&dArr;";
    SpecialChars.upArrowHtml = "&uArr;";
    SpecialChars.leftArrowHtml = "&lArr;";
    SpecialChars.rightArrowHtml = "&rArr;";
    SpecialChars.epsilon = "\u03B5";
    SpecialChars.enDash = "&ndash;";
    SpecialChars.scissors = "\u2702";
    function px(dim) {
        if (dim === 0)
            return dim.toString();
        return dim.toString() + "px";
    }
    exports.px = px;
    /**
     * Remove all children of an HTML DOM object..
     */
    function removeAllChildren(h) {
        while (h.lastChild != null)
            h.removeChild(h.lastChild);
    }
    exports.removeAllChildren = removeAllChildren;
    function beep() {
        const audioCtx = new window.AudioContext();
        var oscillator = audioCtx.createOscillator();
        var gainNode = audioCtx.createGain();
        oscillator.connect(gainNode);
        gainNode.connect(audioCtx.destination);
        gainNode.gain.value = 10;
        oscillator.frequency.value = 800;
        oscillator.type = "sine";
        oscillator.start();
        setTimeout(function () {
            oscillator.stop();
        }, 100);
    }
    exports.beep = beep;
    ;
});
define("errReporter", ["require", "exports", "ui"], function (require, exports, ui_1) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.WebClient = exports.ErrorDisplay = exports.runAfterDelay = exports.ConsoleErrorReporter = void 0;
    /**
     * An error reporter that writes messages to the JavaScript browser console.
     */
    class ConsoleErrorReporter {
        reportError(message) {
            console.log(message);
        }
        clear() {
            // We cannot clear the console
        }
    }
    exports.ConsoleErrorReporter = ConsoleErrorReporter;
    ConsoleErrorReporter.instance = new ConsoleErrorReporter();
    /**
     * Wait the specified time then run the supplied closure.
     * @param milliseconds  Number of milliseconds.
     * @param closure       Closure to run after this delay.
     */
    function runAfterDelay(milliseconds, closure) {
        new Promise(resolve => setTimeout(resolve, 1000))
            .then(() => closure());
    }
    exports.runAfterDelay = runAfterDelay;
    /**
     * This class is used to display error messages in the browser window.
     */
    class ErrorDisplay {
        constructor() {
            this.topLevel = document.createElement("div");
            this.console = document.createElement("div");
            this.console.className = "console";
            const container = document.createElement("span");
            this.topLevel.appendChild(container);
            this.copyButton = document.createElement("span");
            this.copyButton.innerHTML = ui_1.SpecialChars.scissors;
            this.copyButton.title = "copy error to clipboard";
            this.copyButton.style.display = "none";
            this.copyButton.onclick = () => this.copy();
            this.copyButton.style.cssFloat = "right";
            this.copyButton.style.zIndex = "10";
            this.clearButton = document.createElement("span");
            this.clearButton.className = "close";
            this.clearButton.innerHTML = "&times;";
            this.clearButton.title = "clear message";
            this.clearButton.style.display = "none";
            this.clearButton.onclick = () => this.clear();
            this.clearButton.style.cssFloat = "right";
            this.clearButton.style.zIndex = "10";
            container.appendChild(this.clearButton);
            container.appendChild(this.copyButton);
            container.appendChild(this.console);
        }
        getHTMLRepresentation() {
            return this.topLevel;
        }
        reportError(message) {
            console.log(message);
            this.console.innerText = message;
            this.clearButton.style.display = "block";
            this.copyButton.style.display = "block";
        }
        clear() {
            this.console.textContent = "";
            this.clearButton.style.display = "none";
            this.copyButton.style.display = "none";
        }
        copy() {
            navigator.clipboard.writeText(this.console.innerText);
            (0, ui_1.beep)();
        }
    }
    exports.ErrorDisplay = ErrorDisplay;
    /**
     * A Class which knows how to handle get and put requests.
     */
    class WebClient {
        constructor(display) {
            this.display = display;
        }
        get(url, continuation) {
            fetch(url, {
                method: 'GET',
                headers: {
                    Accept: 'application/json',
                },
            }).then(response => {
                if (response.ok) {
                    continuation(response);
                    return;
                }
                this.error(response);
            });
        }
        post(url, data, continuation) {
            fetch(url, {
                method: 'POST',
                headers: {
                    "content-type": 'application/json',
                },
                body: JSON.stringify(data),
            })
                .then(response => {
                if (response.ok) {
                    continuation(response);
                    return;
                }
                this.error(response);
            });
        }
        error(response) {
            this.display.reportError("Error received: " + response.status + "\n" + response.text);
        }
        showText(response) {
            response.text().then(t => this.display.reportError(t));
        }
        showJson(response) {
            response.json().then(t => this.display.reportError(t));
        }
    }
    exports.WebClient = WebClient;
});
define("dbsp", ["require", "exports", "errReporter", "ui"], function (require, exports, errReporter_1, ui_2) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    class IOArray {
        constructor(name, description, display, parent) {
            this.display = display;
            this.parent = parent;
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
                inputNo++;
                let text = document.createElement("span");
                if (name == "input") {
                    text.textContent = block.id + ui_2.SpecialChars.rightArrow;
                    block.style.textAlign = "right";
                }
                else {
                    text.textContent = ui_2.SpecialChars.rightArrow + block.id;
                    block.style.textAlign = "left";
                }
                block.appendChild(text);
                this.root.appendChild(block);
                block.onclick = () => {
                    parent.setLastClicked(block.id);
                    this.display.reportError((0, ui_2.formatJson)(desc));
                };
            }
        }
        getHTMLRepresentation() {
            return this.root;
        }
    }
    /**
     * Represents the visualization of a running DBSP pipeline.
     */
    class Pipeline extends errReporter_1.WebClient {
        constructor(display) {
            super(display);
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
        setLastClicked(id) {
            this.lastClickedID = id;
        }
        getHTMLRepresentation() {
            return this.root;
        }
        status() {
            this.get("status", response => this.status_received(response));
        }
        response_received(response) {
            response.text().then(r => this.show(r));
        }
        shutdown() {
            this.get("shutdown", response => this.response_received(response));
        }
        pause() {
            this.get("pause", response => this.response_received(response));
        }
        error(response) {
            super.error(response);
            clearInterval(this.timer);
        }
        status_received(response) {
            response.json().then(v => this.status_decoded(v));
        }
        createFiller() {
            const filler = document.createElement("div");
            filler.style.flexGrow = "100";
            return filler;
        }
        showBody() {
            if (this.config == null)
                return;
            const data = (0, ui_2.formatJson)(this.config.global_config);
            this.show(data);
        }
        setConfig(config) {
            console.log("Received config");
            this.config = config;
            if (config == null) {
                return;
            }
            (0, ui_2.removeAllChildren)(this.root);
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
                this.setLastClicked(this.body.id);
                this.showBody();
            };
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
        status_decoded(config) {
            this.setConfig(config);
        }
        show(data) {
            this.display.reportError(data);
        }
        start() {
            this.get("start", r => this.response_received(r));
        }
    }
    class DBSP extends errReporter_1.WebClient {
        constructor() {
            super(new errReporter_1.ErrorDisplay());
            this.pipeline = new Pipeline(this.display);
            console.log("Created");
        }
        loaded() {
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
    const _global = window;
    _global.created = function () {
        const dbsp = new DBSP();
        dbsp.loaded();
    };
});
//# sourceMappingURL=app.js.map