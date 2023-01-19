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
            response.text().then(t => this.display.reportError("Error received: " + response.status + "\n" + t));
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
define("dbsp-project", ["require", "exports", "errReporter", "ui"], function (require, exports, errReporter_1, ui_2) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    class ProjectDisplay extends errReporter_1.WebClient {
        constructor(project, parent, list) {
            super(new errReporter_1.ErrorDisplay());
            this.project = project;
            this.parent = parent;
            this.list = list;
            this.root = document.createElement("div");
            this.h1 = document.createElement("h1");
            this.root.appendChild(this.h1);
            const back = document.createElement("button");
            back.textContent = "Back";
            back.title = "Return to the list of projects";
            this.root.appendChild(back);
            back.onclick = () => this.back();
            const controlPanel = document.createElement("div");
            controlPanel.style.background = "lightblue";
            controlPanel.style.width = "100%";
            this.root.appendChild(controlPanel);
            const code = document.createElement("button");
            code.textContent = "Code";
            code.title = "Show the SQL code in this project";
            controlPanel.appendChild(code);
            code.onclick = () => this.fetchCode();
            const status = document.createElement("button");
            status.textContent = "Compile status";
            status.title = "Show the compilation status of the code";
            controlPanel.appendChild(status);
            status.onclick = () => this.fetchStatus();
            const compile = document.createElement("button");
            compile.textContent = "Compile";
            compile.title = "Compile the SQL code into an executable";
            controlPanel.appendChild(compile);
            compile.onclick = () => this.compile();
            const update = document.createElement("button");
            update.textContent = "Update code";
            update.title = "Change the SQL code in this project";
            controlPanel.appendChild(update);
            update.onclick = () => this.update();
            const del = document.createElement("button");
            del.textContent = "Delete";
            del.title = "Delete this project and all associated data";
            controlPanel.appendChild(del);
            del.onclick = () => this.deleteProject();
            const controlPanel1 = document.createElement("div");
            controlPanel1.style.width = "100%";
            controlPanel1.style.background = "lightblue";
            this.root.appendChild(controlPanel1);
            const configs = document.createElement("button");
            configs.textContent = "Configs";
            configs.title = "Show the configurations associated with this project";
            controlPanel1.appendChild(configs);
            configs.onclick = () => this.configs();
            const pipelines = document.createElement("button");
            pipelines.textContent = "Pipelines";
            pipelines.title = "Show the running pipelines associated with this project";
            controlPanel1.appendChild(pipelines);
            pipelines.onclick = () => this.pipelines();
            const newConfig = document.createElement("button");
            newConfig.textContent = "New config";
            newConfig.title = "Upload a new configuration describing a pipeline";
            controlPanel1.appendChild(newConfig);
            newConfig.onclick = () => this.newConfig();
            this.pipelinesDiv = document.createElement("div");
            this.newProjectElement = new NewProject(this.project.name);
            this.root.appendChild(this.pipelinesDiv);
            this.root.appendChild(this.display.getHTMLRepresentation());
            this.refresh();
        }
        refresh() {
            this.h1.textContent = "Project: " + ProjectDisplay.toString(this.project);
        }
        newConfig() {
            const npe = this.newProjectElement.setTitle(null, "Specify a new for the new configuration", "New configuration");
            this.root.appendChild(npe);
            this.newProjectElement.submit.onclick = () => this.submitConfig();
        }
        submitConfig() {
            this.newProjectElement.remove();
            const name = this.newProjectElement.getProjectName(this.display);
            const reader = this.newProjectElement.getFileReader(this.display);
            if (name === null || reader === null)
                return;
            reader.addEventListener("load", () => {
                const data = {
                    "project_id": this.project.project_id,
                    "name": name,
                    "config": reader.result,
                };
                this.display.reportError("Configuration creation requested...");
                this.post("new_config", data, t => this.showText(t));
            });
        }
        pipelines() {
            const data = {
                "project_id": this.project.project_id
            };
            this.display.reportError("Refreshing...");
            this.post("list_project_pipelines", data, r => r.json().then(r => this.showPipelines(r)));
        }
        showPipelines(data) {
            (0, ui_2.removeAllChildren)(this.pipelinesDiv);
            this.display.clear();
            for (let pipeline of data) {
                let one = document.createElement("div");
                this.pipelinesDiv.appendChild(one);
                let date = new Date(pipeline.created.secs_since_epoch * 1000 +
                    pipeline.created.nanos_since_epoch / 10000000);
                let span = document.createElement("span");
                span.textContent = "id=" + pipeline.pipeline_id + " started " + date.toLocaleString();
                one.appendChild(span);
                let kill = document.createElement("button");
                kill.textContent = "Stop";
                kill.title = "Stop this pipeline from execution";
                kill.onclick = () => {
                    this.kill(pipeline);
                    this.pipelines();
                };
                one.appendChild(kill);
                if (pipeline.killed) {
                    kill.disabled = true;
                    kill.title = "Pipeline has been stopped from execution";
                }
                let del = document.createElement("button");
                del.textContent = "Delete";
                del.title = "Delete this pipeline (kill first if still running)";
                del.onclick = () => {
                    this.deletePipeline(pipeline);
                    this.pipelines();
                };
                one.appendChild(del);
                const url = document.createElement("a");
                let location = window.location;
                url.href = location.protocol + "//" + location.hostname + ":" + pipeline.port.toString();
                url.text = "Monitor";
                url.title = "Opens a new tab which allows you to interact with the pipeline";
                url.onclick = (e) => { e.preventDefault(); window.open(url.href, "_blank"); };
                one.appendChild(url);
            }
            if (data.length === 0)
                this.display.reportError("No pipelines exist");
        }
        kill(pipeline) {
            const data = {
                pipeline_id: pipeline.pipeline_id
            };
            this.display.reportError("Pipeline stop requested...");
            this.post("kill_pipeline", data, r => this.showJson(r));
        }
        deletePipeline(pipeline) {
            const data = {
                pipeline_id: pipeline.pipeline_id
            };
            this.display.reportError("Pipeline deletion requested...");
            this.post("delete_pipeline", data, r => this.showJson(r));
        }
        configs() {
            const data = {
                "project_id": this.project.project_id
            };
            this.display.reportError("Configuration list requested...");
            this.post("list_project_configs", data, r => r.json().then(r => this.showConfigs(r)));
        }
        showConfigs(data) {
            (0, ui_2.removeAllChildren)(this.pipelinesDiv);
            this.display.clear();
            for (let config of data) {
                let one = document.createElement("div");
                this.pipelinesDiv.appendChild(one);
                let span = document.createElement("span");
                span.textContent = "Configuration " + config.name + " version " + config.version;
                one.appendChild(span);
                let show = document.createElement("button");
                show.textContent = "Show";
                show.title = "Show this configuration";
                show.onclick = () => this.display.reportError(config.config);
                one.appendChild(show);
                let pipe = document.createElement("button");
                pipe.textContent = "New pipeline";
                pipe.title = "Start a new pipeline with this config";
                pipe.onclick = () => this.newPipeline(config);
                one.appendChild(pipe);
                let del = document.createElement("button");
                del.textContent = "Delete";
                del.title = "Delete this configuration";
                del.onclick = () => {
                    this.deleteConfig(config);
                    this.configs();
                };
                one.appendChild(del);
            }
            if (data.length === 0)
                this.display.reportError("No configurations were uploaded");
        }
        newPipeline(config) {
            const data = {
                "project_id": this.project.project_id,
                "project_version": this.project.version,
                "config_id": config.config_id,
                "config_version": config.version,
            };
            this.display.reportError("Pipeline request initiated...");
            this.post("new_pipeline", data, r => this.showText(r));
        }
        deleteConfig(config) {
            const data = {
                config_id: config.config_id
            };
            this.display.reportError("Deletion request initiated...");
            this.post("delete_config", data, r => this.showJson(r));
        }
        back() {
            this.parent.switchChild(this.list);
            this.list.list();
        }
        deleteProject() {
            const data = {
                "project_id": this.project.project_id
            };
            this.display.reportError("Project deletion initiated...");
            this.post("delete_project", data, _ => this.back());
        }
        update() {
            const npe = this.newProjectElement.setTitle(this.project.name, "Specify new project name", "Update project name and code");
            this.root.appendChild(npe);
            this.newProjectElement.submit.onclick = () => this.submitUpdate();
        }
        submitUpdate() {
            this.newProjectElement.remove();
            const name = this.newProjectElement.getProjectName(this.display);
            const reader = this.newProjectElement.getFileReader(this.display);
            if (name === null || reader === null)
                return;
            reader.addEventListener("load", () => {
                const data = {
                    "project_id": this.project.project_id,
                    "name": name,
                    "code": reader.result,
                };
                this.display.reportError("Project update requested...");
                this.post("update_project", data, t => {
                    this.showText(t);
                    this.fetchStatus();
                });
            });
        }
        compile() {
            const data = {
                "project_id": this.project.project_id,
                "version": this.project.version,
            };
            this.post("compile_project", data, t => {
                t.text().then(t => {
                    this.display.reportError("Compiling in background..." + t);
                    (0, errReporter_1.runAfterDelay)(1000, () => this.fetchStatus());
                });
            });
        }
        fetchStatus() {
            this.get("project_status/" + this.project.project_id, r => this.statusReceived(r));
        }
        statusReceived(response) {
            response.json().then(s => this.showStatus(s));
        }
        showStatus(s) {
            var _a;
            this.project.version = s.version;
            this.refresh();
            if (typeof s.status === 'string') {
                this.display.reportError(s.status);
                if (s.status === 'Compiling') {
                    (0, errReporter_1.runAfterDelay)(1000, () => this.fetchStatus());
                }
            }
            else if (typeof s.status === 'object') {
                const error = (_a = s.status.SqlError) !== null && _a !== void 0 ? _a : s.status.RustError;
                this.display.reportError("Compilation error:\n" + error);
            }
        }
        fetchCode() {
            this.get("project_code/" + this.project.project_id, r => this.codeReceived(r));
        }
        codeReceived(response) {
            response.json().then(s => this.showCode(s));
        }
        showCode(s) {
            this.display.reportError(s.code);
        }
        getHTMLRepresentation() {
            return this.root;
        }
        static toString(project) {
            return "name=" + project.name + " id=" +
                project.project_id.toLocaleString() + " version=" +
                project.version.toLocaleString();
        }
    }
    /**
     * Used for both new project and update project.
     */
    class NewProject {
        constructor(default_name) {
            this.root = document.createElement("div");
            this.root.style.background = "beige";
            this.title = document.createElement("h3");
            this.root.appendChild(this.title);
            this.newProjectName = document.createElement("input");
            this.newProjectName.type = "text";
            this.newProjectName.required = true;
            this.setTitle(default_name !== null && default_name !== void 0 ? default_name : "Project name", "Specify a (new) name for the project", "New project");
            this.root.appendChild(this.newProjectName);
            this.newProjectCode = document.createElement("input");
            this.newProjectCode.title = "Specify a file containing a SQL program that should be executed";
            this.newProjectCode.type = "file";
            this.newProjectCode.accept = "text";
            this.newProjectCode.required = true;
            this.root.appendChild(this.newProjectCode);
            this.submit = document.createElement("button");
            this.submit.textContent = "Submit";
            this.submit.title = "Upload the changes";
            this.root.appendChild(this.submit);
            this.cancel = document.createElement("button");
            this.cancel.textContent = "Cancel";
            this.cancel.title = "Abandon the code changes";
            this.root.appendChild(this.cancel);
            this.cancel.onclick = () => this.root.remove();
        }
        setTitle(name, popup, header) {
            this.newProjectName.value = name !== null && name !== void 0 ? name : "Name";
            this.newProjectName.title = popup;
            this.title.textContent = header;
            return this.root;
        }
        remove() {
            this.root.remove();
        }
        getProjectName(display) {
            if (this.newProjectName.textContent === null) {
                display.reportError("Pease supply a project name");
                return null;
            }
            return this.newProjectName.value;
        }
        getFileReader(display) {
            var _a;
            if (this.newProjectCode.files === null || ((_a = this.newProjectCode.files) === null || _a === void 0 ? void 0 : _a.length) === 0) {
                display.reportError("Pease supply the SQL code for the project");
                return null;
            }
            const reader = new FileReader();
            reader.readAsText(this.newProjectCode.files[0]);
            return reader;
        }
    }
    class ProjectListDisplay extends errReporter_1.WebClient {
        constructor(parent) {
            super(new errReporter_1.ErrorDisplay());
            this.parent = parent;
            this.root = document.createElement("div");
            const h1 = document.createElement("h1");
            h1.textContent = "Project server";
            this.root.appendChild(h1);
            this.table = null;
            console.log("Created");
            const controlPanel = document.createElement("div");
            controlPanel.style.background = "lightblue";
            this.root.appendChild(controlPanel);
            controlPanel.style.width = "100%";
            const newProject = document.createElement("button");
            newProject.title = "Create a new DBSP project.  A project is essentially a SQL program\n" +
                "that can be compiled and instantiated into one or multiple executable pipelines.";
            newProject.textContent = "New project";
            controlPanel.appendChild(newProject);
            newProject.onclick = () => this.newProject();
            const list = document.createElement("button");
            list.title = "Display a list of the known existing projects";
            list.textContent = "Refresh";
            controlPanel.appendChild(list);
            list.onclick = () => this.list();
            this.root.appendChild(this.display.getHTMLRepresentation());
            this.newProjectElement = new NewProject();
        }
        getHTMLRepresentation() {
            return this.root;
        }
        createNewProject() {
            this.newProjectElement.remove();
            const name = this.newProjectElement.getProjectName(this.display);
            const reader = this.newProjectElement.getFileReader(this.display);
            if (name === null || reader === null)
                return;
            reader.addEventListener("load", () => {
                const data = {
                    "name": name,
                    "code": reader.result,
                };
                console.log(data);
                this.display.reportError("Project creation requested...");
                this.post("new_project", data, (r) => {
                    this.showText(r);
                    (0, errReporter_1.runAfterDelay)(1000, () => this.list());
                });
            });
        }
        newProject() {
            const npe = this.newProjectElement.setTitle(null, "Specify project name", "Create a new project");
            this.root.appendChild(npe);
            this.newProjectElement.submit.onclick = () => this.createNewProject();
        }
        list() {
            this.get("list_projects", response => response.json().then(r => this.show(r)));
        }
        removeTable() {
            if (this.table != null) {
                this.table.remove();
            }
            this.table = null;
        }
        show(projects) {
            if (projects.length == 0) {
                this.display.reportError("No active projects");
                return;
            }
            this.removeTable();
            const table = document.createElement("table");
            this.table = table;
            const header = document.createElement("tr");
            table.appendChild(header);
            this.root.appendChild(table);
            for (let project of projects) {
                const tr = document.createElement("tr");
                table.appendChild(tr);
                let td = document.createElement("td");
                tr.appendChild(td);
                const span = document.createElement("span");
                span.textContent = "Project " + ProjectDisplay.toString(project);
                td.appendChild(span);
                const button = document.createElement("button");
                button.title = "Display a view which allows exploring and controlling this project (starting/stopping/editing/etc.)";
                button.textContent = "Explore";
                td.appendChild(button);
                button.onclick = () => this.showProject(project);
            }
        }
        showProject(project) {
            let pd = new ProjectDisplay(project, this.parent, this);
            this.removeTable();
            this.clearError();
            this.parent.switchChild(pd);
        }
        clearError() {
            this.display.clear();
        }
    }
    class RootWindow {
        constructor() {
            this.root = document.createElement("div");
            let child = new ProjectListDisplay(this);
            this.child = child;
            this.switchChild(child);
            child.list();
        }
        switchChild(child) {
            const newRoot = child.getHTMLRepresentation();
            this.child.getHTMLRepresentation().remove();
            this.child = child;
            this.root.appendChild(newRoot);
        }
        getHTMLRepresentation() {
            return this.root;
        }
        loaded() {
            console.log("DBSP Project UI Loaded");
            const body = document.getElementById("body");
            if (!body) {
                console.log("No body");
                return;
            }
            body.appendChild(this.getHTMLRepresentation());
        }
    }
    // Simulate a global function, to be accessed in index.html
    const _global = window;
    _global.created = function () {
        const root = new RootWindow();
        root.loaded();
    };
});
//# sourceMappingURL=app.js.map