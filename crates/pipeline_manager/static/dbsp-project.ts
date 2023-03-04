import { WebClient, ErrorDisplay, runAfterDelay } from "./errReporter"
import { IHtmlElement, removeAllChildren } from './ui';

/**
 * Project description received from web server.
 */
interface Project {
    name: string,
    project_id: number,
    version: number,
    status: CompilationError | string,
}

interface ProjectCode {
    version: number,
    code: string,
}

interface SqlCompilerMessage {
    start_line_number: number,
    start_column: number,
    end_line_number: number,
    end_column: number,
    warning: boolean,
    error_type: string,
    message: string,
}

interface CompilationError {
    SqlError: SqlCompilerMessage[] | null,
    RustError: string | null,
    SystemError: string | null,
}

interface SwitchChild {
    switchChild(child: IHtmlElement): void;
}

interface Config {
    config_id: number,
    project_id: number,
    version: number,
    name: string,
    config: string,
}

interface Pipeline {
    pipeline_id: number,
    project_id: number,
    project_version: number,
    port: number,
    killed: boolean,
    created: string
}

class ProjectDisplay extends WebClient implements IHtmlElement {
    readonly root: HTMLElement;
    readonly newProjectElement: NewProject;
    readonly pipelinesDiv: HTMLElement;
    readonly h1: HTMLElement;

    constructor(protected project: Project, readonly parent: SwitchChild, readonly list: ProjectListDisplay) {
        super(new ErrorDisplay());
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
    
    newConfig(): void {
        const npe = this.newProjectElement.setTitle(
            null, 
            "Specify a new for the new configuration",
            "New configuration");
        this.root.appendChild(npe);
        this.newProjectElement.submit.onclick = () => this.submitConfig();
    }

    submitConfig(): void {
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
            this.post("configs", data, t => this.showText(t));
        });
    }

    pipelines(): void {
        this.display.reportError("Refreshing...");
        this.get("projects/" + this.project.project_id + "/pipelines", r => r.json().then(r => this.showPipelines(r)));
    }

    showPipelines(data: Pipeline[]): void {
        removeAllChildren(this.pipelinesDiv);
        this.display.clear();
        for (let pipeline of data) {
            let one = document.createElement("div");
            this.pipelinesDiv.appendChild(one);
            
            let span = document.createElement("span");
            span.textContent = "id=" + pipeline.pipeline_id + " started " + pipeline.created;
            one.appendChild(span);

            let metadata = document.createElement("button");
            metadata.textContent = "Pipeline metadata";
            metadata.title = "Retrieve pipeline metadata";
            metadata.onclick = () => {
                this.pipeline_metadata(pipeline);
            }
            one.appendChild(metadata);
 
            let status = document.createElement("button");
            status.textContent = "Pipeline status";
            status.title = "Retrieve pipeline status";
            status.onclick = () => {
                this.pipeline_status(pipeline);
            }
            one.appendChild(status);

            let start = document.createElement("button");
            start.textContent = "Start pipeline";
            start.title = "Start pipeline";
            start.onclick = () => {
                this.pipeline_start(pipeline);
            }
            one.appendChild(start);

            let pause = document.createElement("button");
            pause.textContent = "Pause pipeline";
            pause.title = "Pause pipeline";
            pause.onclick = () => {
                this.pipeline_pause(pipeline);
            }
            one.appendChild(pause);
 
            let kill = document.createElement("button");
            kill.textContent = "Shut down";
            kill.title = "Shut down this pipeline";
            kill.onclick = () => {
                this.kill(pipeline);
                this.pipelines();
            }
            one.appendChild(kill);
            if (pipeline.killed) {
                kill.disabled = true;
                kill.title = "Pipeline has been shut down";
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
            url.onclick = (e) => { e.preventDefault(); window.open(url.href, "_blank"); }
            one.appendChild(url);
        }
        if (data.length === 0)
            this.display.reportError("No pipelines exist");
    }

    pipeline_status(pipeline: Pipeline): void {
        this.get("pipelines/" + pipeline.pipeline_id + "/status", r => this.showText(r));
    } 

    pipeline_metadata(pipeline: Pipeline): void {
        this.get("pipelines/" + pipeline.pipeline_id + "/metadata", r => this.showText(r));
    } 

    pipeline_start(pipeline: Pipeline): void {
        const data = {}
        this.post("pipelines/" + pipeline.pipeline_id + "/start", data, r => this.showText(r));
    } 

    pipeline_pause(pipeline: Pipeline): void {
        const data = {}
        this.post("pipelines/" + pipeline.pipeline_id + "/pause", data, r => this.showText(r));
    } 

    kill(pipeline: Pipeline): void {
        const data = {
            pipeline_id: pipeline.pipeline_id
        }
        this.display.reportError("Pipeline stop requested...");
        this.post("pipelines/shutdown", data, r => this.showJson(r));
    } 

    deletePipeline(pipeline: Pipeline): void {
        const data = {}
        this.display.reportError("Pipeline deletion requested...");
        this.delete("pipelines/" + pipeline.pipeline_id, data, r => this.showJson(r));
    }

    configs(): void {
        this.display.reportError("Configuration list requested...");
        this.get("projects/" + this.project.project_id + "/configs", r => r.json().then(r => this.showConfigs(r)));
    }

    showConfigs(data: Config[]): void {
        removeAllChildren(this.pipelinesDiv);
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
            show.onclick = () => 
                this.display.reportError(config.config);
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

    newPipeline(config: Config): void {
        const data = {
            "project_id": this.project.project_id,
            "project_version": this.project.version,
            "config_id": config.config_id,
            "config_version": config.version,
        }
        this.display.reportError("Pipeline request initiated...");
        this.post("pipelines", data, r => this.showText(r));
    }

    deleteConfig(config: Config): void {
        const data = {}
        this.display.reportError("Deletion request initiated...");
        this.delete("configs/" + config.config_id, data, r => this.showJson(r));
    }

    back(): void {
        this.parent.switchChild(this.list);
        this.list.list();
    }

    deleteProject(): void {
        const data = {}
        this.display.reportError("Project deletion initiated...");
        this.delete("projects/" + this.project.project_id, data, _ => this.back());
    }

    update(): void {
        const npe = this.newProjectElement.setTitle(this.project.name, 
            "Specify new project name", 
            "Update project name and code");
        this.root.appendChild(npe);
        this.newProjectElement.submit.onclick = () => this.submitUpdate();
    }

    submitUpdate(): void {
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
            this.patch("projects", data,
                t => {
                    this.showText(t);
                    this.fetchStatus();
            });
        });
    }

    compile(): void {
        const data = {
            "project_id": this.project.project_id,
            "version": this.project.version,
        };
        this.post("projects/compile", data, t => {
            t.text().then(t => {
                this.display.reportError("Compiling in background..." + t);
                runAfterDelay(1000, () => this.fetchStatus());
            });
        });
    }

    fetchStatus(): void {
        this.get("projects/" + this.project.project_id, r => this.statusReceived(r));
    }

    statusReceived(response: Response): void {
        response.json().then(descr => this.showStatus(descr));
    }

    showStatus(descr: Project): void {
        this.project.version = descr.version;
        this.refresh();
        if (typeof descr.status === 'string') {
            this.display.reportError(descr.status);
            if (descr.status === 'CompilingSql' || descr.status == 'CompilingRust') {
                runAfterDelay(1000, () => this.fetchStatus());
            }
        } else if (typeof descr.status === 'object') {
            const error = descr.status.SqlError ?? descr.status.RustError ?? descr.status.SystemError;
            this.display.reportError("Compilation error:\n" + JSON.stringify(error));
        }
    }

    fetchCode(): void {
        this.get("/projects/" + this.project.project_id + "/code", r => this.codeReceived(r));
    }

    codeReceived(response: Response): void {
        response.json().then(s => this.showCode(s));
    }

    showCode(s: ProjectCode): void {
        this.display.reportError(s.code);
    }

    getHTMLRepresentation(): HTMLElement {
        return this.root;
    }

    static toString(project: Project): string {
        return "name=" + project.name + " id=" + 
        project.project_id.toLocaleString() + " version=" +
        project.version.toLocaleString();
    }
}

/**
 * Used for both new project and update project.
 */
class NewProject {
    root: HTMLElement;
    newProjectName: HTMLInputElement;
    newProjectCode: HTMLInputElement;
    title: HTMLElement;
    public submit: HTMLButtonElement;
    public cancel: HTMLButtonElement;

    constructor(default_name?: string) {
        this.root = document.createElement("div");
        this.root.style.background = "beige";
        
        this.title = document.createElement("h3");
        this.root.appendChild(this.title);

        this.newProjectName = document.createElement("input");
        this.newProjectName.type = "text";
        this.newProjectName.required = true;
        this.setTitle(
            default_name ?? "Project name", 
            "Specify a (new) name for the project",
            "New project");
        this.root.appendChild(this.newProjectName);

        this.newProjectCode = document.createElement("input");
        this.newProjectCode.title = "Specify a file containing a SQL program that should be executed"
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
        this.cancel.title = "Abandon the code changes"
        this.root.appendChild(this.cancel);
        this.cancel.onclick = () => this.root.remove();
    }

    setTitle(name: string | null, popup: string, header: string): HTMLElement {
        this.newProjectName.value = name ?? "Name";
        this.newProjectName.title = popup;
        this.title.textContent = header;
        return this.root;
    }

    remove(): void {
        this.root.remove();
    }

    getProjectName(display: ErrorDisplay): string | null {
        if (this.newProjectName.textContent === null) {
            display.reportError("Pease supply a project name");
            return null;
        }
        return this.newProjectName.value;
    }

    getFileReader(display: ErrorDisplay): FileReader | null {
        if (this.newProjectCode.files === null || this.newProjectCode.files?.length === 0) {
            display.reportError("Pease supply the SQL code for the project");
            return null;
        }

        const reader = new FileReader(); 
        reader.readAsText(this.newProjectCode.files[0]);
        return reader;
    }
}

class ProjectListDisplay extends WebClient implements IHtmlElement {
    readonly root: HTMLElement;
    table: HTMLElement | null;
    newProjectElement: NewProject;

    constructor(readonly parent: SwitchChild) {
        super(new ErrorDisplay());
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

    getHTMLRepresentation(): HTMLElement {
        return this.root;
    }

    createNewProject(): void {
        this.newProjectElement.remove();
        const name = this.newProjectElement.getProjectName(this.display);
        const reader = this.newProjectElement.getFileReader(this.display);
        if (name === null || reader === null)
            return;
        reader.addEventListener("load", () => {
            const data = {
                "name": name,
                "description": "",
                "code": reader.result,
            };
            console.log(data);
            this.display.reportError("Project creation requested...");
            this.post("projects", data,
                (r) => {
                    this.showText(r);
                    runAfterDelay(1000, () => this.list());
                }
            );
        });
    }

    newProject(): void {
        const npe = this.newProjectElement.setTitle(null, "Specify project name", "Create a new project");
        this.root.appendChild(npe);
        this.newProjectElement.submit.onclick = () => this.createNewProject();
    }

    list(): void {
        this.get("projects", response => response.json().then(r => this.show(r)));
    }

    removeTable(): void {
        if (this.table != null) {
            this.table.remove();
        }
        this.table = null;
    }

    show(projects: Project[]): void {
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

    showProject(project: Project) {
        let pd = new ProjectDisplay(project, this.parent, this);
        this.removeTable();
        this.clearError();
        this.parent.switchChild(pd);
    }

    clearError(): void {
        this.display.clear();
    }
}

class RootWindow implements IHtmlElement {
    readonly root: HTMLElement;
    child: IHtmlElement;  // one of ProjectDisplay or ProjectListDisplay 

    constructor() {
        this.root = document.createElement("div");
        let child = new ProjectListDisplay(this);
        this.child = child;
        this.switchChild(child);
        child.list();
    }

    switchChild(child: IHtmlElement): void {
        const newRoot = child.getHTMLRepresentation();
        this.child.getHTMLRepresentation().remove();
        this.child = child;
        this.root.appendChild(newRoot);
    }

    getHTMLRepresentation(): HTMLElement {
        return this.root;
    }

    public loaded(): void {
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
const _global = window as any;
_global.created = function () {
    const root = new RootWindow();
    root.loaded();
};
