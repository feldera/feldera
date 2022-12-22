import { IHtmlElement, SpecialChars } from "./ui";
/**
 * Core interface for reporting errors.
 */
export interface ErrorReporter {
    /**
     * Report an error
     * @param message  Text message.
     */
    reportError(message: string): void;
    /**
     * Clear all displayed error messages.
     * (May do nothing for some implementations, such as a console).
     */
    clear(): void;
}

/**
 * An error reporter that writes messages to the JavaScript browser console.
 */
export class ConsoleErrorReporter implements ErrorReporter {
    public static instance: ConsoleErrorReporter = new ConsoleErrorReporter();

    public reportError(message: string): void {
        console.log(message);
    }

    public clear(): void {
        // We cannot clear the console
    }
}

/**
 * This class is used to display error messages in the browser window.
 */
export class ErrorDisplay implements IHtmlElement, ErrorReporter {
    protected topLevel: HTMLElement;
    protected console: HTMLDivElement;
    protected clearButton: HTMLElement;
    protected copyButton: HTMLElement;

    constructor() {
        this.topLevel = document.createElement("div");
        this.console = document.createElement("div");
        this.console.className = "console";
        const container = document.createElement("span");
        this.topLevel.appendChild(container);

        this.copyButton = document.createElement("span");
        this.copyButton.innerHTML = SpecialChars.scissors;
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

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public reportError(message: string): void {
        this.console.innerText = message;
        this.clearButton.style.display = "block";
        this.copyButton.style.display = "block";
    }

    public clear(): void {
        this.console.textContent = "";
        this.clearButton.style.display = "none";
        this.copyButton.style.display = "none";
    }

    public copy(): void {
        navigator.clipboard.writeText(this.console.innerText);
    }
}