import { Point, Rectangle, Size } from "./planar.js";

/** Displays zoom/pan information in a rectangular page */
export class ViewNavigator {
    /*
        Displayed as:
       +----------+ visualized object
       |          |
    +--+----------+---+ screen
    |  |          |   |
    |  |          |   |
    +--+----------+---+
       +----------+
    */

    // Maximum space in pixels allocated for navigator
    readonly MAX_WIDTH = 100;
    readonly MAX_HEIGHT = 100;

    root: HTMLDivElement;
    screen: HTMLDivElement;
    visualized: HTMLDivElement;

    // Build a navigator as a child of the specified parent element.
    constructor(parent: HTMLElement) {
        this.root = document.createElement("div");
        this.root.id = "navigator";
        this.root.style.position = "relative";
        this.root.style.width = `${this.MAX_WIDTH}px`;
        this.root.style.height = `${this.MAX_HEIGHT}px`;
        this.root.title = "Double click to recenter";
        // delete existing children
        parent.innerHTML = "";
        parent.appendChild(this.root);

        this.screen = this.createRectangle("black");
        this.visualized = this.createRectangle("darkgrey");
    }

    setOnDoubleClick(handler: () => void) {
        this.root.ondblclick = handler;
    }

    createRectangle(color: string): HTMLDivElement {
        const rect = document.createElement("div");
        rect.style.position = "absolute";
        rect.style.border = `2px solid ${color}`;
        rect.style.backgroundColor = "transparent";
        this.root.appendChild(rect);
        return rect;
    }

    setPosition(r: Rectangle, rect: HTMLDivElement) {
        rect.style.left = `${r.origin.x}px`;
        rect.style.top = `${r.origin.y}px`;
        rect.style.width = `${r.size.w}px`;
        rect.style.height = `${r.size.h}px`;
    }

    /** Set the view parameters
     * @param screenSize: size of the screen
     * @param upperLeft: coordinates one screen of the upper left corner of the object
     * @parma objectSize: size of the visualized object
     */
    setViewParameters(screenSize: Size, upperLeft: Point, objectSize: Size) {
        let pageRect = new Rectangle(Point.zero(), screenSize);
        let viewRect = new Rectangle(upperLeft, objectSize);
        const boundingBox = pageRect.boundingBox(viewRect);
        // If the origin is negative
        const min = boundingBox.origin.pointwiseMin(Point.zero()).reflect();
        pageRect = pageRect.translate(min);
        viewRect = viewRect.translate(min);

        let yScale = this.MAX_HEIGHT / boundingBox.size.h;
        let xScale = this.MAX_WIDTH / boundingBox.size.w;
        let scale = Math.min(xScale, yScale);

        pageRect = pageRect.scale(scale);
        this.setPosition(pageRect, this.screen);

        viewRect = viewRect.scale(scale);
        this.setPosition(viewRect, this.visualized);
    }
}