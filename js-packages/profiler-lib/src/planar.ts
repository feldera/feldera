// Support for some simple planar geometry objects

import { assert } from "./util.js";

/** A point in the plane. */
export class Point {
    constructor(readonly x: number, readonly y: number) { }

    pointwiseMax(other: Point): Point {
        return new Point(
            Math.max(this.x, other.x),
            Math.max(this.y, other.y)
        )
    }

    pointwiseMin(other: Point): Point {
        return new Point(
            Math.min(this.x, other.x),
            Math.min(this.y, other.y)
        )
    }

    /** Oriented positive distance between two points.
     * If other is not "above" this, the result is null. */
    minus(other: Point): Size | null {
        let x = this.x - other.x;
        let y = this.y - other.y;
        if (x < 0 || y < 0)
            return null;
        return new Size(x, y);
    }

    static zero(): Point {
        return new Point(0, 0);
    }

    scale(amount: number): Point {
        return new Point(this.x * amount, this.y * amount);
    }

    reflect(): Point {
        return new Point(-this.x, -this.y);
    }

    translate(by: Point): Point {
        return new Point(this.x + by.x, this.y + by.y);
    }

    toString(): string {
        return `Point(${this.x}, ${this.y})`;
    }
}

/** A positive size. */
export class Size {
    constructor(readonly w: number, readonly h: number) {
        assert(w >= 0, "width should be positive: " + w);
        assert(h >= 0, "height should be positive: " + h);
    }

    static zero(): Size {
        return new Size(0, 0);
    }

    scale(amount: number): Size {
        return new Size(this.w * amount, this.h * amount);
    }

    toString(): string {
        return `Size(w = ${this.w}, h = ${this.h})`;
    }
}

/* Represents a rectangle with the edges parallel to the X and Y axes.
   The origin is the top-left point.
   Rectangles are oriented and they cannot have negative dimensions. */
export class Rectangle {
    constructor(readonly origin: Point, readonly size: Size) { }

    bottomRight(): Point {
        return new Point(
            this.origin.x + this.size.w,
            this.origin.y + this.size.h
        )
    }

    /** Create a rectangle from two corners; returns null if the bottomRight
     * corner is not to the bottom or right to the topLeft corner. */
    static fromPoints(topLeft: Point, bottomRight: Point): Rectangle | null {
        if (topLeft.x > bottomRight.x)
            return null;
        if (topLeft.y > bottomRight.y)
            return null;
        return new Rectangle(topLeft, bottomRight.minus(topLeft)!);
    }

    // Compute the intersection between two rectangles
    intersect(other: Rectangle): Rectangle | null {
        const topLeft = this.origin.pointwiseMax(other.origin);
        const bottomRight = this.bottomRight().pointwiseMin(other.bottomRight());
        return Rectangle.fromPoints(topLeft, bottomRight);
    }

    // Compute a bounding box that contains both rectangles
    boundingBox(other: Rectangle): Rectangle {
        let origin = this.origin.pointwiseMin(other.origin);
        let bottomRight = this.bottomRight().pointwiseMax(other.bottomRight());
        return Rectangle.fromPoints(origin, bottomRight)!;
    }

    scale(amount: number): Rectangle {
        return new Rectangle(this.origin.scale(amount), this.size.scale(amount));
    }

    translate(point: Point): Rectangle {
        return new Rectangle(this.origin.translate(point), this.size);
    }
}
