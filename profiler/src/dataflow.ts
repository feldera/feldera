// Parse dataflow.json file and create a (reduced) representation of the program representation

import { Comparable } from "./util.js";

// Serialized JSON representation of a Dataflow graph coming from the SQL compiler.
export interface Dataflow {
    calcite_plan: any;
    mir: MirNodes;
    sources: Array<string>;
}

export interface MirNodes {
    [key: string]: MirNode;
}

export interface OutputPort {
    node: string;
    port: number;
}

export interface MirNode {
    operation: string;
    table?: string;
    view?: string;
    inputs: Array<OutputPort>;
    calcite: any;
    positions: Array<JsonPositionRange>;
    persistent_id?: string;
}

export class Sources {
    constructor(readonly lines: Array<string>) {
        this.lines = lines;
    }

    public toString(position: SourcePositionRange): string {
        let result = "";
        if (position.is_empty()) {
            return result;
        }
        for (let line = position.start.line; line <= position.end.line; line++) {
            result += line.toString().padStart(5, " ") + "| " + this.lines[line - 1] + "\n";
        }
        return result;
    }
}

export class SourcePositionRanges {
    constructor(public readonly positions: Array<SourcePositionRange>) { }

    static empty(): SourcePositionRanges {
        return new SourcePositionRanges(new Array<SourcePositionRange>());
    }

    public combine(): SourcePositionRange {
        if (this.positions.length === 0) {
            return SourcePositionRange.empty();
        }
        let min = this.positions[0]!.start;
        let max = this.positions[0]!.end;
        for (const pos of this.positions) {
            min = Comparable.min(min, pos.start);
            max = Comparable.max(max, pos.end);
        }
        return new SourcePositionRange({ start_line_number: min.line, start_column: min.column, end_line_number: max.line, end_column: max.column });
    }
}

interface JsonPositionRange {
    start_line_number: number;
    start_column: number;
    end_line_number: number;
    end_column: number;
}

export class SourcePosition implements Comparable<SourcePosition> {
    constructor(readonly line: number, readonly column: number) { }

    public compareTo(other: SourcePosition): number {
        let c = Comparable.compareTo(this.line, other.line);
        if (c !== 0) return c;
        return Comparable.compareTo(this.column, other.column);
    }
}

export class SourcePositionRange {
    constructor(readonly range: JsonPositionRange) { }

    static empty(): SourcePositionRange {
        return new SourcePositionRange({ start_line_number: 0, start_column: 0, end_line_number: 0, end_column: 0 });
    }

    public get start(): SourcePosition {
        return new SourcePosition(this.range.start_line_number, this.range.start_column);
    }

    public get end(): SourcePosition {
        return new SourcePosition(this.range.end_line_number, this.range.end_column);
    }

    public is_empty(): boolean {
        return this.start.line === 0 && this.start.column === 0 && this.end.line === 0 && this.end.column === 0;
    }
}