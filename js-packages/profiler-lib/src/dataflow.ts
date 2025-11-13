// Parse a dataflow.json file produced by the SQL compiler using the --dataflow flag,
// and create a (reduced) representation of the program representation

import { Comparable } from "./util.js";

/** Serialized JSON representation of a Dataflow graph coming from the SQL compiler. */
export interface Dataflow {
    calcite_plan: any;
    mir: MirNodes;
    sources: Array<string>;
}

export interface MirNodes {
    [key: string]: MirNode | NestedMirNode;
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
    // We don't care about this yet
    calcite: any;
    positions: Array<JsonPositionRange>;
    persistent_id?: string;
}

export interface NestedNodeFixed {
    operation: "nested";
    outputs: Array<OutputPort>;
}

type NestedMirNode = NestedNodeFixed & {
    [key: string]: MirNode;
}

export interface OutputPort {
    node: string,
    output: number,
}

/** Represents the (SQL) sources of a program. */
export class Sources {
    constructor(readonly lines: Array<string>) {
        this.lines = lines;
    }

    prefix(line: number): string {
        return line.toString().padStart(5, " ") + "| ";
    }

    /** Given a position range, generate a string with the relevant source fragment. */
    public toStringFromPositionRange(range: SourcePositionRange): string {
        if (range.is_empty()) return "";
        if (range.range.start_line_number == range.range.end_line_number) {
            let prefix = this.prefix(range.start.line);
            let line = prefix + this.lines[range.start.line - 1] + "\n";
            line += " ".repeat(prefix.length + range.start.column - 1) +
                "^".repeat(range.end.column - range.start.column + 1) + "\n";
            return line;
        } else {
            let result = "";
            for (let line = range.start.line; line <= range.end.line; line++) {
                result += this.prefix(line) + this.lines[line - 1] + "\n";
            }
            return result;
        }
    }

    /** Given multiple position ranges, generate the relevant source fragment. */
    public toString(positions: SourcePositionRanges): string {
        let result = "";
        for (const range of positions.sort()) {
            result += this.toStringFromPositionRange(range);
        }
        return result;
    }
}

/** Zero or more SourcePositionRange values. */
export class SourcePositionRanges {
    constructor(public readonly positions: Array<SourcePositionRange>) { }

    static empty(): SourcePositionRanges {
        return new SourcePositionRanges(new Array<SourcePositionRange>());
    }

    sort(): Array<SourcePositionRange> {
        return this.positions.sort((a, b) => a.start.compareTo(b.start));
    }

    // Append includes deduplication
    append(other: SourcePositionRanges) {
        for (const position of other.positions) {
            if (!this.positions.find(v => v.equals(position))) {
                this.positions.push(position);
            }
        }
    }
}

interface JsonPositionRange {
    start_line_number: number;
    start_column: number;
    end_line_number: number;
    end_column: number;
}

/** A position in the source code; lines and columns are numbered from 1. */
export class SourcePosition implements Comparable<SourcePosition> {
    constructor(readonly line: number, readonly column: number) { }

    public compareTo(other: SourcePosition): number {
        let c = Comparable.compareTo(this.line, other.line);
        if (c !== 0) return c;
        return Comparable.compareTo(this.column, other.column);
    }
}

/** A range between two source positions; the second one must be after the first one. */
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

    public equals(position: SourcePositionRange): unknown {
        return this.range.start_column === position.range.start_column &&
            this.range.start_line_number === position.range.start_line_number &&
            this.range.end_line_number === position.range.end_line_number &&
            this.range.end_column === position.range.end_column;
    }
}