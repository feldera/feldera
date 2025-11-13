/** Rust-like Option type for safe handling of null values. */
export class Option<T> {
    private constructor(private readonly value: T | null) { }

    static some<T>(value: T): Option<T> {
        if (value === null || value === undefined) {
            throw new Error('Cannot create Option.some with null or undefined');
        }
        return new Option(value);
    }

    static none<T>(): Option<T> {
        return new Option<T>(null);
    }

    isSome(): boolean {
        return this.value !== null;
    }

    isNone(): boolean {
        return this.value === null;
    }

    unwrap(): T {
        if (this.isNone()) {
            throw new Error('Called unwrap on Option.none');
        }
        return this.value as T;
    }

    expect(message: string): T {
        if (this.isNone()) {
            throw new Error(message);
        }
        return this.value as T;
    }

    unwrapOr(defaultValue: T): T {
        return this.isSome() ? (this.value as T) : defaultValue;
    }

    map<U>(fn: (value: T) => U): Option<U> {
        return this.isSome() ? Option.some(fn(this.value as T)) : Option.none<U>();
    }

    flatMap<U>(fn: (value: T) => Option<U>): Option<U> {
        return this.isSome() ? fn(this.value as T) : Option.none<U>();
    }

    match<U>(handlers: { some: (value: T) => U; none: () => U }): U {
        return this.isSome() ? handlers.some(this.value as T) : handlers.none();
    }

    toString(): string {
        return this.isSome() ? `Some(${this.value})` : 'None';
    }
}

/** Make sure that the supplied value is a number. */
export function enforceNumber(x: any): number {
    if (typeof x !== "number") {
        throw new TypeError(`Expected a valid number, got: ${x}`);
    }
    return x;
}

/** Throws an error with the specified message. */
export function fail(message: string): never {
    throw new Error(message);
}

/** Throws an error with the specified message if the condition is 'falsey'. */
export function assert(condition: any, message: string): asserts condition {
    if (!condition) {
        fail(message);
    }
}

/** A Map using an Option for lookups. */
export class OMap<K, V> {
    private map = new Map<K, V>();

    get(key: K): Option<V> {
        if (this.map.has(key)) {
            return Option.some(this.map.get(key)!);
        } else {
            return Option.none();
        }
    }

    set(key: K, value: V): void {
        this.map.set(key, value);
    }

    has(key: K): boolean {
        return this.map.has(key);
    }

    entries(): IterableIterator<[K, V]> {
        return this.map.entries();
    }

    keys(): IterableIterator<K> {
        return this.map.keys();
    }

    values(): IterableIterator<V> {
        return this.map.values();
    }

    get size(): number {
        return this.map.size;
    }

    delete(key: K): boolean {
        return this.map.delete(key);
    }

    clear(): void {
        this.map.clear();
    }
}

/** A sublist which includes only specific elements from a list, identified by their indexes. */
export class SubList {
    /** Build a sublist from a function which decides which elements belong to the sublist.
     * The function receives an element index as an argument and returns 'true' if the element is in the sublist. */
    constructor(readonly belongs: (index: number) => boolean) { }

    contains(index: number): boolean {
        return this.belongs(index);
    }

    getSelectedElements<T>(data: Array<T>): Array<T> {
        return data.filter((_, i) => this.contains(i));
    }
}

/** Represents a subset of elements from a set */
export interface SubSet<T> {
    /** Full set that is being subsetted. */
    getFullSet(): Set<T>;
    /** True if the item is in the subset. */
    contains(item: T): boolean;
}

/** A subset which includes every element of the full list. */
export class CompleteSet<T> implements SubSet<T> {
    readonly fullSet: Set<T>;

    constructor(fullSet: Iterable<T>) {
        this.fullSet = new Set(fullSet);
    }

    getFullSet(): Set<T> {
        return this.fullSet;
    }

    contains(item: T): boolean {
        return this.fullSet.has(item);
    }
}

/** A subset which includes only a specific elements from a set. */
export class ExplicitSubSet<T> implements SubSet<T> {
    readonly fullSet: Set<T>;
    readonly elements: Set<T>;

    constructor(fullSet: Set<T>, elements: Set<T>) {
        this.fullSet = fullSet;
        this.elements = elements;
    }

    getFullSet(): Set<T> {
        return this.fullSet;
    }

    contains(item: T): boolean {
        return this.elements.has(item);
    }
}

/** Represents a graph edge. */
export class Edge<T> {
    // Each edge of a graph is supposed to have a distinct id.
    // The from-to information is not sufficient for multigraphs.
    constructor(
        readonly from: T,
        readonly to: T,
        readonly weight: number,
        readonly back: boolean,
        readonly id: string) { }
}

/** Abstract graph class providing some useful algorithms */
// Note with this implementation T is constrained to be a base type.
export class Graph<T> {
    nodes: Set<T>;
    edges: Set<Edge<T>>;
    inEdges = new Map<T, Set<Edge<T>>>();
    outEdges = new Map<T, Set<Edge<T>>>();

    public constructor() {
        this.nodes = new Set();
        this.edges = new Set();
    }

    addNode(node: T): void {
        this.nodes.add(node);
    }

    /** Add an edge; generates a unique id for the edge. */
    addEdge(from: T, to: T, weight: number, back: boolean): void {
        let identical_count = 0;
        for (const e of this.edges) {
            if (e.from === from && e.to === to) {
                identical_count++;
            }
        }
        let id = from + "->" + to;
        if (identical_count > 0) {
            id += ":" + (identical_count + 1);
        }

        let edge = new Edge(from, to, weight, back, id);
        if (!this.nodes.has(from)) {
            throw new Error(`Node ${from} not in graph`);
        }
        if (!this.nodes.has(to)) {
            throw new Error(`Node ${to} not in graph`);
        }
        this.edges.add(edge);

        if (!this.outEdges.has(from)) {
            this.outEdges.set(from, new Set());
        }
        this.outEdges.get(from)!.add(edge);
        if (!this.inEdges.has(to)) {
            this.inEdges.set(to, new Set());
        }
        this.inEdges.get(to)!.add(edge);
    }

    private traverse(node: T, depth: Map<T, number>): number {
        if (depth.has(node)) {
            return depth.get(node)!;
        }
        let maxDepth = 0;
        for (const i of this.inEdges.get(node) ?? []) {
            if (i.back)
                continue;

            let d = this.traverse(i.from, depth);
            maxDepth = Math.max(maxDepth, d + i.weight);
        }
        depth.set(node, maxDepth);
        return maxDepth;
    }

    /** Perform a depth-first search of the graph; return the depth of each node. */
    dfs(): Map<T, number> {
        let depth = new Map<T, number>();
        for (const n of this.nodes) {
            if (!depth.has(n)) {
                this.traverse(n, depth);
            }
        }

        // Heuristic: If a node has a back-edge, assign it a depth which is 1 less than any target.
        for (const e of this.edges) {
            if (e.back) {
                let sourceDepth = depth.get(e.from)!;
                let targetDepth = depth.get(e.to)!;
                if (sourceDepth > targetDepth) {
                    depth.set(e.to, Math.max(0, sourceDepth));
                }
            }
        }
        return depth;
    }

    /** Set of edges that are reachable starting from 'node' and following edges that satisfy 'predicate'. */
    reachableFrom(node: T, predicate: (e: Edge<T>) => boolean): Set<Edge<T>> {
        let result = new Set<Edge<T>>();
        let stack = [node];
        let visited = new Set<T>();
        while (stack.length > 0) {
            let n = stack.pop()!;
            if (visited.has(n)) {
                continue;
            }
            visited.add(n);
            for (const e of this.outEdges.get(n) || []) {
                if (predicate(e)) {
                    stack.push(e.to);
                    result.add(e);
                }
            }
        }
        return result;
    }

    /** Set of edges that are backwards reachable starting from 'node' and following edges that satisfy 'predicate'. */
    canReach(node: T, predicate: (e: Edge<T>) => boolean): Set<Edge<T>> {
        let result = new Set<Edge<T>>();
        let stack = [node];
        let visited = new Set<T>();
        while (stack.length > 0) {
            let n = stack.pop()!;
            if (visited.has(n)) {
                continue;
            }
            visited.add(n);
            for (const e of this.inEdges.get(n) || []) {
                if (predicate(e)) {
                    stack.push(e.from);
                    result.add(e);
                }
            }
        }
        return result;
    }
}

/** A range between two numbers (inclusive).
 * Ranges can be empty (min > max). */
export class NumericRange {
    /** Create a new range between min and max.
        If min > max, the range is empty. */
    constructor(public readonly min: number, public readonly max: number) { }
    /** Check is a range is empty. */
    isEmpty(): boolean { return this.min > this.max; }
    /** Check if a range is a single point. */
    isPoint(): boolean { return this.min === this.max; }
    /** Check if a range contains a specific value. */
    contains(x: number): boolean { return x >= this.min && x <= this.max; }
    /** Return the union of two ranges: the widest range that contains both. */
    union(other: NumericRange): NumericRange {
        if (this.isEmpty()) return other;
        if (other.isEmpty()) return this;
        return new NumericRange(Math.min(this.min, other.min), Math.max(this.max, other.max));
    }
    /** The size of this range. */
    width(): number {
        if (this.isEmpty()) {
            return 0;
        }
        return this.max - this.min;
    }

    /** Return the value of x as a percentage of this range, in the range 0 - 100 if the
     * value is inside the range.
     * If the range is empty or a point return 0. */
    percents(x: number): number {
        if (this.isEmpty() || this.isPoint()) {
            return 100;
        }
        return 100 * (x - this.min) / this.width();
    }

    /* Given a percentage value in this range, return the corresponding value in the range. */
    quantile(percentage: number): number {
        if (this.isEmpty() || this.isPoint()) {
            return this.min;
        }
        return this.min + percentage / 100 * this.width();
    }

    toString(): string {
        if (this.isEmpty()) {
            return "[empty]";
        }
        if (this.isPoint()) {
            return `[${this.min}]`;
        }
        return `[${this.min} - ${this.max}]`;
    }

    /** Return the range of a set of numbers. */
    static getRange(data: Iterable<number>): NumericRange {
        let min = Number.MAX_VALUE;
        let max = Number.MIN_VALUE;
        for (const x of data) {
            min = Math.min(min, x);
            max = Math.max(max, x);
        }
        return new NumericRange(min, max);
    }

    /** Create an empty range. */
    static empty(): NumericRange { return new NumericRange(Number.MAX_VALUE, Number.MIN_VALUE); }
}

/** Interface implemented by values that can be compared. */
export interface Comparable<T> {
    compareTo(other: T): number;
}

/** Implement some operations on comparable values. */
export namespace Comparable {
    /** Comparison for values that have a less than operation */
    export function compareTo<T>(a: T, b: T): number {
        if (a < b) {
            return -1;
        } else if (a > b) {
            return 1;
        } else {
            return 0;
        }
    }

    export function max<T extends Comparable<T>>(a: T, b: T): T {
        return a.compareTo(b) >= 0 ? a : b;
    }

    export function min<T extends Comparable<T>>(a: T, b: T): T {
        return a.compareTo(b) <= 0 ? a : b;
    }
}

/**
 * Interface for objects which can be encoded as strings.
 * The asString method is supposed to return a canonical string representation of the object
 * such that two strings are equal if and only if the two objects are equal. */
export interface EncodableAsString {
    asString(): string;
}

/** Extract the current stack trace.
 * This function is less useful than it seems, because the stack is based on the runtime
 * Javascript code, and not the compile-time typescript code. */
export function getStack(e: Error): string {
    let result = "";
    const lines = e.stack?.split(/\r?\n/);
    if (!lines)
        return e.stack || "";
    const regex = "(.*)http(.*):(\\d+)/(.*)[?]t=(\\d+):(\\d+):(\\d+)";
    for (const line of lines) {
        let l = line;
        let mr = line.match(regex);
        if (mr) {
            l = mr[4]! + ":" + mr[6] + ":" + mr[7];
        }
        result += l + "\n";
    }
    return result;
}

/** Concatenate a sequence of iterators. */
export function* concat<T>(...iters: IterableIterator<T>[]): IterableIterator<T> {
    for (const iter of iters) {
        yield* iter;
    }
}
