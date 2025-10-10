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

export function toNumber(x: any): number {
    if (typeof x !== "number") {
        throw new TypeError(`Expected a valid number, got: ${x}`);
    }
    return x;
}

export function fail(message: string): never {
    throw new Error(message);
}

export function assert(condition: any, message: string): asserts condition {
    if (!condition) {
        fail(message);
    }
}

// A Map with an Option return type in the API.
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

// A sublist which includes only specific elements from a list, identified by their indexes.
export class SubList {
    constructor(readonly belongs: (index: number) => boolean) { }

    contains(index: number): boolean {
        return this.belongs(index);
    }

    getSelectedElements<T>(data: Array<T>): Array<T> {
        return data.filter((_, i) => this.contains(i));
    }
}

// An API which describes a set of elements of a bigger set.
export interface SubSet<T> {
    // Full set that is being subsetted.
    getFullSet(): Set<T>;
    // True if the item is in the subset.
    contains(item: T): boolean;
}

// A subset which includes every element of the full list.
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

// A subset which includes only a specific elements from a set.
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

class Edge<T> {
    constructor(
        readonly from: T,
        readonly to: T,
        readonly weight: number,
        readonly back: boolean) { }
}

/** Abstract graph class providing some useful algorithms */
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

    addEdge(from: T, to: T, weight: number, back: boolean): void {
        let edge = new Edge(from, to, weight, back);
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

    // Perform a depth-first search of the graph; return the depth of each node.
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

    // Set of nodes that are reachable starting from 'node' and following edges that satisfy 'predicate'.
    reachableFrom(node: T, predicate: (e: Edge<T>) => boolean): Set<T> {
        let result = new Set<T>();
        let stack = [node];
        while (stack.length > 0) {
            let n = stack.pop()!;
            if (result.has(n)) {
                continue;
            }
            result.add(n);
            for (const e of this.outEdges.get(n) || []) {
                if (predicate(e)) {
                    stack.push(e.to);
                }
            }
        }
        return result;
    }

    // Set of nodes that are backwards reachable starting from 'node' and following edges that satisfy 'predicate'.
    canReach(node: T, predicate: (e: Edge<T>) => boolean): Set<T> {
        let result = new Set<T>();
        let stack = [node];
        while (stack.length > 0) {
            let n = stack.pop()!;
            if (result.has(n)) {
                continue;
            }
            result.add(n);
            for (const e of this.inEdges.get(n) || []) {
                if (predicate(e)) {
                    stack.push(e.from);
                }
            }
        }
        return result;
    }
}

// A range between two numbers (inclusive).
// Ranges can be empty (min > max).
export class NumericRange {
    // Create a new range between min and max.
    // If min > max, the range is empty.
    constructor(public readonly min: number, public readonly max: number) { }
    // Check is a range is empty.
    isEmpty(): boolean { return this.min > this.max; }
    // Check if a range is a single point.
    isPoint(): boolean { return this.min === this.max; }
    // Check if a range contains a specific value.
    contains(x: number): boolean { return x >= this.min && x <= this.max; }
    // Return the union of two ranges: the widest range that contains both.
    union(other: NumericRange): NumericRange {
        if (this.isEmpty()) return other;
        if (other.isEmpty()) return this;
        return new NumericRange(Math.min(this.min, other.min), Math.max(this.max, other.max));
    }
    width(): number {
        if (this.isEmpty()) {
            return 0;
        }
        return this.max - this.min;
    }

    // Return the value of x as a percentage of this range, in the range 0 - 100.
    // If the range is empty or a point return 0.
    percents(x: number): number {
        if (this.isEmpty() || this.isPoint()) {
            return 100;
        }
        return 100 * (x - this.min) / this.width();
    }

    // Given a percentage value in this range, return the corresponding value in the range.
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

    // Compute the range of a set of numbers.
    static getRange(data: Iterable<number>): NumericRange {
        let min = Number.MAX_VALUE;
        let max = Number.MIN_VALUE;
        for (const x of data) {
            min = Math.min(min, x);
            max = Math.max(max, x);
        }
        return new NumericRange(min, max);
    }
    // Create an empty range.
    static empty(): NumericRange { return new NumericRange(Number.MAX_VALUE, Number.MIN_VALUE); }
}

export interface Comparable<T> {
    compareTo(other: T): number;
}

export namespace Comparable {
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