import type { EncodableAsString } from "./util";

/** A set where each element can have a positive or negative count. */
export class ZSet<T extends EncodableAsString> {
    /** Map the string representation of the data to the value itself and the weight. */
    readonly data: Map<string, [T, number]>;

    constructor() {
        this.data = new Map();
    }

    add(value: T, weight: number) {
        let valueString = value.asString();
        if (this.data.has(valueString)) {
            let w = this.data.get(valueString)![1] + weight;
            if (w === 0) {
                this.data.delete(valueString);
            } else {
                this.data.set(valueString, [value, w]);
            }
        } else {
            this.data.set(valueString, [value, weight]);
        }
    }

    getWeight(value: T): number {
        let valueString = value.asString();
        if (this.data.has(valueString))
            return this.data.get(valueString)![1];
        return 0;
    }

    entries(): IterableIterator<[T, number]> {
        return this.data.values()
    }

    /** Build a ZSet from a Set; each element will have weight 1. */
    static fromSet<T extends EncodableAsString>(data: Set<T>): ZSet<T> {
        let result: ZSet<T> = new ZSet();
        for (const datum of data)
            result.add(datum, 1);
        return result;
    }

    /** Build a ZSet from an iterator; each element will be added with weight 1. */
    static fromIterator<T extends EncodableAsString>(data: Iterable<T>): ZSet<T> {
        let result: ZSet<T> = new ZSet();
        for (const datum of data)
            result.add(datum, 1);
        return result;
    }

    /** Number of distinct values in ZSet, ignoring weights. */
    size(): number {
        return this.data.size;
    }

    copy(): ZSet<T> {
        let result: ZSet<T> = new ZSet();
        for (const e of this.entries()) {
            result.add(e[0], e[1]);
        }
        return result;
    }

    append(other: ZSet<T>): ZSet<T> {
        if (this.size() > other.size()) {
            let result = other.copy();
            for (const entry of this.entries()) {
                result.add(entry[0], entry[1]);
            }
            return result;
        } else {
            let result = this.copy();
            for (const entry of other.entries()) {
                result.add(entry[0], entry[1]);
            }
            return result;
        }
    }

    minus(other: ZSet<T>): ZSet<T> {
        return this.append(other.negate());
    }

    negate(): ZSet<T> {
        let result: ZSet<T> = new ZSet();
        for (const e of this.entries()) {
            result.add(e[0], -e[1]);
        }
        return result;
    }

    toString(): string {
        let result = "";
        for (const entry of this.entries()) {
            if (result !== "")
                result += "\n";
            result += entry[0].asString() + "=>" + entry[1]
        }
        return result;
    }
}