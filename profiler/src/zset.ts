/** A set where each element can have a positive or negative count. */
export class ZSet<T> {
    readonly data: Map<T, number>;

    constructor() {
        this.data = new Map();
    }

    add(value: T, weight: number) {
        if (this.data.has(value)) {
            let w = this.data.get(value)! + weight;
            if (w === 0) {
                this.data.delete(value);
            } else {
                this.data.set(value, w);
            }
        } else {
            this.data.set(value, weight);
        }
    }

    weight(value: T): number {
        if (this.data.has(value))
            return this.data.get(value)!;
        return 0;
    }

    entries(): IterableIterator<[T, number]> {
        return this.data.entries();
    }

    static fromSet<T>(data: Set<T>): ZSet<T> {
        let result: ZSet<T> = new ZSet();
        for (const datum of data)
            result.add(datum, 1);
        return result;
    }

    static fromIterator<T>(data: Iterable<T>): ZSet<T> {
        let result: ZSet<T> = new ZSet();
        for (const datum of data)
            result.add(datum, 1);
        return result;
    }

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

    except(other: ZSet<T>): ZSet<T> {
        return this.append(other.negate());
    }

    negate(): ZSet<T> {
        let result: ZSet<T> = new ZSet();
        for (const e of this.entries()) {
            result.add(e[0], -e[1]);
        }
        return result;
    }

    toString(toString: (value: T) => string): string {
        let result = "";
        for (const entry of this.entries()) {
            if (result !== "")
                result += "\n";
            result += toString(entry[0]) + "=>" + entry[1]
        }
        return result;
    }
}