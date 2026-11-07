package org.dbsp.sqlCompiler.ir.type.user;

/** What the values flowing on a stream mean, and how a step's value is stored.
 * Operators that consume deltas differ from operators that consume collections,
 * so a circuit rewrite must not mix the two kinds on the inputs of one operator.
 * DELTA and COLLECTION streams carry a step's value as a sequence of batches. */
public enum StreamKind {
    /** Each step carries the change of a collection since the previous step. */
    DELTA,
    /** Each step carries a whole collection. */
    COLLECTION,
    /** Each step carries a scalar value that never decreases: a waterline, or a bound that an
     * apply operator computes from waterlines. */
    WATERLINE,
    /** Each step carries the change of a collection since the previous step, as one spine
     * that moves to its single consumer. */
    DELTA_SPINE,
    /** Each step carries the change of a collection since the previous step, as one
     * read-only spine that every worker reads. */
    DELTA_SNAPSHOT,
    /** Each step carries a whole collection, as one read-only spine that every worker reads. */
    COLLECTION_SNAPSHOT
}
