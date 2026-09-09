package org.dbsp.sqlCompiler.ir.type.user;

/** What the values flowing on a stream mean.
 * Operators that consume deltas differ from operators that consume collections,
 * so a circuit rewrite must not mix the two kinds on the inputs of one operator. */
public enum StreamKind {
    /** Each step carries the change of a collection since the previous step. */
    DELTA,
    /** Each step carries a whole collection. */
    COLLECTION,
    /** Each step carries a scalar value that never decreases: a waterline, or a bound that an
     * apply operator computes from waterlines. */
    WATERLINE
}
