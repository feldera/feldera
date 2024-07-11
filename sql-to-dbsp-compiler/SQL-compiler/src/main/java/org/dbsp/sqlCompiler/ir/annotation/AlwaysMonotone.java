package org.dbsp.sqlCompiler.ir.annotation;

/** This annotation indicates that a DBSPOperator has an output that is always monotone.
 * This is intended to mark the part of the circuit that derives from the NOW table. */
public class AlwaysMonotone extends Annotation {
    @Override
    public String toString() {
        return "AlwaysMonotone";
    }
}
