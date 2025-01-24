package org.dbsp.sqlCompiler.circuit.annotation;

import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.UnusedFields;

/** Annotation used on a Map/MapIndex which projects fields from the input.
 * These are created by the {@link UnusedFields} analysis.
 * The outputSize is the size of the output of the original operator
 * that the projection was extracted from.  This information is used
 * to prevent such projections to being merged back into operators if
 * they do not improve the graph. */
public class IsProjection extends Annotation {
    public final int outputSize;

    public IsProjection(int outputSize) {
        this.outputSize = outputSize;
    }

    @Override
    public String toString() {
        return super.toString() + "(" + this.outputSize + ")";
    }
}
