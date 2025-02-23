package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.UnusedFields;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

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
    public void asJson(JsonStream stream) {
        stream.beginObject()
                .appendClass(this)
                .label("outputSize")
                .append(this.outputSize)
                .endObject();
    }

    public static IsProjection fromJson(JsonNode node) {
        int outputSize = Utilities.getIntProperty(node, "outputSize");
        return new IsProjection(outputSize);
    }

    @Override
    public String toString() {
        return super.toString() + "(" + this.outputSize + ")";
    }
}
