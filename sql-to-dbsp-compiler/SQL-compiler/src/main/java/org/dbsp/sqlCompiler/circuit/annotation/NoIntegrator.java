package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

/** Annotation used on a join to indicate that it doesn't need
 * an integrator on one or both sides. */
public class NoIntegrator extends Annotation {
    final boolean notNeededOnLeft;
    final boolean notNeededOnRight;

    public NoIntegrator(boolean notNeededOnLeft, boolean notNeededOnRight) {
        this.notNeededOnLeft = notNeededOnLeft;
        this.notNeededOnRight = notNeededOnRight;
    }

    public static NoIntegrator fromJson(JsonNode node) {
        boolean notNeededOnLeft = Utilities.getBooleanProperty(node, "notNeededOnLeft");
        boolean notNeededOnRight = Utilities.getBooleanProperty(node, "notNeededOnRight");
        return new NoIntegrator(notNeededOnLeft, notNeededOnRight);
    }

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject();
        stream.label("class");
        stream.append(this.getClass().getSimpleName());
        stream.label("notNeededOnLeft");
        stream.append(this.notNeededOnLeft);
        stream.label("notNeededOnRight");
        stream.append(this.notNeededOnRight);
        stream.endObject();
    }

    @Override
    public String toString() {
        return "NoIntegrator[" + this.notNeededOnLeft + "," + this.notNeededOnRight + "]";
    }
}
