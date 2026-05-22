package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

/** Annotation used on operators that are produced for implementing the zero of
 * a global aggregate {@link DBSPAggregateZeroOperator}. */
public class GlobalAggregate extends RegionAnnotation {
    /** All nodes in the same region use the same id */
    public final int id;

    public GlobalAggregate(int id) {
        this.id = id;
    }

    public static GlobalAggregate fromJson(JsonNode node) {
        int id = Utilities.getIntProperty(node, "id");
        return new GlobalAggregate(id);
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof GlobalAggregate that)) return false;
        return this.id == that.id;
    }

    @Override
    public int hashCode() {
        return this.id;
    }

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject().appendClass(this);
        stream.label("id");
        stream.append(this.id);
        stream.endObject();
    }

    @Override
    public String toString() {
        return "GlobalAggregate: " + this.id;
    }

    @Override
    public String getTag() {
        return "agg_zero";
    }

    @Override
    public int getId() {
        return this.id;
    }
}
