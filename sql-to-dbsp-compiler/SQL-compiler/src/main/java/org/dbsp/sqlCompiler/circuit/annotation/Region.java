package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

/** Annotation used on operators should belong in the same DBSP region. */
public class Region extends Annotation {
    public final String regionName;

    public Region(String regionName) {
        this.regionName = regionName;
    }

    public static Region fromJson(JsonNode node) {
        String regionName = Utilities.getStringProperty(node, "regionName");
        return new Region(regionName);
    }

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject().appendClass(this);
        stream.label("regionName");
        stream.append(this.regionName);
        stream.endObject();
    }

    @Override
    public String toString() {
        return "Region " + this.regionName;
    }
}
