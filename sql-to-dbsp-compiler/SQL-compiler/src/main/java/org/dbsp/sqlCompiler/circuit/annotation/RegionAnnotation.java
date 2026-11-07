package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import java.util.Objects;

/** Puts an operator in a region of the DBSP circuit graph.
 * The operators of one region have equal annotations. */
public final class RegionAnnotation extends Annotation {
    /** Labels the region */
    private final String tag;
    /** Unique among the regions with the same tag */
    private final long id;

    public RegionAnnotation(String tag, long id) {
        this.tag = tag;
        this.id = id;
    }

    public String getTag() {
        return this.tag;
    }

    public long getId() {
        return this.id;
    }

    public String asVarName() {
        return this.tag + "_" + this.id;
    }

    public String getTagQuoted() {
        return Utilities.doubleQuote(this.tag, true);
    }

    public static RegionAnnotation fromJson(JsonNode node) {
        String tag = Utilities.getStringProperty(node, "tag");
        long id = Utilities.getLongProperty(node, "id");
        return new RegionAnnotation(tag, id);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RegionAnnotation that)) return false;
        return this.id == that.id && this.tag.equals(that.tag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.tag, this.id);
    }

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject().appendClass(this);
        stream.label("tag");
        stream.append(this.tag);
        stream.label("id");
        stream.append(this.id);
        stream.endObject();
    }

    @Override
    public String toString() {
        return "Region: " + this.tag + " " + this.id;
    }
}
