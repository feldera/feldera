package org.dbsp.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.parser.PropertyList;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragment;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/** A higher-level representation of a PropertyList, after validation (no duplicates) */
public class Properties implements IJson {
    record PropertyValue(
        SourcePositionRange keyPosition,
        SourcePositionRange valuePosition,
        String value) {

        PropertyValue(String value) {
            this(SourcePositionRange.INVALID, SourcePositionRange.INVALID, value);
        }
    }

    final Map<String, PropertyValue> propertyValue;

    public Properties() {
        this.propertyValue = new HashMap<>();
    }

    public Properties(PropertyList list) {
        this.propertyValue = new HashMap<>();
        for (Map.Entry<SqlFragment, SqlFragment> p: list) {
            this.propertyValue.put(p.getKey().getString(),
                    new PropertyValue(p.getKey().getSourcePosition(),
                            p.getValue().getSourcePosition(),
                            p.getValue().getString()));
        }
    }

    @Nullable
    public String getPropertyValue(String propertyName) {
        PropertyValue val = this.propertyValue.get(propertyName);
        if (val != null)
            return val.value;
        return null;
    }

    public boolean hasKey(String propertyName) {
        return this.propertyValue.containsKey(propertyName);
    }

    /** Serialization as JSON for metadata users */
    public JsonNode asJson() {
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        for (var e: this.propertyValue.entrySet()) {
            ObjectNode obj = result.putObject(e.getKey());
            PropertyValue value = e.getValue();
            obj.put("value", value.value);
            obj.set("key_position", value.keyPosition.asJson());
            obj.set("value_position", value.valuePosition.asJson());
        }
        return result;
    }

    /** Serializatino as JSON for reading back the circuit; different
     * from the one above. */
    @Override
    public void asJson(ToJsonInnerVisitor visitor) {
        visitor.stream.beginObject();
        for (var e: this.propertyValue.entrySet()) {
            visitor.stream.label(e.getKey());
            visitor.stream.append(e.getValue().value);
        }
        visitor.stream.endObject();
    }

    public static Properties fromJson(JsonNode node) {
        Properties result = new Properties();
        var it = node.fields();
        while (it.hasNext()) {
            var entry = it.next();
            result.propertyValue.put(entry.getKey(), new PropertyValue(entry.getValue().asText()));
        }
        return result;
    }
}
