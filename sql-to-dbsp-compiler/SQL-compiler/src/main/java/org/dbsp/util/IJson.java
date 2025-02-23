package org.dbsp.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;

import java.util.Iterator;

public interface IJson {
    void asJson(ToJsonInnerVisitor visitor);

    static void toJsonStream(JsonNode node, JsonStream stream) {
        if (node.isObject()) {
            stream.beginObject();
            Iterator<String> it = node.fieldNames();
            while (it.hasNext()) {
                String field = it.next();
                stream.label(field);
                toJsonStream(node.get(field), stream);
            }
            stream.endObject();
        } else if (node.isArray()) {
            stream.beginArray();
            node.forEach(element -> toJsonStream(element, stream));
            stream.endArray();
        } else if (node.isLong()) {
            stream.append(node.asLong());
        } else if (node.isBoolean()) {
            stream.append(node.asBoolean());
        } else if (node.isNull()) {
            stream.append("null");
        } else if (node.isBigDecimal()) {
            stream.append(node.toString());
        } else if (node.isTextual()) {
            stream.append(node.asText());
        } else if (node.isInt()) {
            stream.append(node.asInt());
        } else {
            throw new UnimplementedException();
        }
    }
}
