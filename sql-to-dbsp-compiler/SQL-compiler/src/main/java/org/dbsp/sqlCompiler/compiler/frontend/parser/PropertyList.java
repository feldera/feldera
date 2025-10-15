package org.dbsp.sqlCompiler.compiler.frontend.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.runtime.MapEntry;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/** A list of properties and associated values.  Both are just SqlFragments.
 * The list can contain duplicate properties, but there is a utility method
 * to check for that if needed. */
public class PropertyList implements Iterable<Map.Entry<SqlFragment, SqlFragment>> {
    final List<Map.Entry<SqlFragment, SqlFragment>> propertyValue;

    public PropertyList() {
        this.propertyValue = new ArrayList<>();
    }

    /** Returns the first property value associated with the specified property, or null */
    @Nullable
    public SqlFragment getPropertyValue(String propertyName) {
        for (Map.Entry<SqlFragment, SqlFragment> p: this.propertyValue) {
            if (Objects.requireNonNull(p.getKey()).getString().equals(propertyName))
                return p.getValue();
        }
        return null;
    }

    @Nullable
    public SqlFragment getPropertyKey(String propertyName) {
        for (Map.Entry<SqlFragment, SqlFragment> p: this.propertyValue) {
            if (Objects.requireNonNull(p.getKey()).getString().equals(propertyName))
                return p.getKey();
        }
        return null;
    }

    /** Report an error for duplicate property names */
    public void checkDuplicates(IErrorReporter errorReporter) {
        Map<String, SqlFragment> previous = new HashMap<>();
        for (Map.Entry<SqlFragment, SqlFragment> p: this.propertyValue) {
            String keyString = p.getKey().getString();
            if (previous.containsKey(keyString)) {
                SqlFragment prev = Utilities.getExists(previous, keyString);
                errorReporter.reportError(p.getKey().getSourcePosition(),
                        "Duplicate key", "property " + Utilities.singleQuote(keyString) +
                                " already declared");
                errorReporter.reportError(prev.getSourcePosition(),
                        "Duplicate key", "Previous declaration", true);
                continue;
            }
            Utilities.putNew(previous, keyString, p.getKey());
        }
    }

    public void addProperty(SqlFragment key, SqlFragment value) {
        this.propertyValue.add(new MapEntry<>(key, value));
    }

    @Override
    public Iterator<Map.Entry<SqlFragment, SqlFragment>> iterator() {
        return this.propertyValue.iterator();
    }

    public ObjectNode asJson() {
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        ObjectNode properties = mapper.createObjectNode();
        for (Map.Entry<SqlFragment, SqlFragment> entry: this.propertyValue) {
            ObjectNode prop = properties.putObject(entry.getKey().getString());
            prop.put("value", entry.getValue().getString());
            prop.set("key_position", entry.getKey().getSourcePosition().asJson());
            prop.set("value_position", entry.getValue().getSourcePosition().asJson());
        }
        return properties;
    }
}
