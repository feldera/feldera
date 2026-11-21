package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;
import org.dbsp.util.ICastable;
import org.dbsp.util.Properties;
import org.dbsp.util.Result;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Set;

/** Schema operations needed after relational lowering. No Calcite types or expressions
 * are required to preserve a schema, update usage, or print the generated catalog. */
public interface ProgramSchema extends ICastable {
    ProgramIdentifier getName();
    @Nullable Properties getProperties();
    JsonNode asJson(boolean addColumnCaseSensitivity);
    void markColumnUnused(int index);

    /** Collect the names of all preprocessors from the Properties */
    default void collectPreprocessors(Set<String> result) {
        Properties props = this.getProperties();
        if (props != null) {
            var connectors = props.getPropertyValue(CreateTableStatement.CONNECTORS);
            if (connectors != null) {
                Result<JsonNode> validation = Utilities.validateJson(connectors);
                if (validation.isErr())
                    // Validation errors are handled in the compiler front-end,
                    // so this should not really happen.
                    return;

                JsonNode json = validation.data();
                Utilities.enforce(json != null);
                // None of these "if" statements should "fail" after validation
                if (json.isArray()) {
                    for (Iterator<JsonNode> it = json.elements(); it.hasNext(); ) {
                        JsonNode connector = it.next();
                        JsonNode preprocess = connector.get(CreateTableStatement.PREPROCESSOR);
                        if (preprocess != null) {
                            if (preprocess.isArray()) {
                                for (Iterator<JsonNode> p = preprocess.elements(); p.hasNext(); ) {
                                    JsonNode config = p.next();
                                    if (config.has("name")) {
                                        JsonNode name = config.get("name");
                                        if (name.isTextual()) {
                                            result.add(name.asText());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /** Collect the names of all postprocessors from the Properties */
    default void collectPostprocessors(Set<String> result) {
        Properties props = this.getProperties();
        if (props != null) {
            var connectors = props.getPropertyValue(CreateTableStatement.CONNECTORS);
            if (connectors != null) {
                Result<JsonNode> validation = Utilities.validateJson(connectors);
                if (validation.isErr())
                    // Validation errors are handled in the compiler front-end,
                    // so this should not really happen.
                    return;

                JsonNode json = validation.data();
                Utilities.enforce(json != null);
                // None of these "if" statements should "fail" after validation
                if (json.isArray()) {
                    for (Iterator<JsonNode> it = json.elements(); it.hasNext(); ) {
                        JsonNode connector = it.next();
                        JsonNode preprocess = connector.get(CreateViewStatement.POSTPROCESSOR);
                        if (preprocess != null) {
                            if (preprocess.isArray()) {
                                for (Iterator<JsonNode> p = preprocess.elements(); p.hasNext(); ) {
                                    JsonNode config = p.next();
                                    if (config.has("name")) {
                                        JsonNode name = config.get("name");
                                        if (name.isTextual()) {
                                            result.add(name.asText());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    static void addColumnCaseSensitivity(JsonNode repr) {
        JsonNode component = repr.get("component");
        if (component != null) {
            ProgramSchema.addColumnCaseSensitivity(component);
            return;
        }
        JsonNode key = repr.get("key");
        if (key != null) {
            ProgramSchema.addColumnCaseSensitivity(key);
            return;
        }
        JsonNode value = repr.get("value");
        if (value != null) {
            ProgramSchema.addColumnCaseSensitivity(value);
            return;
        }
        JsonNode fields = repr.get("fields");
        if (fields != null) {
            ArrayNode fieldsArray = (ArrayNode) fields;
            for (JsonNode field: fieldsArray) {
                ObjectNode obj = (ObjectNode) field;
                String name = field.get("name").asText();
                boolean caseSensitive = ProgramIdentifier.needsQuotes(name);
                obj.put("case_sensitive", caseSensitive);
                JsonNode type = field.get("type");
                if (type instanceof ObjectNode) {
                    ProgramSchema.addColumnCaseSensitivity(type);
                }
            }
        }
    }

}
