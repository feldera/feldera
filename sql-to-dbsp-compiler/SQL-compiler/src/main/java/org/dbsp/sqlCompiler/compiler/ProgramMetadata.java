package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.util.Utilities;

import java.util.LinkedHashMap;

/** Represents metadata about the compiled program.
 * Contains a description of all input tables and all views. */
public class ProgramMetadata {
    final LinkedHashMap<String, InputTableDescription> inputTables;
    final LinkedHashMap<String, OutputViewDescription> outputViews;

    public ProgramMetadata() {
        this.inputTables = new LinkedHashMap<>();
        this.outputViews = new LinkedHashMap<>();
    }

    public ObjectNode asJson() {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode inputs = mapper.createArrayNode();
        for (InputTableDescription input: this.inputTables.values())
            inputs.add(input.asJson());
        ArrayNode outputs = mapper.createArrayNode();
        for (OutputViewDescription output: this.outputViews.values())
            outputs.add(output.asJson());
        ObjectNode ios = mapper.createObjectNode();
        ios.set("inputs", inputs);
        ios.set("outputs", outputs);
        return ios;
    }

    public InputTableDescription getTableDescription(String name) {
        return Utilities.getExists(this.inputTables, name);
    }

    public OutputViewDescription getViewDescription(String name) {
        return Utilities.getExists(this.outputViews, name);
    }

    public void addTable(InputTableDescription description) {
        this.inputTables.put(description.getName(), description);
    }

    public void addView(OutputViewDescription description) {
        this.outputViews.put(description.getName(), description);
    }
}
