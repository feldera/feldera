package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DeclareViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema;
import org.dbsp.util.IJson;
import org.dbsp.util.Utilities;

import java.util.LinkedHashMap;

/** Represents metadata about the compiled program.
 * Contains a description of all input tables and all views. */
public class ProgramMetadata implements IJson {
    final LinkedHashMap<ProgramIdentifier, IHasSchema> inputTables;
    final LinkedHashMap<ProgramIdentifier, IHasSchema> outputViews;

    public ProgramMetadata() {
        this.inputTables = new LinkedHashMap<>();
        this.outputViews = new LinkedHashMap<>();
    }

    public ObjectNode asJson() {
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        ArrayNode inputs = mapper.createArrayNode();
        for (IHasSchema input: this.inputTables.values()) {
            if (input.is(DeclareViewStatement.class))
                continue;
            inputs.add(input.asJson(false));
        }
        ArrayNode outputs = mapper.createArrayNode();
        for (IHasSchema output: this.outputViews.values())
            outputs.add(output.asJson(false));
        ObjectNode ios = mapper.createObjectNode();
        ios.set("inputs", inputs);
        ios.set("outputs", outputs);
        return ios;
    }

    public IHasSchema getTableDescription(ProgramIdentifier name) {
        return Utilities.getExists(this.inputTables, name);
    }

    public boolean hasTable(ProgramIdentifier name) {
        return this.inputTables.containsKey(name);
    }

    public boolean hasView(ProgramIdentifier name) {
        return this.outputViews.containsKey(name);
    }

    public IHasSchema getViewDescription(ProgramIdentifier name) {
        return Utilities.getExists(this.outputViews, name);
    }

    public void addTable(IHasSchema description) {
        this.inputTables.put(description.getName(), description);
    }

    public void removeTable(ProgramIdentifier name) {
        this.inputTables.remove(name);
    }

    public void addView(IHasSchema description) {
        this.outputViews.put(description.getName(), description);
    }

    public static ProgramMetadata fromJson(JsonNode node, RelDataTypeFactory typeFactory) {
        ProgramMetadata result = new ProgramMetadata();
        var it = Utilities.getProperty(node, "inputs").elements();
        while (it.hasNext()) {
            JsonNode tbl = it.next();
            IHasSchema sch = IHasSchema.AbstractIHasSchema.fromJson(tbl, typeFactory);
            result.addTable(sch);
        }
        it = Utilities.getProperty(node, "outputs").elements();
        while (it.hasNext()) {
            JsonNode tbl = it.next();
            IHasSchema sch = IHasSchema.AbstractIHasSchema.fromJson(tbl, typeFactory);
            result.addView(sch);
        }
        return result;
    }

    @Override
    public void asJson(ToJsonInnerVisitor visitor) {
        IJson.toJsonStream(this.asJson(), visitor.stream);
    }
}
