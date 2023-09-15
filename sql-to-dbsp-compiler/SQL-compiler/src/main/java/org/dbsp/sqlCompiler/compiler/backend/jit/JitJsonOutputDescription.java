package org.dbsp.sqlCompiler.compiler.backend.jit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataType;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode.jsonFactory;

public class JitJsonOutputDescription implements JitOutputDescription {
    final List<String> columns;

    public JitJsonOutputDescription() {
        this.columns = new ArrayList<>();
    }

    public void addColumn(String column) {
        this.columns.add(column);
    }

    public JsonNode asJson(String file) {
        ObjectNode result = jsonFactory().createObjectNode();
        result.put("file", file);
        ObjectNode kind = result.putObject("kind");
        ObjectNode json = kind.putObject("Json");
        ObjectNode mappings = json.putObject("mappings");
        for (int i = 0; i < this.columns.size(); i++)
            mappings.put(Integer.toString(i), this.columns.get(i));
        return result;
    }
}
