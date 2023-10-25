package org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;
import org.dbsp.util.Linq;

public class JITSourceMapOperator extends JITSourceOperator {
    public final JITRowType keyType;

    public JITSourceMapOperator(long id, JITRowType keyType, JITRowType type, String table) {
        super(id,type, table);
        this.keyType = keyType;
    }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        ObjectNode data = result.putObject("Source");
        ObjectNode layout = data.putObject("layout");
        ArrayNode map = layout.putArray("Map");
        map.add(this.keyType.to(JITRowType.class).getId());
        map.add(this.type.to(JITRowType.class).getId());
        result.set("Source", data);
        data.put("kind", "Upsert");
        data.put("table", this.table);
        return result;
    }
}
