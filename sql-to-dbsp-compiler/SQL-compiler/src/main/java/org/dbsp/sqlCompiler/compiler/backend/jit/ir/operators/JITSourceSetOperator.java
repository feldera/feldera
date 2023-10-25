package org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;

public class JITSourceSetOperator extends JITSourceOperator {
    public JITSourceSetOperator(long id, JITRowType type, String table) {
        super(id, type, table);
    }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        ObjectNode data = result.putObject("Source");
        ObjectNode layout = data.putObject("layout");
        layout.put("Set", this.type.to(JITRowType.class).getId());
        result.set("Source", data);
        data.put("kind", "ZSet");
        data.put("table", this.table);
        return result;
    }
}
