package org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;

import java.util.List;

public class JITStreamDistinctOperator extends JITOperator {
    public JITStreamDistinctOperator(long id, JITRowType type, List<JITOperatorReference> inputs) {
        super(id, "StreamDistinct", "", type, inputs, null, null);
    }

    @Override
    public BaseJsonNode asJson() {
        return this.asJsonWithLayout();
    }
}
