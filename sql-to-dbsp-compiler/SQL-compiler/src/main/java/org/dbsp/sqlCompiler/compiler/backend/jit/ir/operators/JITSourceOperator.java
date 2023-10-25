package org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators;

import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;
import org.dbsp.util.Linq;

public abstract class JITSourceOperator extends JITOperator {
    public final String table;

    public JITSourceOperator(long id, JITRowType type, String table) {
        super(id,"Source", "", type, Linq.list(), null, null);
        this.table = table;
    }
}
