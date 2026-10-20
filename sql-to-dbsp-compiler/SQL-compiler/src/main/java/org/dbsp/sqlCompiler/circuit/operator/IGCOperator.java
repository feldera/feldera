package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;

/** Interface implemented by operators that perform Garbage collection. */
public interface IGCOperator extends IDBSPOuterNode {
    DBSPSimpleOperator asOperator();

    /** The input followed by the integral that the operator garbage collects. */
    OutputPort data();

    /** True if the operator garbage collects the integral after {@code source}. */
    default boolean garbageCollects(DBSPOperator source) {
        return this.data().node() == source;
    }
}
