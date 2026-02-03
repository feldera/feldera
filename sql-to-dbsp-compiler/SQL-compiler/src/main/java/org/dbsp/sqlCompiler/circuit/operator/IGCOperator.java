package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;

/** Interface implemented by operators that perform Garbage collection. */
public interface IGCOperator extends IDBSPOuterNode {
    DBSPSimpleOperator asOperator();
}
