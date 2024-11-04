package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;

/** An expansion usually used for operators that do not expand into different parts */
public class ReplacementExpansion extends OperatorExpansion {
    public final DBSPSimpleOperator replacement;

    public ReplacementExpansion(DBSPSimpleOperator operator) {
        this.replacement = operator;
    }
}
