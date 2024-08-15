package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;

/** An expansion usually used for operators that do not expand into different parts */
public class ReplacementExpansion extends OperatorExpansion {
    public final DBSPOperator replacement;

    public ReplacementExpansion(DBSPOperator operator) {
        this.replacement = operator;
    }
}
