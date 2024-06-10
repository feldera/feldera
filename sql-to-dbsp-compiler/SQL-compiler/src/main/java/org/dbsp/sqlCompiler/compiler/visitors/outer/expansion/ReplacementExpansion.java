package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;

public class ReplacementExpansion extends OperatorExpansion {
    public final DBSPOperator replacement;

    public ReplacementExpansion(DBSPOperator operator) {
        this.replacement = operator;
    }
}
