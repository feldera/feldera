package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;

/** An expansion usually used for operators that do not expand into different parts */
public class ReplacementDeltaExpansion extends OperatorDeltaExpansion {
    public final DBSPSimpleOperator replacement;

    public ReplacementDeltaExpansion(DBSPSimpleOperator operator) {
        this.replacement = operator;
    }
}
