package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.annotation.CompactName;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;

/** Allocate compact names for output streams, to make the Rust easier to read and debug */
public class CompactNames extends CircuitCloneVisitor {
    int id = 0;

    public CompactNames(IErrorReporter errorReporter) {
        super(errorReporter, false);
    }

    @Override
    public VisitDecision preorder(DBSPSimpleOperator operator) {
        operator.addAnnotation(new CompactName("s" + id++));
        return super.preorder(operator);
    }
}
