package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.annotation.CompactName;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;

/** Allocate compact names for output streams, to make the Rust easier to read and debug */
public class CompactNames extends CircuitCloneVisitor {
    int id = 0;

    public CompactNames(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        operator.addAnnotation(new CompactName("s" + id++), DBSPOperator.class);
        return super.preorder(operator);
    }
}
