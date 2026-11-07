package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.annotation.CompactName;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;

/** Allocate compact names for output streams, to make the Rust easier to read and debug.
 * A new name replaces the name that an operator already has. */
public class CompactNames extends CircuitCloneVisitor {
    int id = 0;

    public CompactNames(DBSPCompiler compiler) {
        super(compiler, false);
    }

    void rename(DBSPOperator operator) {
        operator.annotations.remove(CompactName.class);
        operator.addAnnotation(new CompactName("s" + this.id++), DBSPOperator.class);
    }

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        this.rename(operator);
        return super.preorder(operator);
    }

    @Override
    public VisitDecision preorder(DBSPNestedOperator operator) {
        this.rename(operator);
        return super.preorder(operator);
    }
}
