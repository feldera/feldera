package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;

/** Invokes an inner visitor on every relevant field of each operator */
public class CircuitDispatcher extends CircuitVisitor {
    public final InnerVisitor innerVisitor;
    final boolean processDeclarations;

    /** Create a {@link CircuitDispatcher}.
     *
     * @param compiler    Compiler.
     * @param innerVisitor   Visitor to apply to optimize each node's functions.
     * @param processDeclarations  If true, process declarations as well.
     */
    public CircuitDispatcher(DBSPCompiler compiler, InnerVisitor innerVisitor,
                             boolean processDeclarations) {
        super(compiler);
        this.innerVisitor = innerVisitor;
        this.processDeclarations = processDeclarations;
    }

    @Override
    public void postorder(DBSPSimpleOperator operator) {
        this.innerVisitor.setOperatorContext(operator);
        operator.accept(this.innerVisitor);
        this.innerVisitor.setOperatorContext(null);
    }

    @Override
    public void postorder(DBSPDeclaration decl) {
        if (this.processDeclarations) {
            decl.item.accept(this.innerVisitor);
        }
    }

    @Override
    public String toString() {
        return super.toString() + "-" + this.innerVisitor;
    }
}
