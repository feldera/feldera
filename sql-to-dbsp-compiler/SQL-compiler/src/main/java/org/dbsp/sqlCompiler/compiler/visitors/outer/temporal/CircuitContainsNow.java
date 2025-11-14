package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitDispatcher;

/** Apply ContainsNow to every function in a circuit, except the {@link DBSPNowOperator}. */
class CircuitContainsNow extends CircuitDispatcher {
    public CircuitContainsNow(DBSPCompiler compiler) {
        super(compiler, new ContainsNow(compiler, false), false);
    }

    @Override
    public VisitDecision preorder(DBSPNowOperator node) {
        return VisitDecision.STOP;
    }

    public VisitDecision preorder(DBSPOperator node) {
        if (this.found())
            return VisitDecision.STOP;
        return VisitDecision.CONTINUE;
    }

    public boolean found() {
        return this.innerVisitor.to(ContainsNow.class).found;
    }
}
