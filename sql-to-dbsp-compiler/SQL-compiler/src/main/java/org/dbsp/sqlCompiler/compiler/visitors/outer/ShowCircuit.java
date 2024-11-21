package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.CircuitToString;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;

/** Debugging visitor which prints the circuit on stdout. */
@SuppressWarnings("unused")
public class ShowCircuit extends CircuitVisitor {
    public ShowCircuit(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        Token result = super.startVisit(node);
        System.out.println(CircuitToString.toString(node));
        return result;
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        return VisitDecision.STOP;
    }
}
