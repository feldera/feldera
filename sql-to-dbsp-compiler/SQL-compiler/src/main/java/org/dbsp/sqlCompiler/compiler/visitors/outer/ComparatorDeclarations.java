package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Create LazyStatic expressions, and insert declarations in the circuit */
public class ComparatorDeclarations extends CircuitRewriter {
    final FindComparators findComparators;

    public ComparatorDeclarations(DBSPCompiler compiler, FindComparators transform) {
        super(compiler, transform, false);
        this.findComparators = transform;
    }

    @Override
    public void postorder(DBSPCircuit circuit) {
        for (var decl: this.findComparators.newDeclarations)
            this.getUnderConstruction().addDeclaration(new DBSPDeclaration(decl));
        super.postorder(circuit);
    }
}
