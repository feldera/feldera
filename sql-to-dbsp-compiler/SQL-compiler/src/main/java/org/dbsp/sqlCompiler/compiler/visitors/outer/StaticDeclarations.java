package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ImplementStatics;

/** Create expressions that initialize static values lazily, and insert declarations in the circuit */
public class StaticDeclarations extends CircuitRewriter {
    final ImplementStatics lazyStatics;

    public StaticDeclarations(DBSPCompiler compiler, ImplementStatics transform) {
        super(compiler, transform, false);
        this.lazyStatics = transform;
    }

    @Override
    public void postorder(DBSPCircuit circuit) {
        for (var decl: this.lazyStatics.newDeclarations)
            this.getUnderConstructionCircuit().addDeclaration(new DBSPDeclaration(decl));
        super.postorder(circuit);
    }
}
