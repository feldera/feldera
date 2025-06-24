package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.LazyStatics;

/** Create LazyLock expressions, and insert declarations in the circuit */
public class StaticDeclarations extends CircuitRewriter {
    final LazyStatics lazyStatics;

    public StaticDeclarations(DBSPCompiler compiler, LazyStatics transform) {
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
