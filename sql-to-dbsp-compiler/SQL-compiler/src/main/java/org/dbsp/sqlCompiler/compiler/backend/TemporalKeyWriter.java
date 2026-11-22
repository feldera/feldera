package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;
import org.dbsp.util.Shuffle;

import java.util.IdentityHashMap;

/** Include the bindings and object identities used by temporal key equivalence. */
final class TemporalKeyWriter extends LoweringArguments {
    private final IdentityHashMap<Shuffle, Long> shuffles = new IdentityHashMap<>();

    TemporalKeyWriter(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public void postorder(DBSPForExpression loop) {
        this.stream.label("variable").append(loop.variable);
        super.postorder(loop);
    }

    @Override
    public void postorder(DBSPFlatmap flatmap) {
        long identity = this.shuffles.computeIfAbsent(flatmap.shuffle, ignored -> flatmap.id);
        this.stream.label("shuffleIdentity").append(identity);
        super.postorder(flatmap);
    }
}
