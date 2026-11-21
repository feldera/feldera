package org.dbsp.sqlCompiler.compiler.visitors.outer.merkle;

import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.HashString;
import org.dbsp.util.Utilities;

/** Copy the hashes computed by a {@link MerkleOuter} pass into {@link OperatorHash} annotations. */
public class SetOperatorHashes extends CircuitVisitor {
    final MerkleOuter merkle;

    public record ComputedHashes(boolean global, java.util.Map<Long, String> hashes) {}

    public ComputedHashes computedHashes() {
        return new ComputedHashes(this.merkle.includeInputs, this.merkle.hashesById(op -> true));
    }

    public SetOperatorHashes(DBSPCompiler compiler, MerkleOuter merkle) {
        super(compiler);
        this.merkle = merkle;
    }

    @Override
    public void postorder(DBSPOperator operator) {
        HashString hash = Utilities.getExists(this.merkle.operatorHash, operator);
        OperatorHash replacement = new OperatorHash(hash, this.merkle.includeInputs);
        // Rehashing a decoded S3 must replace stale ids while retaining annotation order.
        var annotations = operator.annotations.annotations;
        for (int i = 0; i < annotations.size(); i++) {
            if (annotations.get(i) instanceof OperatorHash previous && previous.global == replacement.global) {
                annotations.set(i, replacement);
                return;
            }
        }
        operator.annotations.add(replacement);
    }
}
