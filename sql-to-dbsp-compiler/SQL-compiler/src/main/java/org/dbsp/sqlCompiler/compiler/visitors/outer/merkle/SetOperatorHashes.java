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

    public SetOperatorHashes(DBSPCompiler compiler, MerkleOuter merkle) {
        super(compiler);
        this.merkle = merkle;
    }

    @Override
    public void postorder(DBSPOperator operator) {
        HashString hash = Utilities.getExists(this.merkle.operatorHash, operator);
        operator.annotations.add(new OperatorHash(hash, this.merkle.includeInputs));
    }
}
