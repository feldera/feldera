package org.dbsp.sqlCompiler.compiler.visitors.outer.merkle;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.HashString;
import org.dbsp.util.Utilities;

import java.util.Map;

/** Give each Delta operator the hash of its recursive circuit as the prefix of its global hash.
 * Two recursive circuits can import the same stream; without a prefix their Delta operators,
 * and the operators that read these Delta operators, have the same persistent ids. */
public class DeltaHashPrefixes extends CircuitVisitor {
    /** The pass that computed the global hashes, without prefixes. */
    final MerkleOuter firstRound;
    /** Maps an operator to the prefix of its global hash. */
    final Map<DBSPOperator, String> prefixes;

    public DeltaHashPrefixes(DBSPCompiler compiler, MerkleOuter firstRound, Map<DBSPOperator, String> prefixes) {
        super(compiler);
        this.firstRound = firstRound;
        this.prefixes = prefixes;
    }

    @Override
    public void postorder(DBSPDeltaOperator operator) {
        DBSPNestedOperator nested = this.getParent().to(DBSPNestedOperator.class);
        HashString component = Utilities.getExists(this.firstRound.operatorHash, nested);
        Utilities.putNew(this.prefixes, operator, component.toString());
    }
}
