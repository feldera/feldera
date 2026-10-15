package org.dbsp.sqlCompiler.compiler.visitors.outer.merkle;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Give each operator that GC operators read on input 0 a prefix made of the hashes of these
 * GC operators.  Copies of an operator whose traces are garbage-collected with different bounds
 * then have different persistent ids.
 *
 * <p>The hashes come from a first {@link MerkleOuter} round: a GC operator reads the operator,
 * so the global hash of the GC operator depends on the global hash of the operator.  The hashes
 * are sorted, so the prefix does not depend on the order in which the GC operators are found. */
public class GcHashPrefixes extends CircuitVisitor {
    /** The pass that computed the global hashes, without prefixes. */
    final MerkleOuter firstRound;
    /** Maps an operator to the prefix of its global hash. */
    final Map<DBSPOperator, String> prefixes;
    /** Maps an operator to the first-round hashes of the GC operators that read it on input 0. */
    final Map<DBSPOperator, List<String>> gcHashes;

    public GcHashPrefixes(DBSPCompiler compiler, MerkleOuter firstRound, Map<DBSPOperator, String> prefixes) {
        super(compiler);
        this.firstRound = firstRound;
        this.prefixes = prefixes;
        this.gcHashes = new HashMap<>();
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.gcHashes.clear();
        return super.startVisit(node);
    }

    @Override
    public void postorder(DBSPOperator operator) {
        if (!operator.is(IGCOperator.class))
            return;
        // Only the trace of input 0 is garbage-collected
        DBSPOperator data = operator.inputs.get(0).operator;
        String hash = Utilities.getExists(this.firstRound.operatorHash, operator).toString();
        this.gcHashes.computeIfAbsent(data, k -> new ArrayList<>()).add(hash);
    }

    @Override
    public void endVisit() {
        for (Map.Entry<DBSPOperator, List<String>> entry: this.gcHashes.entrySet()) {
            List<String> hashes = entry.getValue();
            hashes.sort(String::compareTo);
            String gcPrefix = String.join(";", hashes);
            // A Delta operator already has a prefix
            this.prefixes.merge(entry.getKey(), gcPrefix, (deltaPrefix, gc) -> deltaPrefix + ";" + gc);
        }
        super.endVisit();
    }
}
