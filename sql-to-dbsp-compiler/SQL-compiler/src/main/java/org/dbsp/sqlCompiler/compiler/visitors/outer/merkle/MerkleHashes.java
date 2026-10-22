package org.dbsp.sqlCompiler.compiler.visitors.outer.merkle;

import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;

import java.util.HashMap;
import java.util.Map;

/** Compute the Merkle hashes of all operators and store them as {@link OperatorHash} annotations.
 * The global hashes are the persistent ids; the local hashes name the generated code.
 * Delta operators and the operators that GC operators read first get a prefix that their global hash includes. */
public class MerkleHashes extends Passes {
    public MerkleHashes(DBSPCompiler compiler) {
        super("MerkleHashes", compiler);
        // Maps an operator a prefix hash that is appended to its global hash
        Map<DBSPOperator, String> prefixes = new HashMap<>();
        MerkleOuter unprefixed = new MerkleOuter(compiler, true);
        this.add(unprefixed);
        this.add(new DeltaHashPrefixes(compiler, unprefixed, prefixes));
        this.add(new GcHashPrefixes(compiler, unprefixed, prefixes));
        MerkleOuter global = new MerkleOuter(compiler, true, MerkleOuter.RECURSIVE_STATE_VERSION, prefixes);
        this.add(global);
        this.add(new SetOperatorHashes(compiler, global));
        this.add(new CheckPersistentIds(compiler));
        MerkleOuter local = new MerkleOuter(compiler, false);
        this.add(local);
        this.add(new SetOperatorHashes(compiler, local));
    }
}
