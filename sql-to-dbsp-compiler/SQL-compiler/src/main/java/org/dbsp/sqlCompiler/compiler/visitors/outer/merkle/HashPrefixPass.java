package org.dbsp.sqlCompiler.compiler.visitors.outer.merkle;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** The two prefix passes share the unprefixed Merkle round and its destination map. */
public abstract class HashPrefixPass extends CircuitVisitor {
    final MerkleOuter firstRound;
    final Map<DBSPOperator, String> prefixes;

    HashPrefixPass(DBSPCompiler compiler, MerkleOuter firstRound, Map<DBSPOperator, String> prefixes) {
        super(compiler);
        this.firstRound = firstRound;
        this.prefixes = prefixes;
    }

    abstract boolean readsHash(DBSPOperator operator);

    public record Inputs(Map<Long, String> hashes, Map<Long, String> prefixes) {}

    public Map<Long, String> prefixesById() {
        var result = new HashMap<Long, String>();
        this.prefixes.forEach((operator, prefix) -> result.put(operator.id, prefix));
        return result;
    }

    public Inputs inputs() {
        return new Inputs(this.firstRound.hashesById(this::readsHash), this.prefixesById());
    }

    public void restore(Map<Long, String> updates, Map<Long, DBSPOperator> operators) {
        updates.forEach((id, prefix) -> this.prefixes.put(Objects.requireNonNull(operators.get(id)), prefix));
    }
}
