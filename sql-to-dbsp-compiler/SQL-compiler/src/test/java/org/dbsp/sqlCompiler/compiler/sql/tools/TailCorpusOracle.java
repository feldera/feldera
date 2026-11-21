package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitStatistics;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.TailStatisticsOracle;

import java.io.IOException;
import java.nio.file.Path;

/** Stream both sides of the Java compatibility hash from each frozen SQL compilation. */
public final class TailCorpusOracle extends FrozenCircuitObserver {
    private TailCorpusOracle(DBSPCompiler compiler, ObjectNode observation) {
        super(compiler, observation);
    }

    private void snapshot(String key, DBSPCircuit circuit) {
        if (this.observation.has(key))
            throw new IllegalStateException("Duplicate tail boundary " + key);
        this.observation.set(key, this.snapshot(circuit));
    }

    @Override
    public void before(String container, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {
        if (container.equals("Optimizer") && pass.getName().equals("CheckHints"))
            this.snapshot("input", circuit);
        if (container.equals("Optimizer") && pass.getName().equals("MerkleHashes"))
            this.snapshot("before_hash", circuit);
    }

    @Override
    public void after(String container, int index, int iteration, CircuitTransform pass,
                      DBSPCircuit circuit, boolean completed) {
        if (!completed || circuit == null || !container.equals("Optimizer"))
            return;
        if (pass.getName().equals("MerkleHashes"))
            this.snapshot("after_hash", circuit);
        if (pass instanceof CircuitStatistics statistics) {
            this.snapshot("output", circuit);
            this.observation.set("statistics", TailStatisticsOracle.counters(statistics));
        }
    }

    @Override
    void finish() {
        for (String boundary : new String[] {"input", "before_hash", "after_hash", "output"})
            this.observation.required(boundary);
    }

    public static void main(String[] args) throws IOException {
        run(Path.of(args[0]), TailCorpusOracle::new);
    }
}
