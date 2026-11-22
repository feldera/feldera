package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FindUnboundedState;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.KeyAnalysis;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.LeftJoinChains;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.LosslessCastKeyAnalysis;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.PortTransformOracle;

import java.io.IOException;
import java.nio.file.Path;

/** Compare the ordered candidate keys of every port on the same live optimizer input. */
public final class CandidateKeyOracle extends FrozenCircuitObserver {
    private final DBSPCompiler compiler;
    private int ordinary;
    private int lossless;
    private int ports;
    private int transforms;
    private int keyed;

    private CandidateKeyOracle(DBSPCompiler compiler, ObjectNode observation) {
        super(compiler, observation);
        this.compiler = compiler;
    }

    private void compare(Iterable<DBSPOperator> operators, KeyAnalysis direct, KeyAnalysis hybrid) {
        for (var operator : operators) {
            if (operator instanceof DBSPNestedOperator nested)
                this.compare(nested.getAllOperators(), direct, hybrid);
            for (int i = 0; i < operator.outputCount(); i++) {
                var port = operator.getOutput(i);
                var expected = direct.getKeys(port);
                var actual = hybrid.getKeys(port);
                if (!expected.keys.equals(actual.keys))
                    throw new AssertionError(this.observation + " at " + port + " expected " + expected + ", got " + actual);
                if (PortTransformOracle.compare(port, direct, hybrid))
                    this.transforms++;
                this.ports++;
                if (!expected.isEmpty())
                    this.keyed++;
            }
        }
    }

    @Override
    public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {
        boolean allowCasts = pass instanceof FindUnboundedState;
        if (!allowCasts && !(pass instanceof LeftJoinChains))
            return;
        var options = this.compiler.options.ioOptions;
        boolean previous = options.rustOptimizerPasses;
        try {
            options.rustOptimizerPasses = false;
            var direct = this.analyze(circuit, allowCasts);
            options.rustOptimizerPasses = true;
            var hybrid = this.analyze(circuit, allowCasts);
            this.compare(circuit.getAllOperators(), direct, hybrid);
        } finally {
            options.rustOptimizerPasses = previous;
        }
        if (allowCasts)
            this.lossless++;
        else
            this.ordinary++;
    }

    private KeyAnalysis analyze(DBSPCircuit circuit, boolean allowCasts) {
        var analysis = allowCasts ? new LosslessCastKeyAnalysis(this.compiler) : new KeyAnalysis(this.compiler);
        analysis.apply(circuit);
        return analysis;
    }

    @Override
    public void after(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit, boolean done) {}

    @Override
    void finish() {
        this.observation.put("ordinary", this.ordinary).put("lossless", this.lossless)
                .put("ports", this.ports).put("keyed", this.keyed).put("transforms", this.transforms);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1)
            throw new IllegalArgumentException("Usage: CandidateKeyOracle POPULATION");
        run(Path.of(args[0]), CandidateKeyOracle::new);
    }
}
